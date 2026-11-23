//! Concurrent resumable sync queue with per-provider concurrency limits,
//! token-bucket rate limiting, HTTP 429 cooldown surfacing, and 5xx backoff.

use crate::matching::{MatchConfig, MatchOutcome, RejectReason, select_best_match};
use crate::providers::Provider;
use crate::store::{SyncItem, SyncStatus, SyncStore};
use crate::writer::{
    TrackMetadata, WriteOutcome, WriteRequest, build_track_path, disambiguated_track_path,
    should_skip_download, sweep_stale_temp_files, write_flac_atomic,
};
use crate::{Candidate, Query, Result, SyncError};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex as AsyncMutex, Notify, Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;

/// Longest provider-requested wait honoured after an HTTP 429.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(600);

/// Status reported when a terminal store write failed twice. The row is left
/// in flight, and the next pass recovers it through `recover_interrupted`.
const LEFT_IN_FLIGHT: SyncStatus = SyncStatus::Downloading;

/// Active cooldown surfaced to callers when a provider returns HTTP 429.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCooldown {
    pub provider: String,
    pub retry_at_unix_secs: u64,
    pub remaining_secs: u64,
    pub message: String,
}

/// Non-blocking snapshot published via `tokio::sync::watch` for UI polling.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncSnapshot {
    pub pending: u64,
    pub resolving: u64,
    pub downloading: u64,
    pub done: u64,
    pub unmatched: u64,
    pub failed: u64,
    pub active_tracks: Vec<String>,
    pub cooldowns: BTreeMap<String, ProviderCooldown>,
}

/// Summary of one `process_pending` pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunSummary {
    pub recovered: usize,
    pub processed: usize,
    pub done: usize,
    pub unmatched: usize,
    pub failed: usize,
    /// Items returned to `pending` without a failed attempt, because a provider
    /// was rate limited. They are retried on the next pass.
    pub deferred: usize,
    /// Store writes that failed twice in this pass, as `"<operation>: <error>"`.
    /// The affected rows stay in flight until the next pass recovers them.
    pub store_errors: Vec<String>,
}

/// Configuration for [`SyncQueue`].
#[derive(Debug, Clone)]
pub struct QueueConfig {
    /// Directory where downloaded FLAC files are written.
    pub output_dir: PathBuf,
    /// Maximum concurrent in-flight operations per provider (default: 2).
    pub per_provider_concurrency: usize,
    /// Per-provider token-bucket sustained request rate per second.
    pub per_provider_rps: f64,
    /// Per-provider token-bucket burst size.
    pub per_provider_burst: f64,
    /// Maximum attempts on transient 5xx/network errors before marking `failed` (default: 5).
    pub max_attempts: u32,
    /// Base exponential backoff delay for 5xx/transient retries.
    pub base_backoff: Duration,
    /// Maximum exponential backoff delay cap.
    pub max_backoff: Duration,
    /// Fallback cooldown duration when an HTTP 429 omits `Retry-After`.
    pub default_rate_limit_wait: Duration,
    /// Strict matching configuration and quality ceiling.
    pub match_config: MatchConfig,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("."),
            per_provider_concurrency: 2,
            per_provider_rps: 4.0,
            per_provider_burst: 4.0,
            max_attempts: 5,
            base_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            default_rate_limit_wait: Duration::from_secs(30),
            match_config: MatchConfig::default(),
        }
    }
}

struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_per_sec: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(rps: f64, burst: f64) -> Self {
        let cap = burst.max(1.0);
        Self {
            capacity: cap,
            tokens: cap,
            refill_per_sec: rps.max(0.1),
            last_refill: Instant::now(),
        }
    }

    /// Reserves one token and returns how long the caller must wait before using it.
    ///
    /// The balance may go negative. Each concurrent caller then reserves the next
    /// slot and waits for its own position, instead of sharing one wait.
    fn take_or_wait(&mut self) -> Duration {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        self.last_refill = now;

        self.tokens -= 1.0;
        if self.tokens >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-self.tokens / self.refill_per_sec)
        }
    }
}

/// Clamps a provider's `Retry-After` value, falling back when it is absent.
fn rate_limit_wait(retry_after_secs: Option<u64>, fallback: Duration) -> Duration {
    match retry_after_secs {
        Some(secs) => Duration::from_secs(secs).min(MAX_RETRY_AFTER),
        None => fallback,
    }
}

struct ProviderControl {
    provider: Arc<dyn Provider>,
    semaphore: Arc<Semaphore>,
    bucket: AsyncMutex<TokenBucket>,
    cooldown_until: AsyncMutex<Option<Instant>>,
}

impl ProviderControl {
    fn new(provider: Arc<dyn Provider>, config: &QueueConfig) -> Self {
        Self {
            provider,
            semaphore: Arc::new(Semaphore::new(config.per_provider_concurrency.max(1))),
            bucket: AsyncMutex::new(TokenBucket::new(
                config.per_provider_rps,
                config.per_provider_burst,
            )),
            cooldown_until: AsyncMutex::new(None),
        }
    }

    async fn wait_rate_limit_and_cooldown(&self) {
        // Honour any active provider-wide 429 cooldown first.
        loop {
            let sleep_for = {
                let mut guard = self.cooldown_until.lock().await;
                match *guard {
                    Some(until) if until > Instant::now() => Some(until - Instant::now()),
                    Some(_) => {
                        *guard = None;
                        None
                    }
                    None => None,
                }
            };
            match sleep_for {
                Some(dur) => tokio::time::sleep(dur).await,
                None => break,
            }
        }

        // Then pace via the per-provider token bucket.
        let wait = {
            let mut bucket = self.bucket.lock().await;
            bucket.take_or_wait()
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }

    async fn set_cooldown(&self, duration: Duration) {
        let target = Instant::now() + duration;
        let mut guard = self.cooldown_until.lock().await;
        if guard.is_none_or(|existing| target > existing) {
            *guard = Some(target);
        }
    }
}

#[derive(Default)]
struct LiveState {
    active_tracks: BTreeSet<String>,
    cooldowns: BTreeMap<String, ProviderCooldown>,
}

/// State shared by the items of one `process_pending` pass.
#[derive(Default)]
struct PassState {
    /// Providers that hit their rate limit in this pass. They are not called again until the next pass.
    exhausted: Mutex<BTreeSet<String>>,
    store_errors: Mutex<Vec<String>>,
}

impl PassState {
    fn is_exhausted(&self, provider: &str) -> bool {
        self.exhausted
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(provider)
    }

    fn exhaust(&self, provider: &str) {
        self.exhausted
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(provider.to_string());
    }

    fn record_store_error(&self, message: String) {
        self.store_errors
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(message);
    }

    fn take_store_errors(&self) -> Vec<String> {
        std::mem::take(&mut *self.store_errors.lock().unwrap_or_else(|p| p.into_inner()))
    }
}

/// Resumable concurrent sync queue.
#[derive(Clone)]
pub struct SyncQueue {
    store: SyncStore,
    providers: Arc<Vec<Arc<ProviderControl>>>,
    config: Arc<QueueConfig>,
    live: Arc<Mutex<LiveState>>,
    snapshot_tx: Arc<watch::Sender<SyncSnapshot>>,
    notify: Arc<Notify>,
    /// One async lock per destination path (lowercased), so two items that map to
    /// the same file never download or write it at the same time.
    destination_locks: Arc<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>>,
    /// Set once the stale temp files under the output directory have been swept.
    temp_swept: Arc<AtomicBool>,
}

impl std::fmt::Debug for SyncQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncQueue")
            .field("provider_count", &self.providers.len())
            .field("config", &self.config)
            .finish()
    }
}

impl SyncQueue {
    /// Creates a new sync queue backed by `store` and ordered `providers`.
    pub fn new(
        store: SyncStore,
        providers: Vec<Arc<dyn Provider>>,
        config: QueueConfig,
    ) -> Result<Self> {
        let controls: Vec<Arc<ProviderControl>> = providers
            .into_iter()
            .map(|p| Arc::new(ProviderControl::new(p, &config)))
            .collect();
        let initial_snapshot = build_snapshot(&store, &LiveState::default())?;
        let (tx, _rx) = watch::channel(initial_snapshot);
        Ok(Self {
            store,
            providers: Arc::new(controls),
            config: Arc::new(config),
            live: Arc::new(Mutex::new(LiveState::default())),
            snapshot_tx: Arc::new(tx),
            notify: Arc::new(Notify::new()),
            destination_locks: Arc::new(Mutex::new(HashMap::new())),
            temp_swept: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Returns a `watch::Receiver` for non-blocking UI polling of queue state.
    pub fn subscribe(&self) -> watch::Receiver<SyncSnapshot> {
        self.snapshot_tx.subscribe()
    }

    /// Returns the latest published snapshot immediately.
    pub fn snapshot(&self) -> SyncSnapshot {
        self.snapshot_tx.borrow().clone()
    }

    /// Returns the shared wake-up handle used by [`crate::PlaylistWatcher`].
    pub fn notify_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
    }

    /// Triggers a wake-up of a running background loop.
    pub fn wake(&self) {
        self.notify.notify_one();
    }

    /// Recovers interrupted rows and processes all currently pending tracks.
    pub async fn process_pending(&self) -> Result<RunSummary> {
        self.sweep_temp_files_once().await;
        let recovered = self.store.recover_interrupted()?;
        self.publish_snapshot();

        let pending = self.store.list_pending()?;
        let mut summary = RunSummary {
            recovered,
            processed: pending.len(),
            ..RunSummary::default()
        };

        if pending.is_empty() {
            return Ok(summary);
        }

        let max_workers = self
            .providers
            .len()
            .max(1)
            .saturating_mul(self.config.per_provider_concurrency.max(1));
        let worker_slots = Arc::new(Semaphore::new(max_workers));
        let pass = Arc::new(PassState::default());
        let mut join_set: JoinSet<SyncStatus> = JoinSet::new();

        for item in pending {
            let permit = worker_slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| SyncError::Store("queue worker semaphore closed".to_string()))?;
            let queue = self.clone();
            let pass = Arc::clone(&pass);
            join_set.spawn(async move {
                let _permit = permit;
                queue.process_single_item(item, &pass).await
            });
        }

        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok(SyncStatus::Done) => summary.done += 1,
                Ok(SyncStatus::Unmatched) => summary.unmatched += 1,
                Ok(SyncStatus::Failed) => summary.failed += 1,
                Ok(SyncStatus::Pending) => summary.deferred += 1,
                Ok(_) => {}
                Err(join_err) => log::error!("sync worker task did not finish: {join_err}"),
            }
        }

        summary.store_errors = pass.take_store_errors();
        self.publish_snapshot();
        Ok(summary)
    }

    /// Continuously processes pending tracks whenever notified until `shutdown` becomes `true`.
    ///
    /// Shutdown is observed during a pass too. A pass that is interrupted leaves
    /// its in-flight rows behind, and the next pass recovers them.
    pub async fn run_until_shutdown(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut first = true;
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            if !first {
                tokio::select! {
                    _ = self.notify.notified() => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                        continue;
                    }
                }
            }
            first = false;

            let pass = self.process_pending();
            tokio::pin!(pass);
            loop {
                tokio::select! {
                    result = &mut pass => {
                        result?;
                        break;
                    }
                    changed = shutdown.changed() => {
                        // Dropping the pass aborts its in-flight work; those rows are recovered later.
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    async fn sweep_temp_files_once(&self) {
        if self.temp_swept.swap(true, Ordering::SeqCst) {
            return;
        }
        let root = self.config.output_dir.clone();
        match tokio::task::spawn_blocking(move || sweep_stale_temp_files(&root)).await {
            Ok(0) => {}
            Ok(removed) => {
                log::info!("removed {removed} stale temp file(s) from an interrupted run")
            }
            Err(err) => log::warn!("stale temp file sweep did not finish: {err}"),
        }
    }

    async fn process_single_item(&self, item: SyncItem, pass: &PassState) -> SyncStatus {
        let active_label = format!(
            "{} - {}",
            item.artists.first().map(String::as_str).unwrap_or(""),
            item.title
        );
        self.add_active_track(&active_label);

        let final_status = self.process_single_item_inner(&item, pass).await;

        self.remove_active_track(&active_label);
        self.publish_snapshot();
        final_status
    }

    async fn process_single_item_inner(&self, item: &SyncItem, pass: &PassState) -> SyncStatus {
        let query = match self.store.get_query(&item.playlist_id, &item.track_id) {
            Ok(Some(q)) => q,
            Ok(None) => return self.fail_item(pass, item, "missing track metadata in store"),
            Err(e) => return self.fail_item(pass, item, &e.to_string()),
        };

        if self.providers.is_empty() {
            return self.fail_item(pass, item, "no lossless providers configured");
        }

        self.persist(pass, "mark_resolving", || {
            self.store.update_status(
                &item.playlist_id,
                &item.track_id,
                SyncStatus::Resolving,
                None,
            )
        });
        self.publish_snapshot();

        // Phase 1: Search providers and gather candidates.
        let mut all_candidates: Vec<Candidate> = Vec::new();
        let mut last_error: Option<(u32, SyncError)> = None;
        let mut rate_limited = false;
        let mut skipped_exhausted = false;

        for control in self.providers.iter() {
            let provider_id = control.provider.id();
            if pass.is_exhausted(provider_id) {
                skipped_exhausted = true;
                continue;
            }
            match self
                .call_with_retry(control, item, pass, || control.provider.search(&query))
                .await
            {
                Ok(mut found) => all_candidates.append(&mut found),
                Err((attempts, err)) => {
                    if err.is_rate_limited() {
                        rate_limited = true;
                        pass.exhaust(provider_id);
                    }
                    last_error = Some((attempts, err));
                }
            }
        }

        // A rate-limited provider was not searched, so "no candidates" would be a guess.
        if all_candidates.is_empty() && (rate_limited || skipped_exhausted) {
            return self.defer_item(
                pass,
                item,
                "providers are rate limited; will retry on the next pass",
            );
        }
        if all_candidates.is_empty()
            && let Some((attempts, err)) = last_error
        {
            return self.settle(
                pass,
                "mark_failed",
                || {
                    self.store.mark_failed(
                        &item.playlist_id,
                        &item.track_id,
                        attempts,
                        &err.to_string(),
                    )
                },
                SyncStatus::Failed,
            );
        }

        // Phase 2: Strict matching.
        let (candidate, matched_by, selected_quality) =
            match select_best_match(&query, &all_candidates, &self.config.match_config) {
                MatchOutcome::Matched {
                    candidate,
                    matched_by,
                    selected_quality,
                } => (*candidate, matched_by, selected_quality),
                MatchOutcome::Unmatched { reason } => {
                    let msg = match reason {
                        RejectReason::NoCandidates => "no candidates returned by providers",
                        RejectReason::LossyOnly => "matching candidates were lossy only",
                        RejectReason::NoStrictMatch => {
                            "no candidate satisfied strict matching rules"
                        }
                    };
                    return self.settle(
                        pass,
                        "mark_unmatched",
                        || {
                            self.store
                                .mark_unmatched(&item.playlist_id, &item.track_id, msg)
                        },
                        SyncStatus::Unmatched,
                    );
                }
            };

        // Hold the destination lock from the existence check through the write, so
        // two items that map to one file cannot both download or race the commit.
        let base = self.destination_for(&query, &candidate);
        let lock = self.destination_lock(&base);
        let _destination_guard = lock.lock().await;

        let destination = match self.resolve_destination(item, base) {
            Ok(path) => path,
            Err(e) => return self.fail_item(pass, item, &e.to_string()),
        };

        // Phase 3: Never re-download when the destination already meets or exceeds target quality.
        match should_skip_download(&destination, selected_quality) {
            Ok(Some(existing_quality)) => {
                let path_str = destination.to_string_lossy().into_owned();
                return self.settle(
                    pass,
                    "mark_done",
                    || {
                        self.store.mark_done(
                            &item.playlist_id,
                            &item.track_id,
                            matched_by,
                            candidate.provider,
                            existing_quality,
                            &path_str,
                        )
                    },
                    SyncStatus::Done,
                );
            }
            Ok(None) => {}
            Err(e) => {
                // Fail fast. Downloading would only be rejected by the writer after the full transfer.
                let message = format!(
                    "existing file is not a readable FLAC; move or delete it to retry: {e}"
                );
                return self.fail_item(pass, item, &message);
            }
        }

        let Some(control) = self
            .providers
            .iter()
            .find(|c| c.provider.id() == candidate.provider)
        else {
            return self.fail_item(
                pass,
                item,
                &format!("provider '{}' not registered", candidate.provider),
            );
        };
        if pass.is_exhausted(control.provider.id()) {
            return self.defer_item(
                pass,
                item,
                "provider is rate limited; will retry on the next pass",
            );
        }

        self.persist(pass, "mark_downloading", || {
            self.store.update_status(
                &item.playlist_id,
                &item.track_id,
                SyncStatus::Downloading,
                None,
            )
        });
        self.publish_snapshot();

        // Phase 4: Fetch lossless FLAC stream with retry/cooldown handling.
        let audio_bytes = match self
            .call_with_retry(control, item, pass, || {
                control.provider.fetch(&candidate, selected_quality)
            })
            .await
        {
            Ok(bytes) => bytes,
            Err((attempts, err)) => {
                if err.is_rate_limited() {
                    pass.exhaust(control.provider.id());
                    return self.defer_item(
                        pass,
                        item,
                        "provider is rate limited; will retry on the next pass",
                    );
                }
                if matches!(err, SyncError::LossyOnly(_)) {
                    return self.settle(
                        pass,
                        "mark_unmatched",
                        || {
                            self.store.mark_unmatched(
                                &item.playlist_id,
                                &item.track_id,
                                &err.to_string(),
                            )
                        },
                        SyncStatus::Unmatched,
                    );
                }
                return self.settle(
                    pass,
                    "mark_failed",
                    || {
                        self.store.mark_failed(
                            &item.playlist_id,
                            &item.track_id,
                            attempts,
                            &err.to_string(),
                        )
                    },
                    SyncStatus::Failed,
                );
            }
        };

        // Phase 5: Atomic verify, tag, and write on a blocking pool thread.
        let write_req = WriteRequest {
            destination: destination.clone(),
            audio_bytes,
            quality: selected_quality,
            metadata: build_track_metadata(&query, &candidate),
        };

        let write_result = tokio::task::spawn_blocking(move || write_flac_atomic(&write_req)).await;

        match write_result {
            Ok(Ok(outcome)) => {
                let (path, final_quality) = match outcome {
                    WriteOutcome::Written { path, quality } => (path, quality),
                    WriteOutcome::Upgraded {
                        path, new_quality, ..
                    } => (path, new_quality),
                    WriteOutcome::SkippedExisting {
                        path,
                        existing_quality,
                    } => (path, existing_quality),
                };
                let path_str = path.to_string_lossy().into_owned();
                self.settle(
                    pass,
                    "mark_done",
                    || {
                        self.store.mark_done(
                            &item.playlist_id,
                            &item.track_id,
                            matched_by,
                            candidate.provider,
                            final_quality,
                            &path_str,
                        )
                    },
                    SyncStatus::Done,
                )
            }
            Ok(Err(err)) => self.fail_item(pass, item, &err.to_string()),
            Err(join_err) => self.fail_item(pass, item, &format!("writer task failed: {join_err}")),
        }
    }

    /// Runs one store write. A failure is retried once; a second failure is logged
    /// and recorded in `pass`. Returns whether the write succeeded.
    fn persist(&self, pass: &PassState, what: &str, write: impl Fn() -> Result<()>) -> bool {
        if write().is_ok() {
            return true;
        }
        log::warn!("sync store write '{what}' failed; retrying once");
        match write() {
            Ok(()) => true,
            Err(err) => {
                log::error!("sync store write '{what}' failed again: {err}");
                pass.record_store_error(format!("{what}: {err}"));
                false
            }
        }
    }

    /// Persists a terminal outcome and returns `status`, or `LEFT_IN_FLIGHT` if the write failed.
    fn settle(
        &self,
        pass: &PassState,
        what: &str,
        write: impl Fn() -> Result<()>,
        status: SyncStatus,
    ) -> SyncStatus {
        if self.persist(pass, what, write) {
            status
        } else {
            LEFT_IN_FLIGHT
        }
    }

    fn fail_item(&self, pass: &PassState, item: &SyncItem, message: &str) -> SyncStatus {
        self.settle(
            pass,
            "mark_failed",
            || {
                self.store.mark_failed(
                    &item.playlist_id,
                    &item.track_id,
                    item.attempts + 1,
                    message,
                )
            },
            SyncStatus::Failed,
        )
    }

    /// Returns the item to `pending` without spending an attempt, since the
    /// provider's rate limit is not the track's fault.
    fn defer_item(&self, pass: &PassState, item: &SyncItem, reason: &str) -> SyncStatus {
        self.settle(
            pass,
            "mark_deferred",
            || {
                self.store.record_attempt(
                    &item.playlist_id,
                    &item.track_id,
                    SyncStatus::Pending,
                    item.attempts,
                    Some(reason),
                )
            },
            SyncStatus::Pending,
        )
    }

    async fn call_with_retry<T, F, Fut>(
        &self,
        control: &Arc<ProviderControl>,
        item: &SyncItem,
        pass: &PassState,
        mut op: F,
    ) -> std::result::Result<T, (u32, SyncError)>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let max_attempts = self.config.max_attempts.max(1);
        let mut attempt: u32 = 0;

        loop {
            attempt += 1;
            control.wait_rate_limit_and_cooldown().await;

            let permit = control
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| {
                    (
                        attempt,
                        SyncError::Provider {
                            provider: control.provider.id().to_string(),
                            message: "provider semaphore closed".to_string(),
                        },
                    )
                })?;

            let res = op().await;
            drop(permit);

            match res {
                Ok(value) => {
                    self.clear_cooldown(control.provider.id());
                    return Ok(value);
                }
                Err(SyncError::RateLimited {
                    provider,
                    retry_after_secs,
                    message,
                }) => {
                    let wait =
                        rate_limit_wait(retry_after_secs, self.config.default_rate_limit_wait);
                    let wait_secs = wait.as_secs().max(1);
                    let retry_at = now_unix_secs().saturating_add(wait_secs);

                    control.set_cooldown(wait).await;
                    self.set_cooldown_snapshot(ProviderCooldown {
                        provider: provider.clone(),
                        retry_at_unix_secs: retry_at,
                        remaining_secs: wait_secs,
                        message: message.clone(),
                    });

                    // Exhausted: the caller defers the item and stops using this provider for the pass.
                    if attempt >= max_attempts {
                        return Err((
                            attempt,
                            SyncError::RateLimited {
                                provider,
                                retry_after_secs,
                                message,
                            },
                        ));
                    }

                    self.persist(pass, "record_attempt", || {
                        self.store.record_attempt(
                            &item.playlist_id,
                            &item.track_id,
                            SyncStatus::Resolving,
                            attempt,
                            Some(&message),
                        )
                    });
                    self.publish_snapshot();
                    tokio::time::sleep(wait).await;
                }
                Err(err) if err.is_retryable() => {
                    if attempt >= max_attempts {
                        return Err((attempt, err));
                    }
                    let delay =
                        compute_backoff(self.config.base_backoff, self.config.max_backoff, attempt);
                    self.persist(pass, "record_attempt", || {
                        self.store.record_attempt(
                            &item.playlist_id,
                            &item.track_id,
                            SyncStatus::Resolving,
                            attempt,
                            Some(&err.to_string()),
                        )
                    });
                    self.publish_snapshot();
                    tokio::time::sleep(delay).await;
                }
                Err(err) => {
                    return Err((attempt, err));
                }
            }
        }
    }

    fn destination_for(&self, query: &Query, candidate: &Candidate) -> PathBuf {
        let primary_artist = query
            .album_artist
            .as_deref()
            .or_else(|| query.artists.first().map(String::as_str))
            .or(candidate.album_artist.as_deref())
            .or_else(|| candidate.artists.first().map(String::as_str))
            .unwrap_or("Unknown Artist");
        let album = if !query.album.trim().is_empty() {
            query.album.as_str()
        } else {
            candidate.album.as_deref().unwrap_or("Unknown Album")
        };
        let track_number = query.track_number.or(candidate.track_number);
        build_track_path(
            &self.config.output_dir,
            primary_artist,
            album,
            track_number,
            &query.title,
        )
    }

    /// Returns `base`, or a per-recording variant when another recording already owns it.
    ///
    /// The suffix is added only on a real collision: the file exists and a `done`
    /// item for a different recording (track id, or ISRC) is recorded at that path.
    fn resolve_destination(&self, item: &SyncItem, base: PathBuf) -> Result<PathBuf> {
        if !base.exists() {
            return Ok(base);
        }
        let owner = self.store.find_other_owner(
            &base.to_string_lossy(),
            &item.track_id,
            item.isrc.as_deref(),
        )?;
        match owner {
            Some(_) => Ok(disambiguated_track_path(&base, &item.track_id)),
            None => Ok(base),
        }
    }

    /// Returns the async lock for one destination, creating it on first use.
    fn destination_lock(&self, destination: &Path) -> Arc<AsyncMutex<()>> {
        let key = destination.to_string_lossy().to_lowercase();
        let mut locks = self
            .destination_locks
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        // Drop locks that nobody holds or waits on, so the map tracks only active destinations.
        locks.retain(|_, lock| Arc::strong_count(lock) > 1);
        Arc::clone(
            locks
                .entry(key)
                .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
        )
    }

    fn add_active_track(&self, label: &str) {
        if let Ok(mut live) = self.live.lock() {
            live.active_tracks.insert(label.to_string());
        }
        self.publish_snapshot();
    }

    fn remove_active_track(&self, label: &str) {
        if let Ok(mut live) = self.live.lock() {
            live.active_tracks.remove(label);
        }
    }

    fn set_cooldown_snapshot(&self, cooldown: ProviderCooldown) {
        if let Ok(mut live) = self.live.lock() {
            live.cooldowns.insert(cooldown.provider.clone(), cooldown);
        }
        self.publish_snapshot();
    }

    fn clear_cooldown(&self, provider: &str) {
        let removed = if let Ok(mut live) = self.live.lock() {
            live.cooldowns.remove(provider).is_some()
        } else {
            false
        };
        if removed {
            self.publish_snapshot();
        }
    }

    fn publish_snapshot(&self) {
        let mut live = match self.live.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        // Expired cooldowns are no longer in effect, so drop them.
        let now = now_unix_secs();
        live.cooldowns
            .retain(|_, cooldown| cooldown.retry_at_unix_secs > now);
        if let Ok(snapshot) = build_snapshot(&self.store, &live) {
            let _ = self.snapshot_tx.send(snapshot);
        }
    }
}

fn build_snapshot(store: &SyncStore, live: &LiveState) -> Result<SyncSnapshot> {
    let counts = store.total_counts()?;
    let now = now_unix_secs();
    let mut cooldowns = BTreeMap::new();
    for (provider, cd) in &live.cooldowns {
        let remaining = cd.retry_at_unix_secs.saturating_sub(now);
        cooldowns.insert(
            provider.clone(),
            ProviderCooldown {
                provider: cd.provider.clone(),
                retry_at_unix_secs: cd.retry_at_unix_secs,
                remaining_secs: remaining,
                message: cd.message.clone(),
            },
        );
    }

    Ok(SyncSnapshot {
        pending: counts.pending,
        resolving: counts.resolving,
        downloading: counts.downloading,
        done: counts.done,
        unmatched: counts.unmatched,
        failed: counts.failed,
        active_tracks: live.active_tracks.iter().cloned().collect(),
        cooldowns,
    })
}

fn build_track_metadata(query: &Query, candidate: &Candidate) -> TrackMetadata {
    let artists = if !query.artists.is_empty() {
        query.artists.clone()
    } else {
        candidate.artists.clone()
    };
    let album = if !query.album.trim().is_empty() {
        query.album.clone()
    } else {
        candidate.album.clone().unwrap_or_default()
    };
    TrackMetadata {
        title: query.title.clone(),
        artists,
        album,
        album_artist: query
            .album_artist
            .clone()
            .or_else(|| candidate.album_artist.clone()),
        track_number: query.track_number.or(candidate.track_number),
        disc_number: query.disc_number.or(candidate.disc_number),
        year: query.year.or(candidate.year),
        isrc: query.isrc.clone().or_else(|| candidate.isrc.clone()),
        provider: candidate.provider.to_string(),
        source_url: candidate.source_url.clone(),
        cover_bytes: None,
        cover_mime: None,
        lyrics_lrc: candidate.lyrics_lrc.clone(),
    }
}

fn compute_backoff(base: Duration, max: Duration, attempt: u32) -> Duration {
    let exp = 1u32
        .checked_shl(attempt.saturating_sub(1))
        .unwrap_or(u32::MAX);
    let raw = base.saturating_mul(exp).min(max);
    let jitter_factor: f64 = rand::rng().random_range(0.75..=1.25);
    let jittered = (raw.as_secs_f64() * jitter_factor).max(0.0);
    Duration::from_secs_f64(jittered).min(max)
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_bucket_reserves_a_separate_slot_for_each_waiting_caller() {
        let mut bucket = TokenBucket::new(10.0, 1.0);
        assert_eq!(bucket.take_or_wait(), Duration::ZERO);
        let second = bucket.take_or_wait();
        let third = bucket.take_or_wait();
        assert!(
            second >= Duration::from_millis(90),
            "second caller waits one refill interval, got {second:?}"
        );
        assert!(
            third >= Duration::from_millis(190),
            "third caller waits two refill intervals, got {third:?}"
        );
    }

    #[test]
    fn provider_retry_after_is_capped() {
        let fallback = Duration::from_secs(30);
        assert_eq!(rate_limit_wait(Some(86_400), fallback), MAX_RETRY_AFTER);
        assert_eq!(rate_limit_wait(Some(5), fallback), Duration::from_secs(5));
        assert_eq!(rate_limit_wait(None, fallback), fallback);
    }
}
