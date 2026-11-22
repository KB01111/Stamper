//! Concurrent resumable sync queue with per-provider concurrency limits,
//! token-bucket rate limiting, HTTP 429 cooldown surfacing, and 5xx backoff.

use crate::matching::{MatchConfig, MatchOutcome, RejectReason, select_best_match};
use crate::providers::Provider;
use crate::store::{SyncItem, SyncStatus, SyncStore};
use crate::writer::{
    TrackMetadata, WriteOutcome, WriteRequest, build_track_path, should_skip_download,
    write_flac_atomic,
};
use crate::{Candidate, Query, Result, SyncError};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex as AsyncMutex, Notify, Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;

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

    fn take_or_wait(&mut self) -> Duration {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Duration::ZERO
        } else {
            let deficit = 1.0 - self.tokens;
            let wait_secs = deficit / self.refill_per_sec;
            self.tokens = 0.0;
            Duration::from_secs_f64(wait_secs)
        }
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

/// Resumable concurrent sync queue.
#[derive(Clone)]
pub struct SyncQueue {
    store: SyncStore,
    providers: Arc<Vec<Arc<ProviderControl>>>,
    config: Arc<QueueConfig>,
    live: Arc<Mutex<LiveState>>,
    snapshot_tx: Arc<watch::Sender<SyncSnapshot>>,
    notify: Arc<Notify>,
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
        let mut join_set: JoinSet<SyncStatus> = JoinSet::new();

        for item in pending {
            let permit = worker_slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| SyncError::Store("queue worker semaphore closed".to_string()))?;
            let queue = self.clone();
            join_set.spawn(async move {
                let _permit = permit;
                queue.process_single_item(item).await
            });
        }

        while let Some(joined) = join_set.join_next().await {
            if let Ok(status) = joined {
                match status {
                    SyncStatus::Done => summary.done += 1,
                    SyncStatus::Unmatched => summary.unmatched += 1,
                    SyncStatus::Failed => summary.failed += 1,
                    _ => {}
                }
            }
        }

        self.publish_snapshot();
        Ok(summary)
    }

    /// Continuously processes pending tracks whenever notified until `shutdown` becomes `true`.
    pub async fn run_until_shutdown(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let _ = self.process_pending().await?;
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            tokio::select! {
                _ = self.notify.notified() => {
                    let _ = self.process_pending().await?;
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn process_single_item(&self, item: SyncItem) -> SyncStatus {
        let active_label = format!(
            "{} - {}",
            item.artists.first().map(String::as_str).unwrap_or(""),
            item.title
        );
        self.add_active_track(&active_label);

        let final_status = self.process_single_item_inner(&item).await;

        self.remove_active_track(&active_label);
        self.publish_snapshot();
        final_status
    }

    async fn process_single_item_inner(&self, item: &SyncItem) -> SyncStatus {
        let query = match self.store.get_query(&item.playlist_id, &item.track_id) {
            Ok(Some(q)) => q,
            Ok(None) => {
                let _ = self.store.mark_failed(
                    &item.playlist_id,
                    &item.track_id,
                    item.attempts + 1,
                    "missing track metadata in store",
                );
                return SyncStatus::Failed;
            }
            Err(e) => {
                let _ = self.store.mark_failed(
                    &item.playlist_id,
                    &item.track_id,
                    item.attempts + 1,
                    &e.to_string(),
                );
                return SyncStatus::Failed;
            }
        };

        if self.providers.is_empty() {
            let _ = self.store.mark_failed(
                &item.playlist_id,
                &item.track_id,
                item.attempts + 1,
                "no lossless providers configured",
            );
            return SyncStatus::Failed;
        }

        let _ = self.store.update_status(
            &item.playlist_id,
            &item.track_id,
            SyncStatus::Resolving,
            None,
        );
        self.publish_snapshot();

        // Phase 1: Search providers and gather candidates.
        let mut all_candidates: Vec<Candidate> = Vec::new();
        let mut last_error: Option<(u32, SyncError)> = None;

        for control in self.providers.iter() {
            match self
                .call_with_retry(control, item, || control.provider.search(&query))
                .await
            {
                Ok(mut found) => all_candidates.append(&mut found),
                Err((attempts, err)) => {
                    last_error = Some((attempts, err));
                }
            }
        }

        if all_candidates.is_empty()
            && let Some((attempts, err)) = last_error
        {
            let _ = self.store.mark_failed(
                &item.playlist_id,
                &item.track_id,
                attempts,
                &err.to_string(),
            );
            return SyncStatus::Failed;
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
                    let _ = self
                        .store
                        .mark_unmatched(&item.playlist_id, &item.track_id, msg);
                    return SyncStatus::Unmatched;
                }
            };

        let destination = self.destination_for(&query, &candidate);

        // Phase 3: Never re-download when destination already meets or exceeds target quality.
        match should_skip_download(&destination, selected_quality) {
            Ok(Some(existing_quality)) => {
                let path_str = destination.to_string_lossy().into_owned();
                let _ = self.store.mark_done(
                    &item.playlist_id,
                    &item.track_id,
                    matched_by,
                    candidate.provider,
                    existing_quality,
                    &path_str,
                );
                return SyncStatus::Done;
            }
            Ok(None) => {}
            Err(e) => {
                log::warn!(
                    "could not inspect existing file {}: {e}",
                    destination.display()
                );
            }
        }

        let Some(control) = self
            .providers
            .iter()
            .find(|c| c.provider.id() == candidate.provider)
        else {
            let _ = self.store.mark_failed(
                &item.playlist_id,
                &item.track_id,
                item.attempts + 1,
                &format!("provider '{}' not registered", candidate.provider),
            );
            return SyncStatus::Failed;
        };

        let _ = self.store.update_status(
            &item.playlist_id,
            &item.track_id,
            SyncStatus::Downloading,
            None,
        );
        self.publish_snapshot();

        // Phase 4: Fetch lossless FLAC stream with retry/cooldown handling.
        let audio_bytes = match self
            .call_with_retry(control, item, || {
                control.provider.fetch(&candidate, selected_quality)
            })
            .await
        {
            Ok(bytes) => bytes,
            Err((attempts, err)) => {
                if matches!(err, SyncError::LossyOnly(_)) {
                    let _ = self.store.mark_unmatched(
                        &item.playlist_id,
                        &item.track_id,
                        &err.to_string(),
                    );
                    return SyncStatus::Unmatched;
                }
                let _ = self.store.mark_failed(
                    &item.playlist_id,
                    &item.track_id,
                    attempts,
                    &err.to_string(),
                );
                return SyncStatus::Failed;
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
                let _ = self.store.mark_done(
                    &item.playlist_id,
                    &item.track_id,
                    matched_by,
                    candidate.provider,
                    final_quality,
                    &path_str,
                );
                SyncStatus::Done
            }
            Ok(Err(err)) => {
                let _ = self.store.mark_failed(
                    &item.playlist_id,
                    &item.track_id,
                    item.attempts + 1,
                    &err.to_string(),
                );
                SyncStatus::Failed
            }
            Err(join_err) => {
                let _ = self.store.mark_failed(
                    &item.playlist_id,
                    &item.track_id,
                    item.attempts + 1,
                    &format!("writer task failed: {join_err}"),
                );
                SyncStatus::Failed
            }
        }
    }

    async fn call_with_retry<T, F, Fut>(
        &self,
        control: &Arc<ProviderControl>,
        item: &SyncItem,
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
                    let wait = retry_after_secs
                        .map(Duration::from_secs)
                        .unwrap_or(self.config.default_rate_limit_wait);
                    let wait_secs = wait.as_secs().max(1);
                    let retry_at = now_unix_secs().saturating_add(wait_secs);

                    control.set_cooldown(wait).await;
                    self.set_cooldown_snapshot(ProviderCooldown {
                        provider: provider.clone(),
                        retry_at_unix_secs: retry_at,
                        remaining_secs: wait_secs,
                        message: message.clone(),
                    });

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

                    let _ = self.store.record_attempt(
                        &item.playlist_id,
                        &item.track_id,
                        SyncStatus::Resolving,
                        attempt,
                        Some(&message),
                    );
                    self.publish_snapshot();
                    tokio::time::sleep(wait).await;
                }
                Err(err) if err.is_retryable() => {
                    if attempt >= max_attempts {
                        return Err((attempt, err));
                    }
                    let delay =
                        compute_backoff(self.config.base_backoff, self.config.max_backoff, attempt);
                    let _ = self.store.record_attempt(
                        &item.playlist_id,
                        &item.track_id,
                        SyncStatus::Resolving,
                        attempt,
                        Some(&err.to_string()),
                    );
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
        let live = match self.live.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        if let Ok(snapshot) = build_snapshot(&self.store, &live) {
            self.snapshot_tx.send_replace(snapshot);
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
