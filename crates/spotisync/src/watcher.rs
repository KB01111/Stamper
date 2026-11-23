//! Playlist polling watcher that detects new additions and wakes the sync queue.

use crate::store::SyncStore;
use crate::{Query, Result, SyncError};
use async_trait::async_trait;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, watch};

/// Shortest base polling interval. Shorter configured intervals are raised to this
/// before jitter is applied, so a zero or tiny interval cannot spin the store and API.
const MIN_POLL_INTERVAL: Duration = Duration::from_secs(5);
/// Longest a single playlist fetch may run before that playlist fails for this pass.
const PLAYLIST_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
/// Delay after the first failed poll pass. It doubles on each consecutive failure.
const ERROR_BACKOFF_INITIAL: Duration = Duration::from_secs(5);
/// Upper bound for the delay between retries after failed poll passes.
const ERROR_BACKOFF_MAX: Duration = Duration::from_secs(300);

/// Track metadata returned by a [`PlaylistSource`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaylistTrack {
    pub track_id: String,
    pub isrc: Option<String>,
    pub title: String,
    pub artists: Vec<String>,
    pub album: String,
    pub album_artist: Option<String>,
    pub duration_ms: u64,
    pub track_number: Option<u32>,
    pub disc_number: Option<u32>,
    pub year: Option<u32>,
    pub cover_url: Option<String>,
}

impl PlaylistTrack {
    /// Converts this playlist track into a provider search [`Query`].
    pub fn to_query(&self) -> Query {
        Query {
            isrc: self.isrc.clone(),
            title: self.title.clone(),
            artists: self.artists.clone(),
            album: self.album.clone(),
            duration_ms: self.duration_ms,
            album_artist: self.album_artist.clone(),
            track_number: self.track_number,
            disc_number: self.disc_number,
            year: self.year,
            cover_url: self.cover_url.clone(),
        }
    }
}

/// Source capable of listing the current tracks of a playlist.
#[async_trait]
pub trait PlaylistSource: Send + Sync {
    async fn fetch_playlist_tracks(&self, playlist_id: &str) -> Result<Vec<PlaylistTrack>>;

    /// The account the source reads playlists for, or `None` when it has
    /// none. The caller keeps sync state per account and clears it when this
    /// changes. Sources without accounts keep the default.
    fn account_key(&self) -> Option<String> {
        None
    }
}

/// Configuration for [`PlaylistWatcher`].
#[derive(Debug, Clone)]
pub struct WatcherConfig {
    /// Base polling interval between playlist checks (default: 60s).
    pub poll_interval: Duration,
    /// Symmetric jitter fraction applied to `poll_interval` (default: 0.15 for +/- 15%).
    pub jitter_fraction: f64,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(60),
            jitter_fraction: 0.15,
        }
    }
}

impl WatcherConfig {
    /// Computes the next sleep interval with uniform random jitter within
    /// `base * (1 - jitter_fraction) ..= base * (1 + jitter_fraction)`, where `base` is
    /// `poll_interval` raised to at least [`MIN_POLL_INTERVAL`]. A non-finite
    /// `jitter_fraction` is treated as zero.
    pub fn jittered_interval(&self) -> Duration {
        let base = self.poll_interval.max(MIN_POLL_INTERVAL);
        let jitter = if self.jitter_fraction.is_finite() {
            self.jitter_fraction.clamp(0.0, 0.5)
        } else {
            0.0
        };
        if jitter == 0.0 {
            return base;
        }
        let factor: f64 = rand::rng().random_range((1.0 - jitter)..=(1.0 + jitter));
        Duration::try_from_secs_f64(base.as_secs_f64() * factor).unwrap_or(base)
    }
}

/// Summary of one watcher polling pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PollReport {
    pub playlists_polled: usize,
    pub enqueued_tracks: usize,
    pub errors: Vec<(String, String)>,
}

/// Polls enabled playlists on a jittered interval and enqueues newly added tracks.
#[derive(Clone)]
pub struct PlaylistWatcher {
    store: SyncStore,
    source: Arc<dyn PlaylistSource>,
    queue_notify: Arc<Notify>,
    wake_poll: Arc<Notify>,
    config: WatcherConfig,
    /// Upper bound for one playlist fetch; a timed-out playlist fails for this pass only.
    playlist_fetch_timeout: Duration,
    /// First retry delay after a failed poll pass.
    error_backoff_initial: Duration,
    /// Cap for the retry delay after repeated failed poll passes.
    error_backoff_max: Duration,
}

impl std::fmt::Debug for PlaylistWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlaylistWatcher")
            .field("config", &self.config)
            .finish()
    }
}

impl PlaylistWatcher {
    /// Creates a new playlist watcher bound to `store`, `source`, and the queue's wake-up handle.
    pub fn new(
        store: SyncStore,
        source: Arc<dyn PlaylistSource>,
        queue_notify: Arc<Notify>,
        config: WatcherConfig,
    ) -> Self {
        Self {
            store,
            source,
            queue_notify,
            wake_poll: Arc::new(Notify::new()),
            config,
            playlist_fetch_timeout: PLAYLIST_FETCH_TIMEOUT,
            error_backoff_initial: ERROR_BACKOFF_INITIAL,
            error_backoff_max: ERROR_BACKOFF_MAX,
        }
    }

    /// Triggers an immediate poll cycle when running in [`Self::run_until_shutdown`].
    pub fn trigger_poll(&self) {
        self.wake_poll.notify_one();
    }

    /// Polls every currently enabled playlist once, diffing against `SyncStore`.
    pub async fn poll_once(&self) -> Result<PollReport> {
        let enabled = self.store.list_enabled_playlists()?;
        let mut report = PollReport::default();

        for playlist in enabled {
            // Count inserts even when the playlist fails later, because those rows exist.
            let mut added = 0usize;
            let outcome = self
                .poll_single_playlist(&playlist.playlist_id, &mut added)
                .await;
            report.enqueued_tracks += added;
            match outcome {
                Ok(()) => report.playlists_polled += 1,
                Err(err) => {
                    log::warn!(
                        "spotisync watcher failed to poll playlist '{}': {err}",
                        playlist.playlist_id
                    );
                    report
                        .errors
                        .push((playlist.playlist_id.clone(), err.to_string()));
                }
            }
        }

        Ok(report)
    }

    /// Immediately polls one playlist if it is enabled, waking the queue when new tracks arrive.
    pub async fn poll_playlist_now(&self, playlist_id: &str) -> Result<usize> {
        let enabled = self.store.list_enabled_playlists()?;
        if !enabled.iter().any(|p| p.playlist_id == playlist_id) {
            return Ok(0);
        }
        let mut added = 0usize;
        self.poll_single_playlist(playlist_id, &mut added).await?;
        Ok(added)
    }

    /// Runs the polling loop with jittered intervals until `shutdown` becomes `true`.
    ///
    /// A failed poll pass is logged and retried after an exponential backoff, which
    /// resets after the next successful pass. Shutdown and [`Self::trigger_poll`]
    /// interrupt both the pass and the wait between passes.
    pub async fn run_until_shutdown(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut next_error_delay = self.error_backoff_initial;
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }

            let pass = tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut shutdown) => return Ok(()),
                pass = self.poll_once() => pass,
            };

            let wait = match pass {
                Ok(_) => {
                    next_error_delay = self.error_backoff_initial;
                    self.config.jittered_interval()
                }
                Err(err) => {
                    let delay = next_error_delay.min(self.error_backoff_max);
                    next_error_delay = next_backoff(delay, self.error_backoff_max);
                    log::warn!("spotisync watcher poll pass failed, retrying in {delay:?}: {err}");
                    delay
                }
            };

            tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut shutdown) => return Ok(()),
                _ = self.wake_poll.notified() => {}
                _ = tokio::time::sleep(wait) => {}
            }
        }
    }

    /// Polls one playlist. Each inserted row is counted into `added` and wakes the queue
    /// immediately, so rows stay announced even if a later step of this poll fails.
    async fn poll_single_playlist(&self, playlist_id: &str, added: &mut usize) -> Result<()> {
        let fetch = self.source.fetch_playlist_tracks(playlist_id);
        let tracks = match tokio::time::timeout(self.playlist_fetch_timeout, fetch).await {
            Ok(result) => result?,
            Err(_elapsed) => {
                return Err(SyncError::Provider {
                    provider: "playlist".to_string(),
                    message: format!("fetch timed out after {:?}", self.playlist_fetch_timeout),
                });
            }
        };
        let existing = self.store.existing_track_ids(playlist_id)?;

        for track in tracks {
            if existing.contains(&track.track_id) {
                continue;
            }
            let query = track.to_query();
            if self
                .store
                .enqueue_track(playlist_id, &track.track_id, &query)?
            {
                *added += 1;
                self.queue_notify.notify_one();
            }
        }

        self.store
            .mark_playlist_polled(playlist_id, now_unix_secs())?;
        Ok(())
    }
}

/// Resolves once shutdown is requested. A dropped sender also counts as shutdown.
/// Cancel-safe: dropping the returned future loses no state.
async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

/// Doubles a retry delay, capped at `max`.
fn next_backoff(current: Duration, max: Duration) -> Duration {
    current.saturating_mul(2).min(max)
}

fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching::MatchedBy;
    use crate::providers::Provider;
    use crate::queue::{QueueConfig, SyncQueue};
    use crate::store::SyncStatus;
    use crate::writer::make_test_flac_bytes;
    use crate::{Candidate, Quality};
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    struct FakePlaylistSource {
        playlists: Mutex<HashMap<String, Vec<PlaylistTrack>>>,
        poll_counts: Mutex<HashMap<String, usize>>,
    }

    impl FakePlaylistSource {
        fn new() -> Self {
            Self {
                playlists: Mutex::new(HashMap::new()),
                poll_counts: Mutex::new(HashMap::new()),
            }
        }

        fn set_tracks(&self, playlist_id: &str, tracks: Vec<PlaylistTrack>) {
            self.playlists
                .lock()
                .unwrap()
                .insert(playlist_id.to_string(), tracks);
        }

        fn poll_count(&self, playlist_id: &str) -> usize {
            *self
                .poll_counts
                .lock()
                .unwrap()
                .get(playlist_id)
                .unwrap_or(&0)
        }
    }

    #[async_trait]
    impl PlaylistSource for FakePlaylistSource {
        async fn fetch_playlist_tracks(&self, playlist_id: &str) -> Result<Vec<PlaylistTrack>> {
            *self
                .poll_counts
                .lock()
                .unwrap()
                .entry(playlist_id.to_string())
                .or_insert(0) += 1;
            Ok(self
                .playlists
                .lock()
                .unwrap()
                .get(playlist_id)
                .cloned()
                .unwrap_or_default())
        }
    }

    fn make_track(id: &str, title: &str, isrc: &str) -> PlaylistTrack {
        PlaylistTrack {
            track_id: id.to_string(),
            isrc: Some(isrc.to_string()),
            title: title.to_string(),
            artists: vec!["Daft Punk".to_string()],
            album: "Random Access Memories".to_string(),
            album_artist: Some("Daft Punk".to_string()),
            duration_ms: 337_000,
            track_number: Some(1),
            disc_number: Some(1),
            year: Some(2013),
            cover_url: None,
        }
    }

    #[test]
    fn jittered_interval_stays_within_fifteen_percent() {
        let cfg = WatcherConfig::default();
        assert_eq!(cfg.poll_interval, Duration::from_secs(60));
        for _ in 0..100 {
            let d = cfg.jittered_interval();
            assert!(d >= Duration::from_secs(51), "too short: {d:?}");
            assert!(d <= Duration::from_secs(69), "too long: {d:?}");
        }
    }

    #[tokio::test]
    async fn adding_track_enqueues_once_repeated_polls_create_zero_duplicates() {
        let store = SyncStore::open_in_memory().unwrap();
        store
            .set_playlist_enabled("pl1", "Favorites", true)
            .unwrap();

        let source = Arc::new(FakePlaylistSource::new());
        source.set_tracks(
            "pl1",
            vec![make_track("tr1", "Instant Crush", "USQX91300105")],
        );

        let notify = Arc::new(Notify::new());
        let watcher = PlaylistWatcher::new(
            store.clone(),
            source.clone(),
            notify.clone(),
            WatcherConfig::default(),
        );

        // Poll 1: adds exactly one pending row.
        let r1 = watcher.poll_once().await.unwrap();
        assert_eq!(r1.playlists_polled, 1);
        assert_eq!(r1.enqueued_tracks, 1);
        assert!(r1.errors.is_empty());

        let pending = store.list_pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].track_id, "tr1");
        assert_eq!(pending[0].status, SyncStatus::Pending);

        // Poll 2 (unchanged): creates zero duplicates.
        let r2 = watcher.poll_once().await.unwrap();
        assert_eq!(r2.enqueued_tracks, 0);
        assert_eq!(store.playlist_track_statuses("pl1").unwrap().len(), 1);

        // Even after marking tr1 done, repeated poll must NOT re-enqueue tr1.
        store
            .mark_done(
                "pl1",
                "tr1",
                MatchedBy::Isrc,
                "qobuz",
                Quality::Flac24_96,
                "/music/tr1.flac",
            )
            .unwrap();
        let r3 = watcher.poll_once().await.unwrap();
        assert_eq!(r3.enqueued_tracks, 0);
        assert!(store.list_pending().unwrap().is_empty());

        // Adding a second track produces exactly one new pending row.
        source.set_tracks(
            "pl1",
            vec![
                make_track("tr1", "Instant Crush", "USQX91300105"),
                make_track("tr2", "Get Lucky", "USQX91300108"),
            ],
        );
        let r4 = watcher.poll_once().await.unwrap();
        assert_eq!(r4.enqueued_tracks, 1);
        let pending = store.list_pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].track_id, "tr2");
    }

    #[tokio::test]
    async fn disabled_playlists_are_not_polled_or_enqueued() {
        let store = SyncStore::open_in_memory().unwrap();
        store
            .set_playlist_enabled("pl_on", "Enabled", true)
            .unwrap();
        store
            .set_playlist_enabled("pl_off", "Disabled", false)
            .unwrap();

        let source = Arc::new(FakePlaylistSource::new());
        source.set_tracks(
            "pl_on",
            vec![make_track("tr1", "Track One", "USAA10000001")],
        );
        source.set_tracks(
            "pl_off",
            vec![make_track("tr2", "Track Two", "USAA10000002")],
        );

        let notify = Arc::new(Notify::new());
        let watcher = PlaylistWatcher::new(
            store.clone(),
            source.clone(),
            notify,
            WatcherConfig::default(),
        );

        let report = watcher.poll_once().await.unwrap();
        assert_eq!(report.playlists_polled, 1);
        assert_eq!(report.enqueued_tracks, 1);
        assert_eq!(source.poll_count("pl_on"), 1);
        assert_eq!(
            source.poll_count("pl_off"),
            0,
            "disabled playlist must not be polled"
        );
        assert!(store.playlist_track_statuses("pl_off").unwrap().is_empty());
    }

    struct SimpleProvider {
        fetches: AtomicUsize,
    }

    #[async_trait]
    impl Provider for SimpleProvider {
        fn id(&self) -> &'static str {
            "mock"
        }

        fn max_quality(&self) -> Quality {
            Quality::Flac24_96
        }

        async fn search(&self, q: &Query) -> Result<Vec<Candidate>> {
            Ok(vec![Candidate {
                provider: "mock",
                track_id: "m1".to_string(),
                isrc: q.isrc.clone(),
                title: q.title.clone(),
                artists: q.artists.clone(),
                album: Some(q.album.clone()),
                album_artist: q.album_artist.clone(),
                track_number: q.track_number,
                disc_number: q.disc_number,
                year: q.year,
                bit_depth: Some(24),
                sample_rate_hz: Some(96_000),
                duration_ms: q.duration_ms,
                lossless: true,
                cover_url: None,
                source_url: None,
                lyrics_lrc: None,
            }])
        }

        async fn fetch(&self, _c: &Candidate, _q: Quality) -> Result<Bytes> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            Ok(make_test_flac_bytes(24, 96_000))
        }
    }

    #[tokio::test]
    async fn watcher_wakes_running_queue_via_notify() {
        let dir = tempdir().unwrap();
        let store = SyncStore::open_in_memory().unwrap();
        store
            .set_playlist_enabled("pl1", "Live Sync", true)
            .unwrap();

        let source = Arc::new(FakePlaylistSource::new());
        let provider = Arc::new(SimpleProvider {
            fetches: AtomicUsize::new(0),
        });
        let queue = SyncQueue::new(
            store.clone(),
            vec![provider.clone()],
            QueueConfig {
                output_dir: dir.path().join("music"),
                per_provider_rps: 100.0,
                per_provider_burst: 10.0,
                ..QueueConfig::default()
            },
        )
        .unwrap();

        let watcher = PlaylistWatcher::new(
            store.clone(),
            source.clone(),
            queue.notify_handle(),
            WatcherConfig {
                poll_interval: Duration::from_secs(60),
                jitter_fraction: 0.15,
            },
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let queue_clone = queue.clone();
        let queue_task =
            tokio::spawn(async move { queue_clone.run_until_shutdown(shutdown_rx).await });

        let mut snap_rx = queue.subscribe();

        // Add a track and run one watcher poll; queue should wake and download it.
        source.set_tracks(
            "pl1",
            vec![make_track("tr_wake", "Instant Crush", "USQX91300105")],
        );
        let report = watcher.poll_once().await.unwrap();
        assert_eq!(report.enqueued_tracks, 1);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if snap_rx.borrow().done == 1 {
                    break;
                }
                if snap_rx.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
        .expect("queue should wake and complete track");

        let _ = shutdown_tx.send(true);
        let _ = queue_task.await;

        assert_eq!(provider.fetches.load(Ordering::SeqCst), 1);
        let item = store.get_item("pl1", "tr_wake").unwrap().unwrap();
        assert_eq!(item.status, SyncStatus::Done);
    }

    /// Polls `cond` every few milliseconds for up to five seconds.
    async fn wait_until(mut cond: impl FnMut() -> bool) -> bool {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !cond() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok()
    }

    #[test]
    fn zero_poll_interval_is_raised_to_the_floor_before_jitter() {
        let cfg = WatcherConfig {
            poll_interval: Duration::ZERO,
            jitter_fraction: 0.15,
        };
        for _ in 0..100 {
            let d = cfg.jittered_interval();
            assert!(d >= Duration::from_millis(4_249), "too short: {d:?}");
            assert!(d <= Duration::from_millis(5_751), "too long: {d:?}");
        }

        let no_jitter = WatcherConfig {
            poll_interval: Duration::ZERO,
            jitter_fraction: 0.0,
        };
        assert_eq!(no_jitter.jittered_interval(), MIN_POLL_INTERVAL);
    }

    #[test]
    fn non_finite_jitter_fraction_yields_the_exact_base_interval() {
        for jitter in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let cfg = WatcherConfig {
                poll_interval: Duration::from_secs(60),
                jitter_fraction: jitter,
            };
            assert_eq!(
                cfg.jittered_interval(),
                Duration::from_secs(60),
                "jitter {jitter}"
            );
        }
    }

    #[test]
    fn huge_poll_interval_with_jitter_does_not_panic() {
        let cfg = WatcherConfig {
            poll_interval: Duration::MAX,
            jitter_fraction: 0.5,
        };
        for _ in 0..100 {
            let _ = cfg.jittered_interval();
        }
    }

    #[test]
    fn retry_backoff_doubles_and_caps_at_five_minutes() {
        let mut delay = ERROR_BACKOFF_INITIAL;
        let mut seen = Vec::new();
        for _ in 0..8 {
            seen.push(delay.as_secs());
            delay = next_backoff(delay, ERROR_BACKOFF_MAX);
        }
        assert_eq!(seen, [5, 10, 20, 40, 80, 160, 300, 300]);
    }

    #[tokio::test]
    async fn run_until_shutdown_returns_promptly_when_idle() {
        let store = SyncStore::open_in_memory().unwrap();
        let source = Arc::new(FakePlaylistSource::new());
        let watcher = PlaylistWatcher::new(
            store,
            source,
            Arc::new(Notify::new()),
            WatcherConfig::default(),
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let loop_watcher = watcher.clone();
        let handle =
            tokio::spawn(async move { loop_watcher.run_until_shutdown(shutdown_rx).await });

        // The first pass finishes and the loop sleeps for about a minute.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = shutdown_tx.send(true);
        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("idle loop should stop promptly")
            .unwrap();
        assert!(result.is_ok());
    }

    /// A source whose fetch never completes.
    struct HangingSource;

    #[async_trait]
    impl PlaylistSource for HangingSource {
        async fn fetch_playlist_tracks(&self, _playlist_id: &str) -> Result<Vec<PlaylistTrack>> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn run_until_shutdown_returns_promptly_mid_fetch() {
        let store = SyncStore::open_in_memory().unwrap();
        store.set_playlist_enabled("pl1", "Stuck", true).unwrap();
        let watcher = PlaylistWatcher::new(
            store,
            Arc::new(HangingSource),
            Arc::new(Notify::new()),
            WatcherConfig::default(),
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let loop_watcher = watcher.clone();
        let handle =
            tokio::spawn(async move { loop_watcher.run_until_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!handle.is_finished(), "fetch should still be in flight");
        let _ = shutdown_tx.send(true);
        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("loop stuck in a fetch should still stop promptly")
            .unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn trigger_poll_wakes_sleeping_loop() {
        let store = SyncStore::open_in_memory().unwrap();
        store.set_playlist_enabled("pl1", "Wake", true).unwrap();
        let source = Arc::new(FakePlaylistSource::new());
        let watcher = PlaylistWatcher::new(
            store.clone(),
            source.clone(),
            Arc::new(Notify::new()),
            WatcherConfig::default(),
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let loop_watcher = watcher.clone();
        let handle =
            tokio::spawn(async move { loop_watcher.run_until_shutdown(shutdown_rx).await });

        assert!(
            wait_until(|| source.poll_count("pl1") == 1).await,
            "first pass should run"
        );
        source.set_tracks(
            "pl1",
            vec![make_track("tr_trig", "Triggered", "USAA10000003")],
        );
        // The loop now sleeps for about a minute, so only the trigger can cause this pass.
        watcher.trigger_poll();
        assert!(
            wait_until(|| !store.list_pending().unwrap().is_empty()).await,
            "trigger should run a pass that picks up the new track"
        );

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    #[tokio::test]
    async fn poll_playlist_now_polls_at_once_and_wakes_queue() {
        let store = SyncStore::open_in_memory().unwrap();
        store.set_playlist_enabled("pl1", "Now", true).unwrap();
        store.set_playlist_enabled("pl_off", "Off", false).unwrap();
        let source = Arc::new(FakePlaylistSource::new());
        source.set_tracks("pl1", vec![make_track("tr_now", "Now", "USAA10000004")]);
        source.set_tracks("pl_off", vec![make_track("tr_off", "Off", "USAA10000005")]);
        let notify = Arc::new(Notify::new());
        let watcher = PlaylistWatcher::new(
            store.clone(),
            source.clone(),
            notify.clone(),
            WatcherConfig::default(),
        );

        assert_eq!(watcher.poll_playlist_now("pl_off").await.unwrap(), 0);
        assert_eq!(source.poll_count("pl_off"), 0);

        let added = watcher.poll_playlist_now("pl1").await.unwrap();
        assert_eq!(added, 1);
        assert_eq!(source.poll_count("pl1"), 1);
        assert_eq!(store.list_pending().unwrap().len(), 1);
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("queue should be woken by the immediate poll");
    }

    /// A source where one playlist never answers and the others behave normally.
    struct StallingSource {
        stall_id: &'static str,
        inner: FakePlaylistSource,
    }

    #[async_trait]
    impl PlaylistSource for StallingSource {
        async fn fetch_playlist_tracks(&self, playlist_id: &str) -> Result<Vec<PlaylistTrack>> {
            if playlist_id == self.stall_id {
                return std::future::pending().await;
            }
            self.inner.fetch_playlist_tracks(playlist_id).await
        }
    }

    #[tokio::test]
    async fn fetch_timeout_fails_only_the_stuck_playlist() {
        let store = SyncStore::open_in_memory().unwrap();
        store
            .set_playlist_enabled("pl_stuck", "Stuck", true)
            .unwrap();
        store.set_playlist_enabled("pl_ok", "Fine", true).unwrap();
        let inner = FakePlaylistSource::new();
        inner.set_tracks(
            "pl_ok",
            vec![make_track("tr_ok", "Fine Track", "USAA10000006")],
        );
        let source = Arc::new(StallingSource {
            stall_id: "pl_stuck",
            inner,
        });
        let mut watcher = PlaylistWatcher::new(
            store.clone(),
            source,
            Arc::new(Notify::new()),
            WatcherConfig::default(),
        );
        watcher.playlist_fetch_timeout = Duration::from_millis(50);

        let report = tokio::time::timeout(Duration::from_secs(5), watcher.poll_once())
            .await
            .expect("a stuck fetch must not hang the pass")
            .unwrap();
        assert_eq!(report.playlists_polled, 1);
        assert_eq!(report.enqueued_tracks, 1);
        assert_eq!(report.errors.len(), 1);
        assert_eq!(report.errors[0].0, "pl_stuck");
        assert!(report.errors[0].1.contains("timed out"));
        assert!(
            store
                .playlist_track_statuses("pl_stuck")
                .unwrap()
                .is_empty()
        );
        let pending = store.list_pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].track_id, "tr_ok");
    }

    #[tokio::test]
    async fn insert_followed_by_failure_still_wakes_queue() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("sync.db");
        let store = SyncStore::open(&db_path).unwrap();
        store.set_playlist_enabled("pl1", "Partial", true).unwrap();
        // Fail the final `mark_playlist_polled` write, after the insert has committed.
        let side = rusqlite::Connection::open(&db_path).unwrap();
        side.execute_batch(
            "CREATE TRIGGER fail_poll_mark BEFORE UPDATE ON synced_playlists
             BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END;",
        )
        .unwrap();

        let source = Arc::new(FakePlaylistSource::new());
        source.set_tracks(
            "pl1",
            vec![make_track("tr_partial", "Partial", "USAA10000007")],
        );
        let notify = Arc::new(Notify::new());
        let watcher = PlaylistWatcher::new(
            store.clone(),
            source,
            notify.clone(),
            WatcherConfig::default(),
        );

        let report = watcher.poll_once().await.unwrap();
        assert_eq!(report.playlists_polled, 0);
        assert_eq!(
            report.enqueued_tracks, 1,
            "the committed insert must still be counted"
        );
        assert_eq!(report.errors.len(), 1);
        assert_eq!(store.list_pending().unwrap().len(), 1);
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("queue should be woken for the committed insert");
    }

    #[tokio::test]
    async fn store_error_does_not_stop_the_loop() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("sync.db");
        let store = SyncStore::open(&db_path).unwrap();
        store.set_playlist_enabled("pl1", "Flaky", true).unwrap();
        let source = Arc::new(FakePlaylistSource::new());
        source.set_tracks("pl1", vec![make_track("tr_later", "Later", "USAA10000008")]);
        let mut watcher = PlaylistWatcher::new(
            store.clone(),
            source.clone(),
            Arc::new(Notify::new()),
            WatcherConfig::default(),
        );
        watcher.error_backoff_initial = Duration::from_millis(10);
        watcher.error_backoff_max = Duration::from_millis(40);

        // Hide the table that `list_enabled_playlists` reads so every pass fails in the store.
        let side = rusqlite::Connection::open(&db_path).unwrap();
        side.execute_batch("ALTER TABLE synced_playlists RENAME TO synced_playlists_hidden")
            .unwrap();
        assert!(store.list_enabled_playlists().is_err());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let loop_watcher = watcher.clone();
        let handle =
            tokio::spawn(async move { loop_watcher.run_until_shutdown(shutdown_rx).await });

        // Several failed passes go by with backoff; the loop must stay alive through them.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!handle.is_finished(), "a store error must not end the loop");
        assert_eq!(
            source.poll_count("pl1"),
            0,
            "no playlist is fetched while the store is failing"
        );

        side.execute_batch("ALTER TABLE synced_playlists_hidden RENAME TO synced_playlists")
            .unwrap();
        watcher.trigger_poll();
        assert!(
            wait_until(|| !store.list_pending().unwrap().is_empty()).await,
            "loop should recover once the store does"
        );

        let _ = shutdown_tx.send(true);
        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("loop should stop after recovery")
            .unwrap();
        assert!(result.is_ok());
    }
}
