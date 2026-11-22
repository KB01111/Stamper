//! Playlist polling watcher that detects new additions and wakes the sync queue.

use crate::store::SyncStore;
use crate::{Query, Result};
use async_trait::async_trait;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, watch};

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
    /// `poll_interval * (1 - jitter_fraction) ..= poll_interval * (1 + jitter_fraction)`.
    pub fn jittered_interval(&self) -> Duration {
        let base_secs = self.poll_interval.as_secs_f64();
        if base_secs <= 0.0 {
            return Duration::ZERO;
        }
        let jitter = self.jitter_fraction.clamp(0.0, 0.5);
        if jitter == 0.0 {
            return self.poll_interval;
        }
        let factor: f64 = rand::rng().random_range((1.0 - jitter)..=(1.0 + jitter));
        Duration::from_secs_f64((base_secs * factor).max(0.0))
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
            match self.poll_single_playlist(&playlist.playlist_id).await {
                Ok(added) => {
                    report.playlists_polled += 1;
                    report.enqueued_tracks += added;
                }
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

        if report.enqueued_tracks > 0 {
            self.queue_notify.notify_one();
        }

        Ok(report)
    }

    /// Immediately polls one playlist if it is enabled, waking the queue when new tracks arrive.
    pub async fn poll_playlist_now(&self, playlist_id: &str) -> Result<usize> {
        let enabled = self.store.list_enabled_playlists()?;
        if !enabled.iter().any(|p| p.playlist_id == playlist_id) {
            return Ok(0);
        }
        let added = self.poll_single_playlist(playlist_id).await?;
        if added > 0 {
            self.queue_notify.notify_one();
        }
        Ok(added)
    }

    /// Runs the polling loop with jittered intervals until `shutdown` becomes `true`.
    pub async fn run_until_shutdown(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }

            let _ = self.poll_once().await?;

            let sleep_dur = self.config.jittered_interval();
            tokio::select! {
                _ = tokio::time::sleep(sleep_dur) => {}
                _ = self.wake_poll.notified() => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn poll_single_playlist(&self, playlist_id: &str) -> Result<usize> {
        let tracks = self.source.fetch_playlist_tracks(playlist_id).await?;
        let existing = self.store.existing_track_ids(playlist_id)?;
        let mut added = 0usize;

        for track in tracks {
            if existing.contains(&track.track_id) {
                continue;
            }
            let query = track.to_query();
            if self
                .store
                .enqueue_track(playlist_id, &track.track_id, &query)?
            {
                added += 1;
            }
        }

        self.store
            .mark_playlist_polled(playlist_id, now_unix_secs())?;
        Ok(added)
    }
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
}
