use async_trait::async_trait;
use bytes::Bytes;
use lofty::config::ParseOptions;
use lofty::file::AudioFile;
use lofty::flac::FlacFile;
use spotisync::providers::qobuz::{QobuzConfig, QobuzCredentials, QobuzProvider};
use spotisync::writer::{build_track_path, make_test_flac_bytes};
use spotisync::{
    Candidate, Provider, Quality, Query, QueueConfig, Result, SyncError, SyncQueue, SyncStatus,
    SyncStore,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Queue settings with no real pacing, so tests run quickly.
fn fast_config(output_dir: PathBuf) -> QueueConfig {
    QueueConfig {
        output_dir,
        per_provider_rps: 100.0,
        per_provider_burst: 10.0,
        ..QueueConfig::default()
    }
}

/// Reads one HTTP request head up to the blank line, across however many packets it spans.
async fn read_request_head(socket: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") && buf.len() < 16 * 1024 {
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

fn sample_query(title: &str, isrc: &str, track_num: u32) -> Query {
    Query {
        isrc: Some(isrc.to_string()),
        title: title.to_string(),
        artists: vec!["Daft Punk".to_string()],
        album: "Random Access Memories".to_string(),
        duration_ms: 337_000,
        album_artist: Some("Daft Punk".to_string()),
        track_number: Some(track_num),
        disc_number: Some(1),
        year: Some(2013),
        cover_url: None,
    }
}

struct ScriptedProvider {
    bit_depth: u8,
    sample_rate_hz: u32,
    fetch_calls: AtomicUsize,
    search_calls: AtomicUsize,
    fail_5xx_times: AtomicUsize,
    rate_limit_times: AtomicUsize,
    retry_after_secs: u64,
    lyrics: Option<String>,
    search_delay: Duration,
    fetch_delay: Duration,
}

impl ScriptedProvider {
    fn new(bit_depth: u8, sample_rate_hz: u32) -> Self {
        Self {
            bit_depth,
            sample_rate_hz,
            fetch_calls: AtomicUsize::new(0),
            search_calls: AtomicUsize::new(0),
            fail_5xx_times: AtomicUsize::new(0),
            rate_limit_times: AtomicUsize::new(0),
            retry_after_secs: 1,
            lyrics: Some("[00:05.00] Test lyric line\n".to_string()),
            search_delay: Duration::ZERO,
            fetch_delay: Duration::ZERO,
        }
    }

    fn fetch_count(&self) -> usize {
        self.fetch_calls.load(Ordering::SeqCst)
    }

    fn search_count(&self) -> usize {
        self.search_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Provider for ScriptedProvider {
    fn id(&self) -> &'static str {
        "mock"
    }

    fn max_quality(&self) -> Quality {
        Quality::Flac24_192
    }

    async fn search(&self, q: &Query) -> Result<Vec<Candidate>> {
        self.search_calls.fetch_add(1, Ordering::SeqCst);
        if !self.search_delay.is_zero() {
            tokio::time::sleep(self.search_delay).await;
        }
        if self.rate_limit_times.load(Ordering::SeqCst) > 0 {
            let prev = self.rate_limit_times.fetch_sub(1, Ordering::SeqCst);
            if prev > 0 {
                return Err(SyncError::RateLimited {
                    provider: "mock".to_string(),
                    retry_after_secs: Some(self.retry_after_secs),
                    message: "forced 429 rate limit".to_string(),
                });
            }
        }
        if self.fail_5xx_times.load(Ordering::SeqCst) > 0 {
            let prev = self.fail_5xx_times.fetch_sub(1, Ordering::SeqCst);
            if prev > 0 {
                return Err(SyncError::ServerError {
                    provider: "mock".to_string(),
                    status: 503,
                    message: "forced 503 backend failure".to_string(),
                });
            }
        }

        Ok(vec![Candidate {
            provider: "mock",
            track_id: format!("mock-{}", q.track_number.unwrap_or(1)),
            isrc: q.isrc.clone(),
            title: q.title.clone(),
            artists: q.artists.clone(),
            album: Some(q.album.clone()),
            album_artist: q.album_artist.clone(),
            track_number: q.track_number,
            disc_number: q.disc_number,
            year: q.year,
            bit_depth: Some(self.bit_depth),
            sample_rate_hz: Some(self.sample_rate_hz),
            duration_ms: q.duration_ms,
            lossless: true,
            cover_url: None,
            source_url: Some("https://example.invalid/mock/track".to_string()),
            lyrics_lrc: self.lyrics.clone(),
        }])
    }

    async fn fetch(&self, _c: &Candidate, _q: Quality) -> Result<Bytes> {
        self.fetch_calls.fetch_add(1, Ordering::SeqCst);
        if !self.fetch_delay.is_zero() {
            tokio::time::sleep(self.fetch_delay).await;
        }
        Ok(make_test_flac_bytes(self.bit_depth, self.sample_rate_hz))
    }
}

fn count_tmp_files_recursive(dir: &Path) -> usize {
    let mut total = 0;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                total += count_tmp_files_recursive(&path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("tmp") {
                total += 1;
            }
        }
    }
    total
}

#[tokio::test]
async fn interrupted_run_resumes_cleanly_without_duplicates_or_tmp_files() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sync.db");
    let out_dir = dir.path().join("music");

    // Simulate a previous run that left one track done, one in-flight downloading,
    // one in-flight resolving, and one still pending.
    {
        let store = SyncStore::open(&db_path).expect("open db");
        store
            .enqueue_track(
                "pl1",
                "t1",
                &sample_query("Give Life Back to Music", "USQX91300101", 1),
            )
            .unwrap();
        store
            .enqueue_track(
                "pl1",
                "t2",
                &sample_query("The Game of Love", "USQX91300102", 2),
            )
            .unwrap();
        store
            .enqueue_track(
                "pl1",
                "t3",
                &sample_query("Giorgio by Moroder", "USQX91300103", 3),
            )
            .unwrap();
        store
            .enqueue_track("pl1", "t4", &sample_query("Within", "USQX91300104", 4))
            .unwrap();

        store
            .mark_done(
                "pl1",
                "t1",
                spotisync::MatchedBy::Isrc,
                "mock",
                Quality::Flac16_44,
                "/already/done.flac",
            )
            .unwrap();
        store
            .update_status("pl1", "t2", SyncStatus::Downloading, None)
            .unwrap();
        store
            .update_status("pl1", "t3", SyncStatus::Resolving, None)
            .unwrap();
    }

    // Reopen from disk and resume.
    let store = SyncStore::open(&db_path).expect("reopen db");
    let provider = Arc::new(ScriptedProvider::new(16, 44_100));
    let queue = SyncQueue::new(
        store.clone(),
        vec![provider.clone()],
        QueueConfig {
            output_dir: out_dir.clone(),
            per_provider_concurrency: 2,
            per_provider_rps: 100.0,
            per_provider_burst: 10.0,
            ..QueueConfig::default()
        },
    )
    .expect("queue builds");

    let summary = queue.process_pending().await.expect("resume succeeds");
    assert_eq!(summary.recovered, 2, "t2 and t3 should be recovered");
    assert_eq!(summary.processed, 3, "t2, t3, t4 processed; t1 untouched");
    assert_eq!(summary.done, 3);
    assert_eq!(provider.fetch_count(), 3, "t1 must not be re-fetched");

    // Re-running queue immediately does nothing.
    let second = queue.process_pending().await.expect("second pass");
    assert_eq!(second.processed, 0);
    assert_eq!(provider.fetch_count(), 3);

    // Zero .tmp files anywhere under output directory.
    assert_eq!(count_tmp_files_recursive(&out_dir), 0);

    let all_rows = store.playlist_track_statuses("pl1").unwrap();
    assert_eq!(all_rows.len(), 4);
    assert!(all_rows.iter().all(|r| r.status == SyncStatus::Done));
}

#[tokio::test]
async fn forced_429_triggers_backoff_and_surfaces_visible_cooldown_in_snapshot() {
    let dir = tempdir().expect("tempdir");
    let store = SyncStore::open_in_memory().expect("store");
    store
        .enqueue_track(
            "pl1",
            "t1",
            &sample_query("Instant Crush", "USQX91300105", 5),
        )
        .unwrap();

    let provider = Arc::new(ScriptedProvider::new(24, 96_000));
    provider.rate_limit_times.store(1, Ordering::SeqCst);

    let queue = SyncQueue::new(
        store.clone(),
        vec![provider.clone()],
        QueueConfig {
            output_dir: dir.path().join("music"),
            per_provider_rps: 100.0,
            per_provider_burst: 10.0,
            base_backoff: Duration::from_millis(10),
            ..QueueConfig::default()
        },
    )
    .expect("queue");

    let mut rx = queue.subscribe();
    let seen_cooldown = Arc::new(AtomicUsize::new(0));
    let seen_flag = Arc::clone(&seen_cooldown);

    let observer = tokio::spawn(async move {
        while rx.changed().await.is_ok() {
            let snap = rx.borrow().clone();
            if let Some(cd) = snap.cooldowns.get("mock") {
                assert!(cd.retry_at_unix_secs > 0);
                assert!(cd.message.contains("429"));
                seen_flag.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    let summary = queue.process_pending().await.expect("process");
    drop(queue);
    let _ = observer.await;

    assert_eq!(summary.done, 1);
    assert!(
        seen_cooldown.load(Ordering::SeqCst) >= 1,
        "SyncSnapshot must surface visible per-provider cooldown on 429"
    );
}

#[tokio::test]
async fn server_5xx_exhausts_max_attempts_then_marks_failed() {
    let dir = tempdir().expect("tempdir");
    let store = SyncStore::open_in_memory().expect("store");
    store
        .enqueue_track("pl1", "t1", &sample_query("Touch", "USQX91300107", 7))
        .unwrap();

    let provider = Arc::new(ScriptedProvider::new(16, 44_100));
    provider.fail_5xx_times.store(10, Ordering::SeqCst);

    let queue = SyncQueue::new(
        store.clone(),
        vec![provider],
        QueueConfig {
            output_dir: dir.path().join("music"),
            per_provider_rps: 100.0,
            per_provider_burst: 10.0,
            max_attempts: 5,
            base_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(20),
            ..QueueConfig::default()
        },
    )
    .expect("queue");

    let summary = queue.process_pending().await.expect("process");
    assert_eq!(summary.failed, 1);

    let row = store.get_item("pl1", "t1").unwrap().unwrap();
    assert_eq!(row.status, SyncStatus::Failed);
    assert_eq!(row.attempts, 5);
    assert!(row.error.as_deref().unwrap_or("").contains("503"));
}

#[tokio::test]
async fn equal_quality_rerun_does_zero_downloads_and_higher_quality_upgrades_with_verified_tags() {
    let dir = tempdir().expect("tempdir");
    let out_dir = dir.path().join("music");
    let store = SyncStore::open_in_memory().expect("store");
    store
        .enqueue_track(
            "pl1",
            "t5",
            &sample_query("Instant Crush", "USQX91300105", 5),
        )
        .unwrap();

    // Run 1: Download 16-bit/44.1kHz FLAC.
    let provider_cd = Arc::new(ScriptedProvider::new(16, 44_100));
    let queue_cd = SyncQueue::new(
        store.clone(),
        vec![provider_cd.clone()],
        QueueConfig {
            output_dir: out_dir.clone(),
            per_provider_rps: 100.0,
            per_provider_burst: 10.0,
            ..QueueConfig::default()
        },
    )
    .expect("queue cd");

    let first = queue_cd.process_pending().await.expect("first run");
    assert_eq!(first.done, 1);
    assert_eq!(provider_cd.fetch_count(), 1);

    let item_after_cd = store.get_item("pl1", "t5").unwrap().unwrap();
    let flac_path = Path::new(item_after_cd.path.as_deref().unwrap()).to_path_buf();
    assert!(flac_path.exists());
    let original_bytes = fs::read(&flac_path).unwrap();

    // Run 2: Retry same item at EQUAL quality (16/44.1) -> MUST do zero fetch calls and not overwrite!
    store.retry_item("pl1", "t5").unwrap();
    let second = queue_cd.process_pending().await.expect("equal rerun");
    assert_eq!(second.done, 1);
    assert_eq!(
        provider_cd.fetch_count(),
        1,
        "equal quality re-run must perform ZERO re-downloads"
    );
    assert_eq!(
        fs::read(&flac_path).unwrap(),
        original_bytes,
        "existing file at equal quality must not be overwritten"
    );

    // Run 3: Retry same item with HIGHER quality provider (24/96) -> MUST upgrade cleanly!
    store.retry_item("pl1", "t5").unwrap();
    let provider_hires = Arc::new(ScriptedProvider::new(24, 96_000));
    let queue_hires = SyncQueue::new(
        store.clone(),
        vec![provider_hires.clone()],
        QueueConfig {
            output_dir: out_dir.clone(),
            per_provider_rps: 100.0,
            per_provider_burst: 10.0,
            ..QueueConfig::default()
        },
    )
    .expect("queue hires");

    let third = queue_hires.process_pending().await.expect("upgrade run");
    assert_eq!(third.done, 1);
    assert_eq!(provider_hires.fetch_count(), 1);

    let item_after_upgrade = store.get_item("pl1", "t5").unwrap().unwrap();
    assert_eq!(item_after_upgrade.quality.as_deref(), Some("flac_24_96"));

    // Verify upgraded FLAC stream properties and tags via lofty.
    let mut flac_file = fs::File::open(&flac_path).unwrap();
    let flac = FlacFile::read_from(&mut flac_file, ParseOptions::new()).unwrap();
    assert_eq!(flac.properties().bit_depth(), 24);
    assert_eq!(flac.properties().sample_rate(), 96_000);
    let vorbis = flac.vorbis_comments().expect("vorbis tag present");
    assert_eq!(vorbis.get("TITLE"), Some("Instant Crush"));
    assert_eq!(vorbis.get("ARTIST"), Some("Daft Punk"));
    assert_eq!(vorbis.get("ALBUM"), Some("Random Access Memories"));
    assert_eq!(vorbis.get("TRACKNUMBER"), Some("5"));
    assert_eq!(vorbis.get("DISCNUMBER"), Some("1"));
    assert_eq!(vorbis.get("DATE"), Some("2013"));
    assert_eq!(vorbis.get("ISRC"), Some("USQX91300105"));
    assert_eq!(vorbis.get("PROVIDER"), Some("mock"));
    assert_eq!(
        vorbis.get("SOURCE_URL"),
        Some("https://example.invalid/mock/track")
    );

    // Sidecar .lrc exists and zero .tmp files remain.
    assert!(flac_path.with_extension("lrc").exists());
    assert_eq!(count_tmp_files_recursive(&out_dir), 0);
}

#[tokio::test]
async fn qobuz_provider_fetch_end_to_end_over_local_http_with_429_and_flac_validation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let flac_payload = make_test_flac_bytes(24, 96_000);
    let flac_for_server = flac_payload.clone();
    let stream_url = format!("{base_url}/stream/track10760101.flac");

    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let mut get_file_url_hits = 0usize;
        loop {
            let (mut socket, _) = tokio::select! {
                _ = &mut stop_rx => break,
                accepted = listener.accept() => match accepted {
                    Ok(pair) => pair,
                    Err(_) => break,
                },
            };
            let req = read_request_head(&mut socket).await;

            if req.starts_with("GET /track/search") {
                let body = include_str!("fixtures/qobuz_search_response.json");
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(resp.as_bytes()).await;
            } else if req.starts_with("GET /track/getFileUrl") {
                get_file_url_hits += 1;
                if get_file_url_hits == 1 {
                    let body = r#"{"status":"error"}"#;
                    let resp = format!(
                        "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                } else {
                    let body = format!(
                        r#"{{"track_id":10760101,"url":"{stream_url}","format_id":7,"mime_type":"audio/flac","sampling_rate":96.0,"bit_depth":24,"sample":false}}"#
                    );
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                }
            } else if req.starts_with("GET /stream/") {
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: audio/flac\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    flac_for_server.len()
                );
                let _ = socket.write_all(header.as_bytes()).await;
                let _ = socket.write_all(&flac_for_server).await;
            }
            let _ = socket.shutdown().await;
        }
    });

    let dir = tempdir().unwrap();
    let store = SyncStore::open_in_memory().unwrap();
    store
        .enqueue_track(
            "pl1",
            "t5",
            &sample_query("Instant Crush", "USQX91300105", 5),
        )
        .unwrap();

    let provider = Arc::new(
        QobuzProvider::with_config(
            QobuzCredentials::new("app-id", "auth-token", Some("app-secret".to_string())),
            QobuzConfig {
                base_url,
                ..QobuzConfig::default()
            },
        )
        .unwrap(),
    );

    let queue = SyncQueue::new(
        store.clone(),
        vec![provider],
        QueueConfig {
            output_dir: dir.path().join("music"),
            per_provider_rps: 100.0,
            per_provider_burst: 10.0,
            ..QueueConfig::default()
        },
    )
    .unwrap();

    let mut rx = queue.subscribe();
    let saw_qobuz_cooldown = Arc::new(AtomicUsize::new(0));
    let flag = Arc::clone(&saw_qobuz_cooldown);
    let watcher = tokio::spawn(async move {
        while rx.changed().await.is_ok() {
            if rx.borrow().cooldowns.contains_key("qobuz") {
                flag.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    let summary = queue.process_pending().await.unwrap();
    drop(queue);
    let _ = watcher.await;
    let _ = stop_tx.send(());
    let _ = server.await;

    assert_eq!(summary.done, 1);
    assert!(saw_qobuz_cooldown.load(Ordering::SeqCst) >= 1);

    let row = store.get_item("pl1", "t5").unwrap().unwrap();
    assert_eq!(row.status, SyncStatus::Done);
    assert_eq!(row.provider.as_deref(), Some("qobuz"));
    assert_eq!(row.matched_by.as_deref(), Some("isrc"));
    assert_eq!(row.quality.as_deref(), Some("flac_24_96"));
    let written_path = Path::new(row.path.as_deref().unwrap());
    assert!(written_path.exists());
    let mut written_file = fs::File::open(written_path).unwrap();
    let flac = FlacFile::read_from(&mut written_file, ParseOptions::new()).unwrap();
    assert_eq!(flac.properties().bit_depth(), 24);
    assert_eq!(flac.properties().sample_rate(), 96_000);
}

#[tokio::test]
async fn non_flac_file_at_destination_fails_fast_without_downloading() {
    let dir = tempdir().expect("tempdir");
    let out_dir = dir.path().join("music");
    let store = SyncStore::open_in_memory().expect("store");
    store
        .enqueue_track(
            "pl1",
            "t5",
            &sample_query("Instant Crush", "USQX91300105", 5),
        )
        .unwrap();
    let destination = build_track_path(
        &out_dir,
        "Daft Punk",
        "Random Access Memories",
        Some(5),
        "Instant Crush",
    );
    fs::create_dir_all(destination.parent().unwrap()).unwrap();
    fs::write(&destination, b"ID3\x03\x00 not a flac").unwrap();

    let provider = Arc::new(ScriptedProvider::new(16, 44_100));
    let queue =
        SyncQueue::new(store.clone(), vec![provider.clone()], fast_config(out_dir)).unwrap();

    let summary = queue.process_pending().await.expect("pass");
    assert_eq!(summary.failed, 1);
    assert_eq!(
        provider.fetch_count(),
        0,
        "an unreadable destination must not trigger a full download"
    );
    let row = store.get_item("pl1", "t5").unwrap().unwrap();
    assert_eq!(row.status, SyncStatus::Failed);
    assert!(
        row.error
            .unwrap_or_default()
            .contains("not a readable FLAC")
    );
}

#[tokio::test]
async fn same_file_from_two_playlists_is_downloaded_once() {
    let dir = tempdir().expect("tempdir");
    let out_dir = dir.path().join("music");
    let store = SyncStore::open_in_memory().expect("store");
    let query = sample_query("Instant Crush", "USQX91300105", 5);
    store.enqueue_track("pl1", "t5", &query).unwrap();
    store.enqueue_track("pl2", "t5", &query).unwrap();

    // The delayed fetch keeps the first write in progress while the second item checks the file.
    let mut slow = ScriptedProvider::new(16, 44_100);
    slow.fetch_delay = Duration::from_millis(300);
    let provider = Arc::new(slow);
    let queue = SyncQueue::new(
        store.clone(),
        vec![provider.clone()],
        fast_config(out_dir.clone()),
    )
    .unwrap();

    let summary = queue.process_pending().await.expect("pass");
    assert_eq!(summary.done, 2);
    assert_eq!(
        provider.fetch_count(),
        1,
        "the second playlist must reuse the file written under the destination lock"
    );
    assert_eq!(count_tmp_files_recursive(&out_dir), 0);
}

#[tokio::test]
async fn rate_limit_exhaustion_defers_items_and_skips_the_provider_for_the_pass() {
    let dir = tempdir().expect("tempdir");
    let out_dir = dir.path().join("music");
    let store = SyncStore::open_in_memory().expect("store");
    store
        .enqueue_track(
            "pl1",
            "t1",
            &sample_query("Give Life Back to Music", "USQX91300101", 1),
        )
        .unwrap();
    store
        .enqueue_track(
            "pl1",
            "t2",
            &sample_query("The Game of Love", "USQX91300102", 2),
        )
        .unwrap();

    // The first search gets a 429 and the provider is then healthy again.
    let provider = Arc::new(ScriptedProvider::new(16, 44_100));
    provider.rate_limit_times.store(1, Ordering::SeqCst);
    let queue = SyncQueue::new(
        store.clone(),
        vec![provider.clone()],
        QueueConfig {
            per_provider_concurrency: 1,
            max_attempts: 1,
            ..fast_config(out_dir)
        },
    )
    .unwrap();

    let first = queue.process_pending().await.expect("first pass");
    assert_eq!(first.deferred, 2);
    assert_eq!(first.failed, 0);
    assert_eq!(
        provider.search_count(),
        1,
        "an exhausted provider must not be called again in the same pass"
    );
    for track in ["t1", "t2"] {
        let row = store.get_item("pl1", track).unwrap().unwrap();
        assert_eq!(row.status, SyncStatus::Pending);
        assert_eq!(row.attempts, 0, "rate limiting must not burn an attempt");
    }

    // After Retry-After the next pass completes both tracks.
    let second = queue.process_pending().await.expect("second pass");
    assert_eq!(second.done, 2);
    assert_eq!(provider.search_count(), 3);
}

#[tokio::test]
async fn failed_completion_write_is_reported_and_recovered_on_the_next_pass() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sync.db");
    let out_dir = dir.path().join("music");
    let store = SyncStore::open(&db_path).expect("store");
    store
        .enqueue_track(
            "pl1",
            "t5",
            &sample_query("Instant Crush", "USQX91300105", 5),
        )
        .unwrap();

    // Reject every write that marks a row done, through a second connection.
    let saboteur = rusqlite::Connection::open(&db_path).expect("second connection");
    saboteur
        .execute_batch(
            "CREATE TRIGGER block_done BEFORE UPDATE OF status ON sync_items
             WHEN NEW.status = 'done'
             BEGIN SELECT RAISE(ABORT, 'forced failure'); END;",
        )
        .unwrap();

    let provider = Arc::new(ScriptedProvider::new(16, 44_100));
    let queue =
        SyncQueue::new(store.clone(), vec![provider.clone()], fast_config(out_dir)).unwrap();

    let first = queue.process_pending().await.expect("first pass");
    assert_eq!(
        first.done, 0,
        "an unrecorded completion must not count as done"
    );
    assert_eq!(first.store_errors.len(), 1);
    assert!(first.store_errors[0].starts_with("mark_done:"));
    assert_eq!(
        store.get_item("pl1", "t5").unwrap().unwrap().status,
        SyncStatus::Downloading
    );

    saboteur.execute_batch("DROP TRIGGER block_done;").unwrap();
    let second = queue.process_pending().await.expect("second pass");
    assert_eq!(second.recovered, 1);
    assert_eq!(second.done, 1);
    assert!(second.store_errors.is_empty());
    assert_eq!(
        provider.fetch_count(),
        1,
        "the written file is reused, not downloaded again"
    );
}

#[tokio::test]
async fn shutdown_interrupts_a_slow_pass_and_leaves_its_row_resumable() {
    let dir = tempdir().expect("tempdir");
    let store = SyncStore::open_in_memory().expect("store");
    store
        .enqueue_track("pl1", "t7", &sample_query("Touch", "USQX91300107", 7))
        .unwrap();

    let mut slow = ScriptedProvider::new(16, 44_100);
    slow.search_delay = Duration::from_secs(30);
    let queue = SyncQueue::new(
        store.clone(),
        vec![Arc::new(slow)],
        fast_config(dir.path().join("music")),
    )
    .unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let runner = tokio::spawn({
        let queue = queue.clone();
        async move { queue.run_until_shutdown(shutdown_rx).await }
    });

    // Give the pass time to reach the slow search.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        store.get_item("pl1", "t7").unwrap().unwrap().status,
        SyncStatus::Resolving
    );
    shutdown_tx.send(true).expect("runner is listening");

    let result = tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("shutdown must not wait for the 30 second search")
        .expect("runner task joins");
    assert!(result.is_ok());
    assert_eq!(
        store.recover_interrupted().unwrap(),
        1,
        "the interrupted row must be left for recovery"
    );
}

#[tokio::test]
async fn distinct_recordings_with_the_same_metadata_path_get_distinct_files() {
    let dir = tempdir().expect("tempdir");
    let out_dir = dir.path().join("music");
    let store = SyncStore::open_in_memory().expect("store");
    store
        .enqueue_track(
            "pl1",
            "t1",
            &sample_query("Instant Crush", "USQX91300105", 5),
        )
        .unwrap();
    // Same title, album and track number, but a different ISRC: a different recording.
    store
        .enqueue_track(
            "pl1",
            "t2",
            &sample_query("Instant Crush", "USQX91300199", 5),
        )
        .unwrap();

    let provider = Arc::new(ScriptedProvider::new(16, 44_100));
    let queue =
        SyncQueue::new(store.clone(), vec![provider], fast_config(out_dir.clone())).unwrap();

    let summary = queue.process_pending().await.expect("pass");
    assert_eq!(summary.done, 2);

    let path_of = |track: &str| {
        PathBuf::from(
            store
                .get_item("pl1", track)
                .unwrap()
                .unwrap()
                .path
                .expect("done rows record a path"),
        )
    };
    let (p1, p2) = (path_of("t1"), path_of("t2"));
    assert_ne!(p1, p2, "distinct recordings must not share a file");
    assert!(p1.exists() && p2.exists());

    let plain = build_track_path(
        &out_dir,
        "Daft Punk",
        "Random Access Memories",
        Some(5),
        "Instant Crush",
    );
    assert!(
        p1 == plain || p2 == plain,
        "one recording keeps the plain metadata path"
    );
    assert!(
        p1.to_string_lossy().contains(" [t") || p2.to_string_lossy().contains(" [t"),
        "the other recording gets a disambiguating suffix"
    );
    assert_eq!(count_tmp_files_recursive(&out_dir), 0);
}
