//! End-to-end verification checklist suite for the `spotisync` engine.
//!
//! Exercises the real `QobuzProvider`, `PlaylistWatcher`, `SyncStore`,
//! `SyncQueue`, strict matcher, and atomic FLAC writer against an in-process
//! local HTTP mock Qobuz server (`tokio::net::TcpListener`, zero external
//! network access).

use async_trait::async_trait;
use bytes::Bytes;
use lofty::config::ParseOptions;
use lofty::file::AudioFile;
use lofty::flac::FlacFile;
use lofty::ogg::OggPictureStorage;
use lofty::picture::{MimeType, PictureType};
use serde_json::json;
use spotisync::providers::qobuz::{QobuzConfig, QobuzCredentials, QobuzProvider};
use spotisync::writer::{TrackMetadata, WriteOutcome, WriteRequest, write_flac_atomic};
use spotisync::{
    Candidate, PlaylistSource, PlaylistTrack, PlaylistWatcher, Provider, ProviderCooldown, Quality,
    Query, QueueConfig, Result, SyncQueue, SyncSnapshot, SyncStatus, SyncStore, WatcherConfig,
};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, watch};

// ---------------------------------------------------------------------------
// Minimal valid JPEG & PNG cover art bytes + RFC-9639 PCM FLAC synthesizer
// ---------------------------------------------------------------------------

/// Minimal valid JPEG payload (SOI + JFIF APP0 + EOI).
const SAMPLE_JPEG_COVER: &[u8] = &[
    0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, b'J', b'F', b'I', b'F', 0x00, 0x01, 0x01, 0x00, 0x00, 0x01,
    0x00, 0x01, 0x00, 0x00, 0xFF, 0xD9,
];

/// Minimal valid 1x1 RGB PNG payload (signature + IHDR + IDAT + IEND).
const SAMPLE_PNG_COVER: &[u8] = &[
    0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53,
    0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, 0x08, 0xD7, 0x63, 0xF8, 0xCF, 0xC0, 0x00,
    0x00, 0x03, 0x01, 0x01, 0x00, 0x18, 0xDD, 0x8D, 0xB0, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E,
    0x44, 0xAE, 0x42, 0x60, 0x82,
];

fn flac_crc8(data: &[u8]) -> u8 {
    let mut crc: u8 = 0;
    for &byte in data {
        crc ^= byte;
        for _ in 0..8 {
            if (crc & 0x80) != 0 {
                crc = (crc << 1) ^ 0x07;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

fn flac_crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= u16::from(byte) << 8;
        for _ in 0..8 {
            if (crc & 0x8000) != 0 {
                crc = (crc << 1) ^ 0x8005;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

/// Builds a single RFC-9639 compliant stereo CONSTANT-zero PCM FLAC audio frame
/// of 192 samples with valid CRC-8 header and CRC-16 footer.
fn build_pcm_flac_frame(bit_depth: u8, sample_rate_hz: u32) -> Vec<u8> {
    // Frame header sample-rate codes (RFC 9639). 0b1100 is not 192 kHz: it means
    // "read an 8-bit kHz value after the header", which corrupts the header.
    let sr_code: u8 = match sample_rate_hz {
        44_100 => 0x09,
        48_000 => 0x0A,
        96_000 => 0x0B,
        192_000 => 0x03,
        _ => 0x00,
    };
    let bps_code: u8 = match bit_depth {
        16 => 0x04,
        24 => 0x06,
        _ => 0x00,
    };
    // Block size code 0x1 = 192 samples; channel assignment 0x1 = 2 independent channels.
    let mut frame = Vec::with_capacity(32);
    frame.push(0xFF);
    frame.push(0xF8);
    frame.push((0x01 << 4) | sr_code);
    frame.push((0x01 << 4) | (bps_code << 1));
    frame.push(0x00); // UTF-8 coded frame number 0
    let hdr_crc = flac_crc8(&frame);
    frame.push(hdr_crc);

    let sample_bytes = usize::from(bit_depth / 8).max(2);
    // Left subframe: CONSTANT (type 0) with zero sample value.
    frame.push(0x00);
    frame.extend(std::iter::repeat_n(0u8, sample_bytes));
    // Right subframe: CONSTANT (type 0) with zero sample value.
    frame.push(0x00);
    frame.extend(std::iter::repeat_n(0u8, sample_bytes));

    let crc16 = flac_crc16(&frame);
    frame.extend_from_slice(&crc16.to_be_bytes());
    frame
}

/// Builds a complete synthesized PCM FLAC byte stream with STREAMINFO,
/// optional embedded PICTURE metadata block (JPEG/PNG), and a valid PCM frame.
fn build_synthesized_pcm_flac(
    bit_depth: u8,
    sample_rate_hz: u32,
    cover: Option<(&[u8], &str)>,
) -> Bytes {
    let audio_frame = build_pcm_flac_frame(bit_depth, sample_rate_hz);
    let frame_len = audio_frame.len() as u32;

    let mut buf = Vec::with_capacity(128 + cover.map_or(0, |(b, _)| b.len()));
    buf.extend_from_slice(b"fLaC");

    // STREAMINFO block (type 0, length 34).
    let streaminfo_flags: u8 = if cover.is_some() { 0x00 } else { 0x80 };
    buf.extend_from_slice(&[streaminfo_flags, 0x00, 0x00, 0x22]);
    buf.extend_from_slice(&192u16.to_be_bytes()); // min_block_size
    buf.extend_from_slice(&192u16.to_be_bytes()); // max_block_size
    buf.extend_from_slice(&[
        ((frame_len >> 16) & 0xFF) as u8,
        ((frame_len >> 8) & 0xFF) as u8,
        (frame_len & 0xFF) as u8,
    ]); // min_frame_size
    buf.extend_from_slice(&[
        ((frame_len >> 16) & 0xFF) as u8,
        ((frame_len >> 8) & 0xFF) as u8,
        (frame_len & 0xFF) as u8,
    ]); // max_frame_size

    // 64-bit packed: [20 bits sample_rate][3 bits channels-1][5 bits bps-1][36 bits total_samples]
    let sr = u64::from(sample_rate_hz) & 0xF_FFFF;
    let channels_minus_1: u64 = 1; // stereo
    let bps_minus_1 = u64::from(bit_depth.saturating_sub(1)) & 0x1F;
    let total_samples: u64 = 192;
    let packed: u64 = (sr << 44) | (channels_minus_1 << 41) | (bps_minus_1 << 36) | total_samples;
    buf.extend_from_slice(&packed.to_be_bytes());
    buf.extend_from_slice(&[0u8; 16]); // unset MD5 (valid per RFC 9639)

    // Optional PICTURE block (type 6, is_last = 1).
    if let Some((cover_bytes, mime)) = cover {
        let mut pic_body = Vec::with_capacity(32 + mime.len() + cover_bytes.len());
        pic_body.extend_from_slice(&3u32.to_be_bytes()); // PictureType::CoverFront
        pic_body.extend_from_slice(&(mime.len() as u32).to_be_bytes());
        pic_body.extend_from_slice(mime.as_bytes());
        pic_body.extend_from_slice(&0u32.to_be_bytes()); // empty description
        pic_body.extend_from_slice(&1u32.to_be_bytes()); // width
        pic_body.extend_from_slice(&1u32.to_be_bytes()); // height
        pic_body.extend_from_slice(&24u32.to_be_bytes()); // color depth
        pic_body.extend_from_slice(&0u32.to_be_bytes()); // indexed colors
        pic_body.extend_from_slice(&(cover_bytes.len() as u32).to_be_bytes());
        pic_body.extend_from_slice(cover_bytes);

        let len = pic_body.len() as u32;
        buf.extend_from_slice(&[
            0x86, // is_last = 1, block_type = 6 (PICTURE)
            ((len >> 16) & 0xFF) as u8,
            ((len >> 8) & 0xFF) as u8,
            (len & 0xFF) as u8,
        ]);
        buf.extend_from_slice(&pic_body);
    }

    buf.extend_from_slice(&audio_frame);
    Bytes::from(buf)
}

/// Walks raw FLAC metadata blocks and verifies the trailing PCM audio frame
/// header sync code, CRC-8, and CRC-16 remain intact after tagging.
fn verify_raw_flac_bitstream(path: &Path) {
    let data = fs::read(path).expect("read written flac bytes");
    assert!(
        data.len() >= 42 && &data[..4] == b"fLaC",
        "file {} must begin with fLaC magic",
        path.display()
    );

    let mut offset = 4usize;
    let mut saw_streaminfo = false;
    let mut saw_vorbis = false;
    let mut streaminfo_rate = None;
    loop {
        assert!(
            offset + 4 <= data.len(),
            "truncated metadata block header in {}",
            path.display()
        );
        let header = &data[offset..offset + 4];
        let is_last = (header[0] & 0x80) != 0;
        let block_type = header[0] & 0x7F;
        let block_len =
            ((header[1] as usize) << 16) | ((header[2] as usize) << 8) | (header[3] as usize);
        offset += 4;
        assert!(
            offset + block_len <= data.len(),
            "truncated metadata block payload in {}",
            path.display()
        );
        match block_type {
            0 => {
                saw_streaminfo = true;
                // The packed sample rate is the 20 bits after the 10-byte block sizes and frame sizes.
                if block_len >= 18 {
                    let packed = u64::from_be_bytes(
                        data[offset + 10..offset + 18]
                            .try_into()
                            .expect("eight packed STREAMINFO bytes"),
                    );
                    streaminfo_rate = Some((packed >> 44) as u32);
                }
            }
            4 => saw_vorbis = true,
            _ => {}
        }
        offset += block_len;
        if is_last {
            break;
        }
    }

    assert!(saw_streaminfo, "STREAMINFO block missing");
    assert!(saw_vorbis, "VORBIS_COMMENT block missing");

    let frame = &data[offset..];
    assert!(
        frame.len() >= 8,
        "missing PCM audio frame after metadata blocks in {}",
        path.display()
    );
    assert_eq!(&frame[..2], &[0xFF, 0xF8], "invalid FLAC frame sync header");
    let expected_crc8 = flac_crc8(&frame[..5]);
    assert_eq!(frame[5], expected_crc8, "FLAC frame header CRC-8 mismatch");

    // The frame header's sample-rate code must agree with STREAMINFO. Code 0
    // means "as STREAMINFO says"; any other code names its rate.
    let rate = streaminfo_rate.expect("STREAMINFO carries a sample rate");
    let frame_rate = match frame[2] & 0x0F {
        0 => rate,
        1 => 88_200,
        2 => 176_400,
        3 => 192_000,
        4 => 8_000,
        5 => 16_000,
        6 => 22_050,
        7 => 24_000,
        8 => 32_000,
        9 => 44_100,
        10 => 48_000,
        11 => 96_000,
        _ => 0,
    };
    assert_eq!(
        frame_rate,
        rate,
        "frame header sample-rate code {:#x} disagrees with STREAMINFO {rate} Hz in {}",
        frame[2] & 0x0F,
        path.display()
    );
    let expected_crc16 = flac_crc16(&frame[..frame.len() - 2]);
    let actual_crc16 = u16::from_be_bytes([frame[frame.len() - 2], frame[frame.len() - 1]]);
    assert_eq!(actual_crc16, expected_crc16, "FLAC frame CRC-16 mismatch");
}

// ---------------------------------------------------------------------------
// In-process local mock Qobuz HTTP server (tokio::net::TcpListener)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MockQobuzTrack {
    qobuz_id: u64,
    title: String,
    version: Option<String>,
    artist: String,
    album: String,
    album_artist: String,
    isrc: Option<String>,
    duration_secs: u64,
    track_number: u32,
    disc_number: u32,
    release_date: String,
    bit_depth: u8,
    sample_rate_khz: f64,
    flac_bytes: Bytes,
    /// Optional explicit text search keys that match this catalog entry.
    search_aliases: Vec<String>,
}

struct MockQobuzState {
    catalog: Mutex<Vec<MockQobuzTrack>>,
    search_hits: AtomicUsize,
    get_file_url_hits: AtomicUsize,
    stream_hits: AtomicUsize,
    per_track_stream_hits: Mutex<HashMap<u64, usize>>,
    per_track_file_url_hits: Mutex<HashMap<u64, usize>>,
    /// If > 0, returns HTTP 429 on `/track/getFileUrl` this many times.
    rate_limit_file_url_remaining: AtomicUsize,
    rate_limit_retry_after_secs: u64,
    first_429_at: Mutex<Option<Instant>>,
    retry_after_429_at: Mutex<Option<Instant>>,
    /// Optional deterministic blocking gates for mid-run crash simulation.
    block_search_query_contains: Mutex<Option<String>>,
    block_stream_track_id: Mutex<Option<u64>>,
    search_blocked_notify: Notify,
    stream_blocked_notify: Notify,
    unblock_all: AtomicBool,
    unblock_notify: Notify,
}

impl MockQobuzState {
    fn new(catalog: Vec<MockQobuzTrack>) -> Self {
        Self {
            catalog: Mutex::new(catalog),
            search_hits: AtomicUsize::new(0),
            get_file_url_hits: AtomicUsize::new(0),
            stream_hits: AtomicUsize::new(0),
            per_track_stream_hits: Mutex::new(HashMap::new()),
            per_track_file_url_hits: Mutex::new(HashMap::new()),
            rate_limit_file_url_remaining: AtomicUsize::new(0),
            rate_limit_retry_after_secs: 2,
            first_429_at: Mutex::new(None),
            retry_after_429_at: Mutex::new(None),
            block_search_query_contains: Mutex::new(None),
            block_stream_track_id: Mutex::new(None),
            search_blocked_notify: Notify::new(),
            stream_blocked_notify: Notify::new(),
            unblock_all: AtomicBool::new(false),
            unblock_notify: Notify::new(),
        }
    }

    fn set_catalog(&self, catalog: Vec<MockQobuzTrack>) {
        *self.catalog.lock().unwrap() = catalog;
    }

    fn release_blocks(&self) {
        *self.block_search_query_contains.lock().unwrap() = None;
        *self.block_stream_track_id.lock().unwrap() = None;
        self.unblock_all.store(true, Ordering::SeqCst);
        self.unblock_notify.notify_waiters();
    }

    fn search_hits(&self) -> usize {
        self.search_hits.load(Ordering::SeqCst)
    }

    fn get_file_url_hits(&self) -> usize {
        self.get_file_url_hits.load(Ordering::SeqCst)
    }

    fn stream_hits(&self) -> usize {
        self.stream_hits.load(Ordering::SeqCst)
    }

    fn stream_hits_for(&self, qobuz_id: u64) -> usize {
        *self
            .per_track_stream_hits
            .lock()
            .unwrap()
            .get(&qobuz_id)
            .unwrap_or(&0)
    }

    fn file_url_hits_for(&self, qobuz_id: u64) -> usize {
        *self
            .per_track_file_url_hits
            .lock()
            .unwrap()
            .get(&qobuz_id)
            .unwrap_or(&0)
    }
}

struct MockQobuzServer {
    base_url: String,
    state: Arc<MockQobuzState>,
    shutdown_tx: watch::Sender<bool>,
}

impl MockQobuzServer {
    async fn start(catalog: Vec<MockQobuzTrack>) -> Self {
        let state = Arc::new(MockQobuzState::new(catalog));
        Self::start_with_state(state).await
    }

    async fn start_with_state(state: Arc<MockQobuzState>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock Qobuz server on loopback");
        let addr = listener.local_addr().expect("local addr");
        let base_url = format!("http://{addr}");
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let state_for_loop = Arc::clone(&state);
        let base_for_loop = base_url.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((socket, _)) = accepted else {
                            break;
                        };
                        let st = Arc::clone(&state_for_loop);
                        let base = base_for_loop.clone();
                        tokio::spawn(async move {
                            handle_connection(socket, st, base).await;
                        });
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });

        Self {
            base_url,
            state,
            shutdown_tx,
        }
    }

    fn base_url(&self) -> String {
        self.base_url.clone()
    }

    fn state(&self) -> Arc<MockQobuzState> {
        Arc::clone(&self.state)
    }
}

impl Drop for MockQobuzServer {
    fn drop(&mut self) {
        self.state.release_blocks();
        let _ = self.shutdown_tx.send(true);
    }
}

async fn handle_connection(mut socket: TcpStream, state: Arc<MockQobuzState>, base_url: String) {
    let mut read_buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];

    loop {
        // Read until end of HTTP headers ("\r\n\r\n").
        let header_end = loop {
            if let Some(pos) = find_header_end(&read_buf) {
                break pos;
            }
            let Ok(n) = socket.read(&mut chunk).await else {
                return;
            };
            if n == 0 {
                return;
            }
            read_buf.extend_from_slice(&chunk[..n]);
        };

        let header_bytes = read_buf[..header_end].to_vec();
        read_buf.drain(..header_end + 4);

        let header_text = String::from_utf8_lossy(&header_bytes);
        let mut lines = header_text.lines();
        let Some(request_line) = lines.next() else {
            return;
        };
        let mut parts = request_line.split_whitespace();
        let _method = parts.next().unwrap_or("GET");
        let target = parts.next().unwrap_or("/");

        let client_wants_close = lines.any(|line| {
            line.to_ascii_lowercase().starts_with("connection:")
                && line.to_ascii_lowercase().contains("close")
        });

        let (path, raw_query) = match target.split_once('?') {
            Some((p, q)) => (p, q),
            None => (target, ""),
        };
        let params = parse_query_params(raw_query);

        let (status_line, content_type, extra_headers, body_bytes): (
            &'static str,
            &'static str,
            String,
            Bytes,
        ) = match path {
            "/track/search" => {
                state.search_hits.fetch_add(1, Ordering::SeqCst);
                let q = params.get("query").map(String::as_str).unwrap_or("");

                // Optional blocking hook for deterministic mid-run crash test.
                let should_block = {
                    let needle = state.block_search_query_contains.lock().unwrap();
                    needle
                        .as_deref()
                        .is_some_and(|n| q.to_ascii_lowercase().contains(&n.to_ascii_lowercase()))
                };
                if should_block && !state.unblock_all.load(Ordering::SeqCst) {
                    state.search_blocked_notify.notify_waiters();
                    while !state.unblock_all.load(Ordering::SeqCst) {
                        state.unblock_notify.notified().await;
                    }
                }

                let matches = find_catalog_matches(&state, q);
                let items: Vec<serde_json::Value> = matches
                    .iter()
                    .map(|t| {
                        json!({
                            "id": t.qobuz_id,
                            "title": t.title,
                            "version": t.version,
                            "isrc": t.isrc,
                            "duration": t.duration_secs,
                            "track_number": t.track_number,
                            "media_number": t.disc_number,
                            "streamable": true,
                            "maximum_bit_depth": t.bit_depth,
                            "maximum_sampling_rate": t.sample_rate_khz,
                            "performer": { "name": t.artist },
                            "album": {
                                "title": t.album,
                                "artist": { "name": t.album_artist },
                                "release_date_original": t.release_date,
                                "image": {
                                    "large": format!("{base_url}/cover/{}.jpg", t.qobuz_id)
                                }
                            }
                        })
                    })
                    .collect();
                let payload = json!({ "tracks": { "items": items } }).to_string();
                (
                    "200 OK",
                    "application/json",
                    String::new(),
                    Bytes::from(payload),
                )
            }
            "/track/getFileUrl" => {
                state.get_file_url_hits.fetch_add(1, Ordering::SeqCst);
                let track_id: u64 = params
                    .get("track_id")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                *state
                    .per_track_file_url_hits
                    .lock()
                    .unwrap()
                    .entry(track_id)
                    .or_insert(0) += 1;

                // Optional forced HTTP 429 with Retry-After header.
                let trigger_429 = state
                    .rate_limit_file_url_remaining
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                        if v > 0 { Some(v - 1) } else { None }
                    })
                    .is_ok();

                if trigger_429 {
                    let mut first = state.first_429_at.lock().unwrap();
                    if first.is_none() {
                        *first = Some(Instant::now());
                    }
                    let extra = format!("Retry-After: {}\r\n", state.rate_limit_retry_after_secs);
                    (
                        "429 Too Many Requests",
                        "application/json",
                        extra,
                        Bytes::from_static(
                            br#"{"status":"error","message":"rate limit exceeded"}"#,
                        ),
                    )
                } else {
                    {
                        let first = state.first_429_at.lock().unwrap();
                        if first.is_some() {
                            let mut retry_at = state.retry_after_429_at.lock().unwrap();
                            if retry_at.is_none() {
                                *retry_at = Some(Instant::now());
                            }
                        }
                    }
                    let track = {
                        let cat = state.catalog.lock().unwrap();
                        cat.iter().find(|t| t.qobuz_id == track_id).cloned()
                    };
                    if let Some(t) = track {
                        let format_id: u32 = if t.bit_depth >= 24 && t.sample_rate_khz >= 176.4 {
                            27
                        } else if t.bit_depth >= 24 {
                            7
                        } else {
                            6
                        };
                        let stream_url = format!("{base_url}/stream/{track_id}.flac");
                        let payload = json!({
                            "track_id": track_id,
                            "url": stream_url,
                            "format_id": format_id,
                            "mime_type": "audio/flac",
                            "sampling_rate": t.sample_rate_khz,
                            "bit_depth": t.bit_depth,
                            "sample": false
                        })
                        .to_string();
                        (
                            "200 OK",
                            "application/json",
                            String::new(),
                            Bytes::from(payload),
                        )
                    } else {
                        (
                            "404 Not Found",
                            "application/json",
                            String::new(),
                            Bytes::from_static(br#"{"status":"error"}"#),
                        )
                    }
                }
            }
            p if p.starts_with("/stream/") && p.ends_with(".flac") => {
                let id_part = p.trim_start_matches("/stream/").trim_end_matches(".flac");
                let track_id: u64 = id_part.parse().unwrap_or(0);

                // Optional blocking hook for deterministic mid-run crash test.
                let should_block = *state.block_stream_track_id.lock().unwrap() == Some(track_id);
                if should_block && !state.unblock_all.load(Ordering::SeqCst) {
                    state.stream_blocked_notify.notify_waiters();
                    while !state.unblock_all.load(Ordering::SeqCst) {
                        state.unblock_notify.notified().await;
                    }
                }

                state.stream_hits.fetch_add(1, Ordering::SeqCst);
                *state
                    .per_track_stream_hits
                    .lock()
                    .unwrap()
                    .entry(track_id)
                    .or_insert(0) += 1;

                let flac = {
                    let cat = state.catalog.lock().unwrap();
                    cat.iter()
                        .find(|t| t.qobuz_id == track_id)
                        .map(|t| t.flac_bytes.clone())
                };
                if let Some(bytes) = flac {
                    ("200 OK", "audio/flac", String::new(), bytes)
                } else {
                    (
                        "404 Not Found",
                        "text/plain",
                        String::new(),
                        Bytes::from_static(b"not found"),
                    )
                }
            }
            _ => (
                "404 Not Found",
                "application/json",
                String::new(),
                Bytes::from_static(br#"{"status":"not found"}"#),
            ),
        };

        let conn_hdr = if client_wants_close {
            "close"
        } else {
            "keep-alive"
        };
        let resp_head = format!(
            "HTTP/1.1 {status_line}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: {conn_hdr}\r\n{extra_headers}\r\n",
            body_bytes.len()
        );
        if socket.write_all(resp_head.as_bytes()).await.is_err() {
            return;
        }
        if socket.write_all(&body_bytes).await.is_err() {
            return;
        }
        if client_wants_close {
            let _ = socket.shutdown().await;
            return;
        }
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn parse_query_params(raw: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in raw.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (percent_decode(k), percent_decode(v)),
            None => (percent_decode(pair), String::new()),
        };
        map.insert(k, v);
    }
    map
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3])
                    && let Ok(val) = u8::from_str_radix(hex, 16)
                {
                    out.push(val);
                    i += 3;
                    continue;
                }
                out.push(bytes[i]);
                i += 1;
            }
            other => {
                out.push(other);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn find_catalog_matches(state: &MockQobuzState, query: &str) -> Vec<MockQobuzTrack> {
    let q_norm = query.trim().to_ascii_lowercase();
    if q_norm.is_empty() {
        return Vec::new();
    }
    let cat = state.catalog.lock().unwrap();
    // 1. Exact ISRC match first.
    let isrc_hits: Vec<MockQobuzTrack> = cat
        .iter()
        .filter(|t| {
            t.isrc
                .as_deref()
                .is_some_and(|i| i.eq_ignore_ascii_case(&q_norm))
        })
        .cloned()
        .collect();
    if !isrc_hits.is_empty() {
        return isrc_hits;
    }

    // 2. Fallback text query match ("artist title" or title or explicit alias).
    cat.iter()
        .filter(|t| {
            let combined = format!("{} {}", t.artist, t.title).to_ascii_lowercase();
            let title_lower = t.title.to_ascii_lowercase();
            combined == q_norm
                || q_norm.contains(&title_lower)
                || t.search_aliases
                    .iter()
                    .any(|a| a.eq_ignore_ascii_case(&q_norm))
        })
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// Test helpers: FakePlaylistSource, LyricsEnrichingQobuzProvider, file scans
// ---------------------------------------------------------------------------

struct StaticPlaylistSource {
    playlists: Mutex<HashMap<String, Vec<PlaylistTrack>>>,
}

impl StaticPlaylistSource {
    fn new(playlist_id: &str, tracks: Vec<PlaylistTrack>) -> Self {
        let mut map = HashMap::new();
        map.insert(playlist_id.to_string(), tracks);
        Self {
            playlists: Mutex::new(map),
        }
    }
}

#[async_trait]
impl PlaylistSource for StaticPlaylistSource {
    async fn fetch_playlist_tracks(&self, playlist_id: &str) -> Result<Vec<PlaylistTrack>> {
        Ok(self
            .playlists
            .lock()
            .unwrap()
            .get(playlist_id)
            .cloned()
            .unwrap_or_default())
    }
}

/// Delegates all HTTP calls (`search`, `fetch`) to the real `QobuzProvider`
/// talking to our local mock Qobuz server, attaching optional synced `.lrc`
/// lyrics to matched candidates where supplied by the test fixture.
struct LyricsEnrichingQobuzProvider {
    inner: QobuzProvider,
    lyrics_by_qobuz_id: HashMap<String, String>,
}

impl LyricsEnrichingQobuzProvider {
    fn new(inner: QobuzProvider, lyrics_by_qobuz_id: HashMap<String, String>) -> Self {
        Self {
            inner,
            lyrics_by_qobuz_id,
        }
    }
}

#[async_trait]
impl Provider for LyricsEnrichingQobuzProvider {
    fn id(&self) -> &'static str {
        self.inner.id()
    }

    fn max_quality(&self) -> Quality {
        self.inner.max_quality()
    }

    async fn search(&self, q: &Query) -> Result<Vec<Candidate>> {
        let mut candidates = self.inner.search(q).await?;
        for cand in &mut candidates {
            if let Some(lrc) = self.lyrics_by_qobuz_id.get(&cand.track_id) {
                cand.lyrics_lrc = Some(lrc.clone());
            }
        }
        Ok(candidates)
    }

    async fn fetch(&self, c: &Candidate, q: Quality) -> Result<Bytes> {
        self.inner.fetch(c, q).await
    }
}

fn build_qobuz_provider(base_url: String) -> QobuzProvider {
    QobuzProvider::with_config(
        QobuzCredentials::new(
            "qa-app-id",
            "qa-user-token",
            Some("qa-app-secret".to_string()),
        ),
        QobuzConfig {
            base_url,
            search_limit: 20,
            max_quality: Quality::Flac24_192,
            timeout: Duration::from_secs(10),
        },
    )
    .expect("build QobuzProvider")
}

fn collect_files_by_ext(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(collect_files_by_ext(&path, ext));
            } else if path.extension().and_then(|e| e.to_str()) == Some(ext) {
                out.push(path);
            }
        }
    }
    out
}

struct TrackSpec<'a> {
    idx: u32,
    artist: &'a str,
    artists: Vec<String>,
    album: &'a str,
    title: &'a str,
    isrc: Option<&'a str>,
    disc: u32,
    year: u32,
    duration_ms: u64,
}

fn make_playlist_track(spec: TrackSpec<'_>) -> PlaylistTrack {
    PlaylistTrack {
        track_id: format!("sp_trk_{:03}", spec.idx),
        isrc: spec.isrc.map(ToOwned::to_owned),
        title: spec.title.to_string(),
        artists: spec.artists,
        album: spec.album.to_string(),
        album_artist: Some(spec.artist.to_string()),
        duration_ms: spec.duration_ms,
        track_number: Some(spec.idx),
        disc_number: Some(spec.disc),
        year: Some(spec.year),
        cover_url: None,
    }
}

// ---------------------------------------------------------------------------
// Checklist 1: 10-track public playlist end-to-end sync + FLAC integrity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn checklist_1_ten_track_playlist_end_to_end_sync_and_flac_integrity() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("sync.db");
    let out_dir = dir.path().join("library");

    let mut catalog = Vec::with_capacity(10);
    let mut playlist_tracks = Vec::with_capacity(10);
    let mut lyrics_map = HashMap::new();
    let mut expected_streams: HashMap<String, (u8, u32, u64, bool, Option<String>)> =
        HashMap::new();

    for idx in 1u32..=10 {
        let qobuz_id = 10_000 + u64::from(idx);
        let isrc = format!("USQA125000{idx:02}");
        let title = format!("Horizon Track {idx:02}");
        let primary_artist = if idx % 2 == 1 {
            "Daft Punk"
        } else {
            "Boards of Canada"
        };
        let artists = if idx == 1 {
            vec!["Daft Punk".to_string(), "Julian Casablancas".to_string()]
        } else {
            vec![primary_artist.to_string()]
        };
        let album = if idx <= 5 {
            "Discovery Sessions"
        } else {
            "Tomorrow Harvest"
        };
        let disc = if idx <= 5 { 1 } else { 2 };
        let year = 2015 + idx;
        let duration_secs = 200 + u64::from(idx) * 7;

        let (bit_depth, sample_rate_hz, sample_rate_khz) = match idx % 3 {
            0 => (24u8, 192_000u32, 192.0f64),
            1 => (24u8, 96_000u32, 96.0f64),
            _ => (16u8, 44_100u32, 44.1f64),
        };

        let is_jpeg = idx % 2 == 1;
        let (cover_bytes, cover_mime) = if is_jpeg {
            (SAMPLE_JPEG_COVER, "image/jpeg")
        } else {
            (SAMPLE_PNG_COVER, "image/png")
        };

        let flac_bytes =
            build_synthesized_pcm_flac(bit_depth, sample_rate_hz, Some((cover_bytes, cover_mime)));

        let lrc = if idx <= 5 {
            let text = format!("[00:01.00] Verified lyric line for track {idx:02}\n");
            lyrics_map.insert(qobuz_id.to_string(), text.clone());
            Some(text)
        } else {
            None
        };

        let track = make_playlist_track(TrackSpec {
            idx,
            artist: primary_artist,
            artists,
            album,
            title: &title,
            isrc: Some(&isrc),
            disc,
            year,
            duration_ms: duration_secs * 1000,
        });
        expected_streams.insert(
            track.track_id.clone(),
            (bit_depth, sample_rate_hz, qobuz_id, is_jpeg, lrc),
        );
        playlist_tracks.push(track);

        catalog.push(MockQobuzTrack {
            qobuz_id,
            title,
            version: None,
            artist: primary_artist.to_string(),
            album: album.to_string(),
            album_artist: primary_artist.to_string(),
            isrc: Some(isrc),
            duration_secs,
            track_number: idx,
            disc_number: disc,
            release_date: format!("{year}-05-17"),
            bit_depth,
            sample_rate_khz,
            flac_bytes,
            search_aliases: Vec::new(),
        });
    }

    let server = MockQobuzServer::start(catalog).await;
    let store = SyncStore::open(&db_path).expect("open sqlite store");
    store
        .set_playlist_enabled("pl_public_10", "Public 10-Track Playlist", true)
        .expect("enable playlist");

    let provider = Arc::new(LyricsEnrichingQobuzProvider::new(
        build_qobuz_provider(server.base_url()),
        lyrics_map,
    ));
    let queue = SyncQueue::new(
        store.clone(),
        vec![provider],
        QueueConfig {
            output_dir: out_dir.clone(),
            per_provider_concurrency: 3,
            per_provider_rps: 500.0,
            per_provider_burst: 50.0,
            ..QueueConfig::default()
        },
    )
    .expect("build queue");

    let source = Arc::new(StaticPlaylistSource::new(
        "pl_public_10",
        playlist_tracks.clone(),
    ));
    let watcher = PlaylistWatcher::new(
        store.clone(),
        source,
        queue.notify_handle(),
        WatcherConfig::default(),
    );

    let poll_report = watcher.poll_once().await.expect("poll_once succeeds");
    assert_eq!(poll_report.playlists_polled, 1);
    assert_eq!(poll_report.enqueued_tracks, 10);
    assert!(poll_report.errors.is_empty());

    let summary = queue.process_pending().await.expect("process_pending");
    assert_eq!(summary.processed, 10);
    assert_eq!(summary.done, 10);
    assert_eq!(summary.unmatched, 0);
    assert_eq!(summary.failed, 0);

    let stats = server.state();
    assert_eq!(stats.search_hits(), 10);
    assert_eq!(stats.get_file_url_hits(), 10);
    assert_eq!(stats.stream_hits(), 10);

    let rows = store
        .playlist_track_statuses("pl_public_10")
        .expect("list rows");
    assert_eq!(rows.len(), 10);

    for track in &playlist_tracks {
        let row = rows
            .iter()
            .find(|r| r.track_id == track.track_id)
            .expect("row exists");
        assert_eq!(row.status, SyncStatus::Done);
        assert_eq!(row.provider.as_deref(), Some("qobuz"));
        assert_eq!(row.matched_by.as_deref(), Some("isrc"));

        let flac_path = Path::new(row.path.as_deref().expect("written path"));
        assert!(
            flac_path.exists(),
            "FLAC file missing: {}",
            flac_path.display()
        );

        // Raw FLAC header, metadata blocks, and PCM audio frame CRC verification.
        verify_raw_flac_bitstream(flac_path);

        // Full lofty inspection: stream properties, all 10 Vorbis tags, and cover art.
        let mut file = fs::File::open(flac_path).expect("open written flac");
        let flac = FlacFile::read_from(&mut file, ParseOptions::new())
            .expect("lofty parses written FLAC cleanly");

        let (exp_bits, exp_rate, qobuz_id, is_jpeg, exp_lrc) = &expected_streams[&track.track_id];
        assert_eq!(flac.properties().bit_depth(), *exp_bits);
        assert_eq!(flac.properties().sample_rate(), *exp_rate);
        assert_eq!(flac.properties().channels(), 2);
        // The detected quality follows the file's own stream properties: 24/192
        // for the hi-res tracks, not just the rate the provider advertised.
        assert_eq!(
            row.quality.as_deref(),
            Some(Quality::from_stream_params(Some(*exp_bits), Some(*exp_rate)).as_str()),
            "{} must be detected from its STREAMINFO",
            track.title
        );
        if *exp_rate == 192_000 {
            assert_eq!(row.quality.as_deref(), Some("flac_24_192"));
        }

        let vorbis = flac
            .vorbis_comments()
            .expect("Vorbis comments must be present");
        assert_eq!(vorbis.get("TITLE"), Some(track.title.as_str()));
        let expected_artist = track.artists.join(", ");
        assert_eq!(vorbis.get("ARTIST"), Some(expected_artist.as_str()));
        assert_eq!(vorbis.get("ALBUM"), Some(track.album.as_str()));
        assert_eq!(vorbis.get("ALBUMARTIST"), track.album_artist.as_deref());
        assert_eq!(
            vorbis.get("TRACKNUMBER"),
            Some(track.track_number.unwrap().to_string().as_str())
        );
        assert_eq!(
            vorbis.get("DISCNUMBER"),
            Some(track.disc_number.unwrap().to_string().as_str())
        );
        assert_eq!(
            vorbis.get("DATE"),
            Some(track.year.unwrap().to_string().as_str())
        );
        assert_eq!(vorbis.get("ISRC"), track.isrc.as_deref());
        assert_eq!(vorbis.get("PROVIDER"), Some("qobuz"));
        let expected_source_url = format!("https://play.qobuz.com/track/{qobuz_id}");
        assert_eq!(vorbis.get("SOURCE_URL"), Some(expected_source_url.as_str()));

        assert_eq!(flac.pictures().len(), 1, "embedded cover art must exist");
        let (pic, _info) = &flac.pictures()[0];
        assert_eq!(pic.pic_type(), PictureType::CoverFront);
        if *is_jpeg {
            assert_eq!(pic.mime_type(), Some(&MimeType::Jpeg));
            assert_eq!(pic.data(), SAMPLE_JPEG_COVER);
        } else {
            assert_eq!(pic.mime_type(), Some(&MimeType::Png));
            assert_eq!(pic.data(), SAMPLE_PNG_COVER);
        }

        let lrc_path = flac_path.with_extension("lrc");
        match exp_lrc {
            Some(expected_lyrics) => {
                assert!(
                    lrc_path.exists(),
                    "expected .lrc sidecar for {}",
                    track.track_id
                );
                let actual_lrc = fs::read_to_string(&lrc_path).expect("read .lrc sidecar");
                assert_eq!(actual_lrc, expected_lyrics.trim());
            }
            None => {
                assert!(
                    !lrc_path.exists(),
                    "unexpected .lrc sidecar when no lyrics supplied for {}",
                    track.track_id
                );
            }
        }
    }

    // Also confirm write_flac_atomic embeds cover bytes directly from TrackMetadata (PNG & JPEG).
    let direct_dest = dir.path().join("direct_cover_check.flac");
    let direct_req = WriteRequest {
        destination: direct_dest.clone(),
        audio_bytes: build_synthesized_pcm_flac(24, 96_000, None),
        quality: Quality::Flac24_96,
        metadata: TrackMetadata {
            title: "Direct Cover Test".to_string(),
            artists: vec!["Cover Artist".to_string()],
            album: "Cover Album".to_string(),
            album_artist: Some("Cover Artist".to_string()),
            track_number: Some(1),
            disc_number: Some(1),
            year: Some(2025),
            isrc: Some("USQA99999999".to_string()),
            provider: "qobuz".to_string(),
            source_url: Some("https://play.qobuz.com/track/99999".to_string()),
            cover_bytes: Some(SAMPLE_PNG_COVER.to_vec()),
            cover_mime: Some("image/png".to_string()),
            lyrics_lrc: Some("[00:00.50] Direct lyric\n".to_string()),
        },
    };
    let direct_out = write_flac_atomic(&direct_req).expect("direct write succeeds");
    assert!(matches!(direct_out, WriteOutcome::Written { .. }));
    let mut direct_file = fs::File::open(&direct_dest).expect("open direct flac");
    let direct_flac = FlacFile::read_from(&mut direct_file, ParseOptions::new()).expect("parse");
    assert_eq!(direct_flac.pictures().len(), 1);
    assert_eq!(
        direct_flac.pictures()[0].0.mime_type(),
        Some(&MimeType::Png)
    );
    assert_eq!(direct_flac.pictures()[0].0.data(), SAMPLE_PNG_COVER);
    assert!(direct_dest.with_extension("lrc").exists());

    assert!(
        collect_files_by_ext(&out_dir, "tmp").is_empty(),
        "zero .tmp files may remain in library directory"
    );
}

// ---------------------------------------------------------------------------
// Checklist 2: Mid-run crash/kill and clean resume
// ---------------------------------------------------------------------------

#[tokio::test]
async fn checklist_2_mid_run_crash_kill_and_clean_resume() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("crash_resume.db");
    let out_dir = dir.path().join("library");

    let mut catalog = Vec::with_capacity(10);
    let mut tracks = Vec::with_capacity(10);
    let flac_cd = build_synthesized_pcm_flac(16, 44_100, Some((SAMPLE_JPEG_COVER, "image/jpeg")));

    for idx in 1u32..=10 {
        let qobuz_id = 20_000 + u64::from(idx);
        let isrc = format!("USQA225000{idx:02}");
        let title = format!("Resilient Song {idx:02}");
        tracks.push(make_playlist_track(TrackSpec {
            idx,
            artist: "Crash Test Band",
            artists: vec!["Crash Test Band".to_string()],
            album: "Atomic Resume",
            title: &title,
            isrc: Some(&isrc),
            disc: 1,
            year: 2024,
            duration_ms: 210_000,
        }));
        catalog.push(MockQobuzTrack {
            qobuz_id,
            title,
            version: None,
            artist: "Crash Test Band".to_string(),
            album: "Atomic Resume".to_string(),
            album_artist: "Crash Test Band".to_string(),
            isrc: Some(isrc),
            duration_secs: 210,
            track_number: idx,
            disc_number: 1,
            release_date: "2024-03-01".to_string(),
            bit_depth: 16,
            sample_rate_khz: 44.1,
            flac_bytes: flac_cd.clone(),
            search_aliases: Vec::new(),
        });
    }

    let state = Arc::new(MockQobuzState::new(catalog));
    // Deterministically hold track 5 in `resolving` (/track/search) and track 6 in
    // `downloading` (/stream/20006.flac) once tracks 1..=4 have completed.
    *state.block_search_query_contains.lock().unwrap() = Some("USQA22500005".to_string());
    *state.block_stream_track_id.lock().unwrap() = Some(20_006);

    let server = MockQobuzServer::start_with_state(Arc::clone(&state)).await;

    // Phase 1: Start syncing and abruptly abort mid-run after 4 done + 2 in-flight.
    {
        let store = SyncStore::open(&db_path).expect("open db");
        store
            .set_playlist_enabled("pl_crash", "Crash Playlist", true)
            .expect("enable");
        let provider = Arc::new(build_qobuz_provider(server.base_url()));
        let queue = SyncQueue::new(
            store.clone(),
            vec![provider],
            QueueConfig {
                output_dir: out_dir.clone(),
                per_provider_concurrency: 2,
                per_provider_rps: 500.0,
                per_provider_burst: 50.0,
                ..QueueConfig::default()
            },
        )
        .expect("build queue");

        let source = Arc::new(StaticPlaylistSource::new("pl_crash", tracks.clone()));
        let watcher = PlaylistWatcher::new(
            store.clone(),
            source,
            queue.notify_handle(),
            WatcherConfig::default(),
        );
        let polled = watcher.poll_once().await.expect("poll");
        assert_eq!(polled.enqueued_tracks, 10);

        let mut snap_rx = queue.subscribe();
        let queue_for_run = queue.clone();
        let run_handle = tokio::spawn(async move { queue_for_run.process_pending().await });

        // Wait until 4 tracks are done and 2 tracks are in-flight (1 resolving + 1 downloading).
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let snap = snap_rx.borrow().clone();
                if snap.done == 4 && snap.resolving == 1 && snap.downloading == 1 {
                    break;
                }
                if snap_rx.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
        .expect("queue should reach 4 done and 2 in-flight before simulated crash");

        // Simulate abrupt crash / kill mid-run.
        run_handle.abort();
        let join_err = run_handle.await.expect_err("task should be aborted");
        assert!(join_err.is_cancelled());
    }

    // Release the mock server gates now that the crashed process is gone.
    state.release_blocks();

    // Confirm the 4 completed tracks were downloaded exactly once before the crash.
    for qobuz_id in 20_001u64..=20_004 {
        assert_eq!(
            state.stream_hits_for(qobuz_id),
            1,
            "track {qobuz_id} should have completed before crash"
        );
    }

    // Phase 2: Simulate app restart by opening a fresh SyncStore from disk.
    let reopened_store = SyncStore::open(&db_path).expect("reopen sqlite store");
    let pre_counts = reopened_store
        .total_counts()
        .expect("counts before recover");
    assert_eq!(pre_counts.done, 4);
    assert_eq!(pre_counts.resolving + pre_counts.downloading, 2);
    assert_eq!(pre_counts.pending, 4);

    let recovered = reopened_store
        .recover_interrupted()
        .expect("recover_interrupted");
    assert_eq!(
        recovered, 2,
        "both in-flight tracks (resolving + downloading) must recover to pending"
    );

    let resumed_provider = Arc::new(build_qobuz_provider(server.base_url()));
    let resumed_queue = SyncQueue::new(
        reopened_store.clone(),
        vec![resumed_provider],
        QueueConfig {
            output_dir: out_dir.clone(),
            per_provider_concurrency: 2,
            per_provider_rps: 500.0,
            per_provider_burst: 50.0,
            ..QueueConfig::default()
        },
    )
    .expect("build resumed queue");

    let resume_summary = resumed_queue
        .process_pending()
        .await
        .expect("resumed process_pending");
    assert_eq!(resume_summary.recovered, 0, "already recovered explicitly");
    assert_eq!(
        resume_summary.processed, 6,
        "only the 6 unfinished tracks run"
    );
    assert_eq!(resume_summary.done, 6);
    assert_eq!(resume_summary.failed, 0);
    assert_eq!(resume_summary.unmatched, 0);

    // Confirm all 10 rows are done, zero duplicates, zero .tmp files, and
    // the 4 pre-crash completed tracks were NOT re-downloaded.
    let all_rows = reopened_store
        .playlist_track_statuses("pl_crash")
        .expect("all rows");
    assert_eq!(all_rows.len(), 10, "zero duplicate SQLite rows");
    let unique_ids: HashSet<&str> = all_rows.iter().map(|r| r.track_id.as_str()).collect();
    assert_eq!(unique_ids.len(), 10);
    assert!(all_rows.iter().all(|r| r.status == SyncStatus::Done));

    for qobuz_id in 20_001u64..=20_004 {
        assert_eq!(
            state.stream_hits_for(qobuz_id),
            1,
            "already-completed track {qobuz_id} must NOT be re-downloaded after resume"
        );
        assert_eq!(
            state.file_url_hits_for(qobuz_id),
            1,
            "already-completed track {qobuz_id} must NOT re-request file URL after resume"
        );
    }

    assert_eq!(collect_files_by_ext(&out_dir, "flac").len(), 10);
    assert!(
        collect_files_by_ext(&out_dir, "tmp").is_empty(),
        "zero leftover .tmp files allowed after crash and resume"
    );
    for row in &all_rows {
        let p = Path::new(row.path.as_deref().expect("path set"));
        verify_raw_flac_bitstream(p);
    }
}

// ---------------------------------------------------------------------------
// Checklist 3: Forced HTTP 429 backoff + visible cooldown
// ---------------------------------------------------------------------------

#[tokio::test]
async fn checklist_3_forced_http_429_backoff_and_visible_cooldown() {
    let dir = tempdir().expect("tempdir");
    let out_dir = dir.path().join("library");

    let flac_hires = build_synthesized_pcm_flac(24, 96_000, Some((SAMPLE_PNG_COVER, "image/png")));
    let track = make_playlist_track(TrackSpec {
        idx: 1,
        artist: "Rate Limit Quartet",
        artists: vec!["Rate Limit Quartet".to_string()],
        album: "Backoff Etudes",
        title: "Retry After Two",
        isrc: Some("USQA32500001"),
        disc: 1,
        year: 2024,
        duration_ms: 195_000,
    });

    let state = Arc::new(MockQobuzState::new(vec![MockQobuzTrack {
        qobuz_id: 30_001,
        title: "Retry After Two".to_string(),
        version: None,
        artist: "Rate Limit Quartet".to_string(),
        album: "Backoff Etudes".to_string(),
        album_artist: "Rate Limit Quartet".to_string(),
        isrc: Some("USQA32500001".to_string()),
        duration_secs: 195,
        track_number: 1,
        disc_number: 1,
        release_date: "2024-06-15".to_string(),
        bit_depth: 24,
        sample_rate_khz: 96.0,
        flac_bytes: flac_hires,
        search_aliases: Vec::new(),
    }]));
    state
        .rate_limit_file_url_remaining
        .store(1, Ordering::SeqCst);

    let server = MockQobuzServer::start_with_state(Arc::clone(&state)).await;
    let store = SyncStore::open_in_memory().expect("open store");
    store
        .enqueue_track("pl_429", &track.track_id, &track.to_query())
        .expect("enqueue");

    let provider = Arc::new(build_qobuz_provider(server.base_url()));
    let queue = SyncQueue::new(
        store.clone(),
        vec![provider],
        QueueConfig {
            output_dir: out_dir.clone(),
            per_provider_concurrency: 2,
            per_provider_rps: 200.0,
            per_provider_burst: 20.0,
            ..QueueConfig::default()
        },
    )
    .expect("build queue");

    let mut rx = queue.subscribe();
    let observed_cooldowns: Arc<Mutex<Vec<ProviderCooldown>>> = Arc::new(Mutex::new(Vec::new()));
    let observed_clone = Arc::clone(&observed_cooldowns);
    let observer = tokio::spawn(async move {
        while rx.changed().await.is_ok() {
            let snap = rx.borrow().clone();
            if let Some(cd) = snap.cooldowns.get("qobuz") {
                observed_clone.lock().unwrap().push(cd.clone());
            }
        }
    });

    let summary = queue.process_pending().await.expect("process_pending");
    let final_snapshot = queue.snapshot();
    drop(queue);
    let _ = observer.await;

    assert_eq!(summary.done, 1);
    assert_eq!(summary.failed, 0);
    assert_eq!(
        state.get_file_url_hits(),
        2,
        "first attempt gets 429, second attempt succeeds"
    );
    assert_eq!(state.stream_hits(), 1);

    // Verify visible ProviderCooldown surfaced in SyncSnapshot.
    let cooldowns = observed_cooldowns.lock().unwrap();
    assert!(
        !cooldowns.is_empty(),
        "SyncSnapshot must surface ProviderCooldown on HTTP 429"
    );
    assert!(
        cooldowns.iter().any(|cd| {
            let msg_lower = cd.message.to_ascii_lowercase();
            cd.provider == "qobuz"
                && cd.remaining_secs > 0
                && cd.retry_at_unix_secs > 0
                && (msg_lower.contains("429")
                    || msg_lower.contains("rate")
                    || msg_lower.contains("too many requests")
                    || msg_lower.contains("retry"))
        }),
        "expected visible cooldown with remaining_secs > 0 and rate-limit message, got: {cooldowns:?}"
    );

    // Verify queue actually waited for Retry-After (2 seconds) before retrying.
    let first_429 = state
        .first_429_at
        .lock()
        .unwrap()
        .expect("429 timestamp recorded");
    let retry_at = state
        .retry_after_429_at
        .lock()
        .unwrap()
        .expect("retry timestamp recorded");
    let waited = retry_at.saturating_duration_since(first_429);
    assert!(
        waited >= Duration::from_millis(1800),
        "queue must wait for Retry-After (2s), only waited {waited:?}"
    );

    // Cooldown clears after the retry succeeds.
    assert!(
        final_snapshot.cooldowns.is_empty(),
        "cooldown should clear once provider succeeds"
    );

    let row = store
        .get_item("pl_429", &track.track_id)
        .expect("get item")
        .expect("row present");
    assert_eq!(row.status, SyncStatus::Done);
    let path = Path::new(row.path.as_deref().expect("path"));
    verify_raw_flac_bitstream(path);
}

// ---------------------------------------------------------------------------
// Checklist 4: Negative matching rejects live and remix versions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn checklist_4_negative_matching_rejects_live_and_remix_versions() {
    let dir = tempdir().expect("tempdir");
    let out_dir = dir.path().join("library");
    let flac_cd = build_synthesized_pcm_flac(16, 44_100, Some((SAMPLE_JPEG_COVER, "image/jpeg")));

    // Playlist contains 3 studio tracks:
    // 1. "Neon Skyline" -> provider ONLY returns "(Live at Wembley)"
    // 2. "Starlight Drive" -> provider ONLY returns "(Club Remix)"
    // 3. "Sunset Boulevard" -> provider returns exact studio recording (+ decoy live/remix)
    let studio_live_only = make_playlist_track(TrackSpec {
        idx: 1,
        artist: "The Midnight",
        artists: vec!["The Midnight".to_string()],
        album: "Endless Summer",
        title: "Neon Skyline",
        isrc: Some("USQA40000001"),
        disc: 1,
        year: 2016,
        duration_ms: 240_000,
    });
    let studio_remix_only = make_playlist_track(TrackSpec {
        idx: 2,
        artist: "The Midnight",
        artists: vec!["The Midnight".to_string()],
        album: "Endless Summer",
        title: "Starlight Drive",
        isrc: Some("USQA40000002"),
        disc: 1,
        year: 2016,
        duration_ms: 215_000,
    });
    let studio_exact = make_playlist_track(TrackSpec {
        idx: 3,
        artist: "The Midnight",
        artists: vec!["The Midnight".to_string()],
        album: "Endless Summer",
        title: "Sunset Boulevard",
        isrc: None,
        disc: 1,
        year: 2016,
        duration_ms: 230_000,
    });

    let catalog = vec![
        // Only a Live recording exists for Neon Skyline (different ISRC, matching duration).
        MockQobuzTrack {
            qobuz_id: 40_001,
            title: "Neon Skyline".to_string(),
            version: Some("Live at Wembley".to_string()),
            artist: "The Midnight".to_string(),
            album: "Live at Wembley".to_string(),
            album_artist: "The Midnight".to_string(),
            isrc: Some("USQA49999001".to_string()),
            duration_secs: 240,
            track_number: 1,
            disc_number: 1,
            release_date: "2022-09-01".to_string(),
            bit_depth: 24,
            sample_rate_khz: 96.0,
            flac_bytes: flac_cd.clone(),
            search_aliases: vec!["the midnight neon skyline".to_string()],
        },
        // Only a Club Remix exists for Starlight Drive (different ISRC, matching duration).
        MockQobuzTrack {
            qobuz_id: 40_002,
            title: "Starlight Drive".to_string(),
            version: Some("Club Remix".to_string()),
            artist: "The Midnight".to_string(),
            album: "Remixes Vol. 1".to_string(),
            album_artist: "The Midnight".to_string(),
            isrc: Some("USQA49999002".to_string()),
            duration_secs: 215,
            track_number: 2,
            disc_number: 1,
            release_date: "2021-04-10".to_string(),
            bit_depth: 24,
            sample_rate_khz: 96.0,
            flac_bytes: flac_cd.clone(),
            search_aliases: vec!["the midnight starlight drive".to_string()],
        },
        // Decoy live recording for Sunset Boulevard (even higher resolution, must NOT be chosen).
        MockQobuzTrack {
            qobuz_id: 40_003,
            title: "Sunset Boulevard (Live at Wembley)".to_string(),
            version: None,
            artist: "The Midnight".to_string(),
            album: "Live at Wembley".to_string(),
            album_artist: "The Midnight".to_string(),
            isrc: Some("USQA49999003".to_string()),
            duration_secs: 230,
            track_number: 3,
            disc_number: 1,
            release_date: "2022-09-01".to_string(),
            bit_depth: 24,
            sample_rate_khz: 192.0,
            flac_bytes: flac_cd.clone(),
            search_aliases: vec!["the midnight sunset boulevard".to_string()],
        },
        // Exact studio recording for Sunset Boulevard.
        MockQobuzTrack {
            qobuz_id: 40_004,
            title: "Sunset Boulevard".to_string(),
            version: None,
            artist: "The Midnight".to_string(),
            album: "Endless Summer".to_string(),
            album_artist: "The Midnight".to_string(),
            isrc: Some("USQA40000003".to_string()),
            duration_secs: 230,
            track_number: 3,
            disc_number: 1,
            release_date: "2016-08-05".to_string(),
            bit_depth: 16,
            sample_rate_khz: 44.1,
            flac_bytes: flac_cd,
            search_aliases: vec!["the midnight sunset boulevard".to_string()],
        },
    ];

    let server = MockQobuzServer::start(catalog).await;
    let store = SyncStore::open_in_memory().expect("open store");
    store
        .set_playlist_enabled("pl_neg", "Negative Matching", true)
        .expect("enable");

    let provider = Arc::new(build_qobuz_provider(server.base_url()));
    let queue = SyncQueue::new(
        store.clone(),
        vec![provider],
        QueueConfig {
            output_dir: out_dir.clone(),
            per_provider_concurrency: 2,
            per_provider_rps: 500.0,
            per_provider_burst: 50.0,
            ..QueueConfig::default()
        },
    )
    .expect("build queue");

    let source = Arc::new(StaticPlaylistSource::new(
        "pl_neg",
        vec![
            studio_live_only.clone(),
            studio_remix_only.clone(),
            studio_exact.clone(),
        ],
    ));
    let watcher = PlaylistWatcher::new(
        store.clone(),
        source,
        queue.notify_handle(),
        WatcherConfig::default(),
    );

    let report = watcher.poll_once().await.expect("poll");
    assert_eq!(report.enqueued_tracks, 3);

    let summary = queue.process_pending().await.expect("process");
    assert_eq!(summary.processed, 3);
    assert_eq!(summary.done, 1, "only the studio match may succeed");
    assert_eq!(
        summary.unmatched, 2,
        "live-only and remix-only tracks must be unmatched"
    );
    assert_eq!(summary.failed, 0);

    let live_row = store
        .get_item("pl_neg", &studio_live_only.track_id)
        .unwrap()
        .unwrap();
    assert_eq!(live_row.status, SyncStatus::Unmatched);
    assert!(live_row.path.is_none());

    let remix_row = store
        .get_item("pl_neg", &studio_remix_only.track_id)
        .unwrap()
        .unwrap();
    assert_eq!(remix_row.status, SyncStatus::Unmatched);
    assert!(remix_row.path.is_none());

    let studio_row = store
        .get_item("pl_neg", &studio_exact.track_id)
        .unwrap()
        .unwrap();
    assert_eq!(studio_row.status, SyncStatus::Done);
    assert_eq!(studio_row.matched_by.as_deref(), Some("exact"));
    let written_path = Path::new(studio_row.path.as_deref().expect("studio file written"));
    assert!(written_path.exists());

    // Confirm mock server only served audio for the legitimate studio track (40_004).
    let stats = server.state();
    assert_eq!(stats.get_file_url_hits(), 1);
    assert_eq!(stats.stream_hits(), 1);
    assert_eq!(stats.stream_hits_for(40_004), 1);
    assert_eq!(stats.stream_hits_for(40_001), 0);
    assert_eq!(stats.stream_hits_for(40_002), 0);
    assert_eq!(stats.stream_hits_for(40_003), 0);

    let all_flacs = collect_files_by_ext(&out_dir, "flac");
    assert_eq!(
        all_flacs.len(),
        1,
        "unmatched tracks must never write to disk"
    );
    assert!(collect_files_by_ext(&out_dir, "tmp").is_empty());
}

// ---------------------------------------------------------------------------
// Checklist 5: Re-run completed sync (zero re-downloads at equal quality) +
//              atomic strict quality upgrade (16/44.1 -> 24/96)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn checklist_5_rerun_zero_redownloads_at_equal_quality_and_atomic_upgrade() {
    let dir = tempdir().expect("tempdir");
    let out_dir = dir.path().join("library");

    let flac_16_44 =
        build_synthesized_pcm_flac(16, 44_100, Some((SAMPLE_JPEG_COVER, "image/jpeg")));
    let flac_24_96 = build_synthesized_pcm_flac(24, 96_000, Some((SAMPLE_PNG_COVER, "image/png")));

    let build_catalog = |bit_depth: u8, sr_khz: f64, bytes: &Bytes| -> Vec<MockQobuzTrack> {
        (1u32..=3)
            .map(|idx| MockQobuzTrack {
                qobuz_id: 50_000 + u64::from(idx),
                title: format!("Quality Check {idx:02}"),
                version: None,
                artist: "Audio Precision".to_string(),
                album: "Master Suite".to_string(),
                album_artist: "Audio Precision".to_string(),
                isrc: Some(format!("USQA525000{idx:02}")),
                duration_secs: 180,
                track_number: idx,
                disc_number: 1,
                release_date: "2023-11-11".to_string(),
                bit_depth,
                sample_rate_khz: sr_khz,
                flac_bytes: bytes.clone(),
                search_aliases: Vec::new(),
            })
            .collect()
    };

    let tracks: Vec<PlaylistTrack> = (1u32..=3)
        .map(|idx| {
            make_playlist_track(TrackSpec {
                idx,
                artist: "Audio Precision",
                artists: vec!["Audio Precision".to_string()],
                album: "Master Suite",
                title: &format!("Quality Check {idx:02}"),
                isrc: Some(&format!("USQA525000{idx:02}")),
                disc: 1,
                year: 2023,
                duration_ms: 180_000,
            })
        })
        .collect();

    let server = MockQobuzServer::start(build_catalog(16, 44.1, &flac_16_44)).await;
    let stats = server.state();

    let store = SyncStore::open_in_memory().expect("open store");
    store
        .set_playlist_enabled("pl_qual", "Quality Guard", true)
        .expect("enable");

    let provider = Arc::new(build_qobuz_provider(server.base_url()));
    let queue = SyncQueue::new(
        store.clone(),
        vec![provider],
        QueueConfig {
            output_dir: out_dir.clone(),
            per_provider_concurrency: 2,
            per_provider_rps: 500.0,
            per_provider_burst: 50.0,
            ..QueueConfig::default()
        },
    )
    .expect("queue");

    let source = Arc::new(StaticPlaylistSource::new("pl_qual", tracks.clone()));
    let watcher = PlaylistWatcher::new(
        store.clone(),
        source,
        queue.notify_handle(),
        WatcherConfig::default(),
    );

    // Step 1: Initial 16-bit/44.1 kHz sync.
    assert_eq!(watcher.poll_once().await.unwrap().enqueued_tracks, 3);
    let initial = queue.process_pending().await.unwrap();
    assert_eq!(initial.done, 3);
    assert_eq!(stats.get_file_url_hits(), 3);
    assert_eq!(stats.stream_hits(), 3);

    // Record exact mtime and file contents of all 3 written 16/44.1 FLAC files.
    let mut baseline: Vec<(String, PathBuf, std::time::SystemTime, Vec<u8>)> = Vec::new();
    for track in &tracks {
        let row = store.get_item("pl_qual", &track.track_id).unwrap().unwrap();
        assert_eq!(row.quality.as_deref(), Some("flac_16_44"));
        let path = PathBuf::from(row.path.unwrap());
        let mtime = fs::metadata(&path).unwrap().modified().unwrap();
        let bytes = fs::read(&path).unwrap();
        baseline.push((track.track_id.clone(), path, mtime, bytes));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Step 2: Re-poll unchanged playlist and re-run process_pending -> zero work, zero downloads.
    assert_eq!(watcher.poll_once().await.unwrap().enqueued_tracks, 0);
    let repoll_run = queue.process_pending().await.unwrap();
    assert_eq!(repoll_run.processed, 0);
    assert_eq!(stats.get_file_url_hits(), 3);
    assert_eq!(stats.stream_hits(), 3);

    // Step 3: Force retry at EQUAL quality (16/44.1) -> zero download endpoint requests,
    // identical mtime and content preserved.
    for track in &tracks {
        assert!(store.retry_item("pl_qual", &track.track_id).unwrap());
    }
    let equal_rerun = queue.process_pending().await.unwrap();
    assert_eq!(equal_rerun.processed, 3);
    assert_eq!(equal_rerun.done, 3);
    assert_eq!(
        stats.get_file_url_hits(),
        3,
        "equal quality retry must issue ZERO getFileUrl requests"
    );
    assert_eq!(
        stats.stream_hits(),
        3,
        "equal quality retry must issue ZERO stream download requests"
    );

    for (track_id, path, orig_mtime, orig_bytes) in &baseline {
        let new_mtime = fs::metadata(path).unwrap().modified().unwrap();
        let new_bytes = fs::read(path).unwrap();
        assert_eq!(
            new_mtime, *orig_mtime,
            "file mtime for {track_id} must not change on equal-quality re-run"
        );
        assert_eq!(
            new_bytes, *orig_bytes,
            "file bytes for {track_id} must not change on equal-quality re-run"
        );
    }

    // Step 4: Upgrade catalog to 24-bit/96 kHz and retry -> strict upgrade succeeds atomically.
    stats.set_catalog(build_catalog(24, 96.0, &flac_24_96));
    for track in &tracks {
        assert!(store.retry_item("pl_qual", &track.track_id).unwrap());
    }
    let upgrade_run = queue.process_pending().await.unwrap();
    assert_eq!(upgrade_run.processed, 3);
    assert_eq!(upgrade_run.done, 3);
    assert_eq!(stats.get_file_url_hits(), 6);
    assert_eq!(stats.stream_hits(), 6);

    for (track_id, path, _orig_mtime, orig_bytes) in &baseline {
        let row = store.get_item("pl_qual", track_id).unwrap().unwrap();
        assert_eq!(row.status, SyncStatus::Done);
        assert_eq!(row.quality.as_deref(), Some("flac_24_96"));

        verify_raw_flac_bitstream(path);
        let upgraded_bytes = fs::read(path).unwrap();
        assert_ne!(upgraded_bytes, *orig_bytes, "file should now be 24/96");

        let mut file = fs::File::open(path).unwrap();
        let flac = FlacFile::read_from(&mut file, ParseOptions::new()).unwrap();
        assert_eq!(flac.properties().bit_depth(), 24);
        assert_eq!(flac.properties().sample_rate(), 96_000);
        let vorbis = flac
            .vorbis_comments()
            .expect("vorbis comments after upgrade");
        assert_eq!(vorbis.get("PROVIDER"), Some("qobuz"));
        assert_eq!(flac.pictures().len(), 1);
        assert_eq!(flac.pictures()[0].0.mime_type(), Some(&MimeType::Png));
    }

    // Step 5: Downgrade catalog back to 16/44.1 and retry -> NEVER downgrades.
    stats.set_catalog(build_catalog(16, 44.1, &flac_16_44));
    for track in &tracks {
        assert!(store.retry_item("pl_qual", &track.track_id).unwrap());
    }
    let downgrade_attempt = queue.process_pending().await.unwrap();
    assert_eq!(downgrade_attempt.done, 3);
    assert_eq!(
        stats.stream_hits(),
        6,
        "downgrade attempt must issue ZERO stream downloads"
    );
    for (track_id, path, _, _) in &baseline {
        let row = store.get_item("pl_qual", track_id).unwrap().unwrap();
        assert_eq!(row.quality.as_deref(), Some("flac_24_96"));
        let mut file = fs::File::open(path).unwrap();
        let flac = FlacFile::read_from(&mut file, ParseOptions::new()).unwrap();
        assert_eq!(flac.properties().bit_depth(), 24);
        assert_eq!(flac.properties().sample_rate(), 96_000);
    }

    assert!(
        collect_files_by_ext(&out_dir, "tmp").is_empty(),
        "zero .tmp files may remain after quality upgrades"
    );
}

// ---------------------------------------------------------------------------
// Checklist 6: 200-track stress/throughput + non-blocking < 1 ms snapshots
// ---------------------------------------------------------------------------

#[tokio::test]
async fn checklist_6_two_hundred_track_stress_and_non_blocking_snapshots() {
    let dir = tempdir().expect("tempdir");
    let out_dir = dir.path().join("library");
    let shared_flac =
        build_synthesized_pcm_flac(16, 44_100, Some((SAMPLE_JPEG_COVER, "image/jpeg")));

    let mut catalog = Vec::with_capacity(200);
    let store = SyncStore::open_in_memory().expect("open store");
    store
        .set_playlist_enabled("pl_stress_200", "Stress 200", true)
        .expect("enable");

    for idx in 1u32..=200 {
        let qobuz_id = 60_000 + u64::from(idx);
        let isrc = format!("USQA62500{idx:03}");
        let album_num = (idx - 1) / 20 + 1;
        let album = format!("Throughput Vol {album_num:02}");
        let title = format!("Stress Track {idx:03}");
        let track = make_playlist_track(TrackSpec {
            idx,
            artist: "Throughput Collective",
            artists: vec!["Throughput Collective".to_string()],
            album: &album,
            title: &title,
            isrc: Some(&isrc),
            disc: 1,
            year: 2025,
            duration_ms: 190_000,
        });
        store
            .enqueue_track("pl_stress_200", &track.track_id, &track.to_query())
            .expect("enqueue");

        catalog.push(MockQobuzTrack {
            qobuz_id,
            title,
            version: None,
            artist: "Throughput Collective".to_string(),
            album,
            album_artist: "Throughput Collective".to_string(),
            isrc: Some(isrc),
            duration_secs: 190,
            track_number: idx,
            disc_number: 1,
            release_date: "2025-01-01".to_string(),
            bit_depth: 16,
            sample_rate_khz: 44.1,
            flac_bytes: shared_flac.clone(),
            search_aliases: Vec::new(),
        });
    }

    let server = MockQobuzServer::start(catalog).await;
    let provider = Arc::new(build_qobuz_provider(server.base_url()));
    let queue = SyncQueue::new(
        store.clone(),
        vec![provider],
        QueueConfig {
            output_dir: out_dir.clone(),
            per_provider_concurrency: 4,
            per_provider_rps: 10_000.0,
            per_provider_burst: 200.0,
            ..QueueConfig::default()
        },
    )
    .expect("build stress queue");

    let finished = Arc::new(AtomicBool::new(false));
    let finished_flag = Arc::clone(&finished);
    let queue_for_poll = queue.clone();
    // Keep an active watch subscriber (as the UI does) so watch::Sender::send updates
    // the published SyncSnapshot while polling queue.snapshot() on every simulated frame.
    let snap_rx = queue.subscribe();

    // Simulate a 60 fps UI thread polling `queue.snapshot()` continuously while
    // 200 tracks sync concurrently at concurrency 4.
    let ui_poller = std::thread::spawn(move || -> (usize, Duration, SyncSnapshot) {
        let mut polls = 0usize;
        let mut max_borrow = Duration::ZERO;
        while !finished_flag.load(Ordering::Acquire) {
            let start = Instant::now();
            let snap = queue_for_poll.snapshot();
            let rx_done = snap_rx.borrow().done;
            let elapsed = start.elapsed();
            assert!(snap.done <= 200);
            assert!(rx_done <= 200);
            if elapsed > max_borrow {
                max_borrow = elapsed;
            }
            polls += 1;
            std::thread::yield_now();
        }
        // Final poll after completion.
        let start = Instant::now();
        let snap = queue_for_poll.snapshot();
        let elapsed = start.elapsed();
        if elapsed > max_borrow {
            max_borrow = elapsed;
        }
        polls += 1;
        drop(snap_rx);
        (polls, max_borrow, snap)
    });

    let summary = queue.process_pending().await.expect("200-track stress run");
    finished.store(true, Ordering::Release);

    let (poll_count, max_borrow_latency, final_snap) =
        ui_poller.join().expect("ui poller thread joins cleanly");

    assert_eq!(summary.processed, 200);
    assert_eq!(summary.done, 200);
    assert_eq!(summary.unmatched, 0);
    assert_eq!(summary.failed, 0);

    assert!(
        poll_count >= 10,
        "expected continuous UI frame polling during stress run, only got {poll_count} polls"
    );
    assert!(
        max_borrow_latency < Duration::from_millis(1),
        "every queue.snapshot() borrow must complete in < 1 ms, max was {max_borrow_latency:?} across {poll_count} polls"
    );

    assert_eq!(final_snap.done, 200);
    assert_eq!(final_snap.pending, 0);
    assert_eq!(final_snap.resolving, 0);
    assert_eq!(final_snap.downloading, 0);
    assert_eq!(final_snap.failed, 0);
    assert_eq!(final_snap.unmatched, 0);
    assert!(final_snap.active_tracks.is_empty());

    let stats = server.state();
    assert_eq!(stats.stream_hits(), 200);
    assert_eq!(collect_files_by_ext(&out_dir, "flac").len(), 200);
    assert!(collect_files_by_ext(&out_dir, "tmp").is_empty());

    let rows = store.playlist_track_statuses("pl_stress_200").unwrap();
    assert_eq!(rows.len(), 200);
    assert!(rows.iter().all(|r| r.status == SyncStatus::Done));
}
