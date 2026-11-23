//! Cross-crate end-to-end verification: sync a 10-track playlist through
//! `spotisync` (`QobuzProvider` + `SyncStore` + `SyncQueue` + atomic `lofty`
//! FLAC writer) against an in-process local mock HTTP server, then confirm
//! every written FLAC file probes, decodes, and runs cleanly through
//! Spotifast's own `local_player` (`symphonia` + EQ + `AudioTap`, with the
//! output limiter after volume in the render stage).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::json;
use spotifast::eq::SharedEq;
use spotifast::local_player::{AudioPipeline, decode_flac_probe, decode_flac_samples, volume_gain};
use spotifast::vis::AudioTap;
use spotisync::providers::qobuz::{QobuzConfig, QobuzCredentials, QobuzProvider};
use spotisync::{
    MatchConfig, Provider, Quality, Query, QueueConfig, SyncQueue, SyncStatus, SyncStore,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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

/// Builds a valid RFC-9639 stereo CONSTANT non-zero PCM FLAC stream of 192
/// samples with valid CRC-8 header and CRC-16 footer so `symphonia` decodes
/// non-silent audio.
fn build_nonzero_pcm_flac(bit_depth: u8, sample_rate_hz: u32) -> Vec<u8> {
    let sr_code: u8 = match sample_rate_hz {
        44_100 => 0x09,
        48_000 => 0x0A,
        96_000 => 0x0B,
        // 0b0011 is 192 kHz; 0b1100 would mean "8-bit kHz follows the header".
        192_000 => 0x03,
        _ => 0x00,
    };
    let bps_code: u8 = match bit_depth {
        16 => 0x04,
        24 => 0x06,
        _ => 0x00,
    };

    let mut frame = Vec::with_capacity(32);
    frame.push(0xFF);
    frame.push(0xF8);
    frame.push((0x01 << 4) | sr_code); // 192 samples + sample rate
    frame.push((0x01 << 4) | (bps_code << 1)); // stereo independent + bps
    frame.push(0x00); // frame 0
    frame.push(flac_crc8(&frame));

    // Non-zero constant PCM sample in both channels so decoded amplitude > 0.
    let sample_val: Vec<u8> = match bit_depth {
        24 => vec![0x10, 0x00, 0x00],
        _ => vec![0x10, 0x00],
    };
    // Left subframe: CONSTANT (0x00)
    frame.push(0x00);
    frame.extend_from_slice(&sample_val);
    // Right subframe: CONSTANT (0x00)
    frame.push(0x00);
    frame.extend_from_slice(&sample_val);

    let crc16 = flac_crc16(&frame);
    frame.extend_from_slice(&crc16.to_be_bytes());

    let frame_len = frame.len() as u32;
    let mut buf = Vec::with_capacity(64 + frame.len());
    buf.extend_from_slice(b"fLaC");
    // STREAMINFO (type 0, is_last = 1, len = 34)
    buf.extend_from_slice(&[0x80, 0x00, 0x00, 0x22]);
    buf.extend_from_slice(&192u16.to_be_bytes());
    buf.extend_from_slice(&192u16.to_be_bytes());
    buf.extend_from_slice(&[
        ((frame_len >> 16) & 0xFF) as u8,
        ((frame_len >> 8) & 0xFF) as u8,
        (frame_len & 0xFF) as u8,
    ]);
    buf.extend_from_slice(&[
        ((frame_len >> 16) & 0xFF) as u8,
        ((frame_len >> 8) & 0xFF) as u8,
        (frame_len & 0xFF) as u8,
    ]);
    let sr = u64::from(sample_rate_hz) & 0xF_FFFF;
    let channels_minus_1: u64 = 1;
    let bps_minus_1 = u64::from(bit_depth.saturating_sub(1)) & 0x1F;
    let total_samples: u64 = 192;
    let packed: u64 = (sr << 44) | (channels_minus_1 << 41) | (bps_minus_1 << 36) | total_samples;
    buf.extend_from_slice(&packed.to_be_bytes());
    buf.extend_from_slice(&[0u8; 16]); // unset MD5
    buf.extend_from_slice(&frame);
    buf
}

#[derive(Clone)]
struct MockTrack {
    qobuz_id: u64,
    isrc: String,
    title: String,
    artist: String,
    album: String,
    duration_secs: u64,
    bit_depth: u8,
    sample_rate_hz: u32,
    flac_bytes: Vec<u8>,
}

async fn handle_conn(
    mut stream: TcpStream,
    base_url: String,
    tracks: Arc<HashMap<u64, MockTrack>>,
) {
    let mut req_buf = vec![0u8; 8192];
    let Ok(n) = stream.read(&mut req_buf).await else {
        return;
    };
    if n == 0 {
        return;
    }
    let req = String::from_utf8_lossy(&req_buf[..n]);
    let target = req
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    let (status, content_type, body): (&str, &str, Vec<u8>) = if target.starts_with("/track/search")
    {
        let query = target
            .split('?')
            .nth(1)
            .and_then(|qs| {
                qs.split('&')
                    .find_map(|p| p.strip_prefix("query="))
                    .map(|v| urlencoding::decode(v).unwrap_or_default().into_owned())
            })
            .unwrap_or_default();
        let items: Vec<_> = tracks
            .values()
            .filter(|t| {
                t.isrc.eq_ignore_ascii_case(&query)
                    || query.to_lowercase().contains(&t.title.to_lowercase())
            })
            .map(|t| {
                json!({
                    "id": t.qobuz_id,
                    "title": t.title,
                    "isrc": t.isrc,
                    "duration": t.duration_secs,
                    "track_number": 1,
                    "media_number": 1,
                    "maximum_bit_depth": t.bit_depth,
                    "maximum_sampling_rate": f64::from(t.sample_rate_hz) / 1000.0,
                    "streamable": true,
                    "hires_streamable": t.bit_depth >= 24,
                    "performer": { "name": t.artist },
                    "album": {
                        "title": t.album,
                        "artist": { "name": t.artist },
                        "release_date_original": "2024-05-01"
                    }
                })
            })
            .collect();
        let payload = json!({ "tracks": { "items": items } });
        (
            "200 OK",
            "application/json",
            serde_json::to_vec(&payload).unwrap(),
        )
    } else if target.starts_with("/track/getFileUrl") {
        let track_id = target
            .split('?')
            .nth(1)
            .and_then(|qs| {
                qs.split('&')
                    .find_map(|p| p.strip_prefix("track_id="))
                    .and_then(|v| v.parse::<u64>().ok())
            })
            .unwrap_or(0);
        if let Some(t) = tracks.get(&track_id) {
            let format_id = if t.bit_depth >= 24 { 7 } else { 6 };
            let payload = json!({
                "url": format!("{base_url}/audio/{track_id}.flac"),
                "format_id": format_id,
                "mime_type": "audio/flac",
                "bit_depth": t.bit_depth,
                "sampling_rate": f64::from(t.sample_rate_hz) / 1000.0,
                "sample": false
            });
            (
                "200 OK",
                "application/json",
                serde_json::to_vec(&payload).unwrap(),
            )
        } else {
            ("404 Not Found", "application/json", b"{}".to_vec())
        }
    } else if let Some(rest) = target.strip_prefix("/audio/") {
        let track_id = rest
            .strip_suffix(".flac")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        if let Some(t) = tracks.get(&track_id) {
            ("200 OK", "audio/flac", t.flac_bytes.clone())
        } else {
            ("404 Not Found", "text/plain", b"missing".to_vec())
        }
    } else {
        ("404 Not Found", "text/plain", b"not found".to_vec())
    };

    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes()).await;
    let _ = stream.write_all(&body).await;
    let _ = stream.shutdown().await;
}

#[tokio::test]
async fn synced_ten_track_playlist_plays_through_spotifast_local_player() {
    let uniq = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("spotifast-e2e-sync-playback-{uniq}"));
    let lib_dir = root.join("sync");
    let db_path = root.join("sync.db");
    std::fs::create_dir_all(&lib_dir).unwrap();

    let flac_16_44 = build_nonzero_pcm_flac(16, 44_100);
    let flac_24_96 = build_nonzero_pcm_flac(24, 96_000);

    let mut mock_tracks = HashMap::new();
    let mut playlist_tracks: Vec<(String, Query)> = Vec::new();
    for i in 1..=10u64 {
        let is_hires = i % 2 == 0;
        let bit_depth = if is_hires { 24 } else { 16 };
        let sample_rate_hz = if is_hires { 96_000 } else { 44_100 };
        let qobuz_id = 9000 + i;
        let isrc = format!("USEND26{i:05}");
        let title = format!("Lossless Track {i:02}");
        let artist = format!("Artist {i:02}");
        let album = "Verification Album".to_owned();
        mock_tracks.insert(
            qobuz_id,
            MockTrack {
                qobuz_id,
                isrc: isrc.clone(),
                title: title.clone(),
                artist: artist.clone(),
                album: album.clone(),
                duration_secs: 200 + i,
                bit_depth,
                sample_rate_hz,
                flac_bytes: if is_hires {
                    flac_24_96.clone()
                } else {
                    flac_16_44.clone()
                },
            },
        );
        playlist_tracks.push((
            format!("sp-track-{i:02}"),
            Query {
                isrc: Some(isrc),
                title,
                artists: vec![artist.clone()],
                album,
                album_artist: Some(artist),
                duration_ms: (200 + i) * 1_000,
                track_number: Some(i as u32),
                disc_number: Some(1),
                year: Some(2024),
                cover_url: None,
            },
        ));
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let tracks_arc = Arc::new(mock_tracks);
    let server_base = base_url.clone();
    let server_tracks = Arc::clone(&tracks_arc);
    let server = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let b = server_base.clone();
            let t = Arc::clone(&server_tracks);
            tokio::spawn(async move {
                handle_conn(stream, b, t).await;
            });
        }
    });

    let store = SyncStore::open(&db_path).unwrap();
    store
        .set_playlist_enabled("pl-public-10", "Public 10-Track Playlist", true)
        .unwrap();
    for (track_id, query) in &playlist_tracks {
        assert!(
            store
                .enqueue_track("pl-public-10", track_id, query)
                .unwrap()
        );
    }

    let provider = QobuzProvider::with_config(
        QobuzCredentials::new("test-app-id", "test-user-token", None),
        QobuzConfig {
            base_url: base_url.clone(),
            ..QobuzConfig::default()
        },
    )
    .unwrap();
    let providers: Vec<Arc<dyn Provider>> = vec![Arc::new(provider)];
    let queue = SyncQueue::new(
        store.clone(),
        providers,
        QueueConfig {
            output_dir: lib_dir.clone(),
            per_provider_concurrency: 2,
            per_provider_rps: 50.0,
            per_provider_burst: 50.0,
            match_config: MatchConfig {
                max_quality: Quality::Flac24_96,
                ..MatchConfig::default()
            },
            ..QueueConfig::default()
        },
    )
    .unwrap();

    let summary = queue.process_pending().await.unwrap();
    assert_eq!(summary.done, 10, "expected all 10 tracks to sync cleanly");
    assert_eq!(summary.failed, 0);
    assert_eq!(summary.unmatched, 0);

    let completed = store.list_completed_tracks().unwrap();
    assert_eq!(completed.len(), 10);

    // Verify every synced & lofty-tagged FLAC file plays cleanly through
    // Spotifast's own local_player pipeline (symphonia -> EQ -> AudioTap).
    for (idx, item) in completed.iter().enumerate() {
        assert_eq!(item.status, SyncStatus::Done);
        let path_str = item
            .path
            .as_deref()
            .expect("completed track must have path");
        let path = std::path::Path::new(path_str);
        assert!(path.is_file(), "synced FLAC missing at {}", path.display());

        let info = decode_flac_probe(path)
            .unwrap_or_else(|err| panic!("local_player failed to probe {}: {err}", path.display()));
        assert_eq!(info.channels, 2);
        assert!(info.sample_rate_hz == 44_100 || info.sample_rate_hz == 96_000);
        assert!(info.bit_depth == Some(16) || info.bit_depth == Some(24));
        assert_eq!(info.total_frames, Some(192));

        let (sr, channels, samples) = decode_flac_samples(path).unwrap_or_else(|err| {
            panic!("local_player failed to decode {}: {err}", path.display())
        });
        assert_eq!(sr, info.sample_rate_hz);
        assert_eq!(channels, 2);
        assert_eq!(samples.len(), 192 * 2);
        assert!(
            samples.iter().any(|&s| s.abs() > 1e-4),
            "track {} decoded to silence",
            idx + 1
        );

        let eq: SharedEq = Arc::new(Mutex::new(Default::default()));
        let tap = AudioTap::new();
        let mut pipeline = AudioPipeline::new(eq, Arc::clone(&tap), sr, sr);
        assert!(!pipeline.is_resampling_output());
        // Several passes of the decoded track, as the worker feeds the pipeline.
        let mut out_all = Vec::new();
        for _ in 0..6 {
            let mut stereo_f64: Vec<f64> = samples.iter().map(|&s| f64::from(s)).collect();
            out_all.extend(pipeline.process_stereo_f64(&mut stereo_f64));
        }
        assert_eq!(out_all.len(), samples.len() * 6);
        assert!(out_all.iter().any(|&s| s.abs() > 1e-4));
        assert_eq!(volume_gain(0, true), 0.0);
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

/// A 192 kHz synthesized file must report 24/192 from its STREAMINFO and every
/// frame must decode. The frame header's sample-rate code for 192 kHz is 0b0011;
/// 0b1100 means "read an 8-bit kHz value after the header", which corrupts the
/// header and makes decoders skip the frame.
#[test]
fn hires_192khz_synthesized_flac_reports_24_192_and_decodes_every_frame() {
    let root = std::env::temp_dir().join(format!("spotifast-192khz-header-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("hires_192.flac");
    std::fs::write(&path, build_nonzero_pcm_flac(24, 192_000)).unwrap();

    let info = decode_flac_probe(&path).unwrap();
    assert_eq!(info.sample_rate_hz, 192_000);
    assert_eq!(info.bit_depth, Some(24));
    assert_eq!(info.quality_label(), "24-bit / 192 kHz FLAC");
    assert_eq!(
        Quality::from_stream_params(info.bit_depth, Some(info.sample_rate_hz)),
        Quality::Flac24_192
    );

    let (sr, channels, samples) = decode_flac_samples(&path).unwrap();
    assert_eq!((sr, channels), (192_000, 2));
    assert_eq!(
        samples.len(),
        192 * 2,
        "the frame must decode, not be skipped as corrupt"
    );
    assert!(samples.iter().any(|&s| s.abs() > 1e-4));

    let _ = std::fs::remove_dir_all(&root);
}
