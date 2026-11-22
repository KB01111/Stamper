//! Atomic FLAC writer, upgrade guard, metadata tagger, and `.lrc` sidecar writer.
//!
//! Guarantees:
//! - Only verified FLAC payloads (`fLaC` magic header + valid FLAC stream) are written.
//! - Existing files at equal or higher quality are never overwritten or deleted.
//! - Higher-quality upgrades write to a temp file in the destination directory,
//!   tag with `lofty`, `sync_all()`, and atomically rename into place.
//! - Any error cleans up the temporary file so no partial `.tmp` files remain.

use crate::{Quality, Result, SyncError};
use bytes::Bytes;
use lofty::config::{ParseOptions, WriteOptions};
use lofty::file::AudioFile;
use lofty::flac::FlacFile;
use lofty::ogg::{OggPictureStorage, tag::VorbisComments};
use lofty::picture::{MimeType, Picture, PictureInformation, PictureType};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const FLAC_MAGIC: &[u8; 4] = b"fLaC";
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Metadata tags to embed in the written FLAC file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackMetadata {
    pub title: String,
    pub artists: Vec<String>,
    pub album: String,
    pub album_artist: Option<String>,
    pub track_number: Option<u32>,
    pub disc_number: Option<u32>,
    pub year: Option<u32>,
    pub isrc: Option<String>,
    pub provider: String,
    pub source_url: Option<String>,
    pub cover_bytes: Option<Vec<u8>>,
    pub cover_mime: Option<String>,
    pub lyrics_lrc: Option<String>,
}

/// Request to write a verified FLAC payload atomically.
#[derive(Debug, Clone)]
pub struct WriteRequest {
    pub destination: PathBuf,
    pub audio_bytes: Bytes,
    pub quality: Quality,
    pub metadata: TrackMetadata,
}

/// Result of an atomic FLAC write request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOutcome {
    /// New file was written and tagged.
    Written { path: PathBuf, quality: Quality },
    /// Existing lower-quality file was atomically upgraded.
    Upgraded {
        path: PathBuf,
        previous_quality: Quality,
        new_quality: Quality,
    },
    /// Existing file already met or exceeded the target quality; left untouched.
    SkippedExisting {
        path: PathBuf,
        existing_quality: Quality,
    },
}

/// Builds a deterministic, filesystem-safe FLAC path under `root`.
pub fn build_track_path(
    root: &Path,
    artist: &str,
    album: &str,
    track_number: Option<u32>,
    title: &str,
) -> PathBuf {
    let artist_dir = sanitize_component(artist, "Unknown Artist");
    let album_dir = sanitize_component(album, "Unknown Album");
    let title_part = sanitize_component(title, "Unknown Track");
    let filename = match track_number {
        Some(n) if n > 0 => format!("{n:02} - {title_part}.flac"),
        _ => format!("{title_part}.flac"),
    };
    root.join(artist_dir).join(album_dir).join(filename)
}

/// Checks if `destination` already holds a FLAC file at `>= candidate_quality`.
///
/// Returns `Ok(Some(existing_quality))` when the download can be skipped with
/// zero network traffic, or `Ok(None)` when the file does not exist yet or
/// qualifies for a quality upgrade.
pub fn should_skip_download(
    destination: &Path,
    candidate_quality: Quality,
) -> Result<Option<Quality>> {
    let Some(existing) = inspect_existing_flac_quality(destination)? else {
        return Ok(None);
    };
    if existing >= candidate_quality {
        Ok(Some(existing))
    } else {
        Ok(None)
    }
}

/// Inspects an existing FLAC file's stream quality via `lofty`.
///
/// Returns `Ok(None)` when `path` does not exist.
pub fn inspect_existing_flac_quality(path: &Path) -> Result<Option<Quality>> {
    if !path.exists() {
        return Ok(None);
    }
    let mut file = File::open(path)?;
    let flac = FlacFile::read_from(&mut file, ParseOptions::new())
        .map_err(|e| SyncError::InvalidAudio(format!("read existing {}: {e}", path.display())))?;

    let props = flac.properties();
    Ok(Some(Quality::from_stream_params(
        Some(props.bit_depth()),
        Some(props.sample_rate()),
    )))
}

/// Verifies, tags, and atomically writes a FLAC file (and optional `.lrc` sidecar).
pub fn write_flac_atomic(request: &WriteRequest) -> Result<WriteOutcome> {
    verify_flac_magic(&request.audio_bytes)?;

    let destination = &request.destination;
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .ok_or_else(|| {
            SyncError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "destination path must have a parent directory",
            ))
        })?;

    fs::create_dir_all(parent)?;

    // Enforce the never-downgrade / never-overwrite-equal invariant before writing.
    let existing_quality = inspect_existing_flac_quality(destination)?;
    if let Some(existing) = existing_quality
        && existing >= request.quality
    {
        return Ok(WriteOutcome::SkippedExisting {
            path: destination.clone(),
            existing_quality: existing,
        });
    }

    let tmp_path = unique_temp_path(parent, destination, "flac");
    let mut guard = TempFileGuard::new(tmp_path.clone());

    {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)?;
        file.write_all(&request.audio_bytes)?;
        file.sync_all()?;
    }

    let detected_quality = tag_flac_file(&tmp_path, &request.metadata)?;
    let effective_quality = detected_quality.max(request.quality);

    // Re-check against actual decoded FLAC stream properties when upgrading.
    if let Some(existing) = existing_quality
        && existing >= effective_quality
    {
        return Ok(WriteOutcome::SkippedExisting {
            path: destination.clone(),
            existing_quality: existing,
        });
    }

    // Flush tagged bytes to stable storage before atomic rename.
    // Open with write access because Windows FlushFileBuffers requires it.
    OpenOptions::new().write(true).open(&tmp_path)?.sync_all()?;

    atomic_replace(&tmp_path, destination)?;
    guard.disarm();

    if let Some(lyrics) = request
        .metadata
        .lyrics_lrc
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let lrc_path = destination.with_extension("lrc");
        write_text_atomic(&lrc_path, lyrics)?;
    }

    match existing_quality {
        Some(previous_quality) => Ok(WriteOutcome::Upgraded {
            path: destination.clone(),
            previous_quality,
            new_quality: effective_quality,
        }),
        None => Ok(WriteOutcome::Written {
            path: destination.clone(),
            quality: effective_quality,
        }),
    }
}

/// Verifies that raw audio bytes start with the 4-byte `fLaC` magic marker.
pub fn verify_flac_magic(bytes: &[u8]) -> Result<()> {
    if bytes.len() < FLAC_MAGIC.len() || &bytes[..4] != FLAC_MAGIC {
        return Err(SyncError::InvalidAudio(
            "audio payload is not a valid FLAC stream (missing fLaC header)".to_string(),
        ));
    }
    Ok(())
}

fn tag_flac_file(path: &Path, meta: &TrackMetadata) -> Result<Quality> {
    let mut file = File::open(path)?;
    let mut flac = FlacFile::read_from(&mut file, ParseOptions::new())
        .map_err(|e| SyncError::InvalidAudio(format!("read temp FLAC: {e}")))?;
    drop(file);

    let props = flac.properties();
    let stream_quality =
        Quality::from_stream_params(Some(props.bit_depth()), Some(props.sample_rate()));

    if flac.vorbis_comments().is_none() {
        flac.set_vorbis_comments(VorbisComments::new());
    }
    let vorbis = flac
        .vorbis_comments_mut()
        .ok_or_else(|| SyncError::Tagging("failed to access VorbisComments tag".to_string()))?;

    vorbis.insert("TITLE".to_string(), meta.title.clone());
    if !meta.artists.is_empty() {
        vorbis.insert("ARTIST".to_string(), meta.artists.join(", "));
    }
    if !meta.album.is_empty() {
        vorbis.insert("ALBUM".to_string(), meta.album.clone());
    }
    if let Some(album_artist) = meta.album_artist.as_deref().filter(|s| !s.is_empty()) {
        vorbis.insert("ALBUMARTIST".to_string(), album_artist.to_string());
    }
    if let Some(track) = meta.track_number {
        vorbis.insert("TRACKNUMBER".to_string(), track.to_string());
    }
    if let Some(disc) = meta.disc_number {
        vorbis.insert("DISCNUMBER".to_string(), disc.to_string());
    }
    if let Some(year) = meta.year {
        vorbis.insert("DATE".to_string(), year.to_string());
    }
    if let Some(isrc) = meta.isrc.as_deref().filter(|s| !s.is_empty()) {
        vorbis.insert("ISRC".to_string(), isrc.to_string());
    }
    vorbis.insert("PROVIDER".to_string(), meta.provider.clone());
    if let Some(source_url) = meta.source_url.as_deref().filter(|s| !s.is_empty()) {
        vorbis.insert("SOURCE_URL".to_string(), source_url.to_string());
    }

    if let Some(cover_bytes) = meta.cover_bytes.as_ref().filter(|b| !b.is_empty()) {
        let mime = match meta
            .cover_mime
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("image/png") => MimeType::Png,
            Some("image/gif") => MimeType::Gif,
            Some("image/bmp") => MimeType::Bmp,
            Some("image/tiff") => MimeType::Tiff,
            Some("image/jpeg" | "image/jpg") | None => MimeType::Jpeg,
            Some(other) => MimeType::Unknown(other.to_string()),
        };
        let picture = Picture::unchecked(cover_bytes.clone())
            .pic_type(PictureType::CoverFront)
            .mime_type(mime)
            .build();
        flac.insert_picture(picture, Some(PictureInformation::default()))
            .map_err(|e| SyncError::Tagging(format!("embed cover picture: {e}")))?;
    }

    flac.save_to_path(path, WriteOptions::default())
        .map_err(|e| SyncError::Tagging(format!("save FLAC tags: {e}")))?;

    Ok(stream_quality)
}

fn write_text_atomic(destination: &Path, content: &str) -> Result<()> {
    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    let tmp_path = unique_temp_path(parent, destination, "lrc");
    let mut guard = TempFileGuard::new(tmp_path.clone());

    {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)?;
        file.write_all(content.as_bytes())?;
        file.sync_all()?;
    }

    atomic_replace(&tmp_path, destination)?;
    guard.disarm();
    Ok(())
}

fn atomic_replace(tmp_path: &Path, destination: &Path) -> Result<()> {
    fs::rename(tmp_path, destination)?;
    Ok(())
}

fn unique_temp_path(parent: &Path, destination: &Path, kind: &str) -> PathBuf {
    let stem = destination
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("track");
    let seq = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    parent.join(format!(".spotisync-{stem}-{kind}-{pid}-{seq}.tmp"))
}

fn sanitize_component(raw: &str, fallback: &str) -> String {
    let cleaned: String = raw
        .chars()
        .map(|c| match c {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            c if c.is_control() => '_',
            other => other,
        })
        .collect();
    let trimmed = cleaned.trim().trim_matches('.');
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed.chars().take(120).collect()
    }
}

struct TempFileGuard {
    path: PathBuf,
    armed: bool,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Builds a minimal valid FLAC byte stream with the requested bit depth and sample rate.
///
/// Exposed for deterministic offline testing across the crate and integration tests.
pub fn make_test_flac_bytes(bit_depth: u8, sample_rate_hz: u32) -> Bytes {
    let mut buf = Vec::with_capacity(64);
    // "fLaC" magic.
    buf.extend_from_slice(FLAC_MAGIC);
    // Metadata block header: is_last=1 (0x80), block_type=0 (STREAMINFO), length=34 (0x000022).
    buf.extend_from_slice(&[0x80, 0x00, 0x00, 0x22]);

    // STREAMINFO (34 bytes):
    // min_block_size = 4096, max_block_size = 4096.
    buf.extend_from_slice(&4096u16.to_be_bytes());
    buf.extend_from_slice(&4096u16.to_be_bytes());
    // min_frame_size = 0 (24-bit), max_frame_size = 0 (24-bit).
    buf.extend_from_slice(&[0, 0, 0, 0, 0, 0]);

    // 64-bit packed field:
    // [20 bits sample_rate][3 bits channels-1][5 bits bps-1][36 bits total_samples]
    let sr = u64::from(sample_rate_hz) & 0xF_FFFF;
    let channels_minus_1: u64 = 1; // stereo
    let bps_minus_1 = u64::from(bit_depth.saturating_sub(1)) & 0x1F;
    let total_samples = u64::from(sample_rate_hz); // 1 second
    let packed: u64 = (sr << 44) | (channels_minus_1 << 41) | (bps_minus_1 << 36) | total_samples;
    buf.extend_from_slice(&packed.to_be_bytes());

    // 16-byte MD5 signature of unencoded audio.
    buf.extend_from_slice(&[0u8; 16]);

    Bytes::from(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample_metadata() -> TrackMetadata {
        TrackMetadata {
            title: "Instant Crush".to_string(),
            artists: vec!["Daft Punk".to_string(), "Julian Casablancas".to_string()],
            album: "Random Access Memories".to_string(),
            album_artist: Some("Daft Punk".to_string()),
            track_number: Some(5),
            disc_number: Some(1),
            year: Some(2013),
            isrc: Some("USQX91300105".to_string()),
            provider: "qobuz".to_string(),
            source_url: Some("https://play.qobuz.com/track/10760101".to_string()),
            cover_bytes: Some(vec![0xFF, 0xD8, 0xFF, 0xD9]),
            cover_mime: Some("image/jpeg".to_string()),
            lyrics_lrc: Some("[00:10.00] I didn't want to be the one to forget\n".to_string()),
        }
    }

    #[test]
    fn writes_tags_verifies_via_lofty_and_upgrades_only_on_higher_quality() {
        let dir = tempdir().expect("tempdir");
        let dest = dir.path().join("Daft Punk").join("05 - Instant Crush.flac");

        // Initial 16/44.1 write.
        let req_cd = WriteRequest {
            destination: dest.clone(),
            audio_bytes: make_test_flac_bytes(16, 44_100),
            quality: Quality::Flac16_44,
            metadata: sample_metadata(),
        };
        let out1 = write_flac_atomic(&req_cd).expect("initial write");
        assert_eq!(
            out1,
            WriteOutcome::Written {
                path: dest.clone(),
                quality: Quality::Flac16_44,
            }
        );
        assert!(dest.with_extension("lrc").exists());

        // Equal quality re-write must be skipped without touching file.
        let out_skip = write_flac_atomic(&req_cd).expect("skip equal");
        assert_eq!(
            out_skip,
            WriteOutcome::SkippedExisting {
                path: dest.clone(),
                existing_quality: Quality::Flac16_44,
            }
        );

        // Higher quality 24/96 upgrade must replace cleanly.
        let req_hires = WriteRequest {
            destination: dest.clone(),
            audio_bytes: make_test_flac_bytes(24, 96_000),
            quality: Quality::Flac24_96,
            metadata: sample_metadata(),
        };
        let out_up = write_flac_atomic(&req_hires).expect("upgrade");
        assert_eq!(
            out_up,
            WriteOutcome::Upgraded {
                path: dest.clone(),
                previous_quality: Quality::Flac16_44,
                new_quality: Quality::Flac24_96,
            }
        );

        // Downgrade attempt must be skipped.
        let out_down = write_flac_atomic(&req_cd).expect("skip downgrade");
        assert_eq!(
            out_down,
            WriteOutcome::SkippedExisting {
                path: dest.clone(),
                existing_quality: Quality::Flac24_96,
            }
        );

        // Verify tags and stream properties via lofty.
        let mut file = File::open(&dest).unwrap();
        let flac = FlacFile::read_from(&mut file, ParseOptions::new()).unwrap();
        assert_eq!(flac.properties().bit_depth(), 24);
        assert_eq!(flac.properties().sample_rate(), 96_000);
        let vorbis = flac.vorbis_comments().expect("vorbis tag");
        assert_eq!(vorbis.get("TITLE"), Some("Instant Crush"));
        assert_eq!(vorbis.get("ARTIST"), Some("Daft Punk, Julian Casablancas"));
        assert_eq!(vorbis.get("ALBUM"), Some("Random Access Memories"));
        assert_eq!(vorbis.get("ALBUMARTIST"), Some("Daft Punk"));
        assert_eq!(vorbis.get("TRACKNUMBER"), Some("5"));
        assert_eq!(vorbis.get("DISCNUMBER"), Some("1"));
        assert_eq!(vorbis.get("DATE"), Some("2013"));
        assert_eq!(vorbis.get("ISRC"), Some("USQX91300105"));
        assert_eq!(vorbis.get("PROVIDER"), Some("qobuz"));
        assert_eq!(
            vorbis.get("SOURCE_URL"),
            Some("https://play.qobuz.com/track/10760101")
        );
        assert_eq!(flac.pictures().len(), 1);

        // Verify zero stray .tmp files left behind.
        let tmp_count = fs::read_dir(dest.parent().unwrap())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("tmp"))
            .count();
        assert_eq!(tmp_count, 0);
    }

    #[test]
    fn rejects_non_flac_payload_and_leaves_no_tmp_files() {
        let dir = tempdir().expect("tempdir");
        let dest = dir.path().join("bad.flac");
        let req = WriteRequest {
            destination: dest.clone(),
            audio_bytes: Bytes::from_static(b"ID3\x03\x00not a flac file"),
            quality: Quality::Flac16_44,
            metadata: sample_metadata(),
        };
        let err = write_flac_atomic(&req).expect_err("must reject MP3");
        assert!(matches!(err, SyncError::InvalidAudio(_)));
        assert!(!dest.exists());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}
