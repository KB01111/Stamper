//! Atomic FLAC writer, upgrade guard, and metadata tagger.
//!
//! Full implementation is provided in Milestone M2.

use crate::Quality;
use bytes::Bytes;
use std::path::PathBuf;

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
