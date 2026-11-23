//! Standalone lossless playlist sync engine for Spotifast.
//!
//! `spotisync` resolves playlist tracks against lossless audio providers using
//! strict metadata and ISRC matching, tracks progress in a resumable SQLite
//! store, writes FLAC files atomically with full tags, and watches playlists
//! for new additions.
//!
//! This milestone (M0) only ships the provider layer, the matching rules, the
//! queue and rate-limit types, and the shared data types. [`SyncStore`],
//! [`PlaylistWatcher`] and the writer in [`writer`] are placeholders: they
//! carry no behaviour yet. SQLite persistence lands in M2, the playlist
//! watcher in M3, and the atomic FLAC writer and tagger in M2.

pub mod matching;
pub mod providers;
pub mod queue;
pub mod store;
pub mod watcher;
pub mod writer;

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

pub use matching::{MatchConfig, MatchOutcome, MatchedBy, RejectReason};
pub use providers::{Provider, qobuz::QobuzConfig, qobuz::QobuzCredentials, qobuz::QobuzProvider};
pub use queue::{ProviderCooldown, QueueConfig, RunSummary, SyncQueue, SyncSnapshot};
pub use store::{PlaylistCounts, SyncItem, SyncStatus, SyncStore, SyncTrackMeta, SyncedPlaylist};
pub use watcher::{PlaylistSource, PlaylistTrack, PlaylistWatcher, WatcherConfig};
pub use writer::{TrackMetadata, WriteOutcome, WriteRequest};

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, SyncError>;

/// Errors returned by `spotisync` operations.
#[derive(Debug, Error)]
pub enum SyncError {
    /// Provider rejected credentials or returned HTTP 401/403.
    #[error("authentication invalid: {0}")]
    AuthInvalid(String),

    /// Provider returned HTTP 429 Too Many Requests.
    #[error("rate limited by {provider}: {message}")]
    RateLimited {
        provider: String,
        retry_after_secs: Option<u64>,
        message: String,
    },

    /// Provider returned a 5xx server error.
    #[error("server error from {provider} (HTTP {status}): {message}")]
    ServerError {
        provider: String,
        status: u16,
        message: String,
    },

    /// Requested track or stream could not be found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Provider only offered a lossy or non-FLAC stream.
    #[error("lossy stream rejected: {0}")]
    LossyOnly(String),

    /// Provider served only a preview sample for this track. The credentials
    /// are valid, so this is a per-track failure, not an authentication one.
    #[error("preview sample only: {0}")]
    PreviewOnly(String),

    /// Downloaded audio payload failed FLAC validation.
    #[error("invalid audio data: {0}")]
    InvalidAudio(String),

    /// Underlying HTTP transport error.
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    /// Response or payload failed to parse.
    #[error("parse error: {0}")]
    Parse(String),

    /// Filesystem I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// SQLite store error.
    #[error("store error: {0}")]
    Store(String),

    /// Metadata tagging error.
    #[error("tagging error: {0}")]
    Tagging(String),

    /// Other provider-reported error.
    #[error("provider {provider} error: {message}")]
    Provider { provider: String, message: String },
}

impl SyncError {
    /// Returns true if this error is transient and eligible for retry.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::RateLimited { .. } | Self::ServerError { .. } | Self::Http(_)
        )
    }

    /// Returns true when the error represents an HTTP 429 rate limit.
    pub fn is_rate_limited(&self) -> bool {
        matches!(self, Self::RateLimited { .. })
    }

    /// Returns true when credentials were rejected.
    pub fn is_auth_invalid(&self) -> bool {
        matches!(self, Self::AuthInvalid(_))
    }

    /// Returns the retry-after duration in seconds if this is a rate-limit error.
    pub fn retry_after_secs(&self) -> Option<u64> {
        match self {
            Self::RateLimited {
                retry_after_secs, ..
            } => *retry_after_secs,
            _ => None,
        }
    }
}

/// Lossless audio quality tier, ordered from lowest to highest quality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Quality {
    /// Standard CD-quality lossless FLAC (16-bit / 44.1 kHz).
    #[serde(rename = "flac_16_44", alias = "Flac16_44")]
    Flac16_44,
    /// Hi-Res lossless FLAC (24-bit / up to 96 kHz).
    #[serde(rename = "flac_24_96", alias = "Flac24_96")]
    Flac24_96,
    /// Studio Master lossless FLAC (24-bit / up to 192 kHz).
    #[serde(rename = "flac_24_192", alias = "Flac24_192")]
    Flac24_192,
}

impl Quality {
    /// Canonical storage representation for SQLite and serialization.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Flac16_44 => "flac_16_44",
            Self::Flac24_96 => "flac_24_96",
            Self::Flac24_192 => "flac_24_192",
        }
    }

    /// Parses a canonical quality string.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "flac_16_44" | "flac16_44" | "16/44.1" => Some(Self::Flac16_44),
            "flac_24_96" | "flac24_96" | "24/96" => Some(Self::Flac24_96),
            "flac_24_192" | "flac24_192" | "24/192" => Some(Self::Flac24_192),
            _ => None,
        }
    }

    /// Human-readable label for logs and UI.
    pub const fn display_label(self) -> &'static str {
        match self {
            Self::Flac16_44 => "FLAC 16-bit/44.1kHz",
            Self::Flac24_96 => "FLAC 24-bit/96kHz",
            Self::Flac24_192 => "FLAC 24-bit/192kHz",
        }
    }

    /// Infers a quality tier from FLAC bit depth and sample rate in Hz.
    pub const fn from_stream_params(bit_depth: Option<u8>, sample_rate_hz: Option<u32>) -> Self {
        let bits = match bit_depth {
            Some(b) => b,
            None => 16,
        };
        let rate = match sample_rate_hz {
            Some(r) => r,
            None => 44_100,
        };
        if bits >= 24 && rate >= 176_400 {
            Self::Flac24_192
        } else if bits >= 24 {
            Self::Flac24_96
        } else {
            Self::Flac16_44
        }
    }
}

impl fmt::Display for Quality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.display_label())
    }
}

/// Metadata query describing a track to resolve on a lossless provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Query {
    pub isrc: Option<String>,
    pub title: String,
    pub artists: Vec<String>,
    pub album: String,
    pub duration_ms: u64,
    #[serde(default)]
    pub album_artist: Option<String>,
    #[serde(default)]
    pub track_number: Option<u32>,
    #[serde(default)]
    pub disc_number: Option<u32>,
    #[serde(default)]
    pub year: Option<u32>,
    #[serde(default)]
    pub cover_url: Option<String>,
}

/// Track candidate returned by a lossless provider's search endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Candidate {
    pub provider: &'static str,
    pub track_id: String,
    pub isrc: Option<String>,
    pub title: String,
    pub artists: Vec<String>,
    pub album: Option<String>,
    pub album_artist: Option<String>,
    pub track_number: Option<u32>,
    pub disc_number: Option<u32>,
    pub year: Option<u32>,
    pub bit_depth: Option<u8>,
    pub sample_rate_hz: Option<u32>,
    pub duration_ms: u64,
    pub lossless: bool,
    pub cover_url: Option<String>,
    pub source_url: Option<String>,
    pub lyrics_lrc: Option<String>,
}

impl Candidate {
    /// Returns the candidate's inferred lossless quality tier, or `None` if lossy.
    pub fn quality(&self) -> Option<Quality> {
        if !self.lossless {
            return None;
        }
        Some(Quality::from_stream_params(
            self.bit_depth,
            self.sample_rate_hz,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quality_ordering_increases_with_resolution() {
        assert!(Quality::Flac16_44 < Quality::Flac24_96);
        assert!(Quality::Flac24_96 < Quality::Flac24_192);
        assert!(Quality::Flac24_192 > Quality::Flac16_44);
    }

    #[test]
    fn quality_stream_param_inference() {
        assert_eq!(
            Quality::from_stream_params(Some(16), Some(44_100)),
            Quality::Flac16_44
        );
        assert_eq!(
            Quality::from_stream_params(Some(24), Some(48_000)),
            Quality::Flac24_96
        );
        assert_eq!(
            Quality::from_stream_params(Some(24), Some(96_000)),
            Quality::Flac24_96
        );
        assert_eq!(
            Quality::from_stream_params(Some(24), Some(192_000)),
            Quality::Flac24_192
        );
    }

    #[test]
    fn quality_parse_roundtrip() {
        for q in [Quality::Flac16_44, Quality::Flac24_96, Quality::Flac24_192] {
            assert_eq!(Quality::parse(q.as_str()), Some(q));
        }
        assert_eq!(Quality::parse("mp3_320"), None);
    }

    #[test]
    fn quality_serde_uses_canonical_strings() {
        for q in [Quality::Flac16_44, Quality::Flac24_96, Quality::Flac24_192] {
            let json = serde_json::to_string(&q).expect("serialize quality");
            assert_eq!(json, format!("\"{}\"", q.as_str()));
            let back: Quality = serde_json::from_str(&json).expect("deserialize quality");
            assert_eq!(back, q);
            assert_eq!(Quality::parse(json.trim_matches('"')), Some(q));
        }
    }

    #[test]
    fn quality_serde_accepts_legacy_variant_names() {
        let legacy: Quality = serde_json::from_str("\"Flac24_96\"").expect("legacy name");
        assert_eq!(legacy, Quality::Flac24_96);
    }
}
