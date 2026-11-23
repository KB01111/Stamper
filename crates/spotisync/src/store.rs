//! Resumable SQLite store for playlist sync state.
//!
//! Full SQLite persistence is implemented in Milestone M2.

use serde::{Deserialize, Serialize};

/// Lifecycle state of a tracked playlist item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SyncStatus {
    Pending,
    Resolving,
    Downloading,
    Done,
    Unmatched,
    Failed,
}

impl SyncStatus {
    /// Canonical database value matching the `sync_items.status` CHECK constraint.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Resolving => "resolving",
            Self::Downloading => "downloading",
            Self::Done => "done",
            Self::Unmatched => "unmatched",
            Self::Failed => "failed",
        }
    }

    /// Parses a status value from SQLite.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "resolving" => Some(Self::Resolving),
            "downloading" => Some(Self::Downloading),
            "done" => Some(Self::Done),
            "unmatched" => Some(Self::Unmatched),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// One row from the `sync_items` table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncItem {
    pub playlist_id: String,
    pub track_id: String,
    pub isrc: Option<String>,
    pub title: String,
    pub artists: Vec<String>,
    pub status: SyncStatus,
    pub matched_by: Option<String>,
    pub provider: Option<String>,
    pub quality: Option<String>,
    pub path: Option<String>,
    pub attempts: u32,
    pub error: Option<String>,
    pub updated_at: i64,
}

/// Companion metadata stored for restartable query reconstruction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncTrackMeta {
    pub playlist_id: String,
    pub track_id: String,
    pub album: String,
    pub album_artist: Option<String>,
    pub duration_ms: u64,
    pub track_number: Option<u32>,
    pub disc_number: Option<u32>,
    pub year: Option<u32>,
    pub cover_url: Option<String>,
}

/// Synced playlist toggle record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncedPlaylist {
    pub playlist_id: String,
    pub name: String,
    pub enabled: bool,
    pub last_polled_at: Option<i64>,
}

/// Aggregated item counts per playlist.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaylistCounts {
    pub playlist_id: String,
    pub pending: u64,
    pub resolving: u64,
    pub downloading: u64,
    pub done: u64,
    pub unmatched: u64,
    pub failed: u64,
}

/// Placeholder store handle (implemented with SQLite in Milestone M2).
#[derive(Debug)]
pub struct SyncStore;
