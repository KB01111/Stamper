//! Playlist polling watcher that enqueues new additions for sync.
//!
//! Full implementation is provided in Milestone M3.

use crate::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;

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

/// Source capable of listing the current tracks of a playlist.
#[async_trait]
pub trait PlaylistSource: Send + Sync {
    async fn fetch_playlist_tracks(&self, playlist_id: &str) -> Result<Vec<PlaylistTrack>>;
}

/// Configuration for [`PlaylistWatcher`].
#[derive(Debug, Clone)]
pub struct WatcherConfig {
    pub poll_interval: Duration,
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

/// Placeholder playlist watcher handle (implemented in Milestone M3).
#[derive(Debug)]
pub struct PlaylistWatcher;
