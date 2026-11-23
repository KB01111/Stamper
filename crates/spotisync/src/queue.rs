//! Concurrent resumable sync queue with rate limiting and backoff.
//!
//! Full implementation is provided in Milestone M2.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Active cooldown surfaced to callers when a provider returns HTTP 429.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCooldown {
    pub provider: String,
    pub retry_at_unix_secs: u64,
    pub remaining_secs: u64,
    pub message: String,
}

/// Non-blocking snapshot published via `tokio::sync::watch` for UI polling.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncSnapshot {
    pub pending: u64,
    pub resolving: u64,
    pub downloading: u64,
    pub done: u64,
    pub unmatched: u64,
    pub failed: u64,
    pub active_tracks: Vec<String>,
    pub cooldowns: BTreeMap<String, ProviderCooldown>,
}

/// Configuration for [`SyncQueue`].
#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub per_provider_concurrency: usize,
    pub max_attempts: u32,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            per_provider_concurrency: 2,
            max_attempts: 5,
        }
    }
}

/// Placeholder queue handle (implemented in Milestone M2).
#[derive(Debug)]
pub struct SyncQueue;
