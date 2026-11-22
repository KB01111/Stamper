//! Audio providers capable of searching for and fetching lossless FLAC tracks.

pub mod qobuz;

use crate::{Candidate, Quality, Query, Result};
use async_trait::async_trait;
use bytes::Bytes;

/// Lossless audio provider interface.
#[async_trait]
pub trait Provider: Send + Sync {
    /// Stable provider identifier (for example `"qobuz"`).
    fn id(&self) -> &'static str;

    /// Maximum lossless quality tier this provider can serve.
    fn max_quality(&self) -> Quality;

    /// Searches the provider catalog for candidates matching `q`.
    async fn search(&self, q: &Query) -> Result<Vec<Candidate>>;

    /// Fetches the FLAC audio payload for candidate `c` at up to quality `q`.
    async fn fetch(&self, c: &Candidate, q: Quality) -> Result<Bytes>;
}
