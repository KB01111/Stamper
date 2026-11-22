//! Strict track matching between Spotify queries and provider candidates.

use crate::{Candidate, Quality, Query};
use serde::{Deserialize, Serialize};

/// How a candidate was matched to a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MatchedBy {
    /// Exact ISRC match (case-insensitive, stripped hyphens and spaces).
    Isrc,
    /// Exact normalized title and primary artist match within tight duration tolerance.
    Exact,
    /// High-confidence fuzzy match within duration tolerance and variant guards.
    Fuzzy,
}

impl MatchedBy {
    /// Canonical string identifier for SQLite storage.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Isrc => "isrc",
            Self::Exact => "exact",
            Self::Fuzzy => "fuzzy",
        }
    }
}

/// Reason a set of candidates did not produce a match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectReason {
    /// No candidates were returned by any provider.
    NoCandidates,
    /// Matching candidates existed, but all of them were lossy.
    LossyOnly,
    /// No candidate satisfied the strict matching rules and variant guards.
    NoStrictMatch,
}

/// Configuration controlling matching thresholds and quality ceilings.
#[derive(Debug, Clone)]
pub struct MatchConfig {
    /// Maximum allowed duration delta in milliseconds for exact normalized matches.
    pub exact_duration_tolerance_ms: u64,
    /// Maximum allowed duration delta in milliseconds for fuzzy matches.
    pub fuzzy_duration_tolerance_ms: u64,
    /// Minimum title similarity score (0.0..=1.0) required for fuzzy matching.
    pub fuzzy_title_threshold: f64,
    /// Minimum artist similarity score (0.0..=1.0) required for fuzzy matching.
    pub fuzzy_artist_threshold: f64,
    /// Maximum quality tier to prefer when selecting among lossless candidates.
    pub max_quality: Quality,
}

impl Default for MatchConfig {
    fn default() -> Self {
        Self {
            exact_duration_tolerance_ms: 2_000,
            fuzzy_duration_tolerance_ms: 5_000,
            fuzzy_title_threshold: 0.88,
            fuzzy_artist_threshold: 0.85,
            max_quality: Quality::Flac24_192,
        }
    }
}

/// Outcome of running strict matching on a candidate list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchOutcome {
    /// A lossless candidate matched strictly.
    Matched {
        candidate: Box<Candidate>,
        matched_by: MatchedBy,
        selected_quality: Quality,
    },
    /// No acceptable candidate matched.
    Unmatched { reason: RejectReason },
}

/// Matches a query against candidate results using strict rules.
///
/// Full matching rules are implemented in Milestone M1.
pub fn select_best_match(
    _query: &Query,
    _candidates: &[Candidate],
    _config: &MatchConfig,
) -> MatchOutcome {
    MatchOutcome::Unmatched {
        reason: RejectReason::NoStrictMatch,
    }
}
