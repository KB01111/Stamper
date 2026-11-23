//! Strict track matching between Spotify queries and provider candidates.
//!
//! Matching follows strict ordered rules and never silently substitutes a
//! different recording (such as live, remix, acoustic, karaoke, instrumental,
//! demo, cover, sped-up, slowed, radio edit, extended, clean, or explicit
//! variants, or a different part or volume number):
//!
//! 1. Exact ISRC match (case-insensitive, stripped hyphens and spaces; malformed
//!    and placeholder codes are ignored) -> [`MatchedBy::Isrc`].
//! 2. Exact normalized title + primary artist match within tight duration
//!    tolerance (default +/- 2000 ms) and matching recording variants -> [`MatchedBy::Exact`].
//! 3. High-confidence fuzzy title + primary artist similarity within duration
//!    tolerance (default +/- 5000 ms) and matching recording variants -> [`MatchedBy::Fuzzy`].
//! 4. Otherwise [`MatchOutcome::Unmatched`].
//!
//! The ISRC and exact tiers prefer the highest lossless quality. The fuzzy tier
//! prefers the most similar title and artist, using quality only as a tie-breaker.

use crate::{Candidate, Quality, Query};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// How a candidate was matched to a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MatchedBy {
    /// Exact, well-formed ISRC match (case-insensitive, stripped hyphens and spaces).
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
    /// Quality ceiling applied to every candidate: `min(native, max_quality)`.
    ///
    /// The matcher cannot see the provider's own cap (`Provider::max_quality`)
    /// because [`Candidate`] does not carry it. Callers must set this to the
    /// provider's `max_quality()` (or lower) before matching, otherwise the
    /// selected quality can exceed what the provider can serve.
    pub max_quality: Quality,
}

impl Default for MatchConfig {
    fn default() -> Self {
        Self {
            exact_duration_tolerance_ms: 2_000,
            fuzzy_duration_tolerance_ms: 5_000,
            fuzzy_title_threshold: 0.86,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RecordingVariant {
    Live,
    Remix,
    Acoustic,
    Karaoke,
    Instrumental,
    Acapella,
    Demo,
    CoverOrTribute,
    SpeedAltered,
    RadioEdit,
    Extended,
    Clean,
    Explicit,
}

struct ScoredMatch<'a> {
    candidate: &'a Candidate,
    selected_quality: Quality,
    native_quality: Quality,
    similarity_bps: u32,
    duration_delta_ms: u64,
}

/// Which key ranks the hits of one tier first.
#[derive(Debug, Clone, Copy)]
enum Ranking {
    /// Highest quality first, similarity as tie-breaker (ISRC and exact tiers).
    Quality,
    /// Most similar first, quality as tie-breaker (fuzzy tier).
    Similarity,
}

/// Normalized identity of the query, computed once per call.
struct QueryKey {
    title: String,
    artist: String,
    variants: BTreeSet<RecordingVariant>,
    parts: Option<Vec<u32>>,
    duration_ms: u64,
}

impl QueryKey {
    fn new(query: &Query) -> Self {
        Self {
            title: normalize_title(&query.title),
            artist: normalize_primary_artist(primary_artist(
                &query.artists,
                query.album_artist.as_deref(),
            )),
            variants: extract_recording_variants(&query.title, &query.artists),
            parts: part_numbers(&query.title),
            duration_ms: query.duration_ms,
        }
    }
}

/// Normalized identity of one provider candidate.
struct CandidateKey {
    title: String,
    artist: String,
    variants: BTreeSet<RecordingVariant>,
    parts: Option<Vec<u32>>,
}

impl CandidateKey {
    fn new(cand: &Candidate) -> Self {
        Self {
            title: normalize_title(&cand.title),
            artist: normalize_primary_artist(primary_artist(
                &cand.artists,
                cand.album_artist.as_deref(),
            )),
            variants: extract_recording_variants(&cand.title, &cand.artists),
            parts: part_numbers(&cand.title),
        }
    }
}

/// Selects the best lossless candidate matching `query` according to strict rules.
///
/// `config.max_quality` is the ceiling for the selected quality. Set it to the
/// provider's `max_quality()` (see [`MatchConfig::max_quality`]).
pub fn select_best_match(
    query: &Query,
    candidates: &[Candidate],
    config: &MatchConfig,
) -> MatchOutcome {
    if candidates.is_empty() {
        return MatchOutcome::Unmatched {
            reason: RejectReason::NoCandidates,
        };
    }

    let mut saw_lossy_match = false;

    // Tier 1: exact ISRC match.
    if let Some(q_isrc) = query.isrc.as_deref().and_then(normalize_isrc) {
        let (hits, lossy) = isrc_hits(query, candidates, &q_isrc, config);
        saw_lossy_match |= lossy;
        if let Some(best) = pick_best(hits, Ranking::Quality) {
            return finalize_match(query, best, MatchedBy::Isrc);
        }
    }

    let key = QueryKey::new(query);
    if key.title.is_empty() || key.artist.is_empty() {
        return unmatched_reason(saw_lossy_match);
    }

    // Tier 2: exact normalized title + primary artist within exact duration tolerance.
    let (hits, lossy) = exact_hits(&key, candidates, config);
    saw_lossy_match |= lossy;
    if let Some(best) = pick_best(hits, Ranking::Quality) {
        return finalize_match(query, best, MatchedBy::Exact);
    }

    // Tier 3: strict fuzzy match within fuzzy duration tolerance and variant guards.
    let (hits, lossy) = fuzzy_hits(&key, candidates, config);
    saw_lossy_match |= lossy;
    if let Some(best) = pick_best(hits, Ranking::Similarity) {
        return finalize_match(query, best, MatchedBy::Fuzzy);
    }

    unmatched_reason(saw_lossy_match)
}

fn unmatched_reason(saw_lossy_match: bool) -> MatchOutcome {
    MatchOutcome::Unmatched {
        reason: if saw_lossy_match {
            RejectReason::LossyOnly
        } else {
            RejectReason::NoStrictMatch
        },
    }
}

/// Tier 1 hits: candidates whose normalized ISRC equals the query's.
fn isrc_hits<'a>(
    query: &Query,
    candidates: &'a [Candidate],
    q_isrc: &str,
    config: &MatchConfig,
) -> (Vec<ScoredMatch<'a>>, bool) {
    let mut hits = Vec::new();
    let mut lossy = false;
    for cand in candidates {
        if cand.isrc.as_deref().and_then(normalize_isrc).as_deref() != Some(q_isrc) {
            continue;
        }
        let delta = query.duration_ms.abs_diff(cand.duration_ms);
        record_hit(
            &mut hits,
            &mut lossy,
            cand,
            10_000,
            delta,
            config.max_quality,
        );
    }
    (hits, lossy)
}

/// Tier 2 hits: identical normalized title and primary artist, within the exact window.
fn exact_hits<'a>(
    key: &QueryKey,
    candidates: &'a [Candidate],
    config: &MatchConfig,
) -> (Vec<ScoredMatch<'a>>, bool) {
    let mut hits = Vec::new();
    let mut lossy = false;
    for cand in candidates {
        let delta = key.duration_ms.abs_diff(cand.duration_ms);
        if delta > config.exact_duration_tolerance_ms {
            continue;
        }
        let cand_key = CandidateKey::new(cand);
        if !shares_recording_guards(key, &cand_key)
            || cand_key.title != key.title
            || cand_key.artist != key.artist
        {
            continue;
        }
        record_hit(
            &mut hits,
            &mut lossy,
            cand,
            10_000,
            delta,
            config.max_quality,
        );
    }
    (hits, lossy)
}

/// Tier 3 hits: similar title and artist, within the fuzzy window.
fn fuzzy_hits<'a>(
    key: &QueryKey,
    candidates: &'a [Candidate],
    config: &MatchConfig,
) -> (Vec<ScoredMatch<'a>>, bool) {
    let mut hits = Vec::new();
    let mut lossy = false;
    let query_artist_fuzzy = strip_leading_the(&key.artist);
    for cand in candidates {
        let delta = key.duration_ms.abs_diff(cand.duration_ms);
        if delta > config.fuzzy_duration_tolerance_ms {
            continue;
        }
        let cand_key = CandidateKey::new(cand);
        if !shares_recording_guards(key, &cand_key)
            || cand_key.title.is_empty()
            || cand_key.artist.is_empty()
        {
            continue;
        }

        let title_sim = string_similarity(&key.title, &cand_key.title);
        if title_sim < config.fuzzy_title_threshold {
            continue;
        }

        let artist_sim = string_similarity(&key.artist, &cand_key.artist).max(string_similarity(
            query_artist_fuzzy,
            strip_leading_the(&cand_key.artist),
        ));
        if artist_sim < config.fuzzy_artist_threshold {
            continue;
        }

        let combined_bps = ((title_sim * 0.6 + artist_sim * 0.4) * 10_000.0).round() as u32;
        record_hit(
            &mut hits,
            &mut lossy,
            cand,
            combined_bps,
            delta,
            config.max_quality,
        );
    }
    (hits, lossy)
}

/// Records a candidate that passed a tier's identity checks. Lossy candidates
/// are only remembered, so the caller can report [`RejectReason::LossyOnly`].
fn record_hit<'a>(
    hits: &mut Vec<ScoredMatch<'a>>,
    lossy: &mut bool,
    cand: &'a Candidate,
    similarity_bps: u32,
    duration_delta_ms: u64,
    max_quality: Quality,
) {
    match cand.quality() {
        Some(native) => hits.push(ScoredMatch {
            candidate: cand,
            selected_quality: native.min(max_quality),
            native_quality: native,
            similarity_bps,
            duration_delta_ms,
        }),
        None => *lossy = true,
    }
}

/// Variant and part-number guards shared by the exact and fuzzy tiers.
fn shares_recording_guards(query: &QueryKey, cand: &CandidateKey) -> bool {
    query.variants == cand.variants
        && parts_compatible(query.parts.as_deref(), cand.parts.as_deref())
}

/// Part numbers only conflict when both sides carry one and they differ.
fn parts_compatible(a: Option<&[u32]>, b: Option<&[u32]>) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => x == y,
        _ => true,
    }
}

fn pick_best<'a>(mut matches: Vec<ScoredMatch<'a>>, ranking: Ranking) -> Option<ScoredMatch<'a>> {
    matches.sort_by(|a, b| {
        let by_quality = b.selected_quality.cmp(&a.selected_quality);
        let by_similarity = b.similarity_bps.cmp(&a.similarity_bps);
        let primary = match ranking {
            Ranking::Quality => by_quality.then(by_similarity),
            Ranking::Similarity => by_similarity.then(by_quality),
        };
        primary
            .then_with(|| a.duration_delta_ms.cmp(&b.duration_delta_ms))
            .then_with(|| b.native_quality.cmp(&a.native_quality))
    });
    matches.into_iter().next()
}

fn finalize_match(query: &Query, best: ScoredMatch<'_>, matched_by: MatchedBy) -> MatchOutcome {
    log::info!(
        "spotisync matched query='{}' ({}) -> provider={} track_id={} title='{}' matched_by={} quality={}",
        query.title,
        query.artists.first().map(String::as_str).unwrap_or(""),
        best.candidate.provider,
        best.candidate.track_id,
        best.candidate.title,
        matched_by.as_str(),
        best.selected_quality.as_str(),
    );

    MatchOutcome::Matched {
        candidate: Box::new(best.candidate.clone()),
        matched_by,
        selected_quality: best.selected_quality,
    }
}

/// Primary artist, falling back to the album artist when no track artist is set.
/// Used identically for the query and for candidates.
fn primary_artist<'a>(artists: &'a [String], album_artist: Option<&'a str>) -> Option<&'a str> {
    artists
        .first()
        .map(String::as_str)
        .filter(|s| !s.trim().is_empty())
        .or(album_artist)
}

/// Normalizes an ISRC by stripping spaces and hyphens and uppercasing.
///
/// Returns `None` unless the result is a well-formed ISRC (2 letters, 3
/// alphanumerics, 7 digits) that is not a placeholder.
pub fn normalize_isrc(raw: &str) -> Option<String> {
    let cleaned: String = raw
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .map(|c| c.to_ascii_uppercase())
        .collect();
    is_well_formed_isrc(&cleaned).then_some(cleaned)
}

/// Checks the 12-character ISRC layout and rejects placeholder codes: the
/// unassigned `XX` and `ZZ` country codes, and an all-zero designation.
fn is_well_formed_isrc(code: &str) -> bool {
    let bytes = code.as_bytes();
    if bytes.len() != 12 {
        return false;
    }
    let (country, rest) = bytes.split_at(2);
    let (registrant, designation) = rest.split_at(3);
    let placeholder_country = matches!(country, [b'X', b'X'] | [b'Z', b'Z']);
    let all_zero_designation = designation.iter().all(|&b| b == b'0');
    country.iter().all(u8::is_ascii_uppercase)
        && registrant.iter().all(u8::is_ascii_alphanumeric)
        && designation.iter().all(u8::is_ascii_digit)
        && !placeholder_country
        && !all_zero_designation
}

/// Normalizes a track title for exact and fuzzy comparison.
///
/// Casefolds, folds common Latin diacritics, removes featuring credits and
/// remaster/edition markers, normalizes punctuation, and collapses spaces.
pub fn normalize_title(raw: &str) -> String {
    let folded = fold_diacritics(&raw.to_lowercase());
    let without_brackets = strip_noise_brackets(&folded);
    let without_suffixes = strip_trailing_noise_clauses(&without_brackets);
    let without_feat = strip_inline_feat(&without_suffixes);
    normalize_punctuation_and_spaces(&without_feat)
}

/// Normalizes a primary artist name for exact and fuzzy comparison.
pub fn normalize_primary_artist(raw: Option<&str>) -> String {
    let Some(artist) = raw else {
        return String::new();
    };
    let folded = fold_diacritics(&artist.to_lowercase());
    let without_feat = strip_inline_feat(&folded);
    normalize_punctuation_and_spaces(&without_feat)
}

fn strip_leading_the(s: &str) -> &str {
    s.strip_prefix("the ").unwrap_or(s)
}

fn strip_noise_brackets(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if chars[i] == '(' || chars[i] == '[' {
            let close = if chars[i] == '(' { ')' } else { ']' };
            if let Some(end_offset) = chars[i + 1..].iter().position(|&c| c == close) {
                let inner: String = chars[i + 1..i + 1 + end_offset].iter().collect();
                if is_ignorable_qualifier(&inner) {
                    out.push(' ');
                    i += end_offset + 2;
                    continue;
                }
            }
        }
        out.push(chars[i]);
        i += 1;
    }

    out
}

fn strip_trailing_noise_clauses(input: &str) -> String {
    let mut current = input.trim().to_string();
    while let Some(idx) = current.rfind(" - ") {
        let suffix = current[idx + 3..].trim();
        if is_ignorable_qualifier(suffix) {
            current.truncate(idx);
            current = current.trim_end().to_string();
        } else {
            break;
        }
    }
    current
}

fn strip_inline_feat(input: &str) -> String {
    let markers = [" feat. ", " feat ", " ft. ", " ft ", " featuring "];
    let mut best_cut: Option<usize> = None;
    for marker in markers {
        if let Some(idx) = input.find(marker) {
            best_cut = Some(match best_cut {
                Some(prev) => prev.min(idx),
                None => idx,
            });
        }
    }
    match best_cut {
        Some(idx) => input[..idx].to_string(),
        None => input.to_string(),
    }
}

/// Whether a bracketed or dash-suffixed qualifier is noise that may be dropped
/// from the title. A qualifier that carries a recording variant (including the
/// clean and explicit edits) is never ignorable, and neither is a title phrase
/// that merely starts with "with" (such as "With or Without You").
fn is_ignorable_qualifier(raw: &str) -> bool {
    let cleaned = normalize_punctuation_and_spaces(raw);
    if cleaned.is_empty() {
        return true;
    }
    if cleaned.starts_with("feat ")
        || cleaned.starts_with("ft ")
        || cleaned.starts_with("featuring ")
    {
        return true;
    }

    let tokens: Vec<&str> = cleaned.split_whitespace().collect();
    if tokens.is_empty() {
        return true;
    }

    // Never strip a qualifier that carries a recording-variant marker.
    if !extract_variants_from_segment(&cleaned, true).is_empty() {
        return false;
    }

    // Pure remaster / edition / album-version qualifiers.
    let contains_remaster = tokens
        .iter()
        .any(|t| matches!(*t, "remaster" | "remastered"));
    if contains_remaster {
        return true;
    }

    matches!(
        cleaned.as_str(),
        "album version"
            | "original version"
            | "original mix"
            | "deluxe"
            | "deluxe edition"
            | "anniversary edition"
            | "bonus track"
    )
}

fn normalize_punctuation_and_spaces(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut prev_space = true;

    let chars: Vec<char> = input.chars().collect();
    for (idx, &ch) in chars.iter().enumerate() {
        if ch == '&' {
            if !prev_space {
                out.push(' ');
            }
            out.push_str("and");
            out.push(' ');
            prev_space = true;
            continue;
        }
        // Drop apostrophes inside words ("don't" -> "dont", "rock'n'roll" -> "rocknroll").
        if matches!(ch, '\'' | '\u{2019}' | '\u{2018}') {
            let prev_alnum = idx > 0 && chars[idx - 1].is_alphanumeric();
            let next_alnum = idx + 1 < chars.len() && chars[idx + 1].is_alphanumeric();
            if prev_alnum && next_alnum {
                continue;
            }
        }
        if ch.is_alphanumeric() {
            out.push(ch);
            prev_space = false;
        } else if !prev_space {
            out.push(' ');
            prev_space = true;
        }
    }

    let trimmed = out.trim();
    // Normalize common abbreviations ("pt 1" -> "part 1") for consistent matching.
    let tokens: Vec<&str> = trimmed
        .split_whitespace()
        .map(|tok| match tok {
            "pt" => "part",
            "vol" => "volume",
            other => other,
        })
        .collect();
    tokens.join(" ")
}

/// Folds Latin letters with diacritics or strokes to their base letters.
///
/// Input is lowercased first, so each arm covers both cases. `İ` lowers to `i`
/// plus a combining dot, which is dropped.
fn fold_diacritics(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars().flat_map(char::to_lowercase) {
        match ch {
            'á' | 'à' | 'â' | 'ä' | 'ã' | 'å' | 'ā' | 'ă' | 'ą' => out.push('a'),
            'æ' => out.push_str("ae"),
            'ç' | 'ć' | 'č' => out.push('c'),
            'ď' | 'đ' => out.push('d'),
            'é' | 'è' | 'ê' | 'ë' | 'ē' | 'ę' | 'ě' => out.push('e'),
            'ğ' | 'ģ' => out.push('g'),
            'í' | 'ì' | 'î' | 'ï' | 'ī' | 'ı' => out.push('i'),
            '\u{307}' => {}
            'ł' | 'ľ' | 'ĺ' => out.push('l'),
            'ñ' | 'ń' | 'ň' => out.push('n'),
            'ó' | 'ò' | 'ô' | 'ö' | 'õ' | 'ø' | 'ō' | 'ő' => out.push('o'),
            'œ' => out.push_str("oe"),
            'ř' | 'ŕ' => out.push('r'),
            'ś' | 'š' | 'ş' => out.push('s'),
            'ß' => out.push_str("ss"),
            'ť' | 'ţ' => out.push('t'),
            'ú' | 'ù' | 'û' | 'ü' | 'ū' | 'ů' | 'ű' | 'ų' => out.push('u'),
            'ý' | 'ÿ' => out.push('y'),
            'ź' | 'ž' | 'ż' => out.push('z'),
            other => out.push(other),
        }
    }
    out
}

/// Extracts the part or volume number(s) from a title, such as `[1]` for
/// "Part 1", `[1, 5]` for "Pts. 1-5", or `[2]` for "Part II". Returns `None`
/// when the title names no part.
fn part_numbers(title: &str) -> Option<Vec<u32>> {
    let normalized = normalize_punctuation_and_spaces(&fold_diacritics(&title.to_lowercase()));
    let tokens: Vec<&str> = normalized.split_whitespace().collect();
    for (idx, token) in tokens.iter().enumerate() {
        if !matches!(*token, "part" | "parts" | "pts" | "volume" | "volumes") {
            continue;
        }
        let numbers: Vec<u32> = tokens[idx + 1..]
            .iter()
            .map_while(|t| part_number(t))
            .collect();
        if !numbers.is_empty() {
            return Some(numbers);
        }
    }
    None
}

/// Parses one token after a part keyword: a short digit run or a roman numeral up to ten.
fn part_number(token: &str) -> Option<u32> {
    const ROMAN: [(&str, u32); 10] = [
        ("i", 1),
        ("ii", 2),
        ("iii", 3),
        ("iv", 4),
        ("v", 5),
        ("vi", 6),
        ("vii", 7),
        ("viii", 8),
        ("ix", 9),
        ("x", 10),
    ];
    if !token.is_empty() && token.len() <= 3 && token.bytes().all(|b| b.is_ascii_digit()) {
        return token.parse().ok();
    }
    ROMAN
        .iter()
        .find(|(roman, _)| *roman == token)
        .map(|(_, value)| *value)
}

fn extract_recording_variants(title: &str, artists: &[String]) -> BTreeSet<RecordingVariant> {
    let mut variants = BTreeSet::new();
    let folded_title = fold_diacritics(&title.to_lowercase());

    // Split title into base text and qualifier clauses (parentheses, brackets, and " - " suffixes).
    let (base, qualifiers) = split_base_and_qualifiers(&folded_title);

    for v in extract_variants_from_segment(&base, false) {
        variants.insert(v);
    }
    for qualifier in qualifiers {
        for v in extract_variants_from_segment(&qualifier, true) {
            variants.insert(v);
        }
    }

    // Guard against karaoke/tribute artists masquerading as the original artist.
    for artist in artists {
        let norm_artist =
            normalize_punctuation_and_spaces(&fold_diacritics(&artist.to_lowercase()));
        let tokens: Vec<&str> = norm_artist.split_whitespace().collect();
        if tokens.contains(&"karaoke") {
            variants.insert(RecordingVariant::Karaoke);
        }
        if tokens.contains(&"tribute") {
            variants.insert(RecordingVariant::CoverOrTribute);
        }
    }

    variants
}

fn split_base_and_qualifiers(input: &str) -> (String, Vec<String>) {
    let mut base = String::with_capacity(input.len());
    let mut qualifiers = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if chars[i] == '(' || chars[i] == '[' {
            let close = if chars[i] == '(' { ')' } else { ']' };
            if let Some(end_offset) = chars[i + 1..].iter().position(|&c| c == close) {
                let inner: String = chars[i + 1..i + 1 + end_offset].iter().collect();
                qualifiers.push(inner);
                base.push(' ');
                i += end_offset + 2;
                continue;
            }
        }
        base.push(chars[i]);
        i += 1;
    }

    let mut parts = base.split(" - ");
    let head = parts.next().unwrap_or("").to_string();
    for tail in parts {
        qualifiers.push(tail.to_string());
    }

    (head, qualifiers)
}

// Marker phrases and tokens per recording variant. Tokens match a single word,
// phrases match consecutive words. Qualifier-only markers are handled inline.
const LIVE_PHRASES: &[&str] = &[
    "live at",
    "live from",
    "live in",
    "live on",
    "live session",
    "live version",
    "in concert",
    "bbc session",
    "peel session",
];
const REMIX_TOKENS: &[&str] = &["remix", "remixed", "rmx", "rework", "refix", "bootleg"];
const REMIX_PHRASES: &[&str] = &["club mix", "dance mix", "house mix", "dub mix"];
const ACOUSTIC_TOKENS: &[&str] = &["acoustic", "unplugged"];
const ACOUSTIC_PHRASES: &[&str] = &["piano version", "orchestral version", "string version"];
const KARAOKE_TOKENS: &[&str] = &["karaoke"];
const KARAOKE_PHRASES: &[&str] = &["backing track", "sing along", "minus one"];
const INSTRUMENTAL_TOKENS: &[&str] = &["instrumental"];
const ACAPELLA_TOKENS: &[&str] = &["acapella"];
const ACAPELLA_PHRASES: &[&str] = &["a cappella", "vocals only"];
const DEMO_TOKENS: &[&str] = &["outtake"];
const DEMO_PHRASES: &[&str] = &[
    "demo version",
    "early version",
    "rough mix",
    "alternate take",
    "alt take",
    "alternate version",
];
const COVER_TOKENS: &[&str] = &["tribute"];
const COVER_PHRASES: &[&str] = &[
    "originally performed by",
    "in the style of",
    "made famous by",
    "cover version",
];
const SPEED_TOKENS: &[&str] = &["nightcore", "slowed"];
const SPEED_PHRASES: &[&str] = &["sped up", "speed up", "8d audio"];
const RADIO_PHRASES: &[&str] = &[
    "radio edit",
    "radio version",
    "single edit",
    "single version",
    "short version",
];
const EXTENDED_PHRASES: &[&str] = &[
    "extended mix",
    "extended version",
    "extended edit",
    "long version",
    "club version",
    "12 inch",
];

fn any_token(tokens: &[&str], list: &[&str]) -> bool {
    list.iter().any(|t| tokens.contains(t))
}

fn any_phrase(tokens: &[&str], list: &[&str]) -> bool {
    list.iter().any(|p| contains_token_phrase(tokens, p))
}

fn extract_variants_from_segment(segment: &str, is_qualifier: bool) -> BTreeSet<RecordingVariant> {
    let mut found = BTreeSet::new();
    let cleaned = normalize_punctuation_and_spaces(segment);
    if cleaned.is_empty() {
        return found;
    }

    let tokens: Vec<&str> = cleaned.split_whitespace().collect();
    // Single words that only count as a marker inside a bracket or dash qualifier.
    let qualifier_token = |token: &str| is_qualifier && tokens.contains(&token);

    if any_phrase(&tokens, LIVE_PHRASES) || qualifier_token("live") {
        found.insert(RecordingVariant::Live);
    }

    // Remix / club mix markers (excluding "original mix" and "remaster").
    if any_token(&tokens, REMIX_TOKENS)
        || any_phrase(&tokens, REMIX_PHRASES)
        || (is_qualifier
            && (qualifier_token("dub")
                || qualifier_token("vip")
                || (qualifier_token("mix") && !contains_token_phrase(&tokens, "original mix"))))
    {
        found.insert(RecordingVariant::Remix);
    }

    if any_token(&tokens, ACOUSTIC_TOKENS)
        || any_phrase(&tokens, ACOUSTIC_PHRASES)
        || (is_qualifier && (qualifier_token("stripped") || qualifier_token("orchestral")))
    {
        found.insert(RecordingVariant::Acoustic);
    }

    if any_token(&tokens, KARAOKE_TOKENS) || any_phrase(&tokens, KARAOKE_PHRASES) {
        found.insert(RecordingVariant::Karaoke);
    }

    if any_token(&tokens, INSTRUMENTAL_TOKENS) || (is_qualifier && qualifier_token("inst")) {
        found.insert(RecordingVariant::Instrumental);
    }

    if any_token(&tokens, ACAPELLA_TOKENS) || any_phrase(&tokens, ACAPELLA_PHRASES) {
        found.insert(RecordingVariant::Acapella);
    }

    if any_phrase(&tokens, DEMO_PHRASES)
        || any_token(&tokens, DEMO_TOKENS)
        || qualifier_token("demo")
    {
        found.insert(RecordingVariant::Demo);
    }

    if any_token(&tokens, COVER_TOKENS)
        || any_phrase(&tokens, COVER_PHRASES)
        || qualifier_token("cover")
    {
        found.insert(RecordingVariant::CoverOrTribute);
    }

    if any_phrase(&tokens, SPEED_PHRASES)
        || any_token(&tokens, SPEED_TOKENS)
        || qualifier_token("reverb")
    {
        found.insert(RecordingVariant::SpeedAltered);
    }

    if any_phrase(&tokens, RADIO_PHRASES) {
        found.insert(RecordingVariant::RadioEdit);
    }

    if any_phrase(&tokens, EXTENDED_PHRASES) || qualifier_token("extended") {
        found.insert(RecordingVariant::Extended);
    }

    // Clean and explicit editions are whole-qualifier markers only, so a song
    // titled "Clean" keeps its base title.
    if is_qualifier {
        match cleaned.as_str() {
            "clean" | "clean version" => {
                found.insert(RecordingVariant::Clean);
            }
            "explicit" | "explicit version" => {
                found.insert(RecordingVariant::Explicit);
            }
            _ => {}
        }
    }

    found
}

fn contains_token_phrase(tokens: &[&str], phrase: &str) -> bool {
    let needle: Vec<&str> = phrase.split_whitespace().collect();
    if needle.is_empty() || needle.len() > tokens.len() {
        return false;
    }
    tokens.windows(needle.len()).any(|w| w == needle.as_slice())
}

/// Computes string similarity in `0.0..=1.0` as a fixed blend:
/// `0.45 * jaro_winkler + 0.55 * normalized_levenshtein`.
///
/// Jaro-Winkler rewards a shared prefix, so the edit-distance term carries more
/// weight. Short titles that only share a prefix therefore cannot score high.
pub fn string_similarity(a: &str, b: &str) -> f64 {
    if a == b {
        return 1.0;
    }
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let jw = jaro_winkler(a, b);
    let lev = normalized_levenshtein(a, b);
    0.45 * jw + 0.55 * lev
}

fn normalized_levenshtein(a: &str, b: &str) -> f64 {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let max_len = a_chars.len().max(b_chars.len());
    if max_len == 0 {
        return 1.0;
    }

    let mut prev: Vec<usize> = (0..=b_chars.len()).collect();
    let mut curr = vec![0usize; b_chars.len() + 1];

    for (i, &ac) in a_chars.iter().enumerate() {
        curr[0] = i + 1;
        for (j, &bc) in b_chars.iter().enumerate() {
            let cost = usize::from(ac != bc);
            curr[j + 1] = (prev[j + 1] + 1).min(curr[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    1.0 - (prev[b_chars.len()] as f64 / max_len as f64)
}

fn jaro_winkler(a: &str, b: &str) -> f64 {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let a_len = a_chars.len();
    let b_len = b_chars.len();
    if a_len == 0 && b_len == 0 {
        return 1.0;
    }
    if a_len == 0 || b_len == 0 {
        return 0.0;
    }

    let match_distance = (a_len.max(b_len) / 2).saturating_sub(1);
    let mut a_matched = vec![false; a_len];
    let mut b_matched = vec![false; b_len];
    let mut matches = 0usize;

    for i in 0..a_len {
        let start = i.saturating_sub(match_distance);
        let end = (i + match_distance + 1).min(b_len);
        for j in start..end {
            if !b_matched[j] && a_chars[i] == b_chars[j] {
                a_matched[i] = true;
                b_matched[j] = true;
                matches += 1;
                break;
            }
        }
    }

    if matches == 0 {
        return 0.0;
    }

    let mut transpositions = 0usize;
    let mut k = 0usize;
    for i in 0..a_len {
        if !a_matched[i] {
            continue;
        }
        while !b_matched[k] {
            k += 1;
        }
        if a_chars[i] != b_chars[k] {
            transpositions += 1;
        }
        k += 1;
    }

    let m = matches as f64;
    let jaro =
        (m / a_len as f64 + m / b_len as f64 + (m - (transpositions as f64 / 2.0)) / m) / 3.0;

    let prefix_len = a_chars
        .iter()
        .zip(b_chars.iter())
        .take(4)
        .take_while(|(x, y)| x == y)
        .count();

    jaro + (prefix_len as f64 * 0.1 * (1.0 - jaro))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(title: &str, artist: &str, duration_ms: u64) -> Query {
        Query {
            isrc: None,
            title: title.to_string(),
            artists: vec![artist.to_string()],
            album: "Album".to_string(),
            duration_ms,
            album_artist: None,
            track_number: None,
            disc_number: None,
            year: None,
            cover_url: None,
        }
    }

    fn candidate(
        id: &str,
        title: &str,
        artist: &str,
        duration_ms: u64,
        bit_depth: u8,
        sample_rate_hz: u32,
    ) -> Candidate {
        Candidate {
            provider: "qobuz",
            track_id: id.to_string(),
            isrc: None,
            title: title.to_string(),
            artists: vec![artist.to_string()],
            album: None,
            album_artist: None,
            track_number: None,
            disc_number: None,
            year: None,
            bit_depth: Some(bit_depth),
            sample_rate_hz: Some(sample_rate_hz),
            duration_ms,
            lossless: true,
            cover_url: None,
            source_url: None,
            lyrics_lrc: None,
        }
    }

    #[test]
    fn normalizes_title_noise_without_stripping_variants() {
        assert_eq!(
            normalize_title("Instant Crush (feat. Julian Casablancas)"),
            "instant crush"
        );
        assert_eq!(normalize_title("Heroes - 2017 Remaster"), "heroes");
        assert_eq!(
            normalize_title("Dreams [2004 Remastered Version]"),
            "dreams"
        );
        assert_eq!(
            normalize_title("Hotel California (Live)"),
            "hotel california live"
        );
    }

    #[test]
    fn variant_guard_handles_title_containing_live() {
        let studio = extract_recording_variants("Live Forever", &["Oasis".to_string()]);
        let live_recording =
            extract_recording_variants("Live Forever (Live at Maine Road)", &["Oasis".to_string()]);
        assert!(studio.is_empty());
        assert!(live_recording.contains(&RecordingVariant::Live));
        assert_ne!(studio, live_recording);
    }

    #[test]
    fn isrc_normalization_strips_hyphens_and_case() {
        assert_eq!(
            normalize_isrc("us-qx9-13-00105"),
            Some("USQX91300105".to_string())
        );
        assert_eq!(normalize_isrc("   "), None);
    }

    #[test]
    fn isrc_rejects_malformed_and_placeholder_codes() {
        assert_eq!(normalize_isrc("XX0000000000"), None);
        assert_eq!(normalize_isrc("000000000000"), None);
        assert_eq!(normalize_isrc("ZZ0000000001"), None);
        assert_eq!(normalize_isrc("US-QX9-13-0000"), None);
        assert_eq!(normalize_isrc("1234567890AB"), None);
        assert_eq!(normalize_isrc("USQX9130010"), None);
    }

    #[test]
    fn placeholder_isrc_never_matches_by_isrc() {
        for placeholder in ["XX0000000000", "000000000000"] {
            let mut q = query("Same Title", "Same Artist", 200_000);
            q.isrc = Some(placeholder.to_string());
            let mut c = candidate("c1", "Same Title", "Same Artist", 200_000, 16, 44_100);
            c.isrc = Some(placeholder.to_string());
            match select_best_match(&q, &[c], &MatchConfig::default()) {
                MatchOutcome::Matched { matched_by, .. } => {
                    assert_ne!(matched_by, MatchedBy::Isrc, "placeholder {placeholder}");
                }
                other => panic!("placeholder {placeholder}: expected a match, got {other:?}"),
            }
        }
    }

    #[test]
    fn fuzzy_tier_ranks_similarity_before_quality() {
        // "midnight ci" is a lower-similarity 24/192 recording; "midnight citys" is
        // a near-identical 16/44 recording. The near one must win.
        let far_sim = string_similarity("midnight city", "midnight ci");
        let near_sim = string_similarity("midnight city", "midnight citys");
        assert!(
            far_sim >= 0.86 && far_sim < near_sim,
            "{far_sim} {near_sim}"
        );

        let q = query("Midnight City", "M83", 361_000);
        let candidates = vec![
            candidate("far", "midnight ci", "M83", 361_000, 24, 192_000),
            candidate("near", "midnight citys", "M83", 361_500, 16, 44_100),
        ];
        match select_best_match(&q, &candidates, &MatchConfig::default()) {
            MatchOutcome::Matched {
                candidate,
                matched_by,
                selected_quality,
            } => {
                assert_eq!(matched_by, MatchedBy::Fuzzy);
                assert_eq!(candidate.track_id, "near");
                assert_eq!(selected_quality, Quality::Flac16_44);
            }
            other => panic!("expected fuzzy match, got {other:?}"),
        }
    }

    #[test]
    fn album_artist_fallback_applies_to_query_and_candidates() {
        let mut q = query("Karma Police", "", 264_000);
        q.artists = Vec::new();
        q.album_artist = Some("Radiohead".to_string());
        let mut c = candidate("c1", "Karma Police", "", 264_000, 16, 44_100);
        c.artists = Vec::new();
        c.album_artist = Some("Radiohead".to_string());
        assert!(matches!(
            select_best_match(&q, &[c.clone()], &MatchConfig::default()),
            MatchOutcome::Matched {
                matched_by: MatchedBy::Exact,
                ..
            }
        ));

        let q_plain = query("Karma Police", "Radiohead", 264_000);
        assert!(matches!(
            select_best_match(&q_plain, &[c], &MatchConfig::default()),
            MatchOutcome::Matched {
                matched_by: MatchedBy::Exact,
                ..
            }
        ));
    }

    #[test]
    fn part_numbers_are_extracted_and_compared() {
        assert_eq!(part_numbers("Shine (Pts. 1-5)"), Some(vec![1, 5]));
        assert_eq!(part_numbers("Symphony Part II"), Some(vec![2]));
        assert_eq!(part_numbers("Part of Me (Part 2)"), Some(vec![2]));
        assert_eq!(part_numbers("Part of Me"), None);
        assert!(parts_compatible(Some(&[1]), None));
        assert!(!parts_compatible(Some(&[1]), Some(&[2])));
    }

    #[test]
    fn fold_diacritics_covers_letters_missing_from_the_table() {
        for (input, expected) in [
            ("Łukasz", "lukasz"),
            ("ŁUKASZ", "lukasz"),
            ("Őrség", "orseg"),
            ("Űrhajó", "urhajo"),
            ("Ğül", "gul"),
            ("Řeka", "reka"),
            ("Ěso", "eso"),
            ("Ůly", "uly"),
            ("Šaš", "sas"),
            ("Žluť", "zlut"),
            ("Čaj", "caj"),
            ("Øyvind", "oyvind"),
            ("Đorđe", "dorde"),
            ("Izmır", "izmir"),
            ("İstanbul", "istanbul"),
        ] {
            assert_eq!(fold_diacritics(&input.to_lowercase()), expected, "{input}");
            assert_eq!(fold_diacritics(input), expected, "{input}");
        }
        assert_eq!(
            normalize_primary_artist(Some("Łukasz Gottwald")),
            normalize_primary_artist(Some("Lukasz Gottwald"))
        );
    }

    #[test]
    fn with_prefix_is_kept_as_title_text() {
        assert_eq!(
            normalize_title("With or Without You"),
            "with or without you"
        );
        assert_eq!(
            normalize_title("U2 - With or Without You"),
            "u2 with or without you"
        );
        assert_eq!(normalize_title("Song (With Strings)"), "song with strings");
    }

    #[test]
    fn clean_and_explicit_editions_are_recording_variants() {
        let clean = extract_recording_variants("Song (Clean)", &["Artist".to_string()]);
        let explicit =
            extract_recording_variants("Song (Explicit Version)", &["Artist".to_string()]);
        let plain = extract_recording_variants("Song", &["Artist".to_string()]);
        let titled = extract_recording_variants("Clean", &["Artist".to_string()]);
        assert!(clean.contains(&RecordingVariant::Clean));
        assert!(explicit.contains(&RecordingVariant::Explicit));
        assert!(plain.is_empty());
        assert!(titled.is_empty());
    }
}
