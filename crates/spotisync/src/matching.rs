//! Strict track matching between Spotify queries and provider candidates.
//!
//! Matching follows strict ordered rules and never silently substitutes a
//! different recording (such as live, remix, acoustic, karaoke, instrumental,
//! demo, cover, sped-up, slowed, radio edit, or extended variants):
//!
//! 1. Exact ISRC match (case-insensitive, stripped hyphens and spaces) -> [`MatchedBy::Isrc`].
//! 2. Exact normalized title + primary artist match within tight duration
//!    tolerance (default +/- 2000 ms) and matching recording variants -> [`MatchedBy::Exact`].
//! 3. High-confidence fuzzy title + primary artist similarity within duration
//!    tolerance (default +/- 5000 ms) and matching recording variants -> [`MatchedBy::Fuzzy`].
//! 4. Otherwise [`MatchOutcome::Unmatched`].

use crate::{Candidate, Quality, Query};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

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
}

struct ScoredMatch<'a> {
    candidate: &'a Candidate,
    selected_quality: Quality,
    native_quality: Quality,
    similarity_bps: u32,
    duration_delta_ms: u64,
}

/// Selects the best lossless candidate matching `query` according to strict rules.
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

    // Tier 1: Exact ISRC match.
    if let Some(q_isrc) = query.isrc.as_deref().and_then(normalize_isrc) {
        let mut lossless_hits: Vec<ScoredMatch<'_>> = Vec::new();
        for cand in candidates {
            let Some(c_isrc) = cand.isrc.as_deref().and_then(normalize_isrc) else {
                continue;
            };
            if c_isrc != q_isrc {
                continue;
            }
            if let Some(native_q) = cand.quality() {
                lossless_hits.push(ScoredMatch {
                    candidate: cand,
                    selected_quality: native_q.min(config.max_quality),
                    native_quality: native_q,
                    similarity_bps: 10_000,
                    duration_delta_ms: query.duration_ms.abs_diff(cand.duration_ms),
                });
            } else {
                saw_lossy_match = true;
            }
        }
        if let Some(best) = pick_highest_quality(lossless_hits) {
            return finalize_match(query, best, MatchedBy::Isrc);
        }
    }

    let query_title_norm = normalize_title(&query.title);
    let query_artist_norm = normalize_primary_artist(query.artists.first().map(String::as_str));
    let query_variants = extract_recording_variants(&query.title, &query.artists);

    if query_title_norm.is_empty() || query_artist_norm.is_empty() {
        return MatchOutcome::Unmatched {
            reason: if saw_lossy_match {
                RejectReason::LossyOnly
            } else {
                RejectReason::NoStrictMatch
            },
        };
    }

    // Tier 2: Exact normalized title + primary artist match within exact duration tolerance.
    let mut exact_hits: Vec<ScoredMatch<'_>> = Vec::new();
    for cand in candidates {
        let duration_delta = query.duration_ms.abs_diff(cand.duration_ms);
        if duration_delta > config.exact_duration_tolerance_ms {
            continue;
        }
        let cand_variants = extract_recording_variants(&cand.title, &cand.artists);
        if cand_variants != query_variants {
            continue;
        }

        let cand_title_norm = normalize_title(&cand.title);
        if cand_title_norm != query_title_norm {
            continue;
        }

        let cand_artist_norm = normalize_primary_artist(candidate_primary_artist(cand));
        if cand_artist_norm != query_artist_norm {
            continue;
        }

        if let Some(native_q) = cand.quality() {
            exact_hits.push(ScoredMatch {
                candidate: cand,
                selected_quality: native_q.min(config.max_quality),
                native_quality: native_q,
                similarity_bps: 10_000,
                duration_delta_ms: duration_delta,
            });
        } else {
            saw_lossy_match = true;
        }
    }
    if let Some(best) = pick_highest_quality(exact_hits) {
        return finalize_match(query, best, MatchedBy::Exact);
    }

    // Tier 3: Strict fuzzy match within fuzzy duration tolerance and variant guards.
    let query_artist_fuzzy = strip_leading_the(&query_artist_norm);
    let mut fuzzy_hits: Vec<ScoredMatch<'_>> = Vec::new();
    for cand in candidates {
        let duration_delta = query.duration_ms.abs_diff(cand.duration_ms);
        if duration_delta > config.fuzzy_duration_tolerance_ms {
            continue;
        }
        let cand_variants = extract_recording_variants(&cand.title, &cand.artists);
        if cand_variants != query_variants {
            continue;
        }

        let cand_title_norm = normalize_title(&cand.title);
        let cand_artist_norm = normalize_primary_artist(candidate_primary_artist(cand));
        if cand_title_norm.is_empty() || cand_artist_norm.is_empty() {
            continue;
        }

        let title_sim = string_similarity(&query_title_norm, &cand_title_norm);
        if title_sim < config.fuzzy_title_threshold {
            continue;
        }

        let cand_artist_fuzzy = strip_leading_the(&cand_artist_norm);
        let artist_sim = string_similarity(&query_artist_norm, &cand_artist_norm)
            .max(string_similarity(query_artist_fuzzy, cand_artist_fuzzy));
        if artist_sim < config.fuzzy_artist_threshold {
            continue;
        }

        let combined_bps = ((title_sim * 0.6 + artist_sim * 0.4) * 10_000.0).round() as u32;
        if let Some(native_q) = cand.quality() {
            fuzzy_hits.push(ScoredMatch {
                candidate: cand,
                selected_quality: native_q.min(config.max_quality),
                native_quality: native_q,
                similarity_bps: combined_bps,
                duration_delta_ms: duration_delta,
            });
        } else {
            saw_lossy_match = true;
        }
    }
    if let Some(best) = pick_highest_quality(fuzzy_hits) {
        return finalize_match(query, best, MatchedBy::Fuzzy);
    }

    MatchOutcome::Unmatched {
        reason: if saw_lossy_match {
            RejectReason::LossyOnly
        } else {
            RejectReason::NoStrictMatch
        },
    }
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

fn pick_highest_quality<'a>(mut matches: Vec<ScoredMatch<'a>>) -> Option<ScoredMatch<'a>> {
    if matches.is_empty() {
        return None;
    }
    matches.sort_by(|a, b| {
        b.selected_quality
            .cmp(&a.selected_quality)
            .then_with(|| b.similarity_bps.cmp(&a.similarity_bps))
            .then_with(|| a.duration_delta_ms.cmp(&b.duration_delta_ms))
            .then_with(|| b.native_quality.cmp(&a.native_quality))
    });
    matches.into_iter().next()
}

fn candidate_primary_artist(cand: &Candidate) -> Option<&str> {
    cand.artists
        .first()
        .map(String::as_str)
        .filter(|s| !s.trim().is_empty())
        .or(cand.album_artist.as_deref())
}

/// Normalizes an ISRC by stripping spaces and hyphens and uppercasing.
pub fn normalize_isrc(raw: &str) -> Option<String> {
    let cleaned: String = raw
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .map(|c| c.to_ascii_uppercase())
        .collect();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
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

fn is_ignorable_qualifier(raw: &str) -> bool {
    let cleaned = normalize_punctuation_and_spaces(raw);
    if cleaned.is_empty() {
        return true;
    }
    if cleaned.starts_with("feat ")
        || cleaned.starts_with("ft ")
        || cleaned.starts_with("featuring ")
        || cleaned.starts_with("with ")
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
            | "explicit"
            | "explicit version"
            | "clean"
            | "clean version"
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

fn fold_diacritics(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            'á' | 'à' | 'â' | 'ä' | 'ã' | 'å' | 'ā' => out.push('a'),
            'æ' => out.push_str("ae"),
            'ç' | 'ć' | 'č' => out.push('c'),
            'é' | 'è' | 'ê' | 'ë' | 'ē' => out.push('e'),
            'í' | 'ì' | 'î' | 'ï' | 'ī' => out.push('i'),
            'ñ' | 'ń' => out.push('n'),
            'ó' | 'ò' | 'ô' | 'ö' | 'õ' | 'ø' | 'ō' => out.push('o'),
            'œ' => out.push_str("oe"),
            'ś' | 'š' => out.push('s'),
            'ß' => out.push_str("ss"),
            'ú' | 'ù' | 'û' | 'ü' | 'ū' => out.push('u'),
            'ý' | 'ÿ' => out.push('y'),
            'ź' | 'ž' => out.push('z'),
            other => out.push(other),
        }
    }
    out
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

fn extract_variants_from_segment(segment: &str, is_qualifier: bool) -> BTreeSet<RecordingVariant> {
    let mut found = BTreeSet::new();
    let cleaned = normalize_punctuation_and_spaces(segment);
    if cleaned.is_empty() {
        return found;
    }

    let tokens: Vec<&str> = cleaned.split_whitespace().collect();
    let has_token = |t: &str| tokens.contains(&t);
    let has_phrase = |phrase: &str| contains_token_phrase(&tokens, phrase);

    // Live markers.
    if has_phrase("live at")
        || has_phrase("live from")
        || has_phrase("live in")
        || has_phrase("live on")
        || has_phrase("live session")
        || has_phrase("live version")
        || has_phrase("in concert")
        || has_phrase("bbc session")
        || has_phrase("peel session")
        || (is_qualifier && has_token("live"))
    {
        found.insert(RecordingVariant::Live);
    }

    // Remix / club mix markers (excluding "original mix" and "remaster").
    if has_token("remix")
        || has_token("remixed")
        || has_token("rmx")
        || has_token("rework")
        || has_token("refix")
        || has_token("bootleg")
        || has_phrase("club mix")
        || has_phrase("dance mix")
        || has_phrase("house mix")
        || has_phrase("dub mix")
        || (is_qualifier
            && (has_token("dub")
                || has_token("vip")
                || (has_token("mix") && !has_phrase("original mix"))))
    {
        found.insert(RecordingVariant::Remix);
    }

    // Acoustic / unplugged markers.
    if has_token("acoustic")
        || has_token("unplugged")
        || has_phrase("piano version")
        || has_phrase("orchestral version")
        || has_phrase("string version")
        || (is_qualifier && (has_token("stripped") || has_token("orchestral")))
    {
        found.insert(RecordingVariant::Acoustic);
    }

    // Karaoke markers.
    if has_token("karaoke")
        || has_phrase("backing track")
        || has_phrase("sing along")
        || has_phrase("minus one")
    {
        found.insert(RecordingVariant::Karaoke);
    }

    // Instrumental markers.
    if has_token("instrumental") || (is_qualifier && has_token("inst")) {
        found.insert(RecordingVariant::Instrumental);
    }

    // Acapella markers.
    if has_token("acapella") || has_phrase("a cappella") || has_phrase("vocals only") {
        found.insert(RecordingVariant::Acapella);
    }

    // Demo / alternate take markers.
    if has_phrase("demo version")
        || has_phrase("early version")
        || has_phrase("rough mix")
        || has_phrase("alternate take")
        || has_phrase("alt take")
        || has_phrase("alternate version")
        || has_token("outtake")
        || (is_qualifier && has_token("demo"))
    {
        found.insert(RecordingVariant::Demo);
    }

    // Cover / tribute markers.
    if has_token("tribute")
        || has_phrase("originally performed by")
        || has_phrase("in the style of")
        || has_phrase("made famous by")
        || has_phrase("cover version")
        || (is_qualifier && has_token("cover"))
    {
        found.insert(RecordingVariant::CoverOrTribute);
    }

    // Sped up / slowed / nightcore markers.
    if has_phrase("sped up")
        || has_phrase("speed up")
        || has_token("nightcore")
        || has_token("slowed")
        || has_phrase("8d audio")
        || (is_qualifier && has_token("reverb"))
    {
        found.insert(RecordingVariant::SpeedAltered);
    }

    // Radio edit / single edit markers.
    if has_phrase("radio edit")
        || has_phrase("radio version")
        || has_phrase("single edit")
        || has_phrase("single version")
        || has_phrase("short version")
    {
        found.insert(RecordingVariant::RadioEdit);
    }

    // Extended / 12-inch markers.
    if has_phrase("extended mix")
        || has_phrase("extended version")
        || has_phrase("extended edit")
        || has_phrase("long version")
        || has_phrase("club version")
        || has_phrase("12 inch")
        || (is_qualifier && has_token("extended"))
    {
        found.insert(RecordingVariant::Extended);
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

/// Computes strict string similarity in `0.0..=1.0`, blending Jaro-Winkler and
/// normalized Levenshtein distance so prefix coincidences cannot inflate scores.
pub fn string_similarity(a: &str, b: &str) -> f64 {
    if a == b {
        return 1.0;
    }
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let jw = jaro_winkler(a, b);
    let lev = normalized_levenshtein(a, b);
    // Require both metrics to agree closely so short prefix-similar titles cannot slip through.
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
}
