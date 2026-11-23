use serde::Deserialize;
use spotisync::matching::{MatchConfig, MatchOutcome, MatchedBy, RejectReason, select_best_match};
use spotisync::{Candidate, Quality, Query};

const MATCHING_FIXTURE: &str = include_str!("fixtures/matching_50_tracks.json");
const REVIEW_FIXTURE: &str = include_str!("fixtures/matching_review_cases.json");

#[derive(Debug, Deserialize)]
struct FixtureCase {
    id: String,
    category: String,
    #[serde(default)]
    max_quality: Option<String>,
    query: Query,
    candidates: Vec<FixtureCandidate>,
    expected: ExpectedOutcome,
}

#[derive(Debug, Deserialize)]
struct FixtureCandidate {
    track_id: String,
    isrc: Option<String>,
    title: String,
    artists: Vec<String>,
    #[serde(default)]
    album_artist: Option<String>,
    duration_ms: u64,
    bit_depth: Option<u8>,
    sample_rate_hz: Option<u32>,
    lossless: bool,
}

impl From<FixtureCandidate> for Candidate {
    fn from(c: FixtureCandidate) -> Self {
        Self {
            provider: "qobuz",
            track_id: c.track_id,
            isrc: c.isrc,
            title: c.title,
            artists: c.artists,
            album: None,
            album_artist: c.album_artist,
            track_number: None,
            disc_number: None,
            year: None,
            bit_depth: c.bit_depth,
            sample_rate_hz: c.sample_rate_hz,
            duration_ms: c.duration_ms,
            lossless: c.lossless,
            cover_url: None,
            source_url: None,
            lyrics_lrc: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ExpectedOutcome {
    outcome: String,
    #[serde(default)]
    track_id: Option<String>,
    #[serde(default)]
    quality: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

/// Result of running every fixture case through the matcher.
struct Evaluation {
    total: usize,
    correct: usize,
    wrong_recording_matches: usize,
    failures: Vec<String>,
}

impl Evaluation {
    fn accuracy_percent(&self) -> f64 {
        (self.correct as f64 / self.total as f64) * 100.0
    }
}

fn parse_cases(json: &str) -> Vec<FixtureCase> {
    serde_json::from_str(json).expect("matching fixture JSON parses")
}

fn evaluate(cases: Vec<FixtureCase>) -> Evaluation {
    let total = cases.len();
    let mut correct = 0usize;
    let mut wrong_recording_matches = 0usize;
    let mut failures = Vec::new();

    for case in cases {
        let mut config = MatchConfig::default();
        if let Some(ceiling) = case.max_quality.as_deref() {
            config.max_quality = Quality::parse(ceiling).expect("valid max_quality");
        }
        let candidates: Vec<Candidate> = case.candidates.into_iter().map(Into::into).collect();
        let actual = select_best_match(&case.query, &candidates, &config);

        let matched_expectation = match case.expected.outcome.as_str() {
            "isrc" | "exact" | "fuzzy" => {
                let want_by = match case.expected.outcome.as_str() {
                    "isrc" => MatchedBy::Isrc,
                    "exact" => MatchedBy::Exact,
                    _ => MatchedBy::Fuzzy,
                };
                let want_track = case.expected.track_id.as_deref().unwrap();
                let want_quality =
                    Quality::parse(case.expected.quality.as_deref().unwrap()).unwrap();
                match &actual {
                    MatchOutcome::Matched {
                        candidate,
                        matched_by,
                        selected_quality,
                    } => {
                        if candidate.track_id != want_track {
                            wrong_recording_matches += 1;
                            false
                        } else {
                            *matched_by == want_by && *selected_quality == want_quality
                        }
                    }
                    MatchOutcome::Unmatched { .. } => false,
                }
            }
            "unmatched" => {
                let want_reason = match case.expected.reason.as_deref() {
                    Some("lossy_only") => RejectReason::LossyOnly,
                    Some("no_candidates") => RejectReason::NoCandidates,
                    _ => RejectReason::NoStrictMatch,
                };
                match &actual {
                    MatchOutcome::Matched { .. } => {
                        wrong_recording_matches += 1;
                        false
                    }
                    MatchOutcome::Unmatched { reason } => *reason == want_reason,
                }
            }
            other => panic!("unexpected expected.outcome: {other}"),
        };

        if matched_expectation {
            correct += 1;
        } else {
            failures.push(format!(
                "{} [{}]: expected {:?}, got {:?}",
                case.id, case.category, case.expected, actual
            ));
        }
    }

    Evaluation {
        total,
        correct,
        wrong_recording_matches,
        failures,
    }
}

#[test]
fn fifty_track_matching_fixture_accuracy_and_zero_wrong_recordings() {
    let cases = parse_cases(MATCHING_FIXTURE);
    assert_eq!(cases.len(), 50, "fixture suite must contain 50 tracks");

    let report = evaluate(cases);
    assert!(
        report.failures.is_empty(),
        "{} of {} fixtures failed ({} wrong-recording matches, accuracy {:.1}%):\n{}",
        report.failures.len(),
        report.total,
        report.wrong_recording_matches,
        report.accuracy_percent(),
        report.failures.join("\n")
    );
}

#[test]
fn review_regression_fixtures_hold() {
    let cases = parse_cases(REVIEW_FIXTURE);
    assert!(
        !cases.is_empty(),
        "review regression suite must not be empty"
    );

    let report = evaluate(cases);
    assert!(
        report.failures.is_empty(),
        "{} of {} review regression fixtures failed ({} wrong-recording matches):\n{}",
        report.failures.len(),
        report.total,
        report.wrong_recording_matches,
        report.failures.join("\n")
    );
}

#[test]
fn empty_candidate_list_returns_no_candidates_reason() {
    let query = Query {
        isrc: None,
        title: "Instant Crush".to_string(),
        artists: vec!["Daft Punk".to_string()],
        album: "Random Access Memories".to_string(),
        duration_ms: 337_000,
        album_artist: None,
        track_number: None,
        disc_number: None,
        year: None,
        cover_url: None,
    };
    let outcome = select_best_match(&query, &[], &MatchConfig::default());
    assert_eq!(
        outcome,
        MatchOutcome::Unmatched {
            reason: RejectReason::NoCandidates
        }
    );
}
