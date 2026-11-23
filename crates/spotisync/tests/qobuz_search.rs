use spotisync::providers::qobuz::{
    QobuzConfig, QobuzCredentials, QobuzProvider, parse_search_response,
};
use spotisync::{Candidate, Provider, Quality, Query, SyncError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const SEARCH_FIXTURE: &str = include_str!("fixtures/qobuz_search_response.json");

/// One canned HTTP response, served to one accepted connection.
struct Reply {
    status_line: &'static str,
    headers: String,
    body: String,
}

fn reply(status_line: &'static str, headers: &str, body: impl Into<String>) -> Reply {
    Reply {
        status_line,
        headers: headers.to_string(),
        body: body.into(),
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|pos| pos + 4)
}

/// Reads one full HTTP request: the header block up to the blank line, then
/// the body when a `Content-Length` header announces one.
async fn read_request(socket: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    let header_end = loop {
        if let Some(end) = find_header_end(&buf) {
            break end;
        }
        let n = socket.read(&mut chunk).await.expect("read request headers");
        assert!(n > 0, "client closed the connection before sending headers");
        buf.extend_from_slice(&chunk[..n]);
    };

    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let content_length = head
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);

    while buf.len() < header_end + content_length {
        let n = socket.read(&mut chunk).await.expect("read request body");
        assert!(
            n > 0,
            "client closed the connection before sending the body"
        );
        buf.extend_from_slice(&chunk[..n]);
    }
    String::from_utf8_lossy(&buf[..header_end + content_length]).into_owned()
}

/// Serves `replies` in order, one per accepted connection, and returns the raw
/// request text received for each.
async fn spawn_server(replies: Vec<Reply>) -> (String, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        let mut requests = Vec::new();
        for reply in replies {
            let (mut socket, _) = listener.accept().await.expect("accept connection");
            requests.push(read_request(&mut socket).await);
            let response = format!(
                "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n{}",
                reply.status_line,
                reply.body.len(),
                reply.headers,
                reply.body
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write response");
            let _ = socket.shutdown().await;
        }
        requests
    });
    (format!("http://{addr}"), handle)
}

fn provider_at(base_url: &str, user_token: &str) -> QobuzProvider {
    QobuzProvider::with_config(
        QobuzCredentials::new("app-123", user_token, None),
        QobuzConfig {
            base_url: base_url.to_string(),
            ..QobuzConfig::default()
        },
    )
    .expect("provider builds")
}

fn sample_query(isrc: Option<&str>) -> Query {
    Query {
        isrc: isrc.map(str::to_string),
        title: "Instant Crush".to_string(),
        artists: vec!["Daft Punk".to_string()],
        album: "Random Access Memories".to_string(),
        duration_ms: 337_000,
        album_artist: None,
        track_number: Some(5),
        disc_number: Some(1),
        year: Some(2013),
        cover_url: None,
    }
}

fn sample_candidate() -> Candidate {
    Candidate {
        provider: "qobuz",
        track_id: "10760101".to_string(),
        isrc: Some("USQX91300105".to_string()),
        title: "Instant Crush".to_string(),
        artists: vec!["Daft Punk".to_string()],
        album: Some("Random Access Memories".to_string()),
        album_artist: Some("Daft Punk".to_string()),
        track_number: Some(5),
        disc_number: Some(1),
        year: Some(2013),
        bit_depth: Some(24),
        sample_rate_hz: Some(88_200),
        duration_ms: 337_000,
        lossless: true,
        cover_url: None,
        source_url: None,
        lyrics_lrc: None,
    }
}

fn file_url_json(stream_url: &str, sample: bool) -> String {
    serde_json::json!({
        "url": stream_url,
        "format_id": 27,
        "mime_type": "audio/flac",
        "sample": sample,
    })
    .to_string()
}

#[test]
fn parses_recorded_qobuz_search_fixture() {
    let candidates = parse_search_response(SEARCH_FIXTURE).expect("fixture parses cleanly");
    assert_eq!(candidates.len(), 3);

    let first = &candidates[0];
    assert_eq!(first.provider, "qobuz");
    assert_eq!(first.track_id, "10760101");
    assert_eq!(first.isrc.as_deref(), Some("USQX91300105"));
    assert_eq!(first.title, "Instant Crush");
    assert_eq!(first.artists, vec!["Daft Punk"]);
    assert_eq!(first.album.as_deref(), Some("Random Access Memories"));
    assert_eq!(first.album_artist.as_deref(), Some("Daft Punk"));
    assert_eq!(first.track_number, Some(5));
    assert_eq!(first.disc_number, Some(1));
    assert_eq!(first.year, Some(2013));
    assert_eq!(first.bit_depth, Some(24));
    assert_eq!(first.sample_rate_hz, Some(88_200));
    assert_eq!(first.duration_ms, 337_000);
    assert!(first.lossless);
    assert_eq!(first.quality(), Some(Quality::Flac24_96));
    assert_eq!(
        first.cover_url.as_deref(),
        Some("https://static.qobuz.com/images/covers/66/92/0886443919266_600.jpg")
    );
    assert_eq!(
        first.source_url.as_deref(),
        Some("https://play.qobuz.com/track/10760101")
    );

    let second = &candidates[1];
    assert_eq!(second.title, "Instant Crush (Live)");
    assert_eq!(second.quality(), Some(Quality::Flac16_44));
    assert_eq!(
        second.cover_url.as_deref(),
        Some("https://static.qobuz.com/images/covers/99/99/0886443999999_230.jpg")
    );

    let third = &candidates[2];
    assert_eq!(third.title, "Instant Crush (Radio Edit)");
    assert!(!third.lossless);
    assert_eq!(third.quality(), None);
}

#[tokio::test]
async fn qobuz_provider_search_uses_fixture_offline() {
    let (base_url, handle) = spawn_server(vec![reply("200 OK", "", SEARCH_FIXTURE)]).await;
    let provider = provider_at(&base_url, "tok-abc");

    assert_eq!(provider.id(), "qobuz");
    assert_eq!(provider.max_quality(), Quality::Flac24_192);

    let candidates = provider
        .search(&sample_query(Some("USQX91300105")))
        .await
        .expect("search succeeds");
    assert_eq!(candidates.len(), 3);
    assert_eq!(candidates[0].isrc.as_deref(), Some("USQX91300105"));

    let requests = handle.await.expect("server finished");
    assert_eq!(
        requests.len(),
        1,
        "an eligible ISRC hit must not trigger a text search"
    );
    let raw_request = &requests[0];
    let lower = raw_request.to_ascii_lowercase();
    assert!(raw_request.starts_with("GET /track/search?"));
    assert!(raw_request.contains("query=USQX91300105"));
    assert!(lower.contains("x-app-id: app-123"));
    assert!(lower.contains("x-user-auth-token: tok-abc"));
}

#[tokio::test]
async fn isrc_hit_that_is_not_eligible_falls_back_to_text_search() {
    let isrc_only = r#"{"tracks":{"items":[{"id":7,"title":"Instant Crush","duration":337,"streamable":false,"maximum_bit_depth":24}]}}"#;
    let (base_url, handle) = spawn_server(vec![
        reply("200 OK", "", isrc_only),
        reply("200 OK", "", SEARCH_FIXTURE),
    ])
    .await;
    let provider = provider_at(&base_url, "tok-abc");

    let candidates = provider
        .search(&sample_query(Some("USQX91300105")))
        .await
        .expect("search succeeds");
    assert_eq!(candidates.len(), 3);

    let requests = handle.await.expect("server finished");
    assert_eq!(requests.len(), 2, "text search must follow the ISRC search");
    assert!(requests[0].contains("query=USQX91300105"));
    assert!(requests[1].contains("query=Daft"));
}

#[tokio::test]
async fn qobuz_provider_maps_http_error_statuses() {
    let (base_url, _) = spawn_server(vec![reply(
        "429 Too Many Requests",
        "Retry-After: 42\r\n",
        r#"{"status":"error"}"#,
    )])
    .await;
    let provider = provider_at(&base_url, "tok-abc");

    let err = provider
        .validate_credentials()
        .await
        .expect_err("429 should fail");
    match err {
        SyncError::RateLimited {
            provider,
            retry_after_secs,
            ..
        } => {
            assert_eq!(provider, "qobuz");
            assert_eq!(retry_after_secs, Some(42));
        }
        other => panic!("expected RateLimited, got {other:?}"),
    }

    let (base_url, _) =
        spawn_server(vec![reply("401 Unauthorized", "", r#"{"status":"error"}"#)]).await;
    let provider = provider_at(&base_url, "bad-token");

    let err = provider
        .validate_credentials()
        .await
        .expect_err("401 should fail");
    assert!(err.is_auth_invalid(), "expected AuthInvalid, got {err:?}");

    let (base_url, _) = spawn_server(vec![reply(
        "503 Service Unavailable",
        "",
        r#"{"status":"error"}"#,
    )])
    .await;
    let provider = provider_at(&base_url, "tok-abc");

    let err = provider
        .validate_credentials()
        .await
        .expect_err("503 should fail");
    assert!(matches!(err, SyncError::ServerError { status: 503, .. }));
}

#[tokio::test]
async fn preview_sample_is_a_per_track_error_not_an_auth_failure() {
    let (api_base, _api) = spawn_server(vec![reply(
        "200 OK",
        "",
        file_url_json("http://127.0.0.1:9/preview.flac", true),
    )])
    .await;
    let provider = provider_at(&api_base, "tok-abc");

    let err = provider
        .fetch(&sample_candidate(), Quality::Flac24_96)
        .await
        .expect_err("a preview sample must not be accepted");
    assert!(matches!(err, SyncError::PreviewOnly(_)), "got {err:?}");
    assert!(
        !err.is_auth_invalid(),
        "valid credentials must not look invalid"
    );
}

#[tokio::test]
async fn fetch_rejects_payload_shorter_than_flac_header() {
    let (stream_base, _stream) = spawn_server(vec![reply("200 OK", "", "fLaC")]).await;
    let (api_base, _api) = spawn_server(vec![reply(
        "200 OK",
        "",
        file_url_json(&format!("{stream_base}/track.flac"), false),
    )])
    .await;
    let provider = provider_at(&api_base, "tok-abc");

    let err = provider
        .fetch(&sample_candidate(), Quality::Flac24_96)
        .await
        .expect_err("a 4-byte payload must be rejected");
    assert!(matches!(err, SyncError::InvalidAudio(_)), "got {err:?}");
}

#[tokio::test]
async fn fetch_accepts_payload_at_minimum_flac_header_length() {
    let payload = format!("fLaC{}", "\0".repeat(38));
    let (stream_base, _stream) = spawn_server(vec![reply("200 OK", "", payload.clone())]).await;
    let (api_base, _api) = spawn_server(vec![reply(
        "200 OK",
        "",
        file_url_json(&format!("{stream_base}/track.flac"), false),
    )])
    .await;
    let provider = provider_at(&api_base, "tok-abc");

    let bytes = provider
        .fetch(&sample_candidate(), Quality::Flac24_96)
        .await
        .expect("42-byte FLAC header passes");
    assert_eq!(bytes.len(), 42);
}

#[tokio::test]
async fn transport_errors_do_not_expose_signed_stream_url() {
    // Bind and drop a listener so the stream URL points at a closed port.
    let closed = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback")
        .local_addr()
        .expect("local addr");
    let stream_url = format!("http://{closed}/track.flac?request_sig=SECRETSIG123&request_ts=1");
    let (api_base, _api) =
        spawn_server(vec![reply("200 OK", "", file_url_json(&stream_url, false))]).await;
    let provider = provider_at(&api_base, "tok-abc");

    let err = provider
        .fetch(&sample_candidate(), Quality::Flac24_96)
        .await
        .expect_err("connection to a closed port must fail");
    assert!(matches!(err, SyncError::Http(_)), "got {err:?}");

    let shown = format!("{err} {err:?}");
    assert!(!shown.contains("SECRETSIG123"), "signature leaked: {shown}");
    assert!(!shown.contains("request_sig"), "query leaked: {shown}");
}

#[tokio::test]
async fn credential_client_does_not_follow_cross_origin_redirects() {
    // The redirect target would answer with a valid search result, so a
    // followed redirect would make the search succeed.
    let (other_base, other) = spawn_server(vec![reply("200 OK", "", SEARCH_FIXTURE)]).await;
    let (base_url, _redirecting) = spawn_server(vec![reply(
        "302 Found",
        &format!("Location: {other_base}/track/search?query=x\r\n"),
        "",
    )])
    .await;
    let provider = provider_at(&base_url, "tok-abc");

    let err = provider
        .search(&sample_query(None))
        .await
        .expect_err("cross-origin redirect must not be followed");
    assert!(matches!(err, SyncError::Provider { .. }), "got {err:?}");
    other.abort();
}
