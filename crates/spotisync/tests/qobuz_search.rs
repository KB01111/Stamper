use spotisync::providers::qobuz::{
    QobuzConfig, QobuzCredentials, QobuzProvider, parse_search_response,
};
use spotisync::{Provider, Quality, Query, SyncError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const SEARCH_FIXTURE: &str = include_str!("fixtures/qobuz_search_response.json");

async fn spawn_single_response_server(
    status_line: &'static str,
    extra_headers: &'static str,
    body: String,
) -> (String, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let mut buf = vec![0u8; 8192];
        let n = socket.read(&mut buf).await.expect("read request");
        let request = String::from_utf8_lossy(&buf[..n]).into_owned();
        let response = format!(
            "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n{body}",
            body.len()
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write response");
        let _ = socket.shutdown().await;
        request
    });
    (format!("http://{addr}"), handle)
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
    let (base_url, req_handle) =
        spawn_single_response_server("200 OK", "", SEARCH_FIXTURE.to_string()).await;

    let provider = QobuzProvider::with_config(
        QobuzCredentials::new("app-123", "tok-abc", None),
        QobuzConfig {
            base_url,
            ..QobuzConfig::default()
        },
    )
    .expect("provider builds");

    assert_eq!(provider.id(), "qobuz");
    assert_eq!(provider.max_quality(), Quality::Flac24_192);

    let query = Query {
        isrc: Some("USQX91300105".to_string()),
        title: "Instant Crush".to_string(),
        artists: vec!["Daft Punk".to_string()],
        album: "Random Access Memories".to_string(),
        duration_ms: 337_000,
        album_artist: None,
        track_number: Some(5),
        disc_number: Some(1),
        year: Some(2013),
        cover_url: None,
    };

    let candidates = provider.search(&query).await.expect("search succeeds");
    assert_eq!(candidates.len(), 3);
    assert_eq!(candidates[0].isrc.as_deref(), Some("USQX91300105"));

    let raw_request = req_handle.await.expect("captured request");
    let lower = raw_request.to_ascii_lowercase();
    assert!(raw_request.starts_with("GET /track/search?"));
    assert!(raw_request.contains("query=USQX91300105"));
    assert!(lower.contains("x-app-id: app-123"));
    assert!(lower.contains("x-user-auth-token: tok-abc"));
}

#[tokio::test]
async fn qobuz_provider_maps_http_error_statuses() {
    let (base_url, _) = spawn_single_response_server(
        "429 Too Many Requests",
        "Retry-After: 42\r\n",
        r#"{"status":"error"}"#.to_string(),
    )
    .await;

    let provider = QobuzProvider::with_config(
        QobuzCredentials::new("app-123", "tok-abc", None),
        QobuzConfig {
            base_url,
            ..QobuzConfig::default()
        },
    )
    .expect("provider builds");

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
        spawn_single_response_server("401 Unauthorized", "", r#"{"status":"error"}"#.to_string())
            .await;
    let provider = QobuzProvider::with_config(
        QobuzCredentials::new("app-123", "bad-token", None),
        QobuzConfig {
            base_url,
            ..QobuzConfig::default()
        },
    )
    .expect("provider builds");

    let err = provider
        .validate_credentials()
        .await
        .expect_err("401 should fail");
    assert!(err.is_auth_invalid(), "expected AuthInvalid, got {err:?}");

    let (base_url, _) = spawn_single_response_server(
        "503 Service Unavailable",
        "",
        r#"{"status":"error"}"#.to_string(),
    )
    .await;
    let provider = QobuzProvider::with_config(
        QobuzCredentials::new("app-123", "tok-abc", None),
        QobuzConfig {
            base_url,
            ..QobuzConfig::default()
        },
    )
    .expect("provider builds");

    let err = provider
        .validate_credentials()
        .await
        .expect_err("503 should fail");
    assert!(matches!(err, SyncError::ServerError { status: 503, .. }));
}
