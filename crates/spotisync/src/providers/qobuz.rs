//! Qobuz lossless provider implementation.
//!
//! Communicates with the Qobuz JSON API using user-supplied credentials,
//! enforces FLAC-only responses, maps rate limits and server errors cleanly,
//! and redacts all credentials from `Debug` output and logs.

use crate::providers::Provider;
use crate::{Candidate, Quality, Query, Result, SyncError};
use async_trait::async_trait;
use bytes::Bytes;
use md5::{Digest, Md5};
use reqwest::{Client, Response, StatusCode};
use serde::Deserialize;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Default Qobuz JSON API base URL.
pub const DEFAULT_BASE_URL: &str = "https://www.qobuz.com/api.json/0.2";

/// Stable provider identifier for Qobuz.
pub const PROVIDER_ID: &str = "qobuz";

const FLAC_MAGIC: &[u8; 4] = b"fLaC";
const FORMAT_MP3_320: u32 = 5;
const FORMAT_FLAC_16_44: u32 = 6;
const FORMAT_FLAC_24_96: u32 = 7;
const FORMAT_FLAC_24_192: u32 = 27;

/// User-supplied Qobuz credentials.
///
/// All secret fields are redacted in `Debug` output so credentials are never
/// accidentally written to logs.
#[derive(Clone, PartialEq, Eq)]
pub struct QobuzCredentials {
    pub app_id: String,
    pub user_auth_token: String,
    pub app_secret: Option<String>,
}

impl QobuzCredentials {
    /// Creates a new credential bundle.
    pub fn new(
        app_id: impl Into<String>,
        user_auth_token: impl Into<String>,
        app_secret: Option<String>,
    ) -> Self {
        Self {
            app_id: app_id.into(),
            user_auth_token: user_auth_token.into(),
            app_secret,
        }
    }
}

impl fmt::Debug for QobuzCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QobuzCredentials")
            .field("app_id", &"[REDACTED]")
            .field("user_auth_token", &"[REDACTED]")
            .field(
                "app_secret",
                &self.app_secret.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// Configuration for [`QobuzProvider`].
#[derive(Debug, Clone)]
pub struct QobuzConfig {
    pub base_url: String,
    pub search_limit: u32,
    pub max_quality: Quality,
    pub timeout: Duration,
}

impl Default for QobuzConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            search_limit: 20,
            max_quality: Quality::Flac24_192,
            timeout: Duration::from_secs(30),
        }
    }
}

/// Lossless provider backed by the Qobuz API.
pub struct QobuzProvider {
    credentials: QobuzCredentials,
    config: QobuzConfig,
    client: Client,
}

impl fmt::Debug for QobuzProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QobuzProvider")
            .field("credentials", &self.credentials)
            .field("config", &self.config)
            .finish()
    }
}

impl QobuzProvider {
    /// Constructs a Qobuz provider with default configuration.
    pub fn new(credentials: QobuzCredentials) -> Result<Self> {
        Self::with_config(credentials, QobuzConfig::default())
    }

    /// Constructs a Qobuz provider with custom configuration (for example, a local test base URL).
    pub fn with_config(credentials: QobuzCredentials, config: QobuzConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(config.timeout)
            .user_agent("spotisync/0.1")
            .build()?;
        Ok(Self {
            credentials,
            config,
            client,
        })
    }

    /// Validates credentials via a lightweight authenticated API request.
    pub async fn validate_credentials(&self) -> Result<()> {
        if self.credentials.app_id.trim().is_empty()
            || self.credentials.user_auth_token.trim().is_empty()
        {
            return Err(SyncError::AuthInvalid(
                "Qobuz app_id and user_auth_token must be non-empty".to_string(),
            ));
        }

        let url = format!("{}/favorite/getUserFavorites", self.base_url());
        let response = self
            .client
            .get(&url)
            .header("X-App-Id", &self.credentials.app_id)
            .header("X-User-Auth-Token", &self.credentials.user_auth_token)
            .query(&[("limit", "1")])
            .send()
            .await?;

        let _ = check_http_status(response, "validate_credentials").await?;
        Ok(())
    }

    fn base_url(&self) -> &str {
        self.config.base_url.trim_end_matches('/')
    }

    async fn search_raw(&self, query_text: &str) -> Result<Vec<Candidate>> {
        let url = format!("{}/track/search", self.base_url());
        let limit = self.config.search_limit.to_string();
        let response = self
            .client
            .get(&url)
            .header("X-App-Id", &self.credentials.app_id)
            .header("X-User-Auth-Token", &self.credentials.user_auth_token)
            .query(&[("query", query_text), ("limit", limit.as_str())])
            .send()
            .await?;

        let response = check_http_status(response, "track/search").await?;
        let body = response.text().await?;
        parse_search_response(&body)
    }
}

#[async_trait]
impl Provider for QobuzProvider {
    fn id(&self) -> &'static str {
        PROVIDER_ID
    }

    fn max_quality(&self) -> Quality {
        self.config.max_quality
    }

    async fn search(&self, q: &Query) -> Result<Vec<Candidate>> {
        if let Some(isrc) = q.isrc.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            let candidates = self.search_raw(isrc).await?;
            if !candidates.is_empty() {
                return Ok(candidates);
            }
        }

        let text_query = build_text_query(q);
        if text_query.is_empty() {
            return Ok(Vec::new());
        }
        self.search_raw(&text_query).await
    }

    async fn fetch(&self, c: &Candidate, q: Quality) -> Result<Bytes> {
        if !c.lossless {
            return Err(SyncError::LossyOnly(format!(
                "candidate {} is not marked lossless",
                c.track_id
            )));
        }

        let requested_quality = q.min(self.config.max_quality);
        let format_id = quality_to_format_id(requested_quality);
        let format_id_str = format_id.to_string();

        let mut params: Vec<(&str, String)> = vec![
            ("track_id", c.track_id.clone()),
            ("format_id", format_id_str),
            ("intent", "stream".to_string()),
        ];

        if let Some(secret) = self
            .credentials
            .app_secret
            .as_deref()
            .filter(|s| !s.is_empty())
        {
            let ts = unix_timestamp_secs();
            let sig = sign_get_file_url(&c.track_id, format_id, ts, secret);
            params.push(("request_ts", ts.to_string()));
            params.push(("request_sig", sig));
        }

        let url = format!("{}/track/getFileUrl", self.base_url());
        let response = self
            .client
            .get(&url)
            .header("X-App-Id", &self.credentials.app_id)
            .header("X-User-Auth-Token", &self.credentials.user_auth_token)
            .query(&params)
            .send()
            .await?;

        let response = check_http_status(response, "track/getFileUrl").await?;
        let file_url_resp: QobuzFileUrlResponse = response
            .json()
            .await
            .map_err(|e| SyncError::Parse(format!("failed to decode getFileUrl response: {e}")))?;

        if file_url_resp.sample == Some(true) {
            return Err(SyncError::AuthInvalid(
                "Qobuz returned a preview sample instead of a full stream".to_string(),
            ));
        }

        if file_url_resp.format_id == Some(FORMAT_MP3_320) {
            return Err(SyncError::LossyOnly(
                "Qobuz returned lossy format_id 5 (MP3)".to_string(),
            ));
        }

        if let Some(mime) = file_url_resp.mime_type.as_deref()
            && !mime.to_ascii_lowercase().contains("flac")
        {
            return Err(SyncError::LossyOnly(format!(
                "Qobuz returned non-FLAC mime type: {mime}"
            )));
        }

        let stream_url = file_url_resp
            .url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                SyncError::NotFound(format!("Qobuz returned no stream URL for {}", c.track_id))
            })?;

        let stream_resp = self.client.get(stream_url).send().await?;
        let stream_resp = check_http_status(stream_resp, "stream download").await?;
        let bytes = stream_resp.bytes().await?;

        if bytes.len() < FLAC_MAGIC.len() || &bytes[..4] != FLAC_MAGIC {
            return Err(SyncError::LossyOnly(format!(
                "Qobuz stream payload for {} did not start with FLAC magic header",
                c.track_id
            )));
        }

        Ok(bytes)
    }
}

/// Parses a Qobuz `/track/search` JSON response into normalized [`Candidate`] items.
pub fn parse_search_response(json: &str) -> Result<Vec<Candidate>> {
    let payload: QobuzSearchResponse = serde_json::from_str(json)
        .map_err(|e| SyncError::Parse(format!("invalid Qobuz search JSON: {e}")))?;

    let items = payload.tracks.map(|t| t.items).unwrap_or_default();
    let mut candidates = Vec::with_capacity(items.len());

    for item in items {
        let title = combine_title_and_version(&item.title, item.version.as_deref());
        let album_title = item.album.as_ref().map(|a| a.title.clone());
        let album_artist = item
            .album
            .as_ref()
            .and_then(|a| a.artist.as_ref())
            .map(|a| a.name.clone());

        let mut artists = Vec::new();
        if let Some(performer) = item.performer.as_ref()
            && !performer.name.trim().is_empty()
        {
            artists.push(performer.name.trim().to_string());
        }
        if artists.is_empty()
            && let Some(aa) = album_artist.as_ref()
            && !aa.trim().is_empty()
        {
            artists.push(aa.trim().to_string());
        }

        let year = item
            .album
            .as_ref()
            .and_then(|a| a.release_date_original.as_deref())
            .and_then(parse_year_prefix);

        let cover_url = item
            .album
            .as_ref()
            .and_then(|a| a.image.as_ref())
            .and_then(|img| {
                img.large
                    .clone()
                    .or_else(|| img.small.clone())
                    .or_else(|| img.thumbnail.clone())
            });

        let sample_rate_hz = item.maximum_sampling_rate.map(khz_or_hz_to_hz);
        let bit_depth = item.maximum_bit_depth;
        let streamable = item.streamable.unwrap_or(true);
        let lossless = streamable && bit_depth.unwrap_or(16) >= 16;
        let track_id = item.id.to_string();

        candidates.push(Candidate {
            provider: PROVIDER_ID,
            source_url: Some(format!("https://play.qobuz.com/track/{track_id}")),
            track_id,
            isrc: item
                .isrc
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            title,
            artists,
            album: album_title,
            album_artist,
            track_number: item.track_number,
            disc_number: item.media_number,
            year,
            bit_depth,
            sample_rate_hz,
            duration_ms: item.duration.saturating_mul(1000),
            lossless,
            cover_url,
            lyrics_lrc: None,
        });
    }

    Ok(candidates)
}

/// Maps a [`Quality`] tier to the Qobuz `format_id` parameter.
pub const fn quality_to_format_id(quality: Quality) -> u32 {
    match quality {
        Quality::Flac16_44 => FORMAT_FLAC_16_44,
        Quality::Flac24_96 => FORMAT_FLAC_24_96,
        Quality::Flac24_192 => FORMAT_FLAC_24_192,
    }
}

/// Computes the Qobuz `request_sig` MD5 signature for `track/getFileUrl`.
pub fn sign_get_file_url(track_id: &str, format_id: u32, request_ts: u64, secret: &str) -> String {
    let raw = format!(
        "trackgetFileUrlformat_id{format_id}intentstreamtrack_id{track_id}{request_ts}{secret}"
    );
    let digest = Md5::digest(raw.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

fn build_text_query(q: &Query) -> String {
    let artist = q.artists.first().map(String::as_str).unwrap_or("").trim();
    let title = q.title.trim();
    match (artist.is_empty(), title.is_empty()) {
        (false, false) => format!("{artist} {title}"),
        (true, false) => title.to_string(),
        (false, true) => artist.to_string(),
        (true, true) => String::new(),
    }
}

fn combine_title_and_version(title: &str, version: Option<&str>) -> String {
    let trimmed_title = title.trim();
    let Some(ver) = version.map(str::trim).filter(|v| !v.is_empty()) else {
        return trimmed_title.to_string();
    };
    if trimmed_title
        .to_ascii_lowercase()
        .contains(&ver.to_ascii_lowercase())
    {
        trimmed_title.to_string()
    } else {
        format!("{trimmed_title} ({ver})")
    }
}

fn parse_year_prefix(date_str: &str) -> Option<u32> {
    let prefix = date_str.trim().get(..4)?;
    prefix.parse::<u32>().ok()
}

fn khz_or_hz_to_hz(rate: f64) -> u32 {
    if rate <= 1000.0 {
        (rate * 1000.0).round() as u32
    } else {
        rate.round() as u32
    }
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

async fn check_http_status(response: Response, endpoint: &str) -> Result<Response> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }

    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return Err(SyncError::AuthInvalid(format!(
            "Qobuz {endpoint} rejected credentials with HTTP {}",
            status.as_u16()
        )));
    }

    if status == StatusCode::TOO_MANY_REQUESTS {
        let retry_after_secs = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.trim().parse::<u64>().ok());
        return Err(SyncError::RateLimited {
            provider: PROVIDER_ID.to_string(),
            retry_after_secs,
            message: format!("Qobuz {endpoint} returned HTTP 429 Too Many Requests"),
        });
    }

    if status == StatusCode::NOT_FOUND {
        return Err(SyncError::NotFound(format!(
            "Qobuz {endpoint} returned HTTP 404"
        )));
    }

    if status.is_server_error() {
        return Err(SyncError::ServerError {
            provider: PROVIDER_ID.to_string(),
            status: status.as_u16(),
            message: format!("Qobuz {endpoint} returned HTTP {}", status.as_u16()),
        });
    }

    Err(SyncError::Provider {
        provider: PROVIDER_ID.to_string(),
        message: format!(
            "Qobuz {endpoint} returned unexpected HTTP {}",
            status.as_u16()
        ),
    })
}

#[derive(Debug, Deserialize)]
struct QobuzSearchResponse {
    tracks: Option<QobuzTrackPage>,
}

#[derive(Debug, Deserialize)]
struct QobuzTrackPage {
    #[serde(default)]
    items: Vec<QobuzTrackItem>,
}

#[derive(Debug, Deserialize)]
struct QobuzTrackItem {
    id: u64,
    title: String,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    isrc: Option<String>,
    #[serde(default)]
    duration: u64,
    #[serde(default)]
    track_number: Option<u32>,
    #[serde(default)]
    media_number: Option<u32>,
    #[serde(default)]
    streamable: Option<bool>,
    #[serde(default)]
    maximum_bit_depth: Option<u8>,
    #[serde(default)]
    maximum_sampling_rate: Option<f64>,
    #[serde(default)]
    performer: Option<QobuzNamedEntity>,
    #[serde(default)]
    album: Option<QobuzAlbum>,
}

#[derive(Debug, Deserialize)]
struct QobuzNamedEntity {
    name: String,
}

#[derive(Debug, Deserialize)]
struct QobuzAlbum {
    title: String,
    #[serde(default)]
    artist: Option<QobuzNamedEntity>,
    #[serde(default)]
    release_date_original: Option<String>,
    #[serde(default)]
    image: Option<QobuzImage>,
}

#[derive(Debug, Deserialize)]
struct QobuzImage {
    #[serde(default)]
    large: Option<String>,
    #[serde(default)]
    small: Option<String>,
    #[serde(default)]
    thumbnail: Option<String>,
}

#[derive(Debug, Deserialize)]
struct QobuzFileUrlResponse {
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    format_id: Option<u32>,
    #[serde(default)]
    mime_type: Option<String>,
    #[serde(default)]
    sample: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_debug_redacts_secrets() {
        let creds = QobuzCredentials::new(
            "secret-app-id-123",
            "super-secret-user-token-xyz",
            Some("top-secret-app-secret-999".to_string()),
        );
        let formatted = format!("{creds:?}");
        assert!(!formatted.contains("secret-app-id-123"));
        assert!(!formatted.contains("super-secret-user-token-xyz"));
        assert!(!formatted.contains("top-secret-app-secret-999"));
        assert!(formatted.contains("[REDACTED]"));

        let provider = QobuzProvider::new(creds).expect("provider builds");
        let provider_debug = format!("{provider:?}");
        assert!(!provider_debug.contains("secret-app-id-123"));
        assert!(!provider_debug.contains("super-secret-user-token-xyz"));
        assert!(!provider_debug.contains("top-secret-app-secret-999"));
    }

    #[test]
    fn signature_is_deterministic_lowercase_md5() {
        let sig = sign_get_file_url("12345", 27, 1_700_000_000, "testsecret");
        assert_eq!(sig.len(), 32);
        assert!(sig.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')));
        assert_eq!(
            sig,
            sign_get_file_url("12345", 27, 1_700_000_000, "testsecret")
        );
    }

    #[test]
    fn version_combines_without_duplicating() {
        assert_eq!(
            combine_title_and_version("Get Lucky", Some("Radio Edit")),
            "Get Lucky (Radio Edit)"
        );
        assert_eq!(
            combine_title_and_version("Get Lucky (Radio Edit)", Some("Radio Edit")),
            "Get Lucky (Radio Edit)"
        );
        assert_eq!(combine_title_and_version("Get Lucky", None), "Get Lucky");
    }
}
