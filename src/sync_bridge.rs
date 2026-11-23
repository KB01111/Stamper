//! Application bridge between Spotifast and the headless `spotisync` crate.
//!
//! All SQLite reads/writes, playlist polling, provider credential validation,
//! and lossless downloads run strictly off the UI thread on the backend Tokio
//! runtime. The UI receives debounced [`SyncUiState`] snapshots via a callback.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use spotisync::{
    MatchConfig, PlaylistCounts, PlaylistSource, PlaylistTrack, PlaylistWatcher, Provider,
    QobuzConfig, QobuzCredentials, QobuzProvider, Quality, QueueConfig, SyncError, SyncQueue,
    SyncSnapshot, SyncStatus, SyncStore, SyncedPlaylist, WatcherConfig,
};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::api::models::{PlayableItem, Track, pick_image};
use crate::api::{ApiError, ApiGateway, Operation};
use crate::credentials::{QobuzGrant, Store as CredentialStore};
use crate::paths::AppDirs;
use crate::settings::{SyncSettings, SyncedPlaylistSetting};

/// Page size used when fetching playlist tracks for sync diffing.
const PLAYLIST_SYNC_PAGE_SIZE: u32 = 50;

/// Maximum pages read per playlist poll to guard against non-terminating paging.
const MAX_PLAYLIST_SYNC_PAGES: usize = 200;

/// Minimum spacing between background UI snapshot emissions during bursts.
const SNAPSHOT_DEBOUNCE: Duration = Duration::from_millis(150);

/// Refresh interval while any provider rate-limit cooldown countdown is active.
const COOLDOWN_TICK_INTERVAL: Duration = Duration::from_secs(1);

/// How often the coordinator checks whether the signed-in account changed.
const ACCOUNT_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// How long a worker may take to finish its current step once a stop is
/// requested, before it is aborted.
const WORKER_STOP_GRACE: Duration = Duration::from_secs(5);

/// Callback invoked whenever [`SyncUiState`] updates.
pub type SyncStateHook = Arc<dyn Fn(SyncUiState) + Send + Sync>;

/// Validation state of a lossless provider's credentials.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum CredentialStatus {
    #[default]
    Missing,
    Validating,
    Valid {
        provider: String,
        detail: String,
    },
    Invalid {
        provider: String,
        message: String,
    },
}

/// Per-track sync metadata projected for track tables and queue views.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrackSyncInfo {
    pub status: SyncStatus,
    pub matched_by: Option<String>,
    pub provider: Option<String>,
    pub quality: Option<String>,
    pub path: Option<PathBuf>,
    pub error: Option<String>,
}

impl Default for TrackSyncInfo {
    fn default() -> Self {
        Self {
            status: SyncStatus::Pending,
            matched_by: None,
            provider: None,
            quality: None,
            path: None,
            error: None,
        }
    }
}

/// Completed local lossless track entry shown in the Synced Library view.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncedLibraryTrack {
    pub playlist_id: String,
    pub track_id: String,
    pub title: String,
    pub artists: String,
    pub album: String,
    pub duration_ms: u64,
    pub quality: Option<String>,
    pub provider: Option<String>,
    pub path: PathBuf,
}

/// Complete UI-facing snapshot of playlist sync state, emitted off-thread.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncUiState {
    pub snapshot: SyncSnapshot,
    pub playlists: Vec<SyncedPlaylist>,
    pub playlist_counts: HashMap<String, PlaylistCounts>,
    pub track_statuses: HashMap<(String, String), TrackSyncInfo>,
    pub library_tracks: Vec<SyncedLibraryTrack>,
    pub qobuz_credentials: CredentialStatus,
    /// Non-secret Qobuz app_id hint only (never holds tokens or secrets).
    pub qobuz_app_id_hint: Option<String>,
    /// True while pending, resolving, or downloading tracks remain.
    pub active_running: bool,
    pub last_error: Option<String>,
}

impl SyncUiState {
    /// Returns true if `playlist_id` is currently enabled for lossless syncing.
    pub fn is_playlist_enabled(&self, playlist_id: &str) -> bool {
        self.playlists
            .iter()
            .any(|p| p.playlist_id == playlist_id && p.enabled)
    }

    /// Looks up the sync status for `(playlist_id, track_id)`.
    pub fn track_status(&self, playlist_id: &str, track_id: &str) -> Option<&TrackSyncInfo> {
        self.track_statuses
            .get(&(playlist_id.to_string(), track_id.to_string()))
    }
}

/// Formats a canonical `spotisync` quality string for UI display.
pub fn format_quality_label(raw: &str) -> String {
    Quality::parse(raw)
        .map(|quality| quality.display_label().to_string())
        .unwrap_or_else(|| raw.to_string())
}

/// Spotify playlist reader backed by Spotifast's [`ApiGateway`].
pub struct SpotifyPlaylistSource {
    api: Arc<ApiGateway>,
}

impl SpotifyPlaylistSource {
    pub fn new(api: Arc<ApiGateway>) -> Self {
        Self { api }
    }
}

impl PlaylistSource for SpotifyPlaylistSource {
    fn account_key(&self) -> Option<String> {
        self.api
            .account()
            .map(|account| account.as_str().to_string())
    }

    fn fetch_playlist_tracks<'life0, 'life1, 'async_trait>(
        &'life0 self,
        playlist_id: &'life1 str,
    ) -> Pin<Box<dyn Future<Output = spotisync::Result<Vec<PlaylistTrack>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            if self.api.account().is_none() {
                return Err(SyncError::AuthInvalid(
                    "Spotify account is not signed in".into(),
                ));
            }
            let access = self.api.playlist_access(playlist_id);
            let client = self
                .api
                .client_for(Operation::PlaylistItems(access))
                .await
                .map_err(map_api_error)?;

            let mut tracks = Vec::new();
            let mut offset = 0u32;
            let mut pages_read = 0usize;

            loop {
                let page = client
                    .playlist_items(playlist_id, offset, PLAYLIST_SYNC_PAGE_SIZE)
                    .await
                    .map_err(map_api_error)?;
                pages_read += 1;

                for item in &page.items {
                    if item.is_local {
                        continue;
                    }
                    if let Some(PlayableItem::Track(track)) = item.playable()
                        && let Some(converted) = track_to_playlist_track(track)
                    {
                        tracks.push(converted);
                    }
                }

                match next_page_offset(offset, pages_read, page.next_offset())? {
                    Some(next) => offset = next,
                    None => return Ok(tracks),
                }
            }
        })
    }
}

/// Decides whether a playlist read goes on. The list is complete only when
/// the server runs out of pages. A page cap reached first, or an offset that
/// stops advancing, is an error: a partial list must never be taken as the
/// whole playlist.
fn next_page_offset(
    offset: u32,
    pages_read: usize,
    next: Option<u32>,
) -> spotisync::Result<Option<u32>> {
    let Some(next) = next else {
        return Ok(None);
    };
    if next <= offset {
        return Err(SyncError::Provider {
            provider: "spotify".into(),
            message: "playlist paging stopped advancing; the playlist was not read in full".into(),
        });
    }
    if pages_read >= MAX_PLAYLIST_SYNC_PAGES {
        return Err(SyncError::Provider {
            provider: "spotify".into(),
            message: format!(
                "playlist has more than {} tracks; it was not read in full",
                MAX_PLAYLIST_SYNC_PAGES * PLAYLIST_SYNC_PAGE_SIZE as usize
            ),
        });
    }
    Ok(Some(next))
}

fn map_api_error(error: ApiError) -> SyncError {
    match error {
        ApiError::NotSignedIn | ApiError::SignInExpired { .. } => {
            SyncError::AuthInvalid(error.to_string())
        }
        ApiError::RateLimited | ApiError::QuotaExhausted => SyncError::RateLimited {
            provider: "spotify".into(),
            retry_after_secs: Some(30),
            message: error.to_string(),
        },
        ApiError::Status { status, message } if status >= 500 => SyncError::ServerError {
            provider: "spotify".into(),
            status,
            message,
        },
        ApiError::Status { status: 404, .. } => {
            SyncError::NotFound("Spotify playlist not found".into())
        }
        other => SyncError::Provider {
            provider: "spotify".into(),
            message: other.to_string(),
        },
    }
}

/// Converts a Spotifast API [`Track`] into a `spotisync` [`PlaylistTrack`].
pub fn track_to_playlist_track(track: &Track) -> Option<PlaylistTrack> {
    if track.is_local {
        return None;
    }
    let track_id = track
        .id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .or_else(|| crate::util::uri_id(&track.uri).map(str::to_string))?;
    if track.name.trim().is_empty() {
        return None;
    }

    let isrc = track
        .external_ids
        .isrc
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let artists: Vec<String> = track
        .artists
        .iter()
        .map(|artist| artist.name.trim())
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect();
    let album = track
        .album
        .as_ref()
        .map(|album| album.name.clone())
        .unwrap_or_default();
    let album_artist = track
        .album
        .as_ref()
        .and_then(|album| album.artists.first())
        .map(|artist| artist.name.trim())
        .filter(|name| !name.is_empty())
        .map(str::to_string);
    let year = track
        .album
        .as_ref()
        .and_then(|album| album.year())
        .and_then(|year| year.parse::<u32>().ok());
    let cover_url = track
        .album
        .as_ref()
        .and_then(|album| pick_image(&album.images, 640))
        .map(str::to_string);

    Some(PlaylistTrack {
        track_id,
        isrc,
        title: track.name.clone(),
        artists,
        album,
        album_artist,
        duration_ms: u64::from(track.duration_ms),
        track_number: track.track_number,
        disc_number: track.disc_number,
        year,
        cover_url,
    })
}

enum SyncCmd {
    SetPlaylist {
        id: String,
        name: String,
        enabled: bool,
    },
    TriggerNow,
    RetryTrack {
        playlist_id: String,
        track_id: String,
    },
    UpdateConfig {
        settings: SyncSettings,
        resolved_root: PathBuf,
    },
    SaveQobuzCredentials {
        app_id: String,
        user_auth_token: String,
        app_secret: Option<String>,
    },
    ClearQobuzCredentials,
    ValidateQobuzCredentials,
    QobuzValidated {
        generation: u64,
        /// The credentials being saved, or `None` when re-checking saved ones.
        candidate: Option<QobuzGrant>,
        result: std::result::Result<(), String>,
    },
    Shutdown,
}

/// Non-blocking handle to the background `spotisync` coordinator running on the
/// backend Tokio runtime.
pub struct SyncController {
    commands: mpsc::UnboundedSender<SyncCmd>,
}

impl SyncController {
    /// Starts the background sync coordinator on the active Tokio runtime.
    pub fn start(
        dirs: AppDirs,
        credentials: CredentialStore,
        source: Arc<dyn PlaylistSource>,
        initial_settings: SyncSettings,
        initial_root: PathBuf,
        on_state: SyncStateHook,
    ) -> Self {
        Self::start_inner(
            dirs.sync_db_file(),
            dirs.sync_owner_file(),
            credentials,
            source,
            initial_settings,
            initial_root,
            on_state,
            QobuzConfig::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_inner(
        db_path: PathBuf,
        owner_file: PathBuf,
        credentials: CredentialStore,
        source: Arc<dyn PlaylistSource>,
        initial_settings: SyncSettings,
        initial_root: PathBuf,
        on_state: SyncStateHook,
        qobuz_base_config: QobuzConfig,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let internal_tx = tx.clone();
        tokio::spawn(async move {
            let mut actor = SyncActor {
                db_path,
                owner_file,
                credentials,
                source,
                settings: initial_settings,
                resolved_root: initial_root,
                on_state,
                internal_tx,
                qobuz_base_config,
                account: None,
                store: None,
                queue: None,
                watcher: None,
                worker_shutdown: None,
                worker_tasks: Vec::new(),
                snapshot_rx: None,
                qobuz_grant: None,
                qobuz_status: CredentialStatus::Missing,
                validation_gen: 0,
                last_error: None,
            };
            actor.run(rx).await;
        });
        Self { commands: tx }
    }

    pub fn set_playlist(&self, id: String, name: String, enabled: bool) {
        let _ = self
            .commands
            .send(SyncCmd::SetPlaylist { id, name, enabled });
    }

    pub fn trigger_now(&self) {
        let _ = self.commands.send(SyncCmd::TriggerNow);
    }

    pub fn retry_track(&self, playlist_id: String, track_id: String) {
        let _ = self.commands.send(SyncCmd::RetryTrack {
            playlist_id,
            track_id,
        });
    }

    pub fn update_config(&self, settings: SyncSettings, resolved_root: PathBuf) {
        let _ = self.commands.send(SyncCmd::UpdateConfig {
            settings,
            resolved_root,
        });
    }

    pub fn save_qobuz_credentials(
        &self,
        app_id: String,
        user_auth_token: String,
        app_secret: Option<String>,
    ) {
        let _ = self.commands.send(SyncCmd::SaveQobuzCredentials {
            app_id,
            user_auth_token,
            app_secret,
        });
    }

    pub fn clear_qobuz_credentials(&self) {
        let _ = self.commands.send(SyncCmd::ClearQobuzCredentials);
    }

    pub fn validate_qobuz_credentials(&self) {
        let _ = self.commands.send(SyncCmd::ValidateQobuzCredentials);
    }

    pub fn shutdown(&self) {
        let _ = self.commands.send(SyncCmd::Shutdown);
    }
}

struct SyncActor {
    db_path: PathBuf,
    owner_file: PathBuf,
    credentials: CredentialStore,
    source: Arc<dyn PlaylistSource>,
    settings: SyncSettings,
    resolved_root: PathBuf,
    on_state: SyncStateHook,
    internal_tx: mpsc::UnboundedSender<SyncCmd>,
    qobuz_base_config: QobuzConfig,
    /// The account the sync state is attached to, if any.
    account: Option<String>,
    store: Option<SyncStore>,
    queue: Option<SyncQueue>,
    watcher: Option<PlaylistWatcher>,
    worker_shutdown: Option<watch::Sender<bool>>,
    worker_tasks: Vec<JoinHandle<()>>,
    snapshot_rx: Option<watch::Receiver<SyncSnapshot>>,
    qobuz_grant: Option<QobuzGrant>,
    qobuz_status: CredentialStatus,
    validation_gen: u64,
    last_error: Option<String>,
}

impl SyncActor {
    async fn run(&mut self, mut rx: mpsc::UnboundedReceiver<SyncCmd>) {
        // 1. Load saved Qobuz credentials from the protected store (never logged).
        match self.credentials.load_qobuz().await {
            Ok(Some(grant)) if grant.valid() => {
                self.qobuz_grant = Some(grant.clone());
                self.qobuz_status = CredentialStatus::Validating;
                self.spawn_qobuz_validation(grant, false);
            }
            Ok(_) => {
                self.qobuz_grant = None;
                self.qobuz_status = CredentialStatus::Missing;
            }
            Err(err) => {
                log::warn!("unable to load Qobuz credentials from store: {err}");
                self.qobuz_grant = None;
                self.qobuz_status = CredentialStatus::Invalid {
                    provider: "qobuz".into(),
                    message: err.to_string(),
                };
            }
        }

        // 2. Attach the sync state to the signed-in account, start the workers, and emit.
        self.attach_account().await;
        self.emit_now().await;

        // 3. Event loop with debounced background snapshot updates, cooldown ticks,
        // and account checks.
        let mut dirty_emit = false;
        let mut last_emit = Instant::now();
        let mut cooldown_tick = tokio::time::interval(COOLDOWN_TICK_INTERVAL);
        cooldown_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut account_check = tokio::time::interval(ACCOUNT_CHECK_INTERVAL);
        account_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let debounce_sleep = async {
                if dirty_emit {
                    let target = last_emit + SNAPSHOT_DEBOUNCE;
                    tokio::time::sleep_until(target).await;
                } else {
                    std::future::pending::<()>().await;
                }
            };

            let has_cooldowns = self
                .snapshot_rx
                .as_ref()
                .is_some_and(|rx| !rx.borrow().cooldowns.is_empty());

            let snapshot_changed = async {
                if let Some(snap_rx) = &mut self.snapshot_rx {
                    snap_rx.changed().await
                } else {
                    std::future::pending().await
                }
            };

            tokio::select! {
                maybe_cmd = rx.recv() => {
                    let Some(cmd) = maybe_cmd else {
                        break;
                    };
                    if matches!(cmd, SyncCmd::Shutdown) {
                        break;
                    }
                    self.handle_command(cmd).await;
                    dirty_emit = false;
                    self.emit_now().await;
                    last_emit = Instant::now();
                }
                changed = snapshot_changed => {
                    if changed.is_ok() {
                        if last_emit.elapsed() >= SNAPSHOT_DEBOUNCE {
                            dirty_emit = false;
                            self.emit_now().await;
                            last_emit = Instant::now();
                        } else {
                            dirty_emit = true;
                        }
                    }
                }
                _ = debounce_sleep => {
                    dirty_emit = false;
                    self.emit_now().await;
                    last_emit = Instant::now();
                }
                _ = cooldown_tick.tick(), if has_cooldowns => {
                    dirty_emit = false;
                    self.emit_now().await;
                    last_emit = Instant::now();
                }
                _ = account_check.tick() => {
                    if self.source.account_key() != self.account {
                        self.attach_account().await;
                        dirty_emit = false;
                        self.emit_now().await;
                        last_emit = Instant::now();
                    }
                }
            }
        }

        self.stop_workers().await;
    }

    async fn handle_command(&mut self, cmd: SyncCmd) {
        match cmd {
            SyncCmd::SetPlaylist { id, name, enabled } => {
                if let Some(store) = self.store.clone() {
                    let pid = id.clone();
                    let pname = name.clone();
                    let res = tokio::task::spawn_blocking(move || {
                        store.set_playlist_enabled(&pid, &pname, enabled)
                    })
                    .await;
                    match res {
                        Ok(Ok(())) => {
                            self.last_error = None;
                            if enabled && let Some(watcher) = &self.watcher {
                                watcher.trigger_poll();
                            }
                        }
                        Ok(Err(err)) => {
                            self.last_error = Some(err.to_string());
                        }
                        Err(_) => {
                            self.last_error =
                                Some("Failed to update synced playlist state.".into());
                        }
                    }
                }
            }
            SyncCmd::TriggerNow => {
                if let Some(watcher) = &self.watcher {
                    watcher.trigger_poll();
                }
                if let Some(queue) = &self.queue {
                    queue.wake();
                }
            }
            SyncCmd::RetryTrack {
                playlist_id,
                track_id,
            } => {
                if let Some(store) = self.store.clone() {
                    let res = tokio::task::spawn_blocking(move || {
                        store.retry_item(&playlist_id, &track_id)
                    })
                    .await;
                    match res {
                        Ok(Ok(_)) => {
                            self.last_error = None;
                            if let Some(queue) = &self.queue {
                                queue.wake();
                            }
                        }
                        Ok(Err(err)) => {
                            self.last_error = Some(err.to_string());
                        }
                        Err(_) => {
                            self.last_error = Some("Failed to retry sync item.".into());
                        }
                    }
                }
            }
            SyncCmd::UpdateConfig {
                settings,
                resolved_root,
            } => {
                self.settings = settings;
                self.resolved_root = resolved_root;
                self.rebuild_workers().await;
            }
            SyncCmd::SaveQobuzCredentials {
                app_id,
                user_auth_token,
                app_secret,
            } => {
                let app_id = app_id.trim().to_string();
                let user_auth_token = user_auth_token.trim().to_string();
                let app_secret = app_secret
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty());
                let grant = QobuzGrant {
                    app_id,
                    user_auth_token,
                    app_secret,
                };
                if !grant.valid() {
                    self.qobuz_status = CredentialStatus::Invalid {
                        provider: "qobuz".into(),
                        message: "Enter both App ID and User Auth Token.".into(),
                    };
                    return;
                }
                // Nothing is saved or used until the provider accepts them.
                self.qobuz_status = CredentialStatus::Validating;
                self.spawn_qobuz_validation(grant, true);
            }
            SyncCmd::ClearQobuzCredentials => {
                self.validation_gen = self.validation_gen.wrapping_add(1);
                self.qobuz_grant = None;
                self.qobuz_status = CredentialStatus::Missing;
                if let Err(err) = self.credentials.clear_qobuz().await {
                    log::warn!("unable to clear Qobuz credentials: {err}");
                    self.last_error = Some(err.to_string());
                } else {
                    self.last_error = None;
                }
                self.rebuild_workers().await;
            }
            SyncCmd::ValidateQobuzCredentials => match self.qobuz_grant.clone() {
                Some(grant) if grant.valid() => {
                    self.qobuz_status = CredentialStatus::Validating;
                    self.spawn_qobuz_validation(grant, false);
                }
                _ => {
                    self.qobuz_status = CredentialStatus::Missing;
                }
            },
            SyncCmd::QobuzValidated {
                generation,
                candidate,
                result,
            } => {
                if generation != self.validation_gen {
                    return;
                }
                match (candidate, result) {
                    (Some(grant), Ok(())) => {
                        // Accepted: save it, then use it. A failed save keeps
                        // the previous credentials in place.
                        if let Err(err) = self.credentials.save_qobuz(grant.clone()).await {
                            log::warn!("unable to save Qobuz credentials: {err}");
                            self.last_error = Some(err.to_string());
                            self.qobuz_status = CredentialStatus::Invalid {
                                provider: "qobuz".into(),
                                message: err.to_string(),
                            };
                            return;
                        }
                        self.qobuz_grant = Some(grant);
                        self.last_error = None;
                        self.qobuz_status = CredentialStatus::Valid {
                            provider: "qobuz".into(),
                            detail: "Connected".into(),
                        };
                        self.rebuild_workers().await;
                    }
                    (Some(_), Err(message)) => {
                        // Rejected: the saved credentials stay, and only the
                        // rejection is reported. The message carries no secret.
                        self.qobuz_status = CredentialStatus::Invalid {
                            provider: "qobuz".into(),
                            message: format!("New Qobuz credentials were not saved: {message}"),
                        };
                    }
                    (None, Ok(())) => {
                        self.qobuz_status = CredentialStatus::Valid {
                            provider: "qobuz".into(),
                            detail: "Connected".into(),
                        };
                        self.last_error = None;
                        if let Some(queue) = &self.queue {
                            queue.wake();
                        }
                    }
                    (None, Err(message)) => {
                        self.qobuz_status = CredentialStatus::Invalid {
                            provider: "qobuz".into(),
                            message,
                        };
                    }
                }
            }
            SyncCmd::Shutdown => {}
        }
    }

    /// Checks `grant` with the provider. `candidate` marks new credentials
    /// the user typed: they are saved only if the check passes.
    fn spawn_qobuz_validation(&mut self, grant: QobuzGrant, candidate: bool) {
        self.validation_gen = self.validation_gen.wrapping_add(1);
        let generation = self.validation_gen;
        let tx = self.internal_tx.clone();
        let mut config = self.qobuz_base_config.clone();
        config.max_quality = self.settings.quality_ceiling.to_spotisync();
        let candidate = candidate.then(|| grant.clone());

        tokio::spawn(async move {
            let creds =
                QobuzCredentials::new(grant.app_id, grant.user_auth_token, grant.app_secret);
            let result = match QobuzProvider::with_config(creds, config) {
                Ok(provider) => provider
                    .validate_credentials()
                    .await
                    .map_err(|err| err.to_string()),
                Err(err) => Err(err.to_string()),
            };
            let _ = tx.send(SyncCmd::QobuzValidated {
                generation,
                candidate,
                result,
            });
        });
    }

    fn build_providers(&self) -> Vec<Arc<dyn Provider>> {
        let max_quality = self.settings.quality_ceiling.to_spotisync();
        let mut providers: Vec<Arc<dyn Provider>> = Vec::new();
        for entry in &self.settings.providers {
            if !entry.enabled {
                continue;
            }
            if entry.id == "qobuz"
                && let Some(grant) = &self.qobuz_grant
                && grant.valid()
            {
                let mut config = self.qobuz_base_config.clone();
                config.max_quality = max_quality;
                let creds = QobuzCredentials::new(
                    grant.app_id.clone(),
                    grant.user_auth_token.clone(),
                    grant.app_secret.clone(),
                );
                match QobuzProvider::with_config(creds, config) {
                    Ok(provider) => providers.push(Arc::new(provider)),
                    Err(err) => {
                        log::warn!("unable to initialize Qobuz provider: {err}");
                    }
                }
            }
        }
        providers
    }

    async fn stop_workers(&mut self) {
        let shutdown = self.worker_shutdown.take();
        let tasks = std::mem::take(&mut self.worker_tasks);
        stop_tasks(shutdown, tasks, WORKER_STOP_GRACE).await;
        self.queue = None;
        self.watcher = None;
        self.snapshot_rx = None;
    }

    /// Points the coordinator at the signed-in account's sync state. State
    /// kept for another account is cleared when this account opens it. With no
    /// account, nothing is shown or synced until one signs in.
    async fn attach_account(&mut self) {
        self.stop_workers().await;
        self.store = None;
        let key = self.source.account_key();
        // Recorded before opening, so a failed open is not retried on every check.
        self.account = key.clone();
        self.last_error = None;
        if let Some(account) = key {
            let db_path = self.db_path.clone();
            let owner_file = self.owner_file.clone();
            let seed = self.settings.synced_playlists.clone();
            let opened = tokio::task::spawn_blocking(move || {
                open_for_account(&db_path, &owner_file, &account, &seed)
            })
            .await;
            match opened {
                Ok(Ok(store)) => self.store = Some(store),
                Ok(Err(err)) => {
                    log::error!("unable to open sync database: {err}");
                    self.last_error = Some(format!("Unable to open sync database: {err}"));
                }
                Err(join_err) => {
                    log::error!("sync database initialization panicked: {join_err}");
                    self.last_error = Some("Unable to initialize sync database.".into());
                }
            }
        }
        self.rebuild_workers().await;
    }

    async fn rebuild_workers(&mut self) {
        self.stop_workers().await;
        let Some(store) = self.store.clone() else {
            return;
        };

        let providers = self.build_providers();
        let queue_config = QueueConfig {
            output_dir: self.resolved_root.clone(),
            per_provider_concurrency: usize::from(self.settings.clamped_concurrency()),
            match_config: MatchConfig {
                max_quality: matcher_quality_ceiling(
                    self.settings.quality_ceiling.to_spotisync(),
                    &providers,
                ),
                ..MatchConfig::default()
            },
            ..QueueConfig::default()
        };

        let queue = match SyncQueue::new(store.clone(), providers, queue_config) {
            Ok(queue) => queue,
            Err(err) => {
                log::error!("unable to initialize sync queue: {err}");
                self.last_error = Some(err.to_string());
                return;
            }
        };

        let watcher_config = WatcherConfig {
            poll_interval: Duration::from_secs(self.settings.clamped_poll_interval_secs()),
            ..WatcherConfig::default()
        };
        let watcher = PlaylistWatcher::new(
            store,
            Arc::clone(&self.source),
            queue.notify_handle(),
            watcher_config,
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let queue_rx = shutdown_rx.clone();
        let queue_runner = queue.clone();
        let queue_task = tokio::spawn(async move {
            if let Err(err) = queue_runner.run_until_shutdown(queue_rx).await {
                log::warn!("sync queue loop stopped with error: {err}");
            }
        });

        let watcher_runner = watcher.clone();
        let watcher_task = tokio::spawn(async move {
            if let Err(err) = watcher_runner.run_until_shutdown(shutdown_rx).await {
                log::warn!("playlist watcher loop stopped with error: {err}");
            }
        });

        self.snapshot_rx = Some(queue.subscribe());
        self.queue = Some(queue);
        self.watcher = Some(watcher);
        self.worker_shutdown = Some(shutdown_tx);
        self.worker_tasks = vec![queue_task, watcher_task];
    }

    async fn emit_now(&self) {
        let mut snapshot = self
            .snapshot_rx
            .as_ref()
            .map(|rx| rx.borrow().clone())
            .unwrap_or_default();

        // Refresh remaining_secs on any active provider cooldowns.
        if !snapshot.cooldowns.is_empty() {
            let now = now_unix_secs();
            for cooldown in snapshot.cooldowns.values_mut() {
                cooldown.remaining_secs = cooldown.retry_at_unix_secs.saturating_sub(now);
            }
        }

        let qobuz_credentials = self.qobuz_status.clone();
        let qobuz_app_id_hint = self.qobuz_grant.as_ref().map(|g| g.app_id.clone());
        let last_error = self.last_error.clone();

        let state = if let Some(store) = self.store.clone() {
            let snap_clone = snapshot.clone();
            match tokio::task::spawn_blocking(move || {
                project_ui_state(
                    &store,
                    snap_clone,
                    qobuz_credentials,
                    qobuz_app_id_hint,
                    last_error,
                )
            })
            .await
            {
                Ok(Ok(projected)) => projected,
                Ok(Err(err)) => {
                    log::warn!("unable to project sync UI state: {err}");
                    SyncUiState {
                        snapshot,
                        qobuz_credentials: self.qobuz_status.clone(),
                        qobuz_app_id_hint: self.qobuz_grant.as_ref().map(|g| g.app_id.clone()),
                        last_error: Some(err.to_string()),
                        ..SyncUiState::default()
                    }
                }
                Err(_) => return,
            }
        } else {
            SyncUiState {
                snapshot,
                qobuz_credentials,
                qobuz_app_id_hint,
                last_error,
                ..SyncUiState::default()
            }
        };

        (self.on_state)(state);
    }
}

fn project_ui_state(
    store: &SyncStore,
    mut snapshot: SyncSnapshot,
    qobuz_credentials: CredentialStatus,
    qobuz_app_id_hint: Option<String>,
    last_error: Option<String>,
) -> spotisync::Result<SyncUiState> {
    let totals = store.total_counts()?;
    snapshot.pending = totals.pending;
    snapshot.resolving = totals.resolving;
    snapshot.downloading = totals.downloading;
    snapshot.done = totals.done;
    snapshot.unmatched = totals.unmatched;
    snapshot.failed = totals.failed;

    let playlists = store.list_synced_playlists()?;
    let counts_vec = store.playlist_counts()?;
    let mut playlist_ids = BTreeSet::new();
    for playlist in &playlists {
        playlist_ids.insert(playlist.playlist_id.clone());
    }
    let mut playlist_counts = HashMap::with_capacity(counts_vec.len());
    for counts in counts_vec {
        playlist_ids.insert(counts.playlist_id.clone());
        playlist_counts.insert(counts.playlist_id.clone(), counts);
    }

    let mut track_statuses = HashMap::new();
    for playlist_id in &playlist_ids {
        for item in store.playlist_track_statuses(playlist_id)? {
            track_statuses.insert(
                (item.playlist_id, item.track_id),
                TrackSyncInfo {
                    status: item.status,
                    matched_by: item.matched_by,
                    provider: item.provider,
                    quality: item.quality.as_deref().map(format_quality_label),
                    path: item.path.filter(|p| !p.is_empty()).map(PathBuf::from),
                    error: item.error,
                },
            );
        }
    }

    let completed = store.list_completed_tracks()?;
    let mut library_tracks = Vec::with_capacity(completed.len());
    for item in completed {
        let Some(path) = item.path.filter(|p| !p.is_empty()) else {
            continue;
        };
        let meta = store.get_query(&item.playlist_id, &item.track_id)?;
        let (album, duration_ms) = meta
            .map(|q| (q.album, q.duration_ms))
            .unwrap_or_else(|| (String::new(), 0));
        library_tracks.push(SyncedLibraryTrack {
            playlist_id: item.playlist_id,
            track_id: item.track_id,
            title: item.title,
            artists: item.artists.join(", "),
            album,
            duration_ms,
            quality: item.quality.as_deref().map(format_quality_label),
            provider: item.provider,
            path: PathBuf::from(path),
        });
    }

    let active_running = snapshot.pending + snapshot.resolving + snapshot.downloading > 0;

    Ok(SyncUiState {
        snapshot,
        playlists,
        playlist_counts,
        track_statuses,
        library_tracks,
        qobuz_credentials,
        qobuz_app_id_hint,
        active_running,
        last_error,
    })
}

/// Asks the workers to stop, then waits up to `grace` for each one to finish
/// its current step. Aborting at once would drop a pass at its await point
/// while the blocking work it started keeps running. A worker still busy
/// after the grace period is aborted.
async fn stop_tasks(
    shutdown: Option<watch::Sender<bool>>,
    tasks: Vec<JoinHandle<()>>,
    grace: Duration,
) {
    if let Some(shutdown) = shutdown {
        let _ = shutdown.send(true);
    }
    for mut task in tasks {
        if tokio::time::timeout(grace, &mut task).await.is_err() {
            log::warn!("sync worker did not stop within {grace:?}; aborting it");
            task.abort();
            let _ = task.await;
        }
    }
}

/// The matcher's quality ceiling: the user's choice, lowered to what each
/// provider can serve. The matcher cannot see provider caps itself.
fn matcher_quality_ceiling(user_ceiling: Quality, providers: &[Arc<dyn Provider>]) -> Quality {
    providers.iter().fold(user_ceiling, |ceiling, provider| {
        ceiling.min(provider.max_quality())
    })
}

/// Opens the sync database for `account`. Its state is kept only for the
/// account that made it. State left by another account is removed first,
/// because its playlists and tracks belong to that account. State from before
/// the owner file existed is kept for the account that opens it first.
fn open_for_account(
    db_path: &Path,
    owner_file: &Path,
    account: &str,
    seed: &[SyncedPlaylistSetting],
) -> spotisync::Result<SyncStore> {
    let owner = read_owner(owner_file);
    let kept = match owner.as_deref() {
        Some(owner) if owner != account => {
            remove_sync_files(db_path)?;
            false
        }
        _ => true,
    };
    if owner.as_deref() != Some(account) {
        write_owner(owner_file, account)?;
    }
    let store = SyncStore::open(db_path)?;
    let _ = store.recover_interrupted()?;
    // Playlists enabled in settings belong to whichever account enabled them.
    if kept {
        for pl in seed {
            if !pl.id.trim().is_empty() {
                let _ = store.set_playlist_enabled(&pl.id, &pl.name, pl.enabled);
            }
        }
    }
    Ok(store)
}

fn read_owner(path: &Path) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value.get("account")?.as_str().map(str::to_string)
}

fn write_owner(path: &Path, account: &str) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(&serde_json::json!({ "account": account }))
        .map_err(std::io::Error::other)?;
    fastframe_file::write(path, bytes)
}

/// Removes the database and its write-ahead log files, if they exist.
fn remove_sync_files(db_path: &Path) -> std::io::Result<()> {
    let with_suffix = |suffix: &str| {
        let mut name = db_path.as_os_str().to_owned();
        name.push(suffix);
        PathBuf::from(name)
    };
    for path in [
        db_path.to_path_buf(),
        with_suffix("-wal"),
        with_suffix("-shm"),
    ] {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::models::{Album, ArtistRef, ExternalIds, Image};
    use std::sync::Mutex;
    use tokio::sync::Notify;

    struct FakeSource {
        tracks: Mutex<HashMap<String, Vec<PlaylistTrack>>>,
        account: Mutex<Option<String>>,
    }

    impl FakeSource {
        fn new() -> Self {
            Self {
                tracks: Mutex::new(HashMap::new()),
                account: Mutex::new(Some("account-a".to_string())),
            }
        }

        fn set(&self, playlist_id: &str, items: Vec<PlaylistTrack>) {
            self.tracks
                .lock()
                .unwrap()
                .insert(playlist_id.to_string(), items);
        }

        fn set_account(&self, account: Option<&str>) {
            *self.account.lock().unwrap() = account.map(str::to_string);
        }
    }

    impl PlaylistSource for FakeSource {
        fn fetch_playlist_tracks<'life0, 'life1, 'async_trait>(
            &'life0 self,
            playlist_id: &'life1 str,
        ) -> Pin<
            Box<dyn Future<Output = spotisync::Result<Vec<PlaylistTrack>>> + Send + 'async_trait>,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            let items = self
                .tracks
                .lock()
                .unwrap()
                .get(playlist_id)
                .cloned()
                .unwrap_or_default();
            Box::pin(async move { Ok(items) })
        }

        fn account_key(&self) -> Option<String> {
            self.account.lock().unwrap().clone()
        }
    }

    /// Waits until the coordinator publishes a state that satisfies `ready`.
    async fn wait_for_state(
        rx: &mut watch::Receiver<SyncUiState>,
        ready: impl Fn(&SyncUiState) -> bool,
    ) {
        tokio::time::timeout(Duration::from_secs(10), rx.wait_for(|state| ready(state)))
            .await
            .expect("sync state did not reach the expected value")
            .expect("sync state channel closed");
    }

    async fn spawn_mock_qobuz_server() -> (String, JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let flac_bytes = spotisync::writer::make_test_flac_bytes(24, 96_000).to_vec();
        let stream_url = format!("{base_url}/stream/1.flac");

        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let flac_bytes = flac_bytes.clone();
                let stream_url = stream_url.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let Ok(n) = socket.read(&mut buf).await else {
                        return;
                    };
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let path = req
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");

                    let (status, content_type, body): (&str, &str, Vec<u8>) =
                        if path.starts_with("/favorite/getUserFavorites") {
                            if req
                                .to_ascii_lowercase()
                                .contains("x-user-auth-token: bad-token")
                            {
                                ("401 Unauthorized", "application/json", b"{}".to_vec())
                            } else {
                                ("200 OK", "application/json", b"{}".to_vec())
                            }
                        } else if path.starts_with("/track/search") {
                            let body = serde_json::json!({
                                "tracks": {
                                    "items": [{
                                        "id": 4242,
                                        "title": "Instant Crush",
                                        "isrc": "USQX91300105",
                                        "duration": 337,
                                        "track_number": 5,
                                        "media_number": 1,
                                        "streamable": true,
                                        "maximum_bit_depth": 24,
                                        "maximum_sampling_rate": 96.0,
                                        "performer": { "name": "Daft Punk" },
                                        "album": {
                                            "title": "Random Access Memories",
                                            "release_date_original": "2013-05-17",
                                            "artist": { "name": "Daft Punk" }
                                        }
                                    }]
                                }
                            })
                            .to_string()
                            .into_bytes();
                            ("200 OK", "application/json", body)
                        } else if path.starts_with("/track/getFileUrl") {
                            let body = serde_json::json!({
                                "url": stream_url,
                                "format_id": 7,
                                "mime_type": "audio/flac",
                                "sample": false
                            })
                            .to_string()
                            .into_bytes();
                            ("200 OK", "application/json", body)
                        } else if path.starts_with("/stream/") {
                            ("200 OK", "audio/flac", flac_bytes)
                        } else {
                            ("404 Not Found", "application/json", b"{}".to_vec())
                        };

                    let header = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(header.as_bytes()).await;
                    let _ = socket.write_all(&body).await;
                });
            }
        });

        (base_url, handle)
    }

    fn temp_dirs(tag: &str) -> AppDirs {
        let root = std::env::temp_dir().join(format!(
            "spotifast-sync-bridge-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let dirs = AppDirs {
            config: root.join("config"),
            state: root.join("state"),
            cache: root.join("cache"),
        };
        dirs.ensure().unwrap();
        dirs
    }

    #[test]
    fn track_conversion_extracts_isrc_and_skips_local_files() {
        let track = Track {
            id: Some("tr1".into()),
            name: "Instant Crush".into(),
            uri: "spotify:track:tr1".into(),
            duration_ms: 337_560,
            artists: vec![ArtistRef {
                id: Some("a1".into()),
                name: "Daft Punk".into(),
                uri: None,
            }],
            album: Some(Album {
                id: "al1".into(),
                name: "Random Access Memories".into(),
                release_date: Some("2013-05-17".into()),
                artists: vec![ArtistRef {
                    id: Some("a1".into()),
                    name: "Daft Punk".into(),
                    uri: None,
                }],
                images: vec![Image {
                    url: "https://example.invalid/640.jpg".into(),
                    width: Some(640),
                    height: Some(640),
                }],
                ..Album::default()
            }),
            track_number: Some(5),
            disc_number: Some(1),
            is_local: false,
            external_ids: ExternalIds {
                isrc: Some("USQX91300105".into()),
            },
            ..Track::default()
        };

        let converted = track_to_playlist_track(&track).expect("converts");
        assert_eq!(converted.track_id, "tr1");
        assert_eq!(converted.isrc.as_deref(), Some("USQX91300105"));
        assert_eq!(converted.artists, vec!["Daft Punk"]);
        assert_eq!(converted.album, "Random Access Memories");
        assert_eq!(converted.year, Some(2013));
        assert_eq!(
            converted.cover_url.as_deref(),
            Some("https://example.invalid/640.jpg")
        );

        let local = Track {
            is_local: true,
            ..track
        };
        assert!(track_to_playlist_track(&local).is_none());
    }

    #[tokio::test]
    async fn controller_syncs_enabled_playlist_and_projects_ui_state() {
        let dirs = temp_dirs("sync-e2e");
        let creds = CredentialStore::in_memory(dirs.clone());
        let source = Arc::new(FakeSource::new());
        source.set(
            "pl1",
            vec![PlaylistTrack {
                track_id: "tr1".into(),
                isrc: Some("USQX91300105".into()),
                title: "Instant Crush".into(),
                artists: vec!["Daft Punk".into()],
                album: "Random Access Memories".into(),
                album_artist: Some("Daft Punk".into()),
                duration_ms: 337_000,
                track_number: Some(5),
                disc_number: Some(1),
                year: Some(2013),
                cover_url: None,
            }],
        );

        let latest = Arc::new(Mutex::new(SyncUiState::default()));
        let notify = Arc::new(Notify::new());
        let latest_hook = Arc::clone(&latest);
        let notify_hook = Arc::clone(&notify);
        let on_state: SyncStateHook = Arc::new(move |state| {
            *latest_hook.lock().unwrap() = state;
            notify_hook.notify_waiters();
        });

        let (base_url, server) = spawn_mock_qobuz_server().await;
        let qobuz_cfg = QobuzConfig {
            base_url,
            ..QobuzConfig::default()
        };

        let root = dirs.state.join("sync");
        let controller = SyncController::start_inner(
            dirs.sync_db_file(),
            dirs.sync_owner_file(),
            creds,
            source,
            SyncSettings::default(),
            root.clone(),
            on_state,
            qobuz_cfg,
        );

        controller.save_qobuz_credentials("123456".into(), "valid-token".into(), None);
        // Credentials are used only once accepted, so the playlist follows them.
        tokio::time::timeout(Duration::from_secs(5), async {
            while !matches!(
                latest.lock().unwrap().qobuz_credentials,
                CredentialStatus::Valid { .. }
            ) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("credentials should be accepted");
        controller.set_playlist("pl1".into(), "Favorites".into(), true);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                {
                    let state = latest.lock().unwrap();
                    if state.snapshot.done == 1
                        && !state.library_tracks.is_empty()
                        && matches!(state.qobuz_credentials, CredentialStatus::Valid { .. })
                    {
                        break;
                    }
                }
                notify.notified().await;
            }
        })
        .await
        .expect("track should sync to library");

        let state = latest.lock().unwrap().clone();
        assert_eq!(state.snapshot.done, 1);
        assert!(!state.active_running);
        assert!(state.is_playlist_enabled("pl1"));
        assert_eq!(state.qobuz_app_id_hint.as_deref(), Some("123456"));
        assert_eq!(state.library_tracks.len(), 1);
        assert_eq!(state.library_tracks[0].title, "Instant Crush");
        assert_eq!(state.library_tracks[0].album, "Random Access Memories");
        assert!(state.library_tracks[0].path.exists());
        assert_eq!(
            state.track_status("pl1", "tr1").map(|s| s.status),
            Some(SyncStatus::Done)
        );

        controller.shutdown();
        server.abort();
        let _ = std::fs::remove_dir_all(dirs.config.parent().unwrap());
    }

    #[tokio::test]
    async fn qobuz_credentials_reject_blank_and_clear_cleanly() {
        let dirs = temp_dirs("qobuz-creds");
        let creds = CredentialStore::in_memory(dirs.clone());
        let source = Arc::new(FakeSource::new());

        let latest = Arc::new(Mutex::new(SyncUiState::default()));
        let notify = Arc::new(Notify::new());
        let latest_hook = Arc::clone(&latest);
        let notify_hook = Arc::clone(&notify);
        let on_state: SyncStateHook = Arc::new(move |state| {
            *latest_hook.lock().unwrap() = state;
            notify_hook.notify_waiters();
        });

        let controller = SyncController::start_inner(
            dirs.sync_db_file(),
            dirs.sync_owner_file(),
            creds,
            source,
            SyncSettings::default(),
            dirs.state.join("sync"),
            on_state,
            QobuzConfig::default(),
        );

        // Blank credentials are rejected without persisting.
        controller.save_qobuz_credentials("  ".into(), "".into(), None);
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if matches!(
                    latest.lock().unwrap().qobuz_credentials,
                    CredentialStatus::Invalid { .. }
                ) {
                    break;
                }
                notify.notified().await;
            }
        })
        .await
        .expect("blank credentials should be marked invalid");

        // Clearing returns to Missing.
        controller.clear_qobuz_credentials();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if matches!(
                    latest.lock().unwrap().qobuz_credentials,
                    CredentialStatus::Missing
                ) {
                    break;
                }
                notify.notified().await;
            }
        })
        .await
        .expect("clearing credentials should return Missing");

        controller.shutdown();
        let _ = std::fs::remove_dir_all(dirs.config.parent().unwrap());
    }

    /// A playlist read that stops early is an error, never a partial list.
    #[test]
    fn a_playlist_read_cut_short_is_an_error_not_a_whole_list() {
        assert!(next_page_offset(0, MAX_PLAYLIST_SYNC_PAGES, Some(50)).is_err());
        assert_eq!(
            next_page_offset(0, MAX_PLAYLIST_SYNC_PAGES - 1, Some(50)).unwrap(),
            Some(50)
        );
        assert!(next_page_offset(50, 1, Some(50)).is_err());
        assert_eq!(next_page_offset(50, 4, None).unwrap(), None);
    }

    #[test]
    fn the_matcher_never_chooses_more_than_a_provider_serves() {
        let provider = |max_quality| {
            let config = QobuzConfig {
                max_quality,
                ..QobuzConfig::default()
            };
            let creds =
                QobuzCredentials::new("123456".to_string(), "token".to_string(), None);
            Arc::new(QobuzProvider::with_config(creds, config).unwrap()) as Arc<dyn Provider>
        };
        let narrow = provider(Quality::Flac16_44);
        let full = provider(Quality::Flac24_192);

        assert_eq!(
            matcher_quality_ceiling(Quality::Flac24_192, std::slice::from_ref(&narrow)),
            Quality::Flac16_44
        );
        assert_eq!(
            matcher_quality_ceiling(Quality::Flac16_44, std::slice::from_ref(&full)),
            Quality::Flac16_44
        );
        assert_eq!(
            matcher_quality_ceiling(Quality::Flac24_96, &[full.clone(), narrow.clone()]),
            Quality::Flac16_44
        );
        assert_eq!(
            matcher_quality_ceiling(Quality::Flac24_96, &[]),
            Quality::Flac24_96
        );
    }

    #[tokio::test]
    async fn a_worker_is_given_time_to_finish_its_step_before_it_is_aborted() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&finished);
        let task = tokio::spawn(async move {
            let _ = shutdown_rx.changed().await;
            // The step in progress when the stop is requested.
            tokio::time::sleep(Duration::from_millis(50)).await;
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        stop_tasks(Some(shutdown_tx), vec![task], Duration::from_secs(2)).await;
        assert!(finished.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_worker_that_never_stops_is_aborted_after_the_grace_period() {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(std::future::pending::<()>());
        let abort = task.abort_handle();

        stop_tasks(Some(shutdown_tx), vec![task], Duration::from_millis(50)).await;
        assert!(abort.is_finished());
    }

    /// Sync state belongs to the account that made it: another account starts
    /// empty, and state from before the owner file is kept by the first opener.
    #[test]
    fn sync_state_stays_with_the_account_that_made_it() {
        let dirs = temp_dirs("owner");
        let db = dirs.sync_db_file();
        let owner = dirs.sync_owner_file();

        {
            let store = SyncStore::open(&db).unwrap();
            store.set_playlist_enabled("legacy", "Old", true).unwrap();
        }
        let store = open_for_account(&db, &owner, "account-a", &[]).unwrap();
        assert_eq!(store.list_synced_playlists().unwrap().len(), 1);
        store
            .set_playlist_enabled("pl1", "Favorites", true)
            .unwrap();
        drop(store);

        let store = open_for_account(&db, &owner, "account-a", &[]).unwrap();
        assert_eq!(store.list_synced_playlists().unwrap().len(), 2);
        drop(store);

        let store = open_for_account(&db, &owner, "account-b", &[]).unwrap();
        assert!(store.list_synced_playlists().unwrap().is_empty());
        assert_eq!(read_owner(&owner).as_deref(), Some("account-b"));
        drop(store);

        let _ = std::fs::remove_dir_all(dirs.config.parent().unwrap());
    }

    #[tokio::test]
    async fn sync_state_follows_the_signed_in_account() {
        let dirs = temp_dirs("accounts");
        let creds = CredentialStore::in_memory(dirs.clone());
        let source = Arc::new(FakeSource::new());
        let (state_tx, mut state_rx) = watch::channel(SyncUiState::default());
        let on_state: SyncStateHook = Arc::new(move |state| {
            let _ = state_tx.send(state);
        });
        let controller = SyncController::start_inner(
            dirs.sync_db_file(),
            dirs.sync_owner_file(),
            creds,
            source.clone(),
            SyncSettings::default(),
            dirs.state.join("sync"),
            on_state,
            QobuzConfig::default(),
        );

        controller.set_playlist("pl1".into(), "Favorites".into(), true);
        wait_for_state(&mut state_rx, |s| s.is_playlist_enabled("pl1")).await;

        // Signed out: none of the account's playlists are shown.
        source.set_account(None);
        wait_for_state(&mut state_rx, |s| s.playlists.is_empty()).await;

        // Signed in again as the same account: its state is back.
        source.set_account(Some("account-a"));
        wait_for_state(&mut state_rx, |s| s.is_playlist_enabled("pl1")).await;

        // Another account never sees it.
        source.set_account(Some("account-b"));
        wait_for_state(&mut state_rx, |s| s.playlists.is_empty()).await;

        controller.shutdown();
        let _ = std::fs::remove_dir_all(dirs.config.parent().unwrap());
    }

    /// Rejected new credentials change nothing that is saved or in use.
    #[tokio::test]
    async fn rejected_qobuz_credentials_leave_the_saved_ones_in_place() {
        let dirs = temp_dirs("qobuz-rejected");
        let creds = CredentialStore::in_memory(dirs.clone());
        let saved = creds.clone();
        let source = Arc::new(FakeSource::new());
        let (base_url, server) = spawn_mock_qobuz_server().await;
        let (state_tx, mut state_rx) = watch::channel(SyncUiState::default());
        let on_state: SyncStateHook = Arc::new(move |state| {
            let _ = state_tx.send(state);
        });
        let controller = SyncController::start_inner(
            dirs.sync_db_file(),
            dirs.sync_owner_file(),
            creds,
            source,
            SyncSettings::default(),
            dirs.state.join("sync"),
            on_state,
            QobuzConfig {
                base_url,
                ..QobuzConfig::default()
            },
        );

        controller.save_qobuz_credentials("123456".into(), "valid-token".into(), None);
        wait_for_state(&mut state_rx, |s| {
            matches!(s.qobuz_credentials, CredentialStatus::Valid { .. })
        })
        .await;

        controller.save_qobuz_credentials("999999".into(), "bad-token".into(), None);
        wait_for_state(&mut state_rx, |s| {
            matches!(s.qobuz_credentials, CredentialStatus::Invalid { .. })
        })
        .await;

        let state = state_rx.borrow().clone();
        assert_eq!(state.qobuz_app_id_hint.as_deref(), Some("123456"));
        match &state.qobuz_credentials {
            CredentialStatus::Invalid { message, .. } => {
                assert!(message.contains("not saved"), "{message}");
                assert!(
                    !message.contains("bad-token") && !message.contains("999999"),
                    "{message}"
                );
            }
            other => panic!("expected the rejection, got {other:?}"),
        }
        let kept = saved
            .load_qobuz()
            .await
            .unwrap()
            .expect("saved credentials remain");
        assert_eq!(kept.app_id, "123456");
        assert_eq!(kept.user_auth_token, "valid-token");

        controller.shutdown();
        server.abort();
        let _ = std::fs::remove_dir_all(dirs.config.parent().unwrap());
    }
}
