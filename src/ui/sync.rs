//! Lossless playlist sync UI: Settings section, Sync Queue, Synced Library,
//! track status indicators, and player-bar lossless badge.
//!
//! All state mutations go through [`Action`] values pushed to `app.actions`;
//! nothing here touches SQLite, disk, or network on the UI thread.

use std::collections::BTreeMap;
use std::path::PathBuf;

use egui::{
    Align, Color32, CornerRadius, Frame, Layout, Margin, Rect, Sense, Stroke, Ui, UiBuilder, pos2,
    vec2,
};
use spotisync::{PlaylistCounts, ProviderCooldown, SyncStatus, SyncedPlaylist};

use crate::api::models::PlayableItem;
use crate::app::App;
use crate::i18n::gettext;
use crate::local_player::LocalFlacTrack;
use crate::model::{Action, Page, RowContext};
use crate::settings::SyncQualityCeiling;
use crate::sync_bridge::{CredentialStatus, SyncedLibraryTrack, TrackSyncInfo};
use crate::theme::{self, Icon, Palette};
use crate::util;

use super::settings::{
    RowText, filtered_row, filtered_row_sized, row_matches, section, section_matches,
    setting_slider,
};
use super::widgets;

/// Extracts a Spotify playlist ID from a track table's [`RowContext`].
pub(crate) fn context_playlist_id(context: &RowContext) -> Option<&str> {
    let uri = match context {
        RowContext::Context { uri, .. } => uri.as_str(),
        RowContext::View { context_uri, .. } => context_uri.as_str(),
        _ => return None,
    };
    (util::uri_kind(uri) == Some("playlist"))
        .then(|| util::uri_id(uri))
        .flatten()
}

/// Extracts a Spotify track ID from a [`PlayableItem`].
pub(crate) fn item_track_id(item: &PlayableItem) -> Option<&str> {
    let PlayableItem::Track(track) = item else {
        return None;
    };
    track
        .id
        .as_deref()
        .filter(|id| !id.is_empty())
        .or_else(|| util::uri_id(&track.uri))
}

/// Formats a provider identifier into a human-friendly display name.
fn provider_display_name(id: &str) -> String {
    match id {
        "qobuz" => "Qobuz".to_string(),
        other => other.to_string(),
    }
}

/// Formats a lossless quality string consistently for badges and tooltips.
fn format_quality_badge(raw: &str) -> String {
    let trimmed = raw.trim();
    match trimmed.to_ascii_lowercase().as_str() {
        "flac 16-bit/44.1khz" | "flac_16_44" | "flac 16-bit / 44.1 khz" => {
            "16-bit / 44.1 kHz FLAC".to_string()
        }
        "flac 24-bit/96khz" | "flac_24_96" | "flac 24-bit / 96 khz" => {
            "24-bit / 96 kHz FLAC".to_string()
        }
        "flac 24-bit/192khz" | "flac_24_192" | "flac 24-bit / 192 khz" => {
            "24-bit / 192 kHz FLAC".to_string()
        }
        _ => trimmed.to_string(),
    }
}

/// Formats an active provider rate-limit cooldown for banner display.
pub(crate) fn format_cooldown_banner(cooldown: &ProviderCooldown) -> String {
    if cooldown.remaining_secs > 0 {
        format!(
            "{}: rate limited, retrying in {}s",
            cooldown.provider, cooldown.remaining_secs
        )
    } else if !cooldown.message.trim().is_empty() {
        format!("{}: {}", cooldown.provider, cooldown.message.trim())
    } else {
        format!("{}: rate limited, retrying soon", cooldown.provider)
    }
}

fn status_label(status: SyncStatus) -> &'static str {
    match status {
        SyncStatus::Done => "Synced",
        SyncStatus::Downloading => "Downloading",
        SyncStatus::Resolving => "Resolving",
        SyncStatus::Pending => "Pending",
        SyncStatus::Unmatched => "Unmatched",
        SyncStatus::Failed => "Failed",
    }
}

fn status_color(palette: &Palette, status: SyncStatus) -> Color32 {
    match status {
        SyncStatus::Done => palette.accent,
        SyncStatus::Downloading | SyncStatus::Resolving | SyncStatus::Unmatched => palette.warning,
        SyncStatus::Pending => palette.dim,
        SyncStatus::Failed => palette.danger,
    }
}

fn status_sort_rank(status: SyncStatus) -> u8 {
    match status {
        SyncStatus::Downloading => 0,
        SyncStatus::Resolving => 1,
        SyncStatus::Pending => 2,
        SyncStatus::Failed => 3,
        SyncStatus::Unmatched => 4,
        SyncStatus::Done => 5,
    }
}

fn status_tooltip(info: &TrackSyncInfo) -> String {
    let mut lines = Vec::with_capacity(4);
    lines.push(format!("Sync status: {}", status_label(info.status)));
    if let Some(quality) = info.quality.as_deref().filter(|q| !q.is_empty()) {
        lines.push(format!("Quality: {}", format_quality_badge(quality)));
    }
    if let Some(provider) = info.provider.as_deref().filter(|p| !p.is_empty()) {
        let matched = info
            .matched_by
            .as_deref()
            .filter(|m| !m.is_empty())
            .map(|m| format!(" (matched by {m})"))
            .unwrap_or_default();
        lines.push(format!(
            "Provider: {}{matched}",
            provider_display_name(provider)
        ));
    } else if let Some(matched) = info.matched_by.as_deref().filter(|m| !m.is_empty()) {
        lines.push(format!("Matched by: {matched}"));
    }
    if let Some(err) = info.error.as_deref().filter(|e| !e.is_empty()) {
        lines.push(format!("Error: {err}"));
    }
    lines.join("\n")
}

/// Paints a crisp 7.5pt sync status indicator dot with a hover tooltip.
pub(crate) fn track_status_dot(ui: &mut Ui, palette: &Palette, info: &TrackSyncInfo) {
    let (rect, response) = ui.allocate_exact_size(vec2(10.0, 14.0), Sense::hover());
    if ui.is_rect_visible(rect) {
        paint_status_dot(ui.painter(), rect.center(), palette, info.status);
    }
    let a11y_label = format!("Sync status: {}", status_label(info.status));
    response.widget_info(|| {
        egui::WidgetInfo::labeled(egui::WidgetType::Label, true, a11y_label.clone())
    });
    ui.ctx()
        .accesskit_node_builder(response.id, |node| node.set_label(a11y_label));
    if response.hovered() {
        response.on_hover_text(status_tooltip(info));
    }
}

fn paint_status_dot(
    painter: &egui::Painter,
    center: egui::Pos2,
    palette: &Palette,
    status: SyncStatus,
) {
    let color = status_color(palette, status);
    match status {
        SyncStatus::Unmatched => {
            painter.circle_stroke(center, 3.25, Stroke::new(1.75, color));
        }
        SyncStatus::Downloading | SyncStatus::Resolving => {
            painter.circle_filled(center, 5.0, color.gamma_multiply(0.24));
            painter.circle_filled(center, 3.5, color);
        }
        _ => {
            painter.circle_filled(center, 3.75, color);
        }
    }
}

/// Adds **Sync this playlist** / **Stop syncing** to playlist context menus.
pub(crate) fn playlist_menu_item(ui: &mut Ui, app: &mut App, uri: &str, name: &str) {
    if util::uri_kind(uri) != Some("playlist") {
        return;
    }
    let Some(playlist_id) = util::uri_id(uri) else {
        return;
    };
    let palette = app.palette;
    let enabled = app.sync_state.is_playlist_enabled(playlist_id);
    let (icon, label) = if enabled {
        (Icon::CircleX, gettext(app.locale, "Stop syncing"))
    } else {
        (Icon::Refresh, gettext(app.locale, "Sync this playlist"))
    };
    if widgets::menu_item(ui, &palette, Some(icon), &label) {
        app.actions.push(Action::TogglePlaylistSync {
            playlist_id: playlist_id.to_string(),
            playlist_name: name.to_string(),
            enabled: !enabled,
        });
    }
}

/// Compact synced status pill rendered in a synced playlist's header action row.
pub(crate) fn playlist_sync_pill(app: &mut App, ui: &mut Ui, playlist_uri: &str) {
    if util::uri_kind(playlist_uri) != Some("playlist") {
        return;
    }
    let Some(playlist_id) = util::uri_id(playlist_uri) else {
        return;
    };
    if !app.sync_state.is_playlist_enabled(playlist_id) {
        return;
    }
    let palette = app.palette;
    let counts = app.sync_state.playlist_counts.get(playlist_id);
    let (label, icon, tooltip) = match counts {
        Some(c) => {
            let total = counts_total(c);
            let active = c.pending + c.resolving + c.downloading;
            let text = if total > 0 {
                format!("Synced ({}/{total})", c.done)
            } else {
                "Synced".to_string()
            };
            let icon = if active > 0 {
                Icon::Refresh
            } else {
                Icon::CircleCheck
            };
            let tip = format!(
                "Done: {}, Active: {}, Pending: {}, Unmatched: {}, Failed: {}. Click to open Sync Queue.",
                c.done,
                c.resolving + c.downloading,
                c.pending,
                c.unmatched,
                c.failed
            );
            (text, icon, tip)
        }
        None => (
            "Synced".to_string(),
            Icon::CircleCheck,
            "Lossless playlist sync is enabled. Click to open Sync Queue.".to_string(),
        ),
    };
    let response = theme::soft_button(ui, &palette, Some(icon), &label, false);
    let response = if response.hovered() {
        response.on_hover_text(tooltip)
    } else {
        response
    };
    if response.clicked() {
        app.actions.push(Action::Open(Page::SyncQueue));
    }
}

/// Adds per-track sync actions to a track's context / overflow menu when the
/// track belongs to a synced playlist.
pub(crate) fn track_menu_items(
    ui: &mut Ui,
    app: &mut App,
    item: &PlayableItem,
    context: Option<&RowContext>,
) {
    let Some(playlist_id) = context.and_then(context_playlist_id) else {
        return;
    };
    let Some(track_id) = item_track_id(item) else {
        return;
    };
    let Some(info) = app.sync_state.track_status(playlist_id, track_id).cloned() else {
        return;
    };
    let palette = app.palette;
    let locale = app.locale;
    match info.status {
        SyncStatus::Failed | SyncStatus::Unmatched => {
            widgets::menu_separator(ui, &palette);
            if widgets::menu_item(
                ui,
                &palette,
                Some(Icon::Refresh),
                &gettext(locale, "Retry sync"),
            ) {
                app.actions.push(Action::SyncRetryTrack {
                    playlist_id: playlist_id.to_string(),
                    track_id: track_id.to_string(),
                });
            }
        }
        SyncStatus::Done => {
            if let Some(path) = info.path {
                widgets::menu_separator(ui, &palette);
                if widgets::menu_item(
                    ui,
                    &palette,
                    Some(Icon::Play),
                    &gettext(locale, "Play lossless copy"),
                ) {
                    let local_track = item_to_local_track(
                        item,
                        playlist_id,
                        track_id,
                        path.clone(),
                        info.quality.as_deref(),
                    );
                    app.actions.push(Action::PlaySyncedTrack {
                        path: path.clone(),
                        queue: vec![local_track],
                        index: 0,
                    });
                }
                if widgets::menu_item(
                    ui,
                    &palette,
                    Some(Icon::ExternalLink),
                    &gettext(locale, "Open synced folder"),
                ) {
                    app.actions.push(Action::SyncOpenTrackFolder(path));
                }
            }
        }
        SyncStatus::Pending | SyncStatus::Resolving | SyncStatus::Downloading => {}
    }
}

/// Returns the short lossless badge text for the player bar when local FLAC
/// playback is active.
pub(crate) fn player_bar_badge_label(app: &App) -> Option<String> {
    if !app.local_flac.active {
        return None;
    }
    let label = app
        .local_flac
        .track
        .as_ref()
        .and_then(|t| t.quality_label.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            let formatted = format_quality_badge(s);
            if let Some(stripped) = formatted.strip_suffix(" FLAC") {
                format!("FLAC {stripped}")
            } else if formatted.to_ascii_uppercase().contains("FLAC") {
                formatted
            } else {
                format!("FLAC {formatted}")
            }
        })
        .unwrap_or_else(|| "FLAC".to_string());
    Some(label)
}

/// Paints a compact lossless FLAC badge in the player bar when local FLAC
/// playback is active.
pub(crate) fn player_bar_lossless_badge(app: &App, ui: &mut Ui) {
    let Some(label) = player_bar_badge_label(app) else {
        return;
    };
    let palette = app.palette;
    let fill = super::blend(palette.panel, palette.accent, 0.18);
    let stroke = Stroke::new(1.0, palette.accent.gamma_multiply(0.45));
    Frame::new()
        .fill(fill)
        .stroke(stroke)
        .corner_radius(CornerRadius::same(4))
        .inner_margin(Margin::symmetric(5, 1))
        .show(ui, |ui| {
            theme::text(ui, &label, theme::semibold(10.5), palette.accent);
        });
}

fn synced_to_local_track(track: &SyncedLibraryTrack) -> LocalFlacTrack {
    LocalFlacTrack {
        path: track.path.clone(),
        title: track.title.clone(),
        artists: track
            .artists
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
        album: track.album.clone(),
        duration_ms: track.duration_ms,
        playlist_id: Some(track.playlist_id.clone()),
        track_id: Some(track.track_id.clone()),
        quality_label: track.quality.as_deref().map(format_quality_badge),
        sample_rate_hz: None,
        bit_depth: None,
    }
}

fn item_to_local_track(
    item: &PlayableItem,
    playlist_id: &str,
    track_id: &str,
    path: PathBuf,
    quality: Option<&str>,
) -> LocalFlacTrack {
    match item {
        PlayableItem::Track(track) => LocalFlacTrack {
            path,
            title: track.name.clone(),
            artists: track.artists.iter().map(|a| a.name.clone()).collect(),
            album: track
                .album
                .as_ref()
                .map(|a| a.name.clone())
                .unwrap_or_default(),
            duration_ms: u64::from(track.duration_ms),
            playlist_id: Some(playlist_id.to_string()),
            track_id: Some(track_id.to_string()),
            quality_label: quality.map(format_quality_badge),
            sample_rate_hz: None,
            bit_depth: None,
        },
        PlayableItem::Episode(ep) => LocalFlacTrack {
            path,
            title: ep.name.clone(),
            artists: ep
                .show
                .as_ref()
                .map(|s| vec![s.name.clone()])
                .unwrap_or_default(),
            album: String::new(),
            duration_ms: u64::from(ep.duration_ms),
            playlist_id: Some(playlist_id.to_string()),
            track_id: Some(track_id.to_string()),
            quality_label: quality.map(format_quality_badge),
            sample_rate_hz: None,
            bit_depth: None,
        },
    }
}

fn counts_total(counts: &PlaylistCounts) -> u64 {
    counts.pending
        + counts.resolving
        + counts.downloading
        + counts.done
        + counts.unmatched
        + counts.failed
}

fn merged_synced_playlists(app: &App) -> Vec<SyncedPlaylist> {
    let mut by_id: BTreeMap<String, SyncedPlaylist> = BTreeMap::new();
    for entry in &app.settings.sync.synced_playlists {
        by_id.insert(
            entry.id.clone(),
            SyncedPlaylist {
                playlist_id: entry.id.clone(),
                name: entry.name.clone(),
                enabled: entry.enabled,
                last_polled_at: None,
            },
        );
    }
    for entry in &app.sync_state.playlists {
        by_id.insert(entry.playlist_id.clone(), entry.clone());
    }
    let mut list: Vec<SyncedPlaylist> = by_id.into_values().collect();
    list.sort_by(|a, b| {
        b.enabled
            .cmp(&a.enabled)
            .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
            .then_with(|| a.playlist_id.cmp(&b.playlist_id))
    });
    list
}

fn credential_status_display(palette: &Palette, status: &CredentialStatus) -> (Color32, String) {
    match status {
        CredentialStatus::Valid { detail, .. } => {
            let text = if detail.trim().is_empty() {
                "Valid".to_string()
            } else {
                format!("Valid ({detail})")
            };
            (palette.accent, text)
        }
        CredentialStatus::Validating => (palette.warning, "Validating...".to_string()),
        CredentialStatus::Invalid { message, .. } => {
            (palette.danger, format!("Invalid: {message}"))
        }
        CredentialStatus::Missing => (palette.secondary, "Not configured".to_string()),
    }
}

/// Renders the Settings -> Sync section right after Playback, integrated with
/// the Settings live search filter. Returns true when any part of the section
/// matched `needle` and was rendered.
pub fn settings_section(app: &mut App, ui: &mut Ui, needle: &str) -> bool {
    let palette = app.palette;
    let locale = app.locale;
    let title = gettext(locale, "Sync");
    let resolved_root = app.settings.resolved_sync_root(&app.dirs);
    let has_custom_root = app.settings.sync.library_root.is_some();
    let root_description = if has_custom_root {
        resolved_root.display().to_string()
    } else {
        format!("Default: {}", resolved_root.display())
    };
    let (_, cred_text) = credential_status_display(&palette, &app.sync_state.qobuz_credentials);
    let rows = [
        RowText::new(gettext(locale, "Library root"), root_description),
        RowText::new(
            gettext(locale, "Providers"),
            gettext(
                locale,
                "Enable lossless audio providers and set their resolution priority.",
            ),
        ),
        RowText::new(
            gettext(locale, "Qobuz credentials"),
            format!("Status: {cred_text}. Saved tokens are stored securely and never shown again."),
        ),
        RowText::new(
            gettext(locale, "Quality ceiling"),
            gettext(
                locale,
                "Maximum FLAC resolution requested from lossless providers.",
            ),
        ),
        RowText::new(
            gettext(locale, "Concurrency"),
            gettext(
                locale,
                "Simultaneous downloads per provider (1 to 8, default 2).",
            ),
        ),
        RowText::new(
            gettext(locale, "Synced playlists"),
            gettext(
                locale,
                "Playlists currently tracked for automatic lossless downloads.",
            ),
        ),
    ];

    let sync_keywords = row_matches(
        needle,
        &title,
        "lossless flac qobuz sync playlist library root quality ceiling concurrency credentials token",
    );
    if !sync_keywords && !section_matches(needle, &title, &rows) {
        return false;
    }

    let rows_needle = if sync_keywords { "" } else { needle };

    section(ui, &palette, &title, |ui| {
        // 1. Library root
        let choose_label = gettext(locale, "Choose folder");
        let reset_label = gettext(locale, "Reset to default");
        let pill_width = |label: &str| theme::soft_button_width(ui, label) + 16.0;
        let root_controls_width = pill_width(&choose_label)
            + if has_custom_root {
                pill_width(&reset_label) + 8.0
            } else {
                0.0
            };
        filtered_row_sized(
            ui,
            &palette,
            rows_needle,
            &title,
            &rows[0],
            root_controls_width,
            |ui| {
                ui.spacing_mut().item_spacing.x = 6.0;
                if has_custom_root
                    && theme::pill_button(ui, &palette, &reset_label, false).clicked()
                {
                    app.actions.push(Action::SyncClearLibraryRoot);
                }
                if theme::pill_button(ui, &palette, &choose_label, false).clicked() {
                    app.actions.push(Action::SyncChooseLibraryRoot);
                }
            },
        );

        // 2. Providers (enable & priority)
        if row_matches(rows_needle, &title, "providers qobuz priority enable")
            || rows[1].matches(rows_needle, &title)
        {
            filtered_row(ui, &palette, rows_needle, &title, &rows[1], |_| {});
            let providers = app.settings.sync.providers.clone();
            let multiple = providers.len() > 1;
            if providers.is_empty() {
                theme::subtle(ui, &palette, "No providers configured.");
            } else {
                for (index, provider) in providers.iter().enumerate() {
                    ui.push_id(("sync-provider", &provider.id), |ui| {
                        Frame::new()
                            .fill(palette.surface)
                            .stroke(Stroke::new(1.0, palette.outline))
                            .corner_radius(CornerRadius::same(theme::RADIUS))
                            .inner_margin(Margin::symmetric(12, 8))
                            .show(ui, |ui| {
                                ui.set_width(ui.available_width());
                                ui.horizontal(|ui| {
                                    theme::text(
                                        ui,
                                        format!("{}.", index + 1),
                                        theme::medium(13.0),
                                        palette.secondary,
                                    );
                                    theme::text(
                                        ui,
                                        provider_display_name(&provider.id),
                                        theme::medium(14.0),
                                        palette.text,
                                    );
                                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                                        let mut enabled = provider.enabled;
                                        let label = format!(
                                            "Enable {}",
                                            provider_display_name(&provider.id)
                                        );
                                        if widgets::switch(ui, &palette, &label, &mut enabled)
                                            .changed()
                                        {
                                            app.actions.push(Action::SyncSetProviderEnabled {
                                                provider_id: provider.id.clone(),
                                                enabled,
                                            });
                                        }
                                        if multiple {
                                            let can_down = index + 1 < providers.len();
                                            let down_clicked = ui
                                                .add_enabled_ui(can_down, |ui| {
                                                    theme::icon_button(
                                                        ui,
                                                        Icon::ChevronDown,
                                                        16.0,
                                                        palette.secondary,
                                                        palette.text,
                                                        "Move down",
                                                    )
                                                })
                                                .inner
                                                .clicked();
                                            if down_clicked {
                                                app.actions.push(Action::SyncMoveProvider {
                                                    provider_id: provider.id.clone(),
                                                    up: false,
                                                });
                                            }
                                            let can_up = index > 0;
                                            let up_clicked = ui
                                                .add_enabled_ui(can_up, |ui| {
                                                    theme::icon_button(
                                                        ui,
                                                        Icon::ChevronUp,
                                                        16.0,
                                                        palette.secondary,
                                                        palette.text,
                                                        "Move up",
                                                    )
                                                })
                                                .inner
                                                .clicked();
                                            if up_clicked {
                                                app.actions.push(Action::SyncMoveProvider {
                                                    provider_id: provider.id.clone(),
                                                    up: true,
                                                });
                                            }
                                        }
                                    });
                                });
                            });
                    });
                    ui.add_space(4.0);
                }
            }
            ui.add_space(8.0);
        }

        // 3. Qobuz credentials
        if rows[2].matches(rows_needle, &title) {
            let (status_color, status_text) =
                credential_status_display(&palette, &app.sync_state.qobuz_credentials);
            filtered_row(ui, &palette, rows_needle, &title, &rows[2], |ui| {
                status_pill(ui, &palette, status_color, &status_text);
            });
            Frame::new()
                .fill(palette.surface)
                .stroke(Stroke::new(1.0, palette.outline))
                .corner_radius(CornerRadius::same(theme::RADIUS))
                .inner_margin(Margin::symmetric(12, 10))
                .show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    let field_width = ui.available_width().clamp(160.0, 360.0);
                    let app_id_hint = app
                        .sync_state
                        .qobuz_app_id_hint
                        .as_deref()
                        .filter(|h| !h.is_empty())
                        .map(|h| format!("Saved App ID: {h}"))
                        .unwrap_or_else(|| "App ID".to_string());

                    credential_field(
                        ui,
                        &palette,
                        locale,
                        "sync-qobuz-app-id",
                        "App ID",
                        &app_id_hint,
                        &mut app.sync_qobuz_app_id_input,
                        false,
                        field_width,
                    );
                    ui.add_space(6.0);
                    credential_field(
                        ui,
                        &palette,
                        locale,
                        "sync-qobuz-token",
                        "User Auth Token",
                        "Enter User Auth Token",
                        &mut app.sync_qobuz_token_input,
                        true,
                        field_width,
                    );
                    ui.add_space(6.0);
                    credential_field(
                        ui,
                        &palette,
                        locale,
                        "sync-qobuz-secret",
                        "App Secret (optional)",
                        "Optional App Secret",
                        &mut app.sync_qobuz_secret_input,
                        true,
                        field_width,
                    );
                    ui.add_space(10.0);

                    let effective_app_id = if app.sync_qobuz_app_id_input.trim().is_empty() {
                        app.sync_state.qobuz_app_id_hint.clone().unwrap_or_default()
                    } else {
                        app.sync_qobuz_app_id_input.trim().to_string()
                    };
                    let can_save = !effective_app_id.is_empty()
                        && !app.sync_qobuz_token_input.trim().is_empty()
                        && !matches!(
                            app.sync_state.qobuz_credentials,
                            CredentialStatus::Validating
                        );
                    let can_validate = !matches!(
                        app.sync_state.qobuz_credentials,
                        CredentialStatus::Missing | CredentialStatus::Validating
                    );
                    let can_clear =
                        !matches!(app.sync_state.qobuz_credentials, CredentialStatus::Missing)
                            || !app.sync_qobuz_app_id_input.is_empty()
                            || !app.sync_qobuz_token_input.is_empty()
                            || !app.sync_qobuz_secret_input.is_empty();

                    ui.horizontal_wrapped(|ui| {
                        ui.spacing_mut().item_spacing = vec2(8.0, 6.0);
                        let save_clicked = ui
                            .add_enabled_ui(can_save, |ui| {
                                theme::pill_button(ui, &palette, "Save & validate", true)
                            })
                            .inner
                            .clicked();
                        if save_clicked {
                            let secret = app.sync_qobuz_secret_input.trim();
                            app.actions.push(Action::SyncSaveQobuzCredentials {
                                app_id: effective_app_id,
                                user_auth_token: app.sync_qobuz_token_input.trim().to_string(),
                                app_secret: (!secret.is_empty()).then(|| secret.to_string()),
                            });
                        }
                        let validate_clicked = ui
                            .add_enabled_ui(can_validate, |ui| {
                                theme::soft_button(
                                    ui,
                                    &palette,
                                    Some(Icon::Check),
                                    "Validate",
                                    false,
                                )
                            })
                            .inner
                            .clicked();
                        if validate_clicked {
                            app.actions.push(Action::SyncValidateQobuzCredentials);
                        }
                        let clear_clicked = ui
                            .add_enabled_ui(can_clear, |ui| {
                                theme::soft_button(ui, &palette, Some(Icon::Trash), "Clear", false)
                            })
                            .inner
                            .clicked();
                        if clear_clicked {
                            app.actions.push(Action::SyncClearQobuzCredentials);
                        }
                    });
                });
            ui.add_space(10.0);
        }

        // 4. Quality ceiling
        if rows[3].matches(rows_needle, &title) {
            let quality_options = [
                (SyncQualityCeiling::Flac16_44, "16-bit / 44.1 kHz (CD)"),
                (SyncQualityCeiling::Flac24_96, "24-bit / 96 kHz (Hi-Res)"),
                (
                    SyncQualityCeiling::Flac24_192,
                    "24-bit / 192 kHz (Hi-Res Max)",
                ),
            ];
            let current_quality = app.settings.sync.quality_ceiling;
            filtered_row(ui, &palette, rows_needle, &title, &rows[3], |_| {});
            if let Some(picked) =
                widgets::wrapped_chips(ui, &palette, &quality_options, current_quality)
                && picked != current_quality
            {
                app.actions.push(Action::SyncSetQualityCeiling(picked));
            }
            ui.add_space(10.0);
        }

        // 5. Concurrency
        filtered_row(ui, &palette, rows_needle, &title, &rows[4], |ui| {
            let mut concurrency = app.settings.sync.clamped_concurrency();
            let response = setting_slider(
                ui,
                &palette,
                &mut concurrency,
                1..=8,
                |slider| slider.step_by(1.0),
                |value| value.speed(0.1),
            );
            if response.changed() {
                app.actions.push(Action::SyncSetConcurrency(concurrency));
            }
        });

        // 6. Synced playlists table
        if rows[5].matches(rows_needle, &title) {
            filtered_row(ui, &palette, rows_needle, &title, &rows[5], |ui| {
                if theme::soft_button(ui, &palette, Some(Icon::Refresh), "Sync now", false)
                    .clicked()
                {
                    app.actions.push(Action::SyncTriggerNow);
                }
            });
            let playlists = merged_synced_playlists(app);
            if playlists.is_empty() {
                theme::subtle(
                    ui,
                    &palette,
                    "No playlists synced yet. Open any playlist menu and choose \"Sync this playlist\".",
                );
                ui.add_space(8.0);
            } else {
                for playlist in playlists {
                    let counts = app
                        .sync_state
                        .playlist_counts
                        .get(&playlist.playlist_id)
                        .cloned()
                        .unwrap_or_default();
                    ui.push_id(("settings-synced-playlist", &playlist.playlist_id), |ui| {
                        Frame::new()
                            .fill(palette.surface)
                            .stroke(Stroke::new(1.0, palette.outline))
                            .corner_radius(CornerRadius::same(theme::RADIUS))
                            .inner_margin(Margin::symmetric(12, 8))
                            .show(ui, |ui| {
                                ui.set_width(ui.available_width());
                                ui.horizontal_wrapped(|ui| {
                                    ui.spacing_mut().item_spacing = vec2(10.0, 4.0);
                                    if theme::link(
                                        ui,
                                        &playlist.name,
                                        theme::medium(14.0),
                                        palette.text,
                                    )
                                    .clicked()
                                    {
                                        app.actions.push(Action::Open(Page::Playlist(
                                            playlist.playlist_id.clone(),
                                        )));
                                    }
                                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                                        let label = if playlist.enabled {
                                            "Stop syncing"
                                        } else {
                                            "Resume syncing"
                                        };
                                        if theme::soft_button(ui, &palette, None, label, false)
                                            .clicked()
                                        {
                                            app.actions.push(Action::TogglePlaylistSync {
                                                playlist_id: playlist.playlist_id.clone(),
                                                playlist_name: playlist.name.clone(),
                                                enabled: !playlist.enabled,
                                            });
                                        }
                                    });
                                });
                                ui.add_space(4.0);
                                let active = counts.resolving + counts.downloading;
                                ui.horizontal_wrapped(|ui| {
                                    ui.spacing_mut().item_spacing = vec2(12.0, 4.0);
                                    count_chip(
                                        ui,
                                        &palette,
                                        SyncStatus::Pending,
                                        "Pending",
                                        counts.pending,
                                    );
                                    count_chip(
                                        ui,
                                        &palette,
                                        SyncStatus::Downloading,
                                        "Active",
                                        active,
                                    );
                                    count_chip(ui, &palette, SyncStatus::Done, "Done", counts.done);
                                    count_chip(
                                        ui,
                                        &palette,
                                        SyncStatus::Unmatched,
                                        "Unmatched",
                                        counts.unmatched,
                                    );
                                    count_chip(
                                        ui,
                                        &palette,
                                        SyncStatus::Failed,
                                        "Failed",
                                        counts.failed,
                                    );
                                });
                            });
                    });
                    ui.add_space(6.0);
                }
            }
        }
    });
    true
}

#[allow(clippy::too_many_arguments)]
fn credential_field(
    ui: &mut Ui,
    palette: &Palette,
    locale: crate::i18n::Locale,
    id: &str,
    label: &str,
    hint: &str,
    value: &mut String,
    password: bool,
    width: f32,
) {
    ui.vertical(|ui| {
        ui.spacing_mut().item_spacing.y = 3.0;
        theme::text(ui, label, theme::medium(12.5), palette.secondary);
        Frame::new()
            .fill(palette.panel)
            .stroke(Stroke::new(1.0, palette.outline))
            .corner_radius(CornerRadius::same(6))
            .inner_margin(Margin::symmetric(10, 6))
            .show(ui, |ui| {
                let mut edit = egui::TextEdit::singleline(value)
                    .id(egui::Id::new(id))
                    .hint_text(egui::RichText::new(hint).color(palette.dim))
                    .font(theme::regular(13.5))
                    .frame(egui::Frame::NONE)
                    .desired_width(width);
                if password {
                    edit = edit.password(true);
                }
                let response = widgets::text_edit(ui, locale, edit);
                ui.ctx()
                    .accesskit_node_builder(response.id, |node| node.set_label(label));
            });
    });
}

fn status_pill(ui: &mut Ui, palette: &Palette, color: Color32, text: &str) {
    let fill = super::blend(palette.panel, color, 0.16);
    Frame::new()
        .fill(fill)
        .stroke(Stroke::new(1.0, color.gamma_multiply(0.45)))
        .corner_radius(CornerRadius::same(10))
        .inner_margin(Margin::symmetric(8, 3))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.spacing_mut().item_spacing.x = 5.0;
                let (dot_rect, _) = ui.allocate_exact_size(vec2(7.0, 12.0), Sense::hover());
                ui.painter().circle_filled(dot_rect.center(), 3.2, color);
                theme::text(ui, text, theme::medium(12.0), color);
            });
        });
}

fn count_chip(ui: &mut Ui, palette: &Palette, status: SyncStatus, label: &str, count: u64) {
    ui.horizontal(|ui| {
        ui.spacing_mut().item_spacing.x = 4.0;
        let (dot_rect, _) = ui.allocate_exact_size(vec2(8.0, 14.0), Sense::hover());
        paint_status_dot(ui.painter(), dot_rect.center(), palette, status);
        theme::text(
            ui,
            format!("{label}: {count}"),
            theme::regular(12.5),
            if count > 0 {
                palette.text
            } else {
                palette.secondary
            },
        );
    });
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum QueueFilter {
    #[default]
    All,
    Active,
    Pending,
    Done,
    Unmatched,
    Failed,
}

impl QueueFilter {
    fn matches(self, status: SyncStatus) -> bool {
        match self {
            Self::All => true,
            Self::Active => matches!(status, SyncStatus::Resolving | SyncStatus::Downloading),
            Self::Pending => status == SyncStatus::Pending,
            Self::Done => status == SyncStatus::Done,
            Self::Unmatched => status == SyncStatus::Unmatched,
            Self::Failed => status == SyncStatus::Failed,
        }
    }
}

struct QueueRowEntry {
    playlist_id: String,
    playlist_name: String,
    track_id: String,
    title: String,
    artists: String,
    info: TrackSyncInfo,
}

fn collect_queue_rows(app: &App, filter: QueueFilter) -> Vec<QueueRowEntry> {
    let mut playlist_names: BTreeMap<&str, &str> = BTreeMap::new();
    for pl in &app.sync_state.playlists {
        playlist_names.insert(pl.playlist_id.as_str(), pl.name.as_str());
    }
    let mut library_lookup: BTreeMap<(&str, &str), &SyncedLibraryTrack> = BTreeMap::new();
    for track in &app.sync_state.library_tracks {
        library_lookup.insert((track.playlist_id.as_str(), track.track_id.as_str()), track);
    }

    let mut rows = Vec::with_capacity(app.sync_state.track_statuses.len());
    for ((playlist_id, track_id), info) in &app.sync_state.track_statuses {
        if !filter.matches(info.status) {
            continue;
        }
        let (title, artists) = if let Some(cached) = app.track_cache.get(track_id) {
            (cached.name.clone(), cached.artist_names())
        } else if let Some(lib) = library_lookup.get(&(playlist_id.as_str(), track_id.as_str())) {
            (lib.title.clone(), lib.artists.clone())
        } else {
            (format!("Track {track_id}"), String::new())
        };
        let playlist_name = playlist_names
            .get(playlist_id.as_str())
            .copied()
            .or_else(|| app.known_playlist(playlist_id).map(|p| p.name.as_str()))
            .unwrap_or(playlist_id.as_str())
            .to_string();
        rows.push(QueueRowEntry {
            playlist_id: playlist_id.clone(),
            playlist_name,
            track_id: track_id.clone(),
            title,
            artists,
            info: info.clone(),
        });
    }
    rows.sort_by(|a, b| {
        status_sort_rank(a.info.status)
            .cmp(&status_sort_rank(b.info.status))
            .then_with(|| a.playlist_id.cmp(&b.playlist_id))
            .then_with(|| a.track_id.cmp(&b.track_id))
    });
    rows
}

fn cooldown_banners(app: &App, ui: &mut Ui) {
    let palette = app.palette;
    for cooldown in app.sync_state.snapshot.cooldowns.values() {
        let message = format_cooldown_banner(cooldown);
        let fill = super::blend(palette.panel, palette.warning, 0.16);
        Frame::new()
            .fill(fill)
            .stroke(Stroke::new(1.0, palette.warning.gamma_multiply(0.55)))
            .corner_radius(CornerRadius::same(theme::RADIUS))
            .inner_margin(Margin::symmetric(12, 8))
            .show(ui, |ui| {
                ui.set_width(ui.available_width());
                ui.horizontal_wrapped(|ui| {
                    ui.spacing_mut().item_spacing.x = 8.0;
                    theme::icon(ui, Icon::CircleAlert, 16.0, palette.warning);
                    theme::text(ui, &message, theme::medium(13.0), palette.text);
                });
            });
        ui.add_space(6.0);
    }
    if let Some(error) = app
        .sync_state
        .last_error
        .as_deref()
        .filter(|e| !e.is_empty())
    {
        let fill = super::blend(palette.panel, palette.danger, 0.14);
        Frame::new()
            .fill(fill)
            .stroke(Stroke::new(1.0, palette.danger.gamma_multiply(0.5)))
            .corner_radius(CornerRadius::same(theme::RADIUS))
            .inner_margin(Margin::symmetric(12, 8))
            .show(ui, |ui| {
                ui.set_width(ui.available_width());
                ui.horizontal_wrapped(|ui| {
                    ui.spacing_mut().item_spacing.x = 8.0;
                    theme::icon(ui, Icon::CircleAlert, 16.0, palette.danger);
                    theme::text(ui, error, theme::regular(13.0), palette.text);
                });
            });
        ui.add_space(6.0);
    }
}

fn progress_summary(app: &App, ui: &mut Ui, compact: bool) {
    let palette = app.palette;
    let snap = &app.sync_state.snapshot;
    let total =
        snap.pending + snap.resolving + snap.downloading + snap.done + snap.unmatched + snap.failed;
    let done_frac = if total > 0 {
        snap.done as f32 / total as f32
    } else {
        0.0
    };
    let active_frac = if total > 0 {
        (snap.resolving + snap.downloading) as f32 / total as f32
    } else {
        0.0
    };

    Frame::new()
        .fill(
            palette
                .surface
                .gamma_multiply(if palette.dark { 0.75 } else { 1.0 }),
        )
        .stroke(Stroke::new(1.0, palette.outline))
        .corner_radius(CornerRadius::same(theme::RADIUS))
        .inner_margin(Margin::symmetric(
            if compact { 10 } else { 16 },
            if compact { 10 } else { 12 },
        ))
        .show(ui, |ui| {
            ui.set_width(ui.available_width());
            ui.horizontal(|ui| {
                let headline = if total == 0 {
                    "Idle".to_string()
                } else {
                    format!("{} of {} synced", snap.done, total)
                };
                theme::text(
                    ui,
                    headline,
                    theme::semibold(if compact { 13.5 } else { 15.0 }),
                    palette.text,
                );
                if total > 0 {
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        let pct = ((snap.done * 100) / total.max(1)).min(100);
                        theme::text(
                            ui,
                            format!("{pct}%"),
                            theme::medium(13.0),
                            palette.secondary,
                        );
                    });
                }
            });
            ui.add_space(6.0);
            let bar_height = if compact { 6.0 } else { 8.0 };
            let (bar_rect, _) =
                ui.allocate_exact_size(vec2(ui.available_width(), bar_height), Sense::hover());
            let radius = bar_height / 2.0;
            ui.painter()
                .rect_filled(bar_rect, radius, palette.surface_active);
            let done_w = bar_rect.width() * done_frac.clamp(0.0, 1.0);
            if done_w > 0.5 {
                let done_rect = Rect::from_min_size(bar_rect.min, vec2(done_w, bar_height));
                ui.painter().rect_filled(done_rect, radius, palette.accent);
            }
            let active_w = (bar_rect.width() * active_frac.clamp(0.0, 1.0))
                .min((bar_rect.width() - done_w).max(0.0));
            if active_w > 0.5 {
                let active_rect = Rect::from_min_size(
                    pos2(bar_rect.left() + done_w, bar_rect.top()),
                    vec2(active_w, bar_height),
                );
                ui.painter()
                    .rect_filled(active_rect, radius, palette.warning);
            }
            ui.add_space(8.0);
            ui.horizontal_wrapped(|ui| {
                ui.spacing_mut().item_spacing = vec2(if compact { 10.0 } else { 14.0 }, 4.0);
                count_chip(ui, &palette, SyncStatus::Pending, "Pending", snap.pending);
                count_chip(
                    ui,
                    &palette,
                    SyncStatus::Resolving,
                    "Resolving",
                    snap.resolving,
                );
                count_chip(
                    ui,
                    &palette,
                    SyncStatus::Downloading,
                    "Downloading",
                    snap.downloading,
                );
                count_chip(ui, &palette, SyncStatus::Done, "Done", snap.done);
                count_chip(
                    ui,
                    &palette,
                    SyncStatus::Unmatched,
                    "Unmatched",
                    snap.unmatched,
                );
                count_chip(ui, &palette, SyncStatus::Failed, "Failed", snap.failed);
            });
        });
}

/// Full-page Sync Queue view (`Page::SyncQueue`).
pub fn queue_view(app: &mut App, ui: &mut Ui) {
    let palette = app.palette;
    ui.add_space(8.0);
    ui.horizontal_wrapped(|ui| {
        theme::text(ui, "Sync Queue", theme::bold(28.0), palette.text);
        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
            ui.spacing_mut().item_spacing.x = 8.0;
            if theme::pill_button(ui, &palette, "Sync now", true).clicked() {
                app.actions.push(Action::SyncTriggerNow);
            }
            if theme::soft_button(ui, &palette, Some(Icon::Disc), "Synced Library", false).clicked()
            {
                app.actions.push(Action::Open(Page::SyncedLibrary));
            }
            if theme::soft_button(ui, &palette, Some(Icon::Settings), "Sync settings", false)
                .clicked()
            {
                app.actions.push(Action::Open(Page::Settings));
            }
        });
    });
    ui.add_space(10.0);

    cooldown_banners(app, ui);
    progress_summary(app, ui, false);
    ui.add_space(12.0);

    let filter_id = egui::Id::new("sync-queue-filter");
    let mut filter = ui
        .data(|d| d.get_temp::<QueueFilter>(filter_id))
        .unwrap_or_default();
    if let Some(picked) = widgets::chips(
        ui,
        &palette,
        &[
            (QueueFilter::All, "All"),
            (QueueFilter::Active, "Active"),
            (QueueFilter::Pending, "Pending"),
            (QueueFilter::Done, "Done"),
            (QueueFilter::Unmatched, "Unmatched"),
            (QueueFilter::Failed, "Failed"),
        ],
        filter,
    ) {
        filter = picked;
        ui.data_mut(|d| d.insert_temp(filter_id, filter));
    }
    ui.add_space(10.0);

    let rows = collect_queue_rows(app, filter);
    if rows.is_empty() {
        let (heading, body) = if app.sync_state.track_statuses.is_empty() {
            (
                "No synced tracks yet",
                "Enable sync on any playlist from its menu to queue lossless downloads.",
            )
        } else {
            (
                "No matching items",
                "Try another status filter to see the rest of the sync queue.",
            )
        };
        widgets::empty_state(ui, &palette, Icon::Refresh, heading, body);
        return;
    }

    let row_height = 58.0;
    widgets::virtual_rows(ui, rows.len(), row_height, |ui, index| {
        let entry = &rows[index];
        queue_item_row(app, ui, entry, row_height, false);
    });
}

/// Compact Sync Queue tab inside the right-hand Queue side panel (`QueueTab::Sync`).
pub fn queue_panel(app: &mut App, ui: &mut Ui) {
    let palette = app.palette;
    ui.horizontal_wrapped(|ui| {
        ui.spacing_mut().item_spacing = vec2(6.0, 4.0);
        if theme::soft_button(ui, &palette, Some(Icon::Refresh), "Sync now", false).clicked() {
            app.actions.push(Action::SyncTriggerNow);
        }
        if theme::soft_button(ui, &palette, Some(Icon::Expand), "Full view", false).clicked() {
            app.actions.push(Action::Open(Page::SyncQueue));
        }
    });
    ui.add_space(8.0);

    cooldown_banners(app, ui);
    progress_summary(app, ui, true);
    ui.add_space(10.0);

    let rows = collect_queue_rows(app, QueueFilter::All);
    if rows.is_empty() {
        theme::subtle(
            ui,
            &palette,
            "No tracks queued for sync. Use a playlist's menu to start syncing.",
        );
        return;
    }

    let row_height = 54.0;
    widgets::virtual_rows(ui, rows.len(), row_height, |ui, index| {
        let entry = &rows[index];
        queue_item_row(app, ui, entry, row_height, true);
    });
}

fn queue_item_row(app: &mut App, ui: &mut Ui, entry: &QueueRowEntry, height: f32, compact: bool) {
    let palette = app.palette;
    let width = ui.available_width();
    let (rect, response) = ui.allocate_exact_size(vec2(width, height), Sense::hover());
    if !ui.is_rect_visible(rect) {
        return;
    }
    let hovered = ui.rect_contains_pointer(rect);
    if hovered {
        ui.painter().rect_filled(
            rect,
            CornerRadius::same(6),
            palette
                .surface_hover
                .gamma_multiply(if palette.dark { 0.7 } else { 1.0 }),
        );
    }

    let action_width = match entry.info.status {
        SyncStatus::Failed | SyncStatus::Unmatched => {
            if compact {
                34.0
            } else {
                84.0
            }
        }
        SyncStatus::Done if entry.info.path.is_some() => {
            if compact {
                34.0
            } else {
                72.0
            }
        }
        _ => 0.0,
    };

    let dot_center = pos2(rect.left() + 14.0, rect.center().y);
    paint_status_dot(ui.painter(), dot_center, &palette, entry.info.status);

    let text_left = rect.left() + 28.0;
    let text_right = (rect.right() - action_width - 8.0).max(text_left + 40.0);
    let text_rect = Rect::from_min_max(
        pos2(text_left, rect.top() + 6.0),
        pos2(text_right, rect.bottom() - 6.0),
    );
    let mut text_ui = ui.new_child(
        UiBuilder::new()
            .max_rect(text_rect)
            .layout(Layout::top_down(Align::LEFT)),
    );
    text_ui.set_clip_rect(text_rect.intersect(ui.clip_rect()));
    text_ui.spacing_mut().item_spacing = vec2(6.0, 2.0);

    text_ui.horizontal(|ui| {
        theme::text(ui, &entry.title, theme::medium(13.5), palette.text);
        if !compact && let Some(quality) = entry.info.quality.as_deref().filter(|q| !q.is_empty()) {
            small_tag(ui, &palette, &format_quality_badge(quality), palette.accent);
        }
        if !compact
            && let Some(matched) = entry.info.matched_by.as_deref().filter(|m| !m.is_empty())
        {
            small_tag(ui, &palette, matched, palette.secondary);
        }
    });

    text_ui.horizontal(|ui| {
        let mut parts = Vec::new();
        if !entry.artists.is_empty() {
            parts.push(entry.artists.as_str());
        }
        if !compact && !entry.playlist_name.is_empty() {
            parts.push(entry.playlist_name.as_str());
        }
        let meta_line = if let Some(err) = entry.info.error.as_deref().filter(|e| !e.is_empty()) {
            if parts.is_empty() {
                err.to_string()
            } else {
                format!("{} · {err}", parts.join(" · "))
            }
        } else if parts.is_empty() {
            status_label(entry.info.status).to_string()
        } else {
            format!(
                "{} · {}",
                parts.join(" · "),
                status_label(entry.info.status)
            )
        };
        let meta_color = match entry.info.status {
            SyncStatus::Failed => palette.danger,
            SyncStatus::Unmatched => palette.warning,
            _ => palette.secondary,
        };
        theme::text(ui, meta_line, theme::regular(12.0), meta_color);
    });

    if action_width > 0.0 {
        let action_rect = Rect::from_min_max(
            pos2(rect.right() - action_width - 4.0, rect.top()),
            pos2(rect.right() - 4.0, rect.bottom()),
        );
        let mut action_ui = ui.new_child(
            UiBuilder::new()
                .max_rect(action_rect)
                .layout(Layout::right_to_left(Align::Center)),
        );
        action_ui.spacing_mut().item_spacing.x = 4.0;
        match entry.info.status {
            SyncStatus::Failed | SyncStatus::Unmatched => {
                let clicked = if compact {
                    theme::icon_button(
                        &mut action_ui,
                        Icon::Refresh,
                        16.0,
                        palette.secondary,
                        palette.text,
                        "Retry sync",
                    )
                    .clicked()
                } else {
                    theme::soft_button(
                        &mut action_ui,
                        &palette,
                        Some(Icon::Refresh),
                        "Retry",
                        false,
                    )
                    .clicked()
                };
                if clicked {
                    app.actions.push(Action::SyncRetryTrack {
                        playlist_id: entry.playlist_id.clone(),
                        track_id: entry.track_id.clone(),
                    });
                }
            }
            SyncStatus::Done => {
                if let Some(path) = &entry.info.path {
                    if theme::icon_button(
                        &mut action_ui,
                        Icon::ExternalLink,
                        16.0,
                        palette.secondary,
                        palette.text,
                        "Open synced folder",
                    )
                    .clicked()
                    {
                        app.actions.push(Action::SyncOpenTrackFolder(path.clone()));
                    }
                    if !compact
                        && theme::icon_button(
                            &mut action_ui,
                            Icon::Play,
                            16.0,
                            palette.accent,
                            palette.accent_hover,
                            "Play lossless copy",
                        )
                        .clicked()
                    {
                        let track = LocalFlacTrack {
                            path: path.clone(),
                            title: entry.title.clone(),
                            artists: entry
                                .artists
                                .split(',')
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                                .map(str::to_string)
                                .collect(),
                            album: String::new(),
                            duration_ms: 0,
                            playlist_id: Some(entry.playlist_id.clone()),
                            track_id: Some(entry.track_id.clone()),
                            quality_label: entry.info.quality.as_deref().map(format_quality_badge),
                            sample_rate_hz: None,
                            bit_depth: None,
                        };
                        app.actions.push(Action::PlaySyncedTrack {
                            path: path.clone(),
                            queue: vec![track],
                            index: 0,
                        });
                    }
                }
            }
            _ => {}
        }
    }

    if response.hovered() {
        response.on_hover_text(status_tooltip(&entry.info));
    }
}

fn small_tag(ui: &mut Ui, palette: &Palette, text: &str, color: Color32) {
    let fill = super::blend(palette.panel, color, 0.14);
    Frame::new()
        .fill(fill)
        .corner_radius(CornerRadius::same(4))
        .inner_margin(Margin::symmetric(5, 1))
        .show(ui, |ui| {
            theme::text(ui, text, theme::medium(11.0), color);
        });
}

/// First-class Synced Library view (`Page::SyncedLibrary`) listing downloaded
/// lossless FLAC tracks playable directly through `LocalFlacPlayer`.
pub fn library_view(app: &mut App, ui: &mut Ui) {
    let palette = app.palette;
    let locale = app.locale;
    let total_count = app.sync_state.library_tracks.len();

    let filter_id = egui::Id::new("synced-library-filter");
    let mut filter = ui
        .data(|d| d.get_temp::<String>(filter_id))
        .unwrap_or_default();
    let needle = filter.trim().to_lowercase();

    let filtered_indices: Vec<usize> = app
        .sync_state
        .library_tracks
        .iter()
        .enumerate()
        .filter(|(_, track)| {
            needle.is_empty()
                || track.title.to_lowercase().contains(&needle)
                || track.artists.to_lowercase().contains(&needle)
                || track.album.to_lowercase().contains(&needle)
                || track
                    .quality
                    .as_deref()
                    .is_some_and(|q| q.to_lowercase().contains(&needle))
                || track
                    .provider
                    .as_deref()
                    .is_some_and(|p| p.to_lowercase().contains(&needle))
        })
        .map(|(index, _)| index)
        .collect();

    ui.add_space(8.0);
    ui.horizontal_wrapped(|ui| {
        ui.vertical(|ui| {
            theme::text(ui, "Synced Library", theme::bold(28.0), palette.text);
            let count_text = if total_count == 1 {
                "1 lossless track".to_string()
            } else {
                format!("{total_count} lossless tracks")
            };
            let root = app.settings.resolved_sync_root(&app.dirs);
            theme::text(
                ui,
                format!("{count_text} · {}", root.display()),
                theme::regular(13.0),
                palette.secondary,
            );
        });
    });
    ui.add_space(12.0);

    ui.horizontal_wrapped(|ui| {
        ui.spacing_mut().item_spacing = vec2(10.0, 6.0);
        let play_all_clicked = ui
            .add_enabled_ui(!filtered_indices.is_empty(), |ui| {
                theme::pill_button(ui, &palette, "Play all", true)
            })
            .inner
            .clicked();
        if play_all_clicked {
            let queue: Vec<LocalFlacTrack> = filtered_indices
                .iter()
                .map(|&i| synced_to_local_track(&app.sync_state.library_tracks[i]))
                .collect();
            if let Some(first) = queue.first() {
                app.actions.push(Action::PlaySyncedTrack {
                    path: first.path.clone(),
                    queue,
                    index: 0,
                });
            }
        }
        if theme::soft_button(ui, &palette, Some(Icon::ExternalLink), "Open folder", false)
            .clicked()
        {
            let root = app.settings.resolved_sync_root(&app.dirs);
            app.actions.push(Action::SyncOpenTrackFolder(root));
        }
        if theme::soft_button(ui, &palette, Some(Icon::Refresh), "Sync Queue", false).clicked() {
            app.actions.push(Action::Open(Page::SyncQueue));
        }
        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
            widgets::search_field(
                ui,
                &palette,
                locale,
                egui::Id::new("synced-library-search-field"),
                &mut filter,
                "Filter lossless library",
                ui.available_width().clamp(160.0, 260.0),
            );
        });
    });
    ui.data_mut(|d| d.insert_temp(filter_id, filter));
    ui.add_space(12.0);

    if total_count == 0 {
        widgets::empty_state(
            ui,
            &palette,
            Icon::Disc,
            "No lossless tracks downloaded yet",
            "Sync a playlist from its menu and configure a lossless provider in Settings to build your local FLAC library.",
        );
        ui.add_space(8.0);
        ui.horizontal(|ui| {
            if theme::pill_button(ui, &palette, "Open Sync settings", true).clicked() {
                app.actions.push(Action::Open(Page::Settings));
            }
        });
        return;
    }

    if filtered_indices.is_empty() {
        widgets::empty_state(
            ui,
            &palette,
            Icon::Search,
            "No matching lossless tracks",
            "Clear or adjust the filter to see all downloaded tracks.",
        );
        return;
    }

    // Column header bar
    let width = ui.available_width();
    let show_album = width > 620.0;
    let show_provider = width > 780.0;
    let cols = LibraryColumns::for_width(width, show_album, show_provider);
    library_header(ui, &palette, cols);

    let playing_path = app
        .local_flac
        .active
        .then(|| app.local_flac.track.as_ref().map(|t| t.path.clone()))
        .flatten();
    let is_playing_audio =
        app.local_flac.active && app.local_flac.playback == crate::player::Playback::Playing;

    let tracks_snapshot = app.sync_state.library_tracks.clone();
    let row_height = theme::ROW_HEIGHT;
    widgets::virtual_rows(ui, filtered_indices.len(), row_height, |ui, visible_idx| {
        let track_idx = filtered_indices[visible_idx];
        let track = &tracks_snapshot[track_idx];
        let is_current = playing_path.as_ref().is_some_and(|p| p == &track.path);
        let row_playing = is_current && is_playing_audio;
        library_track_row(
            app,
            ui,
            track,
            visible_idx,
            cols,
            row_height,
            is_current,
            row_playing,
            &filtered_indices,
            &tracks_snapshot,
        );
    });
}

#[derive(Clone, Copy)]
struct LibraryColumns {
    number: f32,
    album: f32,
    quality: f32,
    provider: f32,
    duration: f32,
    folder: f32,
}

impl LibraryColumns {
    fn for_width(width: f32, show_album: bool, show_provider: bool) -> Self {
        Self {
            number: 40.0,
            album: if show_album {
                (width * 0.22).clamp(130.0, 260.0)
            } else {
                0.0
            },
            quality: 164.0,
            provider: if show_provider { 84.0 } else { 0.0 },
            duration: 56.0,
            folder: 36.0,
        }
    }
}

fn library_header(ui: &mut Ui, palette: &Palette, cols: LibraryColumns) {
    let width = ui.available_width();
    let (rect, _) = ui.allocate_exact_size(vec2(width, 28.0), Sense::hover());
    let painter = ui.painter();
    let font = theme::medium(12.0);
    let color = palette.secondary;
    let cy = rect.center().y;
    let mut x = rect.left() + 8.0;

    painter.text(
        pos2(x + cols.number / 2.0, cy),
        egui::Align2::CENTER_CENTER,
        "#",
        font.clone(),
        color,
    );
    x += cols.number;

    let right_fixed = cols.quality + cols.provider + cols.duration + cols.folder + 8.0;
    let title_right = rect.right() - right_fixed - cols.album;
    painter.text(
        pos2(x, cy),
        egui::Align2::LEFT_CENTER,
        "Title",
        font.clone(),
        color,
    );
    x = title_right;

    if cols.album > 0.0 {
        painter.text(
            pos2(x, cy),
            egui::Align2::LEFT_CENTER,
            "Album",
            font.clone(),
            color,
        );
        x += cols.album;
    }
    painter.text(
        pos2(x, cy),
        egui::Align2::LEFT_CENTER,
        "Quality",
        font.clone(),
        color,
    );
    x += cols.quality;
    if cols.provider > 0.0 {
        painter.text(
            pos2(x, cy),
            egui::Align2::LEFT_CENTER,
            "Provider",
            font.clone(),
            color,
        );
        x += cols.provider;
    }
    painter.text(
        pos2(x + cols.duration - 6.0, cy),
        egui::Align2::RIGHT_CENTER,
        "Time",
        font,
        color,
    );
    painter.hline(
        rect.x_range(),
        rect.bottom() - 0.5,
        Stroke::new(1.0, palette.outline),
    );
    ui.add_space(4.0);
}

#[allow(clippy::too_many_arguments)]
fn library_track_row(
    app: &mut App,
    ui: &mut Ui,
    track: &SyncedLibraryTrack,
    visible_idx: usize,
    cols: LibraryColumns,
    row_height: f32,
    is_current: bool,
    row_playing: bool,
    filtered_indices: &[usize],
    all_tracks: &[SyncedLibraryTrack],
) {
    let palette = app.palette;
    let width = ui.available_width();
    let (rect, response) = ui.allocate_exact_size(vec2(width, row_height), Sense::click());
    response.widget_info(|| {
        egui::WidgetInfo::selected(
            egui::WidgetType::Button,
            true,
            is_current,
            format!("Play {}, {}", track.title, track.artists),
        )
    });
    if !ui.is_rect_visible(rect) && !response.has_focus() && !response.clicked() {
        return;
    }
    let hovered = ui.rect_contains_pointer(rect) || response.has_focus();
    if is_current {
        ui.painter().rect_filled(
            rect,
            CornerRadius::same(6),
            palette
                .accent
                .gamma_multiply(if hovered { 0.16 } else { 0.10 }),
        );
    } else if hovered {
        ui.painter().rect_filled(
            rect,
            CornerRadius::same(6),
            palette
                .surface_hover
                .gamma_multiply(if palette.dark { 0.7 } else { 1.0 }),
        );
    }

    let painter = ui.painter().clone();
    let mut x = rect.left() + 8.0;

    // Number / play indicator
    let num_rect = Rect::from_min_size(pos2(x, rect.top()), vec2(cols.number, row_height));
    if hovered {
        let icon = if row_playing {
            Icon::PauseFilled
        } else {
            Icon::PlayFilled
        };
        theme::paint_icon(ui, icon, num_rect, 14.0, palette.text);
    } else if row_playing {
        theme::paint_icon(ui, Icon::AudioLines, num_rect, 16.0, palette.accent);
    } else {
        painter.text(
            num_rect.center(),
            egui::Align2::CENTER_CENTER,
            (visible_idx + 1).to_string(),
            theme::regular(13.5),
            if is_current {
                palette.accent
            } else {
                palette.secondary
            },
        );
    }
    x += cols.number;

    let right_fixed = cols.quality + cols.provider + cols.duration + cols.folder + 8.0;
    let title_right = (rect.right() - right_fixed - cols.album).max(x + 60.0);
    let title_rect =
        Rect::from_min_max(pos2(x, rect.top()), pos2(title_right - 10.0, rect.bottom()));
    let title_color = if is_current {
        palette.accent
    } else {
        palette.text
    };
    let mut title_ui = ui.new_child(
        UiBuilder::new()
            .max_rect(title_rect)
            .layout(Layout::top_down(Align::LEFT)),
    );
    title_ui.set_clip_rect(title_rect.intersect(ui.clip_rect()));
    title_ui.spacing_mut().item_spacing = vec2(6.0, 2.0);
    title_ui.add_space(((row_height - 36.0) / 2.0).max(4.0));
    theme::text(
        &mut title_ui,
        &track.title,
        theme::medium(14.0),
        title_color,
    );
    theme::text(
        &mut title_ui,
        &track.artists,
        theme::regular(12.5),
        if hovered {
            palette.text
        } else {
            palette.secondary
        },
    );

    x = title_right;

    // Album
    if cols.album > 0.0 {
        let cell = Rect::from_min_max(
            pos2(x, rect.top()),
            pos2(x + cols.album - 10.0, rect.bottom()),
        );
        let clipped = painter.with_clip_rect(cell.intersect(ui.clip_rect()));
        crate::bidi::paint_line(
            &clipped,
            cell.left(),
            cell.right(),
            cell.center().y,
            &track.album,
            theme::regular(13.0),
            palette.secondary,
        );
        x += cols.album;
    }

    // Quality badge
    let quality_rect =
        Rect::from_min_size(pos2(x, rect.top()), vec2(cols.quality - 8.0, row_height));
    let mut quality_ui = ui.new_child(
        UiBuilder::new()
            .max_rect(quality_rect)
            .layout(Layout::left_to_right(Align::Center)),
    );
    quality_ui.set_clip_rect(quality_rect.intersect(ui.clip_rect()));
    let quality_text = track
        .quality
        .as_deref()
        .filter(|q| !q.is_empty())
        .map(format_quality_badge)
        .unwrap_or_else(|| "FLAC".to_string());
    small_tag(&mut quality_ui, &palette, &quality_text, palette.accent);
    x += cols.quality;

    // Provider
    if cols.provider > 0.0 {
        let provider_label = track
            .provider
            .as_deref()
            .map(provider_display_name)
            .unwrap_or_else(|| "Local".to_string());
        painter.text(
            pos2(x, rect.center().y),
            egui::Align2::LEFT_CENTER,
            provider_label,
            theme::regular(12.5),
            palette.secondary,
        );
        x += cols.provider;
    }

    // Duration
    let duration_label = if track.duration_ms > 0 {
        util::format_duration_ms(u32::try_from(track.duration_ms).unwrap_or(u32::MAX))
    } else {
        "--:--".to_string()
    };
    painter.text(
        pos2(x + cols.duration - 6.0, rect.center().y),
        egui::Align2::RIGHT_CENTER,
        duration_label,
        theme::regular(13.0),
        palette.secondary,
    );
    x += cols.duration;

    // Open folder button
    let folder_rect = Rect::from_min_size(pos2(x, rect.top()), vec2(cols.folder, row_height));
    let mut folder_ui = ui.new_child(
        UiBuilder::new()
            .max_rect(folder_rect)
            .layout(Layout::centered_and_justified(egui::Direction::LeftToRight)),
    );
    let open_folder_clicked = theme::icon_button(
        &mut folder_ui,
        Icon::ExternalLink,
        16.0,
        palette.secondary,
        palette.text,
        "Open synced folder",
    )
    .clicked();
    if open_folder_clicked {
        app.actions
            .push(Action::SyncOpenTrackFolder(track.path.clone()));
    }

    // Row click / double click to play through LocalFlacPlayer
    let build_play_action = || {
        let queue: Vec<LocalFlacTrack> = filtered_indices
            .iter()
            .map(|&i| synced_to_local_track(&all_tracks[i]))
            .collect();
        Action::PlaySyncedTrack {
            path: track.path.clone(),
            queue,
            index: visible_idx,
        }
    };
    let on_play_control = response
        .interact_pointer_pos()
        .is_some_and(|pos| num_rect.contains(pos));
    let accessible_click = response.clicked() && response.interact_pointer_pos().is_none();
    if !open_folder_clicked
        && (response.double_clicked()
            || accessible_click
            || (response.clicked() && on_play_control))
    {
        if is_current && on_play_control {
            app.actions.push(Action::LocalFlacPlayPause);
        } else {
            app.actions.push(build_play_action());
        }
    }

    egui::Popup::context_menu(&response)
        .frame(widgets::menu_frame(&palette))
        .show(|ui| {
            ui.set_min_width(200.0);
            if widgets::menu_item(ui, &palette, Some(Icon::Play), "Play lossless copy") {
                app.actions.push(build_play_action());
            }
            if widgets::menu_item(ui, &palette, Some(Icon::ExternalLink), "Open synced folder") {
                app.actions
                    .push(Action::SyncOpenTrackFolder(track.path.clone()));
            }
            if !track.playlist_id.is_empty()
                && widgets::menu_item(ui, &palette, Some(Icon::ListMusic), "Go to playlist")
            {
                app.actions
                    .push(Action::Open(Page::Playlist(track.playlist_id.clone())));
            }
        });
}
