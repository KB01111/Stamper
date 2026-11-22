//! Resumable SQLite store for playlist sync state.
//!
//! Uses `rusqlite` with bundled SQLite and WAL mode so concurrent reads and
//! crash recovery work identically on Linux, macOS, and Windows.

use crate::matching::MatchedBy;
use crate::{Quality, Query, Result, SyncError};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

const SCHEMA_SQL: &str = "
CREATE TABLE IF NOT EXISTS sync_items (
  playlist_id TEXT,
  track_id TEXT,
  isrc TEXT,
  title TEXT,
  artists TEXT,
  status TEXT CHECK(status IN ('pending','resolving','downloading','done','unmatched','failed')),
  matched_by TEXT,
  provider TEXT,
  quality TEXT,
  path TEXT,
  attempts INTEGER DEFAULT 0,
  error TEXT,
  updated_at INTEGER,
  PRIMARY KEY (playlist_id, track_id)
);

CREATE TABLE IF NOT EXISTS synced_playlists (
  playlist_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  last_polled_at INTEGER
);

CREATE TABLE IF NOT EXISTS sync_track_meta (
  playlist_id TEXT NOT NULL,
  track_id TEXT NOT NULL,
  album TEXT NOT NULL DEFAULT '',
  album_artist TEXT,
  duration_ms INTEGER NOT NULL DEFAULT 0,
  track_number INTEGER,
  disc_number INTEGER,
  year INTEGER,
  cover_url TEXT,
  PRIMARY KEY (playlist_id, track_id)
);

CREATE INDEX IF NOT EXISTS idx_sync_items_status ON sync_items (status);
";

/// Lifecycle state of a tracked playlist item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SyncStatus {
    Pending,
    Resolving,
    Downloading,
    Done,
    Unmatched,
    Failed,
}

impl SyncStatus {
    /// Canonical database value matching the `sync_items.status` CHECK constraint.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Resolving => "resolving",
            Self::Downloading => "downloading",
            Self::Done => "done",
            Self::Unmatched => "unmatched",
            Self::Failed => "failed",
        }
    }

    /// Parses a status value from SQLite.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "resolving" => Some(Self::Resolving),
            "downloading" => Some(Self::Downloading),
            "done" => Some(Self::Done),
            "unmatched" => Some(Self::Unmatched),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// One row from the `sync_items` table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncItem {
    pub playlist_id: String,
    pub track_id: String,
    pub isrc: Option<String>,
    pub title: String,
    pub artists: Vec<String>,
    pub status: SyncStatus,
    pub matched_by: Option<String>,
    pub provider: Option<String>,
    pub quality: Option<String>,
    pub path: Option<String>,
    pub attempts: u32,
    pub error: Option<String>,
    pub updated_at: i64,
}

/// Companion metadata stored for restartable query reconstruction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncTrackMeta {
    pub playlist_id: String,
    pub track_id: String,
    pub album: String,
    pub album_artist: Option<String>,
    pub duration_ms: u64,
    pub track_number: Option<u32>,
    pub disc_number: Option<u32>,
    pub year: Option<u32>,
    pub cover_url: Option<String>,
}

/// Synced playlist toggle record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncedPlaylist {
    pub playlist_id: String,
    pub name: String,
    pub enabled: bool,
    pub last_polled_at: Option<i64>,
}

/// Aggregated item counts per playlist (or overall total when `playlist_id` is empty).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaylistCounts {
    pub playlist_id: String,
    pub pending: u64,
    pub resolving: u64,
    pub downloading: u64,
    pub done: u64,
    pub unmatched: u64,
    pub failed: u64,
}

/// Thread-safe handle to the `spotisync` SQLite database.
#[derive(Clone, Debug)]
pub struct SyncStore {
    conn: Arc<Mutex<Connection>>,
}

impl SyncStore {
    /// Opens (or creates) a SQLite sync store at `path` and initializes the schema.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path).map_err(store_err)?;
        Self::init_connection(conn)
    }

    /// Opens an ephemeral in-memory SQLite store for tests.
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(store_err)?;
        Self::init_connection(conn)
    }

    fn init_connection(conn: Connection) -> Result<Self> {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;",
        )
        .map_err(store_err)?;
        conn.execute_batch(SCHEMA_SQL).map_err(store_err)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| SyncError::Store("sqlite mutex poisoned".to_string()))
    }

    /// Resets rows left in `resolving` or `downloading` after a crash back to `pending`.
    pub fn recover_interrupted(&self) -> Result<usize> {
        let conn = self.lock()?;
        let now = now_unix_secs();
        let changed = conn
            .execute(
                "UPDATE sync_items
                 SET status = 'pending', updated_at = ?1
                 WHERE status IN ('resolving', 'downloading')",
                params![now],
            )
            .map_err(store_err)?;
        Ok(changed)
    }

    /// Enqueues a track for syncing using `INSERT OR IGNORE`.
    ///
    /// Returns `true` if a new `pending` row was inserted, or `false` if the
    /// `(playlist_id, track_id)` pair already existed in any state.
    pub fn enqueue_track(&self, playlist_id: &str, track_id: &str, query: &Query) -> Result<bool> {
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(store_err)?;
        let now = now_unix_secs();
        let artists_json = serde_json::to_string(&query.artists)
            .map_err(|e| SyncError::Store(format!("serialize artists: {e}")))?;

        let inserted = tx
            .execute(
                "INSERT OR IGNORE INTO sync_items
                 (playlist_id, track_id, isrc, title, artists, status, attempts, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6)",
                params![
                    playlist_id,
                    track_id,
                    query.isrc.as_deref(),
                    &query.title,
                    artists_json,
                    now
                ],
            )
            .map_err(store_err)?;

        tx.execute(
            "INSERT OR IGNORE INTO sync_track_meta
             (playlist_id, track_id, album, album_artist, duration_ms, track_number, disc_number, year, cover_url)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                playlist_id,
                track_id,
                &query.album,
                query.album_artist.as_deref(),
                query.duration_ms as i64,
                query.track_number.map(i64::from),
                query.disc_number.map(i64::from),
                query.year.map(i64::from),
                query.cover_url.as_deref(),
            ],
        )
        .map_err(store_err)?;

        tx.commit().map_err(store_err)?;
        Ok(inserted > 0)
    }

    /// Returns all items currently in `pending` state ordered by `updated_at`.
    pub fn list_pending(&self) -> Result<Vec<SyncItem>> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT playlist_id, track_id, isrc, title, artists, status,
                        matched_by, provider, quality, path, attempts, error, updated_at
                 FROM sync_items
                 WHERE status = 'pending'
                 ORDER BY updated_at ASC, playlist_id ASC, track_id ASC",
            )
            .map_err(store_err)?;
        let rows = stmt
            .query_map([], row_to_sync_item)
            .map_err(store_err)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(store_err)?;
        Ok(rows)
    }

    /// Reconstructs a full [`Query`] for a tracked `(playlist_id, track_id)` pair.
    pub fn get_query(&self, playlist_id: &str, track_id: &str) -> Result<Option<Query>> {
        let conn = self.lock()?;
        let row = conn
            .query_row(
                "SELECT i.isrc, i.title, i.artists,
                        COALESCE(m.album, ''), m.album_artist, COALESCE(m.duration_ms, 0),
                        m.track_number, m.disc_number, m.year, m.cover_url
                 FROM sync_items i
                 LEFT JOIN sync_track_meta m
                   ON i.playlist_id = m.playlist_id AND i.track_id = m.track_id
                 WHERE i.playlist_id = ?1 AND i.track_id = ?2",
                params![playlist_id, track_id],
                |row| {
                    let artists_raw: String = row.get(2)?;
                    Ok(Query {
                        isrc: row.get(0)?,
                        title: row.get(1)?,
                        artists: decode_artists(&artists_raw),
                        album: row.get(3)?,
                        album_artist: row.get(4)?,
                        duration_ms: row.get::<_, i64>(5)?.max(0) as u64,
                        track_number: row.get::<_, Option<i64>>(6)?.map(|v| v as u32),
                        disc_number: row.get::<_, Option<i64>>(7)?.map(|v| v as u32),
                        year: row.get::<_, Option<i64>>(8)?.map(|v| v as u32),
                        cover_url: row.get(9)?,
                    })
                },
            )
            .optional()
            .map_err(store_err)?;
        Ok(row)
    }

    /// Returns a single tracked item by primary key.
    pub fn get_item(&self, playlist_id: &str, track_id: &str) -> Result<Option<SyncItem>> {
        let conn = self.lock()?;
        let row = conn
            .query_row(
                "SELECT playlist_id, track_id, isrc, title, artists, status,
                        matched_by, provider, quality, path, attempts, error, updated_at
                 FROM sync_items
                 WHERE playlist_id = ?1 AND track_id = ?2",
                params![playlist_id, track_id],
                row_to_sync_item,
            )
            .optional()
            .map_err(store_err)?;
        Ok(row)
    }

    /// Updates an item's lifecycle status and optional error string.
    pub fn update_status(
        &self,
        playlist_id: &str,
        track_id: &str,
        status: SyncStatus,
        error: Option<&str>,
    ) -> Result<()> {
        let conn = self.lock()?;
        let now = now_unix_secs();
        conn.execute(
            "UPDATE sync_items
             SET status = ?1, error = ?2, updated_at = ?3
             WHERE playlist_id = ?4 AND track_id = ?5",
            params![status.as_str(), error, now, playlist_id, track_id],
        )
        .map_err(store_err)?;
        Ok(())
    }

    /// Updates an item's attempt counter and error while keeping or setting status.
    pub fn record_attempt(
        &self,
        playlist_id: &str,
        track_id: &str,
        status: SyncStatus,
        attempts: u32,
        error: Option<&str>,
    ) -> Result<()> {
        let conn = self.lock()?;
        let now = now_unix_secs();
        conn.execute(
            "UPDATE sync_items
             SET status = ?1, attempts = ?2, error = ?3, updated_at = ?4
             WHERE playlist_id = ?5 AND track_id = ?6",
            params![
                status.as_str(),
                i64::from(attempts),
                error,
                now,
                playlist_id,
                track_id
            ],
        )
        .map_err(store_err)?;
        Ok(())
    }

    /// Marks an item as `done` with its resolved provider, match tier, quality, and file path.
    pub fn mark_done(
        &self,
        playlist_id: &str,
        track_id: &str,
        matched_by: MatchedBy,
        provider: &str,
        quality: Quality,
        path: &str,
    ) -> Result<()> {
        let conn = self.lock()?;
        let now = now_unix_secs();
        conn.execute(
            "UPDATE sync_items
             SET status = 'done',
                 matched_by = ?1,
                 provider = ?2,
                 quality = ?3,
                 path = ?4,
                 error = NULL,
                 updated_at = ?5
             WHERE playlist_id = ?6 AND track_id = ?7",
            params![
                matched_by.as_str(),
                provider,
                quality.as_str(),
                path,
                now,
                playlist_id,
                track_id
            ],
        )
        .map_err(store_err)?;
        Ok(())
    }

    /// Marks an item as `failed` with its final attempt count and error description.
    pub fn mark_failed(
        &self,
        playlist_id: &str,
        track_id: &str,
        attempts: u32,
        error: &str,
    ) -> Result<()> {
        self.record_attempt(
            playlist_id,
            track_id,
            SyncStatus::Failed,
            attempts,
            Some(error),
        )
    }

    /// Marks an item as `unmatched` with the reason it was rejected.
    pub fn mark_unmatched(&self, playlist_id: &str, track_id: &str, reason: &str) -> Result<()> {
        let conn = self.lock()?;
        let now = now_unix_secs();
        conn.execute(
            "UPDATE sync_items
             SET status = 'unmatched', error = ?1, updated_at = ?2
             WHERE playlist_id = ?3 AND track_id = ?4",
            params![reason, now, playlist_id, track_id],
        )
        .map_err(store_err)?;
        Ok(())
    }

    /// Resets a failed, unmatched, or done item back to `pending` with zero attempts.
    pub fn retry_item(&self, playlist_id: &str, track_id: &str) -> Result<bool> {
        let conn = self.lock()?;
        let now = now_unix_secs();
        let changed = conn
            .execute(
                "UPDATE sync_items
                 SET status = 'pending', attempts = 0, error = NULL, updated_at = ?1
                 WHERE playlist_id = ?2 AND track_id = ?3",
                params![now, playlist_id, track_id],
            )
            .map_err(store_err)?;
        Ok(changed > 0)
    }

    /// Returns per-playlist aggregated status counts.
    pub fn playlist_counts(&self) -> Result<Vec<PlaylistCounts>> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT playlist_id,
                        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN status = 'resolving' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN status = 'downloading' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN status = 'unmatched' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)
                 FROM sync_items
                 GROUP BY playlist_id
                 ORDER BY playlist_id ASC",
            )
            .map_err(store_err)?;

        let rows = stmt
            .query_map([], |row| {
                Ok(PlaylistCounts {
                    playlist_id: row.get(0)?,
                    pending: row.get::<_, i64>(1)?.max(0) as u64,
                    resolving: row.get::<_, i64>(2)?.max(0) as u64,
                    downloading: row.get::<_, i64>(3)?.max(0) as u64,
                    done: row.get::<_, i64>(4)?.max(0) as u64,
                    unmatched: row.get::<_, i64>(5)?.max(0) as u64,
                    failed: row.get::<_, i64>(6)?.max(0) as u64,
                })
            })
            .map_err(store_err)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(store_err)?;
        Ok(rows)
    }

    /// Returns overall aggregated counts across all tracked items.
    pub fn total_counts(&self) -> Result<PlaylistCounts> {
        let conn = self.lock()?;
        let counts = conn
            .query_row(
                "SELECT COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN status = 'resolving' THEN 1 ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN status = 'downloading' THEN 1 ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN status = 'unmatched' THEN 1 ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0)
                 FROM sync_items",
                [],
                |row| {
                    Ok(PlaylistCounts {
                        playlist_id: String::new(),
                        pending: row.get::<_, i64>(0)?.max(0) as u64,
                        resolving: row.get::<_, i64>(1)?.max(0) as u64,
                        downloading: row.get::<_, i64>(2)?.max(0) as u64,
                        done: row.get::<_, i64>(3)?.max(0) as u64,
                        unmatched: row.get::<_, i64>(4)?.max(0) as u64,
                        failed: row.get::<_, i64>(5)?.max(0) as u64,
                    })
                },
            )
            .map_err(store_err)?;
        Ok(counts)
    }

    /// Returns all tracked items for one playlist.
    pub fn playlist_track_statuses(&self, playlist_id: &str) -> Result<Vec<SyncItem>> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT playlist_id, track_id, isrc, title, artists, status,
                        matched_by, provider, quality, path, attempts, error, updated_at
                 FROM sync_items
                 WHERE playlist_id = ?1
                 ORDER BY updated_at ASC, track_id ASC",
            )
            .map_err(store_err)?;
        let rows = stmt
            .query_map(params![playlist_id], row_to_sync_item)
            .map_err(store_err)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(store_err)?;
        Ok(rows)
    }

    /// Returns all completed (`done`) tracks ordered by most recently updated.
    pub fn list_completed_tracks(&self) -> Result<Vec<SyncItem>> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT playlist_id, track_id, isrc, title, artists, status,
                        matched_by, provider, quality, path, attempts, error, updated_at
                 FROM sync_items
                 WHERE status = 'done'
                 ORDER BY updated_at DESC, title ASC",
            )
            .map_err(store_err)?;
        let rows = stmt
            .query_map([], row_to_sync_item)
            .map_err(store_err)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(store_err)?;
        Ok(rows)
    }

    /// Returns the set of track IDs already present in `sync_items` for a playlist.
    pub fn existing_track_ids(&self, playlist_id: &str) -> Result<HashSet<String>> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare("SELECT track_id FROM sync_items WHERE playlist_id = ?1")
            .map_err(store_err)?;
        let rows = stmt
            .query_map(params![playlist_id], |row| row.get::<_, String>(0))
            .map_err(store_err)?
            .collect::<std::result::Result<HashSet<_>, _>>()
            .map_err(store_err)?;
        Ok(rows)
    }

    /// Enables or disables syncing for a playlist, upserting its display name.
    pub fn set_playlist_enabled(&self, playlist_id: &str, name: &str, enabled: bool) -> Result<()> {
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO synced_playlists (playlist_id, name, enabled)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(playlist_id) DO UPDATE SET
               name = excluded.name,
               enabled = excluded.enabled",
            params![playlist_id, name, i64::from(enabled)],
        )
        .map_err(store_err)?;
        Ok(())
    }

    /// Records the last poll timestamp for a synced playlist.
    pub fn mark_playlist_polled(&self, playlist_id: &str, polled_at: i64) -> Result<()> {
        let conn = self.lock()?;
        conn.execute(
            "UPDATE synced_playlists SET last_polled_at = ?1 WHERE playlist_id = ?2",
            params![polled_at, playlist_id],
        )
        .map_err(store_err)?;
        Ok(())
    }

    /// Lists all tracked playlists (both enabled and disabled).
    pub fn list_synced_playlists(&self) -> Result<Vec<SyncedPlaylist>> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT playlist_id, name, enabled, last_polled_at
                 FROM synced_playlists
                 ORDER BY name ASC, playlist_id ASC",
            )
            .map_err(store_err)?;
        let rows = stmt
            .query_map([], |row| {
                Ok(SyncedPlaylist {
                    playlist_id: row.get(0)?,
                    name: row.get(1)?,
                    enabled: row.get::<_, i64>(2)? != 0,
                    last_polled_at: row.get(3)?,
                })
            })
            .map_err(store_err)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(store_err)?;
        Ok(rows)
    }

    /// Lists only playlists currently enabled for syncing.
    pub fn list_enabled_playlists(&self) -> Result<Vec<SyncedPlaylist>> {
        Ok(self
            .list_synced_playlists()?
            .into_iter()
            .filter(|p| p.enabled)
            .collect())
    }
}

fn row_to_sync_item(row: &rusqlite::Row<'_>) -> rusqlite::Result<SyncItem> {
    let artists_raw: String = row.get(4)?;
    let status_raw: String = row.get(5)?;
    let status = SyncStatus::parse(&status_raw).unwrap_or(SyncStatus::Failed);
    Ok(SyncItem {
        playlist_id: row.get(0)?,
        track_id: row.get(1)?,
        isrc: row.get(2)?,
        title: row.get(3)?,
        artists: decode_artists(&artists_raw),
        status,
        matched_by: row.get(6)?,
        provider: row.get(7)?,
        quality: row.get(8)?,
        path: row.get(9)?,
        attempts: row.get::<_, i64>(10)?.max(0) as u32,
        error: row.get(11)?,
        updated_at: row.get(12)?,
    })
}

fn decode_artists(raw: &str) -> Vec<String> {
    if let Ok(parsed) = serde_json::from_str::<Vec<String>>(raw) {
        return parsed;
    }
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn store_err(e: rusqlite::Error) -> SyncError {
    SyncError::Store(e.to_string())
}

fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_query() -> Query {
        Query {
            isrc: Some("USQX91300105".to_string()),
            title: "Instant Crush".to_string(),
            artists: vec!["Daft Punk".to_string(), "Julian Casablancas".to_string()],
            album: "Random Access Memories".to_string(),
            duration_ms: 337_560,
            album_artist: Some("Daft Punk".to_string()),
            track_number: Some(5),
            disc_number: Some(1),
            year: Some(2013),
            cover_url: Some("https://example.invalid/cover.jpg".to_string()),
        }
    }

    #[test]
    fn enqueue_ignore_recover_and_lifecycle_transitions() {
        let store = SyncStore::open_in_memory().expect("open memory store");
        let q = sample_query();

        assert!(store.enqueue_track("pl1", "tr1", &q).expect("insert"));
        assert!(
            !store.enqueue_track("pl1", "tr1", &q).expect("ignore dup"),
            "second enqueue must be ignored"
        );

        let restored = store
            .get_query("pl1", "tr1")
            .expect("query")
            .expect("exists");
        assert_eq!(restored, q);

        store
            .update_status("pl1", "tr1", SyncStatus::Downloading, None)
            .expect("mark downloading");
        assert!(store.list_pending().expect("pending").is_empty());

        let recovered = store.recover_interrupted().expect("recover");
        assert_eq!(recovered, 1);
        assert_eq!(store.list_pending().expect("pending").len(), 1);

        store
            .mark_done(
                "pl1",
                "tr1",
                MatchedBy::Isrc,
                "qobuz",
                Quality::Flac24_96,
                "/music/instant_crush.flac",
            )
            .expect("mark done");

        let completed = store.list_completed_tracks().expect("completed");
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].status, SyncStatus::Done);
        assert_eq!(completed[0].matched_by.as_deref(), Some("isrc"));
        assert_eq!(completed[0].quality.as_deref(), Some("flac_24_96"));

        let counts = store.playlist_counts().expect("counts");
        assert_eq!(counts.len(), 1);
        assert_eq!(counts[0].done, 1);

        assert!(store.retry_item("pl1", "tr1").expect("retry"));
        assert_eq!(
            store.get_item("pl1", "tr1").unwrap().unwrap().status,
            SyncStatus::Pending
        );
    }

    #[test]
    fn synced_playlist_toggles_and_filtering() {
        let store = SyncStore::open_in_memory().expect("open");
        store
            .set_playlist_enabled("pl1", "Morning", true)
            .expect("enable");
        store
            .set_playlist_enabled("pl2", "Archive", false)
            .expect("disable");

        assert_eq!(store.list_synced_playlists().unwrap().len(), 2);
        let enabled = store.list_enabled_playlists().unwrap();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].playlist_id, "pl1");
    }
}
