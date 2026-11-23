---
title: Lossless Library Sync
description: Sync Spotify playlists to local lossless FLAC files using third-party providers and play them directly in Spotifast.
---

# Lossless Library Sync

Spotifast includes an optional lossless library sync layer (`crates/spotisync`) that mirrors selected Spotify playlists to local FLAC files and plays those files directly through Spotifast's own audio pipeline.

**Spotify Premium is still required for playback; this adds fidelity and offline copies, not entitlement.**

Nothing in Spotifast circumvents DRM, extracts keys from Spotify, or redistributes Spotify binaries. Sync reads public playlist metadata (ISRC, title, artists, album, duration, track and disc numbers, release year, and cover URL) through your existing Spotify Web API grant. This access is strictly read-only: Spotifast never modifies playlists on Spotify's side during sync. Matching lossless releases are then resolved and downloaded over ordinary HTTP APIs from third-party lossless providers that you configure yourself (starting with Qobuz). Local (`spotify:local:`) rows inside Spotify playlists are ignored by sync.

## Quick start

1. Open **Settings -> Sync** in Spotifast.
2. Under **Qobuz credentials**, enter your Qobuz `app_id`, your `user_auth_token`, and (if required by your Qobuz account or endpoint) your optional `app_secret`.
3. Click **Save & validate**.
   - Spotifast persists the credentials in your operating system's protected credential store and immediately validates them off the UI thread with a lightweight authenticated request (`GET /favorite/getUserFavorites?limit=1`).
   - The status indicator updates from `Validating` to `Valid` (or `Invalid` with the error reason). You can re-run **Validate** or remove credentials with **Clear** at any time.
4. Optionally pick a custom **Library root**, adjust **Quality ceiling** and **Concurrency**, and confirm **Qobuz** is enabled in the provider priority list.
5. Open any playlist in Spotifast, open its overflow menu (`...`) or context menu, and choose **Sync this playlist**. To stop polling that playlist later, choose **Stop syncing**.
6. Monitor progress in the **Sync Queue** view (or the **Sync** tab in the Queue side panel), or click **Sync now** to run an immediate poll and wake the download queue.
7. Play downloaded tracks directly from **Synced Library** in the sidebar or from a synced track's context menu (**Play lossless copy**).

### Where provider credentials live

Provider credentials are never written to `settings.json`, never logged, and redacted (`[REDACTED]`) in debug formatting. They are stored through `keyring-core` under the service name `rocks.spotifast.Spotifast`:

- **Linux:** Secret Service (such as GNOME Keyring or KWallet).
- **macOS:** Keychain.
- **Windows:** Credential Manager (preferring Enterprise persistence with Local persistence fallback), plus an encrypted copy sealed for the current Windows user with DPAPI under `<state_dir>/credential-storage/`.

Qobuz credentials use their own slot (`qobuz`), separate from Spotify's grants (`shared-web`, `personal-web`, `playback`, and `soloist-key`). Signing out of Spotify revokes only Spotify slots and preserves your Qobuz credentials. Clearing Qobuz credentials in **Settings -> Sync** revokes and deletes only the `qobuz` slot.

## Library layout, atomic writes, FLAC-only policy, and tags

### Library root resolution order

Spotifast resolves the destination root directory (`Settings::resolved_sync_root`) in this exact order:

1. `sync.library_root` from `settings.json`, when set and non-blank.
2. `<local_files_folder>/sync`, when `local_files_folder` is configured in `settings.json`.
3. `<state_dir>/sync`, using Spotifast's platform state directory:
   - Linux: `~/.local/state/spotifast/sync`
   - macOS: `~/Library/Application Support/me.paolino.spotifast/sync`
   - Windows (installed): `%LOCALAPPDATA%\paolino\spotifast\data\sync`
   - Windows (portable): `spotifast-data\state\sync` next to the executable

Inside the resolved root, tracks are placed at a deterministic, filesystem-safe path:

```text
<library_root>/<Artist>/<Album>/<NN> - <Title>.flac
```

When a track has no positive track number, the filename omits the numeric prefix (`<Title>.flac`). Path components replace reserved filesystem characters (`< > : " / \ | ? *` and control characters) with `_`, trim surrounding whitespace and dots, and cap length at 120 characters.

### Atomic writes

Every download is written safely off the UI thread:

1. Verify the downloaded bytes begin with the 4-byte `fLaC` magic header.
2. Create a unique hidden temporary file in the destination directory (`.spotisync-<stem>-flac-<pid>-<seq>.tmp`), write the payload, and call `sync_all()` (`fsync` on Unix, `FlushFileBuffers` on Windows).
3. Parse and tag the temporary file with `lofty`, verify the decoded FLAC stream properties, and call `sync_all()` again on the tagged file.
4. Atomically rename the temporary file over the final `.flac` path.

If any step fails, a drop guard deletes the temporary file immediately so no partial `.tmp` files are left behind.

### FLAC-only policy

Spotifast enforces a strict lossless-only policy and never transcodes lossy audio to FLAC:

- Search candidates not marked lossless are rejected.
- Qobuz format ID `5` (MP3 320 kbps), non-FLAC MIME types, preview samples (`sample: true`), and payloads without a valid `fLaC` stream header are rejected.
- Requested Qobuz format IDs map directly to lossless tiers:
  - `6`: FLAC 16-bit / 44.1 kHz (`flac_16_44`)
  - `7`: FLAC 24-bit / up to 96 kHz (`flac_24_96`)
  - `27`: FLAC 24-bit / up to 192 kHz (`flac_24_192`)
- When a track only exists as a lossy release on configured providers, Spotifast skips downloading it and records the track as `unmatched` with the reason in `sync.db`.

### Vorbis tags and `.lrc` sidecars

Each written FLAC file is tagged with `lofty` using standard Vorbis Comments:

| Tag | Contents |
| --- | --- |
| `TITLE` | Track title |
| `ARTIST` | Comma-separated artist names |
| `ALBUM` | Album title |
| `ALBUMARTIST` | Album artist (when present) |
| `TRACKNUMBER` | Track number (when present) |
| `DISCNUMBER` | Disc number (when present) |
| `DATE` | Four-digit release year (when present) |
| `ISRC` | International Standard Recording Code (when present) |
| `PROVIDER` | Source provider ID (for example `qobuz`) |
| `SOURCE_URL` | Provider track URL (for example `https://play.qobuz.com/track/<id>`) |

When cover image bytes are provided in the write request, they are embedded as a FLAC front-cover picture block (`PictureType::CoverFront`, supporting JPEG, PNG, GIF, BMP, and TIFF). When synced LRC lyrics are supplied for a track, Spotifast also writes a UTF-8 `.lrc` sidecar atomically alongside the `.flac` file using the same stem.

### Quality-upgrade-only rule and no automatic deletion

- **Never downgrade or overwrite equal quality:** Before downloading (and again after inspecting the decoded temporary FLAC stream), `spotisync` checks any existing file at the destination path with `lofty`. If the existing file already meets or exceeds the selected quality tier (`flac_16_44 < flac_24_96 < flac_24_192`), the download is skipped and the file is left untouched. Only a strictly higher lossless tier triggers an atomic replacement.
- **No automatic deletion:** Removing a track from a Spotify playlist or turning off sync for a playlist never deletes local `.flac` or `.lrc` files on disk.

## Strict matching rules and audit trail

When resolving a Spotify track against provider search candidates, `spotisync` evaluates three ordered tiers and stops at the first tier that produces a lossless match. Within a tier, the highest quality candidate up to `quality_ceiling` wins, breaking ties by similarity score, smallest duration difference, and highest native quality.

1. **`isrc` (Tier 1):** Exact ISRC match after stripping spaces and hyphens and uppercasing.
2. **`exact` (Tier 2):** Exact match on normalized title and normalized primary artist within **+/-2 seconds (`2000 ms`)** duration tolerance, with identical recording-variant sets. Normalization casefolds text, folds common Latin diacritics, removes featuring credits and remaster/edition qualifiers (such as `(2013 Remaster)`, `(Deluxe Edition)`, `(Album Version)`), and standardizes punctuation (`&` -> `and`, `pt` -> `part`, `vol` -> `volume`).
3. **`fuzzy` (Tier 3):** Blended similarity score (`0.45 * Jaro-Winkler + 0.55 * normalized Levenshtein`) requiring title similarity >= `0.86` and primary artist similarity >= `0.85` (allowing a leading `the ` difference on artist names) within **+/-5 seconds (`5000 ms`)** duration tolerance, with identical recording-variant sets.
4. **`unmatched`:** If no candidate passes Tiers 1 to 3, the item is marked `unmatched` with one of three recorded reasons:
   - `no candidates returned by providers`
   - `matching candidates were lossy only`
   - `no candidate satisfied strict matching rules`

### Recording-variant guard

To prevent silent substitution of the wrong recording, `spotisync` extracts recording-variant markers from both the query and every candidate (across titles, parenthetical/bracketed qualifiers, `-` suffixes, and artist names):

- Live (`live at`, `live from`, `live in`, `live on`, `live session`, `live version`, `in concert`, `bbc session`, `peel session`, or `live` qualifier)
- Remix / club mix (`remix`, `remixed`, `rmx`, `rework`, `refix`, `bootleg`, `club mix`, `dance mix`, `house mix`, `dub mix`, or `dub` / `vip` / non-original `mix` qualifiers)
- Acoustic (`acoustic`, `unplugged`, `piano version`, `orchestral version`, `string version`, or `stripped` / `orchestral` qualifiers)
- Karaoke (`karaoke`, `backing track`, `sing along`, `minus one`, or `karaoke` in artist)
- Instrumental (`instrumental` or `inst` qualifier)
- Acapella (`acapella`, `a cappella`, `vocals only`)
- Demo / alternate take (`demo version`, `early version`, `rough mix`, `alternate take`, `alt take`, `alternate version`, `outtake`, or `demo` qualifier)
- Cover / tribute (`tribute`, `originally performed by`, `in the style of`, `made famous by`, `cover version`, `cover` qualifier, or `tribute` in artist)
- Speed-altered (`sped up`, `speed up`, `nightcore`, `slowed`, `8d audio`, or `reverb` qualifier)
- Radio / single edit (`radio edit`, `radio version`, `single edit`, `single version`, `short version`)
- Extended (`extended mix`, `extended version`, `extended edit`, `long version`, `club version`, `12 inch`, or `extended` qualifier)

If the candidate's variant set does not equal the query's variant set, it is rejected in Tiers 2 and 3. A studio track is never replaced by a live, remix, acoustic, karaoke, or radio-edit recording, and a live or remix track in your playlist is matched only against the same variant.

### Audit trail in `sync.db`

Every completed row in `sync_items` records `matched_by` (`isrc`, `exact`, or `fuzzy`), `provider`, canonical `quality` (`flac_16_44`, `flac_24_96`, or `flac_24_192`), `path`, `attempts`, `error`, and `updated_at`. Hovering a track's sync status dot in a playlist view or viewing the **Sync Queue** surfaces this audit information directly in the UI.

## Queue, concurrency, rate limiting, and crash-safe state

### SQLite database (`<state_dir>/sync.db`)

All sync state is stored in `<state_dir>/sync.db` using `rusqlite` with bundled SQLite configured with `PRAGMA journal_mode = WAL`, `PRAGMA synchronous = NORMAL`, and `PRAGMA busy_timeout = 5000`. Portable Windows copies exclude `sync.db-wal` and `sync.db-shm` when initializing from an installed state directory.

The schema has three tables:

- **`sync_items`** (`PRIMARY KEY (playlist_id, track_id)`, indexed on `status`): `playlist_id`, `track_id`, `isrc`, `title`, `artists` (JSON array), `status` (`pending`, `resolving`, `downloading`, `done`, `unmatched`, `failed`), `matched_by`, `provider`, `quality`, `path`, `attempts`, `error`, `updated_at`.
- **`synced_playlists`** (`PRIMARY KEY (playlist_id)`): `playlist_id`, `name`, `enabled`, `last_polled_at`.
- **`sync_track_meta`** (`PRIMARY KEY (playlist_id, track_id)`): `playlist_id`, `track_id`, `album`, `album_artist`, `duration_ms`, `track_number`, `disc_number`, `year`, `cover_url`.

### Concurrency, rate limiting, and retries

- **Per-provider concurrency:** Controlled by a per-provider semaphore defaulting to `2` in-flight requests, configurable between `1` and `8` (`sync.concurrency`).
- **Token-bucket pacing:** Each provider has an independent token bucket (default `4.0` requests/second sustained with a burst of `4.0`) that paces both search and download calls.
- **HTTP 429 cooldowns:** When a provider responds with HTTP `429 Too Many Requests`, `spotisync` reads the `Retry-After` header in seconds (defaulting to `30` seconds if omitted), pauses all workers for that provider until the deadline, and publishes a live `ProviderCooldown` banner to the UI with a 1-second countdown timer.
- **5xx and transport backoff:** Transient `5xx` responses and HTTP transport errors retry up to `5` attempts (`max_attempts = 5`) using exponential backoff starting at `1` second, capped at `30` seconds, with +/-25% random jitter. After 5 failed attempts, the item is marked `failed` with the last error message and can be retried from the UI (**Retry sync**).
- **Crash-safe resumption:** On startup and before each queue pass, `SyncStore::recover_interrupted()` resets any rows left in `resolving` or `downloading` back to `pending`. Completed (`done`) rows are never re-enqueued or re-downloaded.
- **Background UI updates and tray badge:** All SQLite reads/writes, playlist polling, validation, and downloads run off the UI thread on the backend Tokio runtime. Snapshots are debounced (150 ms) before updating the UI, and while pending, resolving, or downloading items remain, Spotifast displays a sync badge and remaining count in the system tray menu.

## Playlist watcher

The background `PlaylistWatcher` polls every enabled playlist in `synced_playlists` using Spotifast's existing Spotify Web API gateway (50 items per page, capped at 200 pages per poll):

- **Polling interval:** Defaults to `60` seconds (`sync.poll_interval_secs`, clamped to `15..=3600`) with +/-15% uniform random jitter (51 to 69 seconds at the default setting) to spread requests naturally.
- **Idempotent diffing:** Newly seen `(playlist_id, track_id)` pairs are inserted with `INSERT OR IGNORE` as `pending` rows; tracks already tracked in any state are skipped so repeated polls create zero duplicates.
- **Instant wake-up:** Whenever a poll enqueues one or more tracks, the watcher notifies `SyncQueue` immediately via `tokio::sync::Notify`. Enabling a playlist or clicking **Sync now** triggers an immediate poll without waiting for the next timer tick.

## Direct lossless playback (`LocalFlacPlayer`)

Downloaded FLAC tracks play directly through Spotifast's dedicated `LocalFlacPlayer` (`src/local_player.rs`) on a background worker thread, independent of librespot and without requiring `spotify:local:` tag matching:

```text
FLAC file
  -> symphonia (FLAC decode & accurate seeking)
  -> eq::Processor (10-band EQ at native track sample rate, >= 32,001 Hz)
  -> vis::AudioTap (post-EQ, pre-volume, 44.1 kHz for spectrum / scope / MilkDrop)
  -> limiter::Limiter (look-ahead soft-knee limiter at native sample rate)
  -> resample::Resampler (only when track sample rate != output device sample rate)
  -> fastframe-audio (cubic volume curve with 30 ms gain ramp, 50 ms transport fade)
```

Key properties of local lossless playback:

- **No unnecessary resampling:** When the output device runs at the FLAC file's native sample rate (for example 44.1 kHz, 96 kHz, or 192 kHz), audio passes straight through without sample-rate conversion.
- **Consistent EQ and visualisers:** Spotifast's 10-band equalizer shapes the signal at the track's native sample rate, and the visualiser tap sits post-EQ and pre-volume (resampled to 44.1 kHz for the visualiser tap only when needed), so the spectrum analyser, oscilloscope, and MilkDrop follow the EQ and continue animating even at zero volume.
- **Existing players untouched:** Starting local FLAC playback cleanly pauses librespot if it is active. Stopping local playback releases the OS audio stream completely so switching back to librespot, Spotify Soloist, or Spotify Connect works without contention.

## Settings reference

Sync preferences live under the `"sync"` object in Spotifast's `settings.json` (located in your platform config directory, for example `~/.config/spotifast/settings.json` on Linux). Older `settings.json` files without `"sync"` load cleanly with defaults.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `sync.library_root` | `string \| null` | `null` | Custom directory for downloaded FLAC files. When `null` or blank, falls back to `<local_files_folder>/sync`, then `<state_dir>/sync`. |
| `sync.providers` | `array<{ id: string, enabled: boolean }>` | `[{"id": "qobuz", "enabled": true}]` | Ordered provider priority list. Providers are queried in list order when enabled and configured with valid credentials. |
| `sync.quality_ceiling` | `"flac16_44" \| "flac24_96" \| "flac24_192"` | `"flac24_192"` | Maximum lossless FLAC tier requested from providers (`16-bit / 44.1 kHz`, `24-bit / 96 kHz`, or `24-bit / 192 kHz`). Note: `sync.db` stores canonical labels as `flac_16_44`, `flac_24_96`, and `flac_24_192`. |
| `sync.concurrency` | `integer` (`1..=8`) | `2` | Maximum concurrent in-flight search or download requests per provider. Values outside `1..=8` are clamped at runtime. |
| `sync.poll_interval_secs` | `integer` (`15..=3600`) | `60` | Base playlist polling interval in seconds before +/-15% random jitter is applied. Values outside `15..=3600` are clamped at runtime. |
| `sync.synced_playlists` | `array<{ id: string, name: string, enabled: boolean }>` | `[]` | Persisted per-playlist sync toggles mirrored into `sync.db` on startup. |

See [sync-settings.sample.json](examples/sync-settings.sample.json) for a complete, valid JSON example of the `"sync"` block.

> **Security note:** Provider credentials (`app_id`, `user_auth_token`, `app_secret`) are never stored in `settings.json`. Configure them only through **Settings -> Sync** so they remain protected by your operating system's credential store.

## Troubleshooting

### Invalid or rejected Qobuz credentials

- **Symptom:** **Settings -> Sync** shows `Invalid` (HTTP 401 or 403), or downloads fail with `Qobuz returned a preview sample instead of a full stream`.
- **What to check:**
  - Verify your `app_id` and `user_auth_token` are current and belong to an active Qobuz subscription with lossless streaming access (accounts without streaming entitlement return preview samples).
  - If `track/getFileUrl` requires signed requests, provide your `app_secret` as well and click **Save & validate**.
  - On Linux, make sure a Secret Service daemon (GNOME Keyring or KWallet) is running and unlocked so Spotifast can store and read the `qobuz` credential slot.

### HTTP 429 rate-limit cooldowns

- **Symptom:** The **Sync Queue** shows a warning banner such as `qobuz: rate limited, retrying in <N>s`.
- **What to do:**
  - Wait for the automatic countdown to finish. Spotifast respects the server's `Retry-After` header and resumes automatically once the cooldown expires.
  - If you sync large playlists frequently and hit 429 responses often, lower **Concurrency** in **Settings -> Sync** to `1` or `2`.

### Unmatched tracks

- **Symptom:** A track displays an amber `unmatched` dot in the playlist view or **Sync Queue**.
- **Why it happens:**
  - Hover the status dot (or inspect the **Sync Queue** row) to read the recorded reason.
  - `no candidates returned by providers`: the track is not in the provider's catalog or regional storefront.
  - `no candidate satisfied strict matching rules`: results existed, but none passed ISRC, normalized exact (+/-2 s), or strict fuzzy (+/-5 s) matching with an identical recording variant (for example, the provider only had a live, remix, radio edit, or karaoke version while your playlist has the studio track).
  - After catalog updates or fixing provider settings, right-click the track (or open its `...` menu) and select **Retry sync**.

### Lossy-only releases

- **Symptom:** A track is marked `unmatched` with `matching candidates were lossy only` or `lossy stream rejected`.
- **Why it happens:** Spotifast refuses to download MP3/lossy streams or transcode them to FLAC. Confirm your provider account tier supports lossless FLAC streaming, or wait until a lossless master is available for that release before using **Retry sync**.

### Interrupted syncs or missing files after moving folders

- **Interrupted syncs:** Closing or crashing Spotifast mid-download is safe. On the next start, `sync.db` automatically resets any `resolving` or `downloading` rows back to `pending` and resumes without touching completed files.
- **Changing library root:** Updating `sync.library_root` directs future downloads to the new folder without deleting files from the previous folder. If you move existing FLAC files yourself, keep the `<Artist>/<Album>/<NN> - <Title>.flac` layout so the upgrade guard recognizes existing copies.
