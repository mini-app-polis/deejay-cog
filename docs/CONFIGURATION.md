# Configuration reference

Every environment variable **deejay-cog** reads, where it comes from in
production, and what reads it.

**In production** the Lambda gets its configuration from two places, both
declared in `mini-app-polis/infra` (`cogs.tf` and `modules/cog-worker`):

- **Secrets** are loaded from SSM Parameter Store at cold start, synced from
  Doppler (`mini_app_polis.load_secrets`, called when the package is first
  imported). Required ones fail the cold start when missing; optional ones
  are left unset.
- **Plain settings** are set on the function's environment.

**Locally**, run under `doppler run` or put values in `.env`, which
`config.py` loads. See `.env.example`.

---

## Environment and API

| Variable | Production source | Required | Description |
|----------|-------------------|----------|-------------|
| `ENVIRONMENT` | Function env (`production`) | No | Which environment this process is in. Unset resolves to `local`. Selects which API variables are read, and prefixes non-production report titles. |
| `KAIANO_API_BASE_URL` | Function env | Yes in production | api-kaianolevine-com base URL, read **in production only**. |
| `KAIANO_API_BASE_URL_DEV` | — | No | The base URL read **everywhere else**. There is no fallback between the two, so a dev process never reaches production by forgetting a variable. |
| `DEEJAY_COG_API_KEY` | SSM (required) | Yes | This cog's own API key. The API attributes every call to `deejay-cog` by it. The shared client falls back to `KAIANO_API_KEY` when it is unset. |

Every API call resolves the base URL the same way
(`mini_app_polis.environment.api_base_url()`): ingest, live plays, the
Spotify snapshot and the run report. With no URL resolved:

- **process-new-files** counts each set as a failed ingest. The CSV is not
  archived, and the sheet uploaded for it is removed.
- **ingest-live-history** skips.
- **Run reports** are logged, not sent.

---

## Google

| Variable | Production source | Required | Description |
|----------|-------------------|----------|-------------|
| `GOOGLE_CREDENTIALS_JSON` | SSM (required) | Yes | Service-account JSON for Drive and Sheets (`GoogleAPI.from_env()`). |
| `CSV_SOURCE_FOLDER_ID` | Code default | No | Drop zone swept by process-new-files. |
| `DJ_SETS_FOLDER_ID` | Code default | No | Parent of the year folders (each with an `Archive` subfolder) and the `Summary` folder. |
| `VDJ_HISTORY_FOLDER_ID` | Function env | No | VirtualDJ history folder read by ingest-live-history. Passed to the shared Drive facade's `get_all_m3u_files`. |
| `TIMEZONE` | Code default (`America/Chicago`) | No | Timezone live plays are stamped in, and the "today" the repair pass counts back from. |
| `REPAIR_LOOKBACK_DAYS` | Code default (`183`) | No | How far back, by set date, the repair pass checks sets for a missing API set or Spotify playlist. |
| `REPAIR_MAX_PER_RUN` | Code default (`3`) | No | Repairs one sweep may make; the rest wait for later sweeps. |

The folder IDs default to the production folders in `config.py`; there is
no separate development Drive.

---

## Spotify

| Variable | Production source | Required | Description |
|----------|-------------------|----------|-------------|
| `SPOTIPY_CLIENT_ID` | SSM (optional) | For Spotify | Spotify app client ID. |
| `SPOTIPY_CLIENT_SECRET` | SSM (optional) | For Spotify | Spotify app client secret. |
| `SPOTIPY_REFRESH_TOKEN` | SSM (optional) | For Spotify | OAuth refresh token. Generated once with `scripts/get_spotify_refresh_token.py`; see [SPOTIFY_SETUP.md](SPOTIFY_SETUP.md). |
| `SPOTIPY_REDIRECT_URI` | Function env | No | Read by common-python-utils' Spotify client. Defaults to `http://127.0.0.1:8888/callback`. Must match the Spotify dashboard. |
| `SPOTIFY_RADIO_PLAYLIST_ID` | SSM (optional) | No | The standing radio playlist. When unset, every synced set counts a Spotify failure. |

All three credentials must be set, or every Spotify step is skipped (logged,
not counted as a failure).

---

## Observability

| Variable | Production source | Required | Description |
|----------|-------------------|----------|-------------|
| `SENTRY_DSN` | SSM (optional) | No | Sentry DSN, initialised once per cold start in `worker.py`. |
| `LOGGING_LEVEL` | — | No | Log level (`DEBUG`, `INFO`, …), read by the shared logger. Defaults to `INFO`. |

---

## Local-only flows

| Variable | Default | Used by |
|----------|---------|---------|
| `OUTPUT_NAME` | `DJ Set Collection` | update-dj-set-collection: the collection spreadsheet. |
| `TEMP_TAB_NAME` | `TempClear` | update-dj-set-collection: the scratch tab. |
| `SUMMARY_TAB_NAME` | `Summary_Tab` | update-dj-set-collection |
| `SUMMARY_FOLDER_NAME` | `Summary` | generate-summaries: folder under `DJ_SETS_FOLDER_ID`. |
| `DEEJAY_SET_COLLECTION_JSON_PATH` | `v1/deejay-sets/deejay_set_collection.json` | update-dj-set-collection: where the JSON snapshot is written. |
| `MUSIC_UPLOAD_SOURCE_FOLDER_ID` | Code default | retag-music: folder of files to identify. |
| `MUSIC_TAGGING_OUTPUT_FOLDER_ID` | Code default | retag-music: where identified files go. |
| `ACOUSTID_API_KEY` | — | retag-music: required. |
| `RETAG_MIN_CONFIDENCE` | `0.90` | retag-music: minimum match confidence. |
| `RETAG_MAX_CANDIDATES` | `5` | retag-music: candidates inspected per file. |
| `MAX_UPLOADS_PER_RUN` | `200` | retag-music: per-run upload ceiling. |

The columns and column order of summary sheets (`ALLOWED_HEADERS`,
`desiredOrder`) are constants in `config.py`, not environment variables.

### retag-music system dependencies

retag-music needs two binaries that are not Python packages: `ffmpeg` (audio
decoding) and `fpcalc` (fingerprinting, from chromaprint).

```bash
sudo apt-get install -y ffmpeg libchromaprint-tools   # Ubuntu/Debian
brew install ffmpeg chromaprint                       # macOS
```

No other flow needs them, and the Lambda runtime does not have them.
