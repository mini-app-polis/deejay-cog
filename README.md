# deejay-cog

Processes DJ set CSV files from Google Drive into Google Sheets (organized by year), builds a master collection spreadsheet and JSON snapshot, generates per-year summary sheets, and pushes the collection JSON to **kaiano-api** for the site.

---

## What this processor does

This repository is the backend cog for a Drive-based DJ set pipeline. It reads CSV files (and optionally other files) from a configured Google Drive source folder, normalizes and uploads them as Google Sheets into year-based folders, maintains a master "DJ Set Collection" spreadsheet and a JSON snapshot of that collection, generates per-year summary sheets with deduplication, and can push the JSON snapshot to the api-kaianolevine-com repo. It is triggered by watcher-cog via Prefect, or manually via `workflow_dispatch`.

---

## Inputs

- **Source location**: A single Google Drive folder (ID set by `CSV_SOURCE_FOLDER_ID`). This is the “drop zone” for files to process.
- **File format**: CSVs exported from DJ software (e.g. Rekordbox), with filenames starting with a four-digit year (e.g. `2024-01-15_My_Set.csv`). Non-CSV files that start with a year are also supported (moved into the year folder without conversion).
- **Origin**: Files are placed in this folder by Google Apps Script. **watcher-cog** detects new files and fires the Prefect flow run.

---

## Outputs

- **Google Sheets in year folders**: Each processed CSV is uploaded as a Google Sheet into a year subfolder (e.g. `2024`) under `DJ_SETS_FOLDER_ID`. The original file is moved into an `Archive` subfolder under that year.
- **Master collection**: A single Google Sheet (name from `OUTPUT_NAME`) in the DJ Sets folder, with one tab per year and a Summary tab, listing all sets with Date, Name, and Link. Built/updated by `update_deejay_set_collection.py`.
- **Summary sheet**: Per-year “{Year} Summary” sheets in the Summary subfolder, aggregating and deduplicating track data from that year’s set sheets. Built by `generate_summaries.py` (which uses `deduplicate_summary.py`).
- **JSON snapshot**: A JSON file (path from `DEEJAY_SET_COLLECTION_JSON_PATH`, default `v1/deejay-sets/deejay_set_collection.json`) describing the collection. The **update_dj_set_collection** workflow copies this file into the **api-kaianolevine-com** repo and commits/pushes.

---

## Scripts

| Script | Purpose |
|--------|--------|
| **process_new_files.py** | Lists the CSV source folder, normalizes status prefixes on filenames, and processes each file: extracts year from filename, uploads CSVs as Sheets into the correct year folder (with formatting), moves originals to Archive, and handles duplicates/failures (e.g. `FAILED_`, `possible_duplicate_`). |
| **update_deejay_set_collection.py** | Scans the DJ Sets folder (year subfolders + Summary), builds/updates the master collection spreadsheet (tabs per year + Summary), and writes the JSON snapshot to the path given by `DEEJAY_SET_COLLECTION_JSON_PATH`. |
| **generate_summaries.py** | Prefect flow (`generate-summaries`) with evaluation hooks: for each year folder, finds or creates the “{Year} Summary” sheet in the Summary folder, aggregating and filtering columns from that year’s set sheets (using `ALLOWED_HEADERS` and `desiredOrder`), then runs deduplication on the summary sheet. |
| **deduplicate_summary.py** | CLI to deduplicate one or more summary spreadsheets in-place (merge duplicate rows, sum Count column). Used by `generate_summaries.py` and can be run standalone with spreadsheet IDs. |
| **spotify_sync.py** | Provides Spotify sync helpers used by `process_new_files.py`: searches Spotify for tracks from a processed set, updates the radio playlist, and creates/rebuilds a per-set history playlist. |
| **ingest_live_history.py** | Prefect flow (`ingest-live-history`) with evaluation hooks: reads the most recent VirtualDJ `.m3u` history file from Drive and POSTs the parsed plays to the api-kaianolevine-com `/v1/live-plays` endpoint. |

---

## Environment variables

Required for Drive/Sheets and logging:

| Variable | Description |
|----------|-------------|
| **GOOGLE_CREDENTIALS_JSON** | Full JSON content of the Google service account (or user) credentials used for Drive and Sheets API. |
| **LOGGING_LEVEL** | Log level (e.g. `DEBUG`, `INFO`). |

Drive/Sheets layout (from **common-python-utils** config):

| Variable | Description |
|----------|-------------|
| **CSV_SOURCE_FOLDER_ID** | Google Drive folder ID for the CSV/file drop zone. |
| **DJ_SETS_FOLDER_ID** | Google Drive folder ID for the main DJ Sets tree (year folders and Summary live here). |
| **OUTPUT_NAME** | Name of the master “DJ Set Collection” spreadsheet. |
| **TEMP_TAB_NAME** | Name of the temporary tab used while building the collection sheet. |
| **SUMMARY_TAB_NAME** | Name of the Summary tab in the collection spreadsheet. |
| **SUMMARY_FOLDER_NAME** | Name of the Summary subfolder under the DJ Sets folder (e.g. `Summary`). |
| **ALLOWED_HEADERS** | List of column names (from set sheets) to include in summary sheets. |
| **desiredOrder** | Order of columns in summary sheets (subset of allowed headers). |
| **DEEJAY_SET_COLLECTION_JSON_PATH** | File path for the JSON snapshot (default: `v1/deejay-sets/deejay_set_collection.json`). |

Spotify (for CSV pipeline sync via `process_new_files.py`):

| Variable | Description |
|----------|-------------|
| **SPOTIPY_CLIENT_ID** | Spotify application client ID. |
| **SPOTIPY_CLIENT_SECRET** | Spotify application client secret. |
| **SPOTIPY_REFRESH_TOKEN** | OAuth refresh token for the Spotify account. Generated once via the Spotipy OAuth flow. |
| **SPOTIPY_REDIRECT_URI** | OAuth redirect URI (default: `http://127.0.0.1:8888/callback`). |
| **SPOTIFY_RADIO_PLAYLIST_ID** | Spotify playlist ID for the standing radio playlist. Skipped if unset. |
| **SPOTIFY_PLAYLIST_SNAPSHOT_JSON_PATH** | Output path for the Spotify playlist snapshot JSON (default: `v1/spotify/spotify_playlists.json`). |

API and Prefect:

| Variable | Description |
|----------|-------------|
| **ANTHROPIC_API_KEY** | Anthropic API key — required for post-run pipeline evaluation across all four flows (together with `KAIANO_API_BASE_URL`). |
| **KAIANO_API_BASE_URL** | Base URL for deejay-marvel-api. If unset, API ingest and post-run pipeline evaluation are both skipped. |
| **KAIANO_API_OWNER_ID** | Owner ID sent with API requests. Falls back to `OWNER_ID`. |
| **OWNER_ID** | Fallback owner ID if `KAIANO_API_OWNER_ID` is not set. |
| **PREFECT_API_KEY** | Prefect Cloud API key. |
| **PREFECT_API_URL** | Prefect Cloud workspace API URL. |
| **PREFECT_ACCOUNT_SLUG** | Prefect account slug (used in workflow login step). |
| **PREFECT_WORKSPACE_SLUG** | Prefect workspace slug (used in workflow login step). |
| **KAIANO_API_REPO_TOKEN** | GitHub token for cloning and pushing kaiano-api. Workflows only. |

For one-time Spotify OAuth setup and obtaining a refresh token, see [docs/SPOTIFY_SETUP.md](docs/SPOTIFY_SETUP.md).

In GitHub Actions, `GOOGLE_CREDENTIALS_JSON` is typically a **secret** and `LOGGING_LEVEL` a **variable**; other values are usually set in the **common-python-utils** config (env or repo variables) used by this cog.

---

## GitHub Actions workflows

| Workflow | Trigger | Purpose |
|----------|--------|--------|
| **process_new_csv_files** | `repository_dispatch` (`new_csv_dj_sets`), `workflow_dispatch` | Runs `process_new_files.py` to ingest new files, syncs matched tracks to Spotify, and copies the Spotify playlist snapshot JSON to api-kaianolevine-com. |
| **update_live_history** | `repository_dispatch` (`vdj_history`), `workflow_dispatch` | Runs `ingest_live_history.py` to read the most recent VDJ `.m3u` file and POST plays to deejay-marvel-api. |
| **update_dj_set_collection** | `repository_dispatch` (`updated_dj_sets`), `workflow_dispatch` | Runs `update_deejay_set_collection.py`, then copies the JSON snapshot into **api-kaianolevine-com** and pushes. |
| **generate_summaries** | `repository_dispatch` (`generate-summary`), `workflow_dispatch` | Runs `generate_summaries.py` to (re)build missing per-year summary sheets. |
| **CI** | Push and pull request to `main` | Runs ruff and pytest with coverage on every PR/push; on push to `main`, runs **semantic-release** to version, update `CHANGELOG.md`, tag, and create GitHub releases. |

The **google-app-script-trigger** repo sends `repository_dispatch` events to this repo when new CSVs are added or when the collection/summaries should be updated.

---

## Running locally with uv

- **Prerequisites**: Python ≥ 3.11, [uv](https://docs.astral.sh/uv/).
- **Install and run**:

```bash
git clone git@github.com:mini-app-polis/deejay-cog.git
cd deejay-cog
uv sync --all-extras
uv run pre-commit install
uv run python -u src/deejay_cog/process_new_files.py   # or update_deejay_set_collection.py, generate_summaries.py
```

For `deduplicate_summary.py` (spreadsheet IDs as arguments):

```bash
uv run python -u src/deejay_cog/deduplicate_summary.py <spreadsheet_id> [spreadsheet_id ...]
```

---

## Running tests

```bash
uv sync --all-extras
uv run pytest
```

With coverage:

```bash
uv run pytest --cov=src --cov-report=term-missing
```

## Running pre-commit

Install hooks (once, after cloning):
```bash
uv run pre-commit install
```

Run manually against all files at any time:
```bash
uv run pre-commit run --all-files
```

Hooks run automatically on `git commit`. Pre-commit runs ruff (lint + format) and pygrep checks. The same checks run in CI — fixing locally before pushing avoids a round-trip.

---

## Versioning

This repo uses **semantic-release** for automated versioning. Versions are determined automatically from commit messages on merge to `main`:

- `feat:` → minor version bump
- `fix:` → patch version bump
- `BREAKING CHANGE` in commit body → major bump
- `chore` / `docs` / `refactor` / `test` / `ci` → no version bump

Note: use an explicit **BREAKING CHANGE** footer for major bumps, not the `feat!:` shorthand.

Never manually edit the version in `pyproject.toml`.  
Never manually edit `CHANGELOG.md`.  
Both are managed automatically on merge to `main`.

---

## Dependencies

- **common-python-utils** ([GitHub main branch](https://github.com/mini-app-polis/common-python-utils)): Shared config, logging, and Google Drive/Sheets helpers. Declared in `pyproject.toml` and installed by `uv sync`.

---

## License

MIT © Kaiano Levine
