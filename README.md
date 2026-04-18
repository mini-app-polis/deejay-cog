# deejay-cog

Processes DJ set CSV files from Google Drive into Google Sheets (organized by year), can build a master collection spreadsheet and JSON snapshot, generates per-year summary sheets for local validation, and integrates with **api-kaianolevine-com** for ingest and evaluations.

---

## What this processor does

This repository is the backend cog for a Drive-based DJ set pipeline. It reads CSV files (and optionally other files) from a configured Google Drive source folder, normalizes and uploads them as Google Sheets into year-based folders, and can maintain collection and summary artifacts for cross-checks during the PostgreSQL migration.

**Production** flows run on Railway under `python -m deejay_cog.main` (Prefect `serve()`). **watcher-cog** detects Drive changes and creates Prefect runs per **PIPE-008** (watcher-cog → Prefect → cog).

---

## Flow inventory

### Production (served on Railway)

| Flow | Script | Notes |
|------|--------|--------|
| **process-new-csv-files** | `process_new_files.py` | New CSVs → Sheets → archive → API ingest; optional Spotify sync. |
| **ingest-live-history** | `ingest_live_history.py` | Most recent VirtualDJ `.m3u` from Drive → `POST /v1/live-plays` (not every history file). |

### Local-only (not served; PostgreSQL cutover helpers)

| Flow | Script |
|------|--------|
| **update-dj-set-collection** | `update_deejay_set_collection.py` |
| **generate-summaries** | `generate_summaries.py` |

### Work in progress (not served; `ffmpeg` / `fpcalc` required)

| Flow | Script |
|------|--------|
| **retag-music** | `retag_music.py` |

---

## Inputs

- **Source location**: Google Drive folder (`CSV_SOURCE_FOLDER_ID`) — drop zone for files to process.
- **File format**: CSVs with filenames starting with a four-digit year (e.g. `2024-01-15_My_Set.csv`). Non-CSV files that start with a year can be moved into the year folder without conversion.
- **Origin**: Files are placed in the folder by your Drive workflow; **watcher-cog** schedules Prefect when new files appear.

---

## Outputs

- **Google Sheets in year folders** — Each processed CSV becomes a Sheet under `DJ_SETS_FOLDER_ID`; originals go to each year’s `Archive` folder.
- **Master collection** (local flow) — `update_deejay_set_collection.py` can rebuild the master spreadsheet and JSON snapshot (`DEEJAY_SET_COLLECTION_JSON_PATH`).
- **Summary sheets** (local flow) — `generate_summaries.py` builds “{Year} Summary” sheets using `deduplicate_summary.py`.
- **API** — When configured, CSV ingest and live plays POST to **api-kaianolevine-com**; pipeline evaluations use the same base URL plus Anthropic for gated production posts.

---

## Scripts

| Script | Purpose |
|--------|--------|
| **process_new_files.py** | Production CSV pipeline (also the `main` Prefect deployment). |
| **ingest_live_history.py** | Production live-play ingest from the latest `.m3u`. |
| **update_deejay_set_collection.py** | Local-only collection + JSON rebuild. |
| **generate_summaries.py** | Local-only summary generation. |
| **retag_music.py** | Local-only / WIP tagging pipeline (system binaries required). |
| **deduplicate_summary.py** | CLI / helper to deduplicate summary spreadsheets. |
| **spotify_sync.py** | Spotify helpers used from `process_new_files.py`. |

---

## Environment variables

Required for Drive/Sheets and logging:

| Variable | Description |
|----------|-------------|
| **GOOGLE_CREDENTIALS_JSON** | Service account (or user) JSON for Google APIs. |
| **LOGGING_LEVEL** | e.g. `DEBUG`, `INFO`. |

Layout and behavior keys live in **common-python-utils** / `config` (see [docs/CONFIGURATION.md](docs/CONFIGURATION.md)).

API and Prefect (production):

| Variable | Description |
|----------|-------------|
| **ANTHROPIC_API_KEY** | With **`KAIANO_API_BASE_URL`**, enables **production** `post_run_finding` posts to pipeline evaluations. |
| **KAIANO_API_BASE_URL** | api-kaianolevine-com base URL; required for API ingest and for gated evaluation posts. |
| **KAIANO_API_CLERK_MACHINE_SECRET** | M2M auth for the API client (see common-python-utils). |
| **PREFECT_API_KEY** / **PREFECT_API_URL** | Prefect Cloud worker authentication. |

Spotify variables (`SPOTIPY_*`, `SPOTIFY_RADIO_PLAYLIST_ID`) are optional; if incomplete, Spotify steps are skipped.

---

## Running locally with uv (DOC-013)

**Prerequisites:** Python ≥ 3.11, [uv](https://docs.astral.sh/uv/).

```bash
git clone git@github.com:mini-app-polis/deejay-cog.git
cd deejay-cog
uv sync --all-extras
uv run pre-commit install
uv run pre-commit run --all-files
```

**Production flows (Prefect runner)** — same entrypoint as Railway:

```bash
uv run python -m deejay_cog.main
```

**Local-only / WIP flows** — run modules directly. These call `post_run_finding(..., production_only=False)` and **do not** write to the production `pipeline_evaluations` API, **even if** `KAIANO_API_BASE_URL` and `ANTHROPIC_API_KEY` are set in your shell:

```bash
uv run python -m deejay_cog.generate_summaries
uv run python -m deejay_cog.update_deejay_set_collection
uv run python -m deejay_cog.retag_music   # requires ffmpeg + fpcalc on PATH
```

**One-off CSV processing** (without Prefect):

```bash
uv run python -u src/deejay_cog/process_new_files.py
```

**deduplicate_summary** (spreadsheet IDs as arguments):

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

---

## Running pre-commit

```bash
uv run pre-commit install
```

```bash
uv run pre-commit run --all-files
```

Hooks match CI (ruff and related checks).

---

## Versioning

**semantic-release** on merge to `main`: conventional commits drive version bumps; do not hand-edit `pyproject.toml` or `CHANGELOG.md`.

---

## Dependencies

- **common-python-utils** — shared Google helpers and config ([GitHub](https://github.com/mini-app-polis/common-python-utils)).
- **evaluator-cog** — pipeline evaluation client (used from `deejay_cog._pipeline_eval` for production posts).

---

## License

MIT © Kaiano Levine
