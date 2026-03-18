# deejay-set-processor

Processes DJ set CSV files from Google Drive into Google Sheets (organized by year), builds a master collection spreadsheet and JSON snapshot, generates per-year summary sheets, and pushes the collection JSON to **kaiano-api** for the site.

---

## What this processor does

This repository is the backend processor for a Drive-based DJ set pipeline. It reads CSV files (and optionally other files) from a configured Google Drive source folder, normalizes and uploads them as Google Sheets into year-based folders, maintains a master “DJ Set Collection” spreadsheet and a JSON snapshot of that collection, generates per-year summary sheets with deduplication, and can push the JSON snapshot to the kaiano-api repo. It is intended to be triggered by GitHub Actions (via `repository_dispatch` from the **google-app-script-trigger** repo or manually via `workflow_dispatch`).

---

## Inputs

- **Source location**: A single Google Drive folder (ID set by `CSV_SOURCE_FOLDER_ID`). This is the “drop zone” for files to process.
- **File format**: CSVs exported from DJ software (e.g. Rekordbox), with filenames starting with a four-digit year (e.g. `2024-01-15_My_Set.csv`). Non-CSV files that start with a year are also supported (moved into the year folder without conversion).
- **Origin**: Files are typically placed in this folder by Google Apps Script (from **google-app-script-trigger**), which then triggers the processor via GitHub Actions.

---

## Outputs

- **Google Sheets in year folders**: Each processed CSV is uploaded as a Google Sheet into a year subfolder (e.g. `2024`) under `DJ_SETS_FOLDER_ID`. The original file is moved into an `Archive` subfolder under that year.
- **Master collection**: A single Google Sheet (name from `OUTPUT_NAME`) in the DJ Sets folder, with one tab per year and a Summary tab, listing all sets with Date, Name, and Link. Built/updated by `update_deejay_set_collection.py`.
- **Summary sheet**: Per-year “{Year} Summary” sheets in the Summary subfolder, aggregating and deduplicating track data from that year’s set sheets. Built by `generate_summaries.py` (which uses `deduplicate_summary.py`).
- **JSON snapshot**: A JSON file (path from `DEEJAY_SET_COLLECTION_JSON_PATH`, default `v1/deejay-sets/deejay_set_collection.json`) describing the collection. The **update_dj_set_collection** workflow copies this file into the **kaiano-api** repo at `website/src/json-site-data/deejay-sets/deejay_set_collection.json` and commits/pushes.

---

## Scripts

| Script | Purpose |
|--------|--------|
| **process_new_files.py** | Lists the CSV source folder, normalizes status prefixes on filenames, and processes each file: extracts year from filename, uploads CSVs as Sheets into the correct year folder (with formatting), moves originals to Archive, and handles duplicates/failures (e.g. `FAILED_`, `possible_duplicate_`). |
| **update_deejay_set_collection.py** | Scans the DJ Sets folder (year subfolders + Summary), builds/updates the master collection spreadsheet (tabs per year + Summary), and writes the JSON snapshot to the path given by `DEEJAY_SET_COLLECTION_JSON_PATH`. |
| **generate_summaries.py** | For each year folder, finds or creates the “{Year} Summary” sheet in the Summary folder, aggregating and filtering columns from that year’s set sheets (using `ALLOWED_HEADERS` and `desiredOrder`), then runs deduplication on the summary sheet. |
| **deduplicate_summary.py** | CLI to deduplicate one or more summary spreadsheets in-place (merge duplicate rows, sum Count column). Used by `generate_summaries.py` and can be run standalone with spreadsheet IDs. |

---

## Environment variables

Required for Drive/Sheets and logging:

| Variable | Description |
|----------|-------------|
| **GOOGLE_CREDENTIALS_JSON** | Full JSON content of the Google service account (or user) credentials used for Drive and Sheets API. |
| **LOGGING_LEVEL** | Log level (e.g. `DEBUG`, `INFO`). |

Drive/Sheets layout (from **kaiano-common-utils** config):

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

In GitHub Actions, `GOOGLE_CREDENTIALS_JSON` is typically a **secret** and `LOGGING_LEVEL` a **variable**; other values are usually set in the **kaiano-common-utils** config (env or repo variables) used by this processor.

---

## GitHub Actions workflows

| Workflow | Trigger | Purpose |
|----------|--------|--------|
| **process_new_csv_files** | `repository_dispatch` (`new_csv_dj_sets`), `workflow_dispatch` | Runs `process_new_files.py` to ingest new files from the CSV source folder. |
| **update_dj_set_collection** | `repository_dispatch` (`updated_dj_sets`), `workflow_dispatch` | Runs `update_deejay_set_collection.py`, then copies the JSON snapshot into **kaiano-api** and pushes. |
| **generate_summaries** | `repository_dispatch` (`generate-summary`), `workflow_dispatch` | Runs `generate_summaries.py` to (re)build missing per-year summary sheets. |
| **test-and-version** | Push, pull_request | Runs pre-commit (ruff), tests, and coverage. On push to `main`, auto-increments PATCH and tags `vX.Y.Z`. |
| **manual-version-bump** | `workflow_dispatch` (input: minor / major) | Runs tests then bumps MINOR or MAJOR version, commits and tags, and pushes. |

The **google-app-script-trigger** repo sends `repository_dispatch` events to this repo when new CSVs are added or when the collection/summaries should be updated.

---

## Running locally with uv

- **Prerequisites**: Python ≥ 3.11, [uv](https://docs.astral.sh/uv/).
- **Install and run**:

```bash
git clone git@github.com:kaianolevine/deejay-set-processor.git
cd deejay-set-processor
uv sync --all-extras
uv run python -u src/deejay_set_processor/process_new_files.py   # or update_deejay_set_collection.py, generate_summaries.py
```

For `deduplicate_summary.py` (spreadsheet IDs as arguments):

```bash
uv run python -u src/deejay_set_processor/deduplicate_summary.py <spreadsheet_id> [spreadsheet_id ...]
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

## Versioning

- **PATCH**: Automatically bumped on every push to `main` by the **test-and-version** workflow; a new tag `vX.Y.Z` is created and pushed.
- **MINOR / MAJOR**: Bumped manually via the **manual-version-bump** workflow (`workflow_dispatch`), choosing minor or major.

---

## Dependencies

- **kaiano-common-utils** ([GitHub main branch](https://github.com/kaianolevine/kaiano-common-utils)): Shared config, logging, and Google Drive/Sheets helpers. Declared in `pyproject.toml` and installed by `uv sync`.

---

## License

MIT © Kaiano Levine
