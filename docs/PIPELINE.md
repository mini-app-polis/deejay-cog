# Drive ingestion pipeline

This document describes the full pipeline from CSV drop to JSON output and where **deejay-cog** fits in.

---

## End-to-end flow

1. **CSV drop**  
   CSV files (and optionally other files) are placed into a designated Google Drive folder (the “CSV source folder”). Filenames must start with a four-digit year (e.g. `2024-01-15_My_Set.csv`).

2. **Trigger**  
   The **google-app-script-trigger** repo (or similar) detects new files or a request to refresh and sends a `repository_dispatch` event to this repo’s GitHub Actions.

3. **Process new files** (this repo)  
   The **process_new_csv_files** workflow runs `process_new_files.py` (Prefect flow `process-new-csv-files`), which:
   - Normalizes status prefixes on filenames in the source folder (`FAILED_`, `possible_duplicate_`, `Copy of `).
   - For each file, extracts the year from the filename.
   - For CSVs: downloads, normalizes (e.g. strip `sep=`, empty lines), uploads as a Google Sheet into the corresponding year folder under the DJ Sets folder, applies formatting, moves the original file into that year’s `Archive` subfolder.
   - For non-CSV files that start with a year: moves them into the year folder (or renames with `possible_duplicate_` if a file with the same base name already exists).
   - On upload/processing failure: renames the source file with a `FAILED_` prefix.
   - On duplicate base name in the year folder: renames the source file with a `possible_duplicate_` prefix.

4. **Update collection** (this repo)  
   The **update_dj_set_collection** workflow runs `update_deejay_set_collection.py`, which:
   - Builds or updates the master “DJ Set Collection” Google Sheet (tabs per year + Summary).
   - Writes the JSON snapshot to the path configured by `DEEJAY_SET_COLLECTION_JSON_PATH`.
   - The workflow then copies that JSON into the **kaiano-api** repo and commits/pushes.

5. **Generate summaries** (this repo)  
   The **generate_summaries** workflow runs `generate_summaries.py`, which:
   - For each year folder, ensures a “{Year} Summary” sheet exists in the Summary folder.
   - Aggregates and filters columns from that year’s set sheets, then runs `deduplicate_summary` on the summary sheet.

6. **JSON output**  
   The JSON snapshot is consumed by **kaiano-api** (e.g. for the site’s DJ sets data).

---

## Orchestration

All four pipeline entrypoints are Prefect flows. Each run is visible in Prefect Cloud with flow- and task-level logs (where tasks exist) and retry tracking. All four flows register **`on_failure`** and **`on_crashed`** hooks that post a **direct finding** to the evaluator API when the flow fails or crashes before completion.

- **`process-new-csv-files`** (`process_new_files.py`) — **Tasks:** `process-csv-file` (per CSV: download → normalize → upload → archive → ingest / Spotify); `normalize-csv`; `upload-to-sheets`; `ingest-to-api` (retries: 2, delay: 30s); `sync-to-spotify`.
- **`update-dj-set-collection`** (`update_deejay_set_collection.py`) — No named tasks; single flow body.
- **`generate-summaries`** (`generate_summaries.py`) — No named tasks; single flow body.
- **`ingest-live-history`** (`ingest_live_history.py`) — **Tasks:** `process-m3u-file`.

View runs: [app.prefect.cloud](https://app.prefect.cloud)

---

## Observability

- **Post-run evaluation:** At the end of each successful run, all four flows call `pipeline_evaluator.evaluator.evaluate_pipeline_run` (from **evaluator-cog**) to post an evaluation to the deejay-marvel-api evaluations endpoint. Counters and context are flow-specific (CSV stats, collection/summary metrics, live ingest summary, etc.).
- **Prefect hooks:** All four flows have **`on_failure`** and **`on_crashed`** hooks that post an immediate error/warn finding if the flow exits in a failed or crashed state before normal completion.
- **GitHub Actions failure step:** All four pipeline workflows include a **Report failure to evaluator** step (`if: failure()`) that runs a **`curl` POST** to the same evaluations API. It runs even when earlier steps fail (for example **`uv sync`** or checkout), so infra failures are reported without relying on Python or a virtualenv.
- **Gating:** End-of-run evaluation is performed only when both **`ANTHROPIC_API_KEY`** and **`KAIANO_API_BASE_URL`** are set (see [CONFIGURATION.md](CONFIGURATION.md)).

---

## Deprecation Plan

The following workflows are validation-layer only and will be removed once PostgreSQL is confirmed as the source of truth for all DJ set data:

- **update-dj-set-collection**: Rebuilds Google Sheets master collection and JSON snapshot. Kept for cross-validation against PostgreSQL data.
- **generate-summaries**: Generates per-year summary sheets. Kept for cross-validation against PostgreSQL data.

**process-new-csv-files** is permanent and will be extended with additional functionality in future phases.

---

## Where this processor fits

- **deejay-cog** is the backend that performs steps 3–5: ingest CSVs into Sheets, maintain the collection sheet and JSON, and build summary sheets.
- **google-app-script-trigger** (or equivalent) is responsible for detecting Drive events and triggering the right workflow via `repository_dispatch`.

---

## Repository dispatch events

| Event type | Triggered by | Workflow run |
|------------|--------------|--------------|
| `new_csv_dj_sets` | New CSV/files in the source folder (e.g. from Apps Script) | **process_new_csv_files** |
| `updated_dj_sets` | Request to refresh the collection and push JSON to kaiano-api | **update_dj_set_collection** |
| `generate-summary` | Request to (re)generate per-year summary sheets | **generate_summaries** |
| `vdj_history` | Request to ingest VirtualDJ live history (`.m3u`) into the API | **update_live_history** |

---

## Error handling behavior

- **FAILED_ prefix**: If uploading/formatting a CSV fails, the processor renames the source file to `FAILED_<original_name>` so it can be inspected and retried (e.g. after fixing or re-running prefix normalization).
- **possible_duplicate_ prefix**: If a file with the same base name already exists in the target year folder, the newly processed file is renamed to `possible_duplicate_<original_name>` (and the existing file is left as-is). Summary generation skips years that still contain `FAILED_` or `_Cleaned` set files.
- **Archive subfolder**: After a CSV is successfully uploaded as a Sheet, the original file is moved into the year folder’s `Archive` subfolder so the source folder can be refilled without name clashes.
- **Spotify sync**: Sync runs only when **all three** of **`SPOTIPY_CLIENT_ID`**, **`SPOTIPY_CLIENT_SECRET`**, and **`SPOTIPY_REFRESH_TOKEN`** are set. If any are missing, Spotify sync is **skipped** with a warning (not treated as a hard error); the rest of the CSV pipeline continues.
