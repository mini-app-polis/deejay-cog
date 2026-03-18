# Drive ingestion pipeline

This document describes the full pipeline from CSV drop to JSON output and where **deejay-set-processor** fits in.

---

## End-to-end flow

1. **CSV drop**  
   CSV files (and optionally other files) are placed into a designated Google Drive folder (the “CSV source folder”). Filenames must start with a four-digit year (e.g. `2024-01-15_My_Set.csv`).

2. **Trigger**  
   The **google-app-script-trigger** repo (or similar) detects new files or a request to refresh and sends a `repository_dispatch` event to this repo’s GitHub Actions.

3. **Process new files** (this repo)  
   The **process_new_csv_files** workflow runs `process_new_files.py`, which:
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

## Where this processor fits

- **deejay-set-processor** is the backend that performs steps 3–5: ingest CSVs into Sheets, maintain the collection sheet and JSON, and build summary sheets.
- **google-app-script-trigger** (or equivalent) is responsible for detecting Drive events and triggering the right workflow via `repository_dispatch`.

---

## Repository dispatch events

| Event type | Triggered by | Workflow run |
|------------|--------------|--------------|
| `new_csv_dj_sets` | New CSV/files in the source folder (e.g. from Apps Script) | **process_new_csv_files** |
| `updated_dj_sets` | Request to refresh the collection and push JSON to kaiano-api | **update_dj_set_collection** |
| `generate-summary` | Request to (re)generate per-year summary sheets | **generate_summaries** |

---

## Error handling behavior

- **FAILED_ prefix**: If uploading/formatting a CSV fails, the processor renames the source file to `FAILED_<original_name>` so it can be inspected and retried (e.g. after fixing or re-running prefix normalization).
- **possible_duplicate_ prefix**: If a file with the same base name already exists in the target year folder, the newly processed file is renamed to `possible_duplicate_<original_name>` (and the existing file is left as-is). Summary generation skips years that still contain `FAILED_` or `_Cleaned` set files.
- **Archive subfolder**: After a CSV is successfully uploaded as a Sheet, the original file is moved into the year folder’s `Archive` subfolder so the source folder can be refilled without name clashes.
