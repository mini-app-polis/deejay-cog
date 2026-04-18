# Drive ingestion pipeline

This document describes how **deejay-cog** fits into the Drive → Prefect → API pipeline.

---

## Flow tiers

### Production (served via `prefect.serve()` on Railway)

| Flow | Module | Role |
|------|--------|------|
| **process-new-csv-files** | `process_new_files.py` | Triggered by **watcher-cog** when new CSVs appear in the source folder: normalize, upload to Sheets, archive, ingest to API, Spotify sync. |
| **ingest-live-history** | `ingest_live_history.py` | Triggered by **watcher-cog** when new VirtualDJ `.m3u` files appear in the VDJ history folder; reads Drive, parses plays, and POSTs to `/v1/live-plays`. **Processes only the most recent `.m3u` file**, not the full history list. |

### Local-only (not served; retained for manual validation during the PostgreSQL cutover)

| Flow | Module | Role |
|------|--------|------|
| **update-dj-set-collection** | `update_deejay_set_collection.py` | Rebuilds the master DJ set collection spreadsheet and JSON snapshot. |
| **generate-summaries** | `generate_summaries.py` | Builds or refreshes per-year summary sheets and deduplication. |

### Work in progress (not served; system-dependency blockers)

| Flow | Module | Role |
|------|--------|------|
| **retag-music** | `retag_music.py` | Drive → AcoustID/MusicBrainz tagging pipeline. Requires `ffmpeg` and `fpcalc` on the host; not deployed on Railway today. |

---

## Trigger architecture

**watcher-cog** polls Google Drive (or receives push notifications, depending on deployment). When new files match configured rules, it calls the **Prefect Cloud API** to create a flow run for the appropriate deployment. The **Prefect worker** on Railway runs `python -m deejay_cog.main`, which serves the production flows in-process.

Per **PIPE-008** in ecosystem-standards, **watcher-cog → Prefect → cog** is the canonical trigger pattern (not GitHub Actions as orchestrator).

---

## End-to-end data path (production CSV flow)

1. **CSV drop** — Files land in the CSV source folder (`CSV_SOURCE_FOLDER_ID`). CSV names must start with a four-digit year (e.g. `2024-01-15_My_Set.csv`).
2. **Prefect run** — watcher-cog schedules **process-new-csv-files**.
3. **Processing** — Prefix normalization, per-file year detection, CSV → Sheet upload, archive moves, API ingest, optional Spotify playlist push.

Live history follows the same pattern for `.m3u` files and **ingest-live-history**.

---

## Orchestration

All five entrypoints are Prefect `@flow` functions. Production flows register **`on_failure`** / **`on_crashed`** hooks via `deejay_cog._pipeline_eval.make_failure_hook`, which log locally and may post a **direct finding** (WARN vs ERROR) when gated.

View runs: [app.prefect.cloud](https://app.prefect.cloud)

---

## Observability

- **Pipeline evaluation** — Production flows call `deejay_cog._pipeline_eval.post_run_finding` at normal completion: **exactly one** finding per run, with `direct_severity` mapped to the evaluator (SUCCESS intent is sent as **INFO** to match evaluator-cog’s allowed direct severities). Severity is **SUCCESS** (stored as INFO) for clean completion including intentional skips (e.g. API URL unset); **WARN** when the run finished but real-issue counters are non-zero; **ERROR** only from the failure/crash hook.
- **Local-only and WIP flows** use `production_only=False`: hooks still log, but **no** HTTP posts to `pipeline_evaluations`, regardless of environment variables.
- **Gating for production posts** — `post_run_finding` posts only when `production_only=True` **and** both **`KAIANO_API_BASE_URL`** and **`ANTHROPIC_API_KEY`** are set (see [CONFIGURATION.md](CONFIGURATION.md)).

---

## Deprecation plan

**update-dj-set-collection** and **generate-summaries** are validation-layer utilities. They will be **retired when PostgreSQL is confirmed as the source of truth** for DJ set data — not on a GitHub workflow removal timeline, because Actions are no longer the orchestrator for this cog.

**process-new-csv-files** and **ingest-live-history** remain the long-lived production paths.

---

## Error handling behavior

- **FAILED_ prefix** — Failed CSV processing renames the source file to `FAILED_<original_name>` for inspection and retry.
- **possible_duplicate_ prefix** — When a basename collision occurs in the year folder, the new file is renamed with this prefix.
- **Summary generation** — Skips years that still contain `FAILED_` or `_Cleaned` set files until cleaned up.
- **Archive subfolder** — After a successful Sheet upload, the original CSV is moved under that year’s `Archive` folder.
- **Spotify sync** — Runs only when **SPOTIPY_CLIENT_ID**, **SPOTIPY_CLIENT_SECRET**, and **SPOTIPY_REFRESH_TOKEN** are all set; otherwise it is skipped (not a hard error for the rest of the CSV pipeline).
