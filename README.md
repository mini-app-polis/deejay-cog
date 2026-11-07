# deejay-cog

Processes DJ set CSV files from Google Drive into Google Sheets (organized by year), can build a master collection spreadsheet and JSON snapshot, generates per-year summary sheets for local validation, and sends sets, live plays and run reports to **api-kaianolevine-com**.

---

## What this processor does

This repository is the backend cog for a Drive-based DJ set pipeline. It reads CSV files (and optionally other files) from a configured Google Drive source folder, normalizes and uploads them as Google Sheets into year-based folders, and can maintain collection and summary artifacts for cross-checks during the PostgreSQL migration.

**Production** runs on AWS Lambda behind its own FIFO SQS queue — see [ADR-0006](docs/decisions/ADR-0006-lambda-behind-sqs.md). The queue, function and alarm are declared in [`mini-app-polis/infra`](https://github.com/mini-app-polis/infra). **watcher-cog** detects Drive changes and POSTs `/v1/deejay/runs` with a `mode` (one of the production modes below); api-kaianolevine-com enqueues one message, and `deejay_cog.worker.lambda_handler` runs the matching flow. Nothing runs between triggers, except the library backfill, which is meant to be sent nightly ([ADR-0003](docs/decisions/ADR-0003-on-demand-no-cadence.md), amended).

---

## Flow inventory

### Production (run by the Lambda worker, one flow per queue message)

| Mode | Underlying flow | Script | Notes |
|------|-----------------|--------|--------|
| `process-new-files` | **process-new-csv-files** | `process_new_files.py` | New CSVs → Sheets → archive → API ingest; optional Spotify sync. |
| `ingest-live-history` | **ingest-live-history** | `ingest_live_history.py` | Most recent VirtualDJ `.m3u` from Drive → `POST /v1/live-plays` (not every history file). |
| `retag-music` | **retag-music** | `retag_music_from_catalog.py` | Tags the music upload folder's files from the catalog; a file with no usable title or artist goes to the fingerprint function. |
| `new-music` | **new-music** | `new_music.py` | New music intake: identifies each file in the new-music input folder (catalog, else the fingerprint function), sends a tagged copy named `{title}_{artist}` to the destination folder and keeps the original in processed-originals; anything not identified with confidence goes to needs-review. Off unless `NEW_MUSIC_MODE` is `dry-run` or `write`. |
| `library-backfill` | **library-backfill** | `library_backfill.py` | Library backfill: retags the files already in the music library (`LIBRARY_ROOT_FOLDER_ID`) from the catalog, in place, never renaming, moving or deleting one; the next `LIBRARY_BACKFILL_CHUNK_SIZE` files each run, from a manifest and cursor kept in `MUSIC_TAGGING_STATE_FOLDER_ID`. Meant to run nightly (the trigger is wired outside this repo). Off unless `LIBRARY_BACKFILL_MODE` is `dry-run` or `write`. Each run first does the library scan (`library_scan.py`): files added to or removed from the library are registered or marked not present in the library registry, metadata only; off unless `LIBRARY_SCAN_MODE` is `dry-run` or `write`. Then the library identify step (`library_identify.py`): unidentified files are identified by their tags or the fingerprint function, linked to the catalog and written with its values in place; off unless `LIBRARY_IDENTIFY_MODE` is `dry-run` or `write`. After the chunk it runs the library reconcile (`library_reconcile.py`): the next `LIBRARY_RECONCILE_CHUNK_SIZE` linked files compared with the catalog, every difference grouped by kind, reading only; off unless `LIBRARY_RECONCILE_MODE` is `dry-run`. Then the library sync (`library_sync.py`) writes catalog edits and display-rule changes to the linked files they affect; off unless `LIBRARY_SYNC_MODE` is `dry-run` or `write`. Right after the scan, the library trash step (`library_trash.py`) moves the copies the owner sent to the trash on the WCS duplicates page to Drive's trash, never deleting one; off unless `LIBRARY_TRASH_MODE` is `dry-run` or `write`. Last, the duplicate report (`library_duplicates.py`) groups files that look like copies of one song, marks different edits and release labels kept on purpose, and in `write` sends the groups to the API for that page; it never writes music; off unless `LIBRARY_DUPLICATES_MODE` is `dry-run` or `write`. `library_nightly.py` runs the seven steps. |

The **fingerprint function** (`fingerprint.py`) is a second Lambda behind its own queue: it identifies one such file by its sound (AcoustID, MusicBrainz) and tags it, or for a new-music file takes it in with the new-music steps — see [ADR-0007](docs/decisions/ADR-0007-music-tagging-redesign.md). Every flow that writes music goes through `music_safety.py` (modes, the audio check, run records), and `music_rollback.py` undoes a run from its record.

### Local-only (not served; PostgreSQL cutover helpers)

| Flow | Script |
|------|--------|
| **update-dj-set-collection** | `update_deejay_set_collection.py` |
| **generate-summaries** | `generate_summaries.py` |

---

## Inputs

- **Source location**: Google Drive folder (`CSV_SOURCE_FOLDER_ID`) — drop zone for files to process.
- **File format**: CSVs with filenames starting with a four-digit year (e.g. `2024-01-15_My_Set.csv`). Non-CSV files that start with a year can be moved into the year folder without conversion.
- **Origin**: Files are placed in the folder by your Drive workflow; **watcher-cog** asks the API to enqueue a run when new files appear.

---

## Outputs

- **Google Sheets in year folders** — Each processed CSV becomes a Sheet under `DJ_SETS_FOLDER_ID`; originals go to each year’s `Archive` folder.
- **Master collection** (local flow) — `update_deejay_set_collection.py` can rebuild the master spreadsheet and JSON snapshot (`DEEJAY_SET_COLLECTION_JSON_PATH`).
- **Summary sheets** (local flow) — `generate_summaries.py` builds “{Year} Summary” sheets using `deduplicate_summary.py`.
- **API** — Sets go to `POST /v1/ingest`, live plays to `POST /v1/live-plays`, the Spotify playlist catalog to `POST /v1/spotify/playlists`, and one run report per production run to `POST /v1/notify`.

---

## Scripts

| Script | Purpose |
|--------|--------|
| **process_new_files.py** | Production CSV pipeline (the `process-new-files` mode). |
| **ingest_live_history.py** | Production live-play ingest from the latest `.m3u`. |
| **update_deejay_set_collection.py** | Local-only collection + JSON rebuild. |
| **generate_summaries.py** | Local-only summary generation. |
| **retag_music_from_catalog.py** | Production music tagging from the catalog (the `retag-music` mode). |
| **fingerprint.py** | The fingerprint function: identifies untagged music files by their sound. |
| **new_music.py** | Production new-music intake (the `new-music` mode). |
| **library_backfill.py** | Production library backfill (the `library-backfill` mode). |
| **music_safety.py** | The safety layer every music-writing flow uses: modes, the audio check, run records. |
| **music_rollback.py** | Local CLI: undo a music-tagging run from its run record (dry run unless `--apply`). |
| **deduplicate_summary.py** | CLI / helper to deduplicate summary spreadsheets. |
| **spotify_sync.py** | Spotify helpers used from `process_new_files.py`. |

---

## Environment variables

Every variable this cog reads is listed, with its default and whether it is
required, in [`.env.example`](.env.example).
[docs/CONFIGURATION.md](docs/CONFIGURATION.md) covers how the deployed
worker loads them and what each part does when one is missing.

---

## Running locally with uv (DOC-013)

**Prerequisites:** Python ≥ 3.11, [uv](https://docs.astral.sh/uv/), and the
[Doppler CLI](https://docs.doppler.com/docs/install-cli). Secrets come from
this repo's own Doppler `dev` config — nothing reads a `.env` file, and local
commands use `dev`. A command that must act on production would say
`--config prd` explicitly (ecosystem-standards CFG-003); none here does.

```bash
brew install gnupg dopplerhq/cli/doppler   # once per machine
doppler login                              # once per machine

git clone git@github.com:mini-app-polis/deejay-cog.git
cd deejay-cog
doppler setup                              # reads doppler.yaml: mini-app-polis-ecosystem / dev
uv sync --all-extras
uv run pre-commit install
uv run pre-commit run --all-files
uv run check-doppler-keys                  # every required .env.example name is in dev
```

Anything that needs secrets runs under `doppler run -- …`; the tests do not.

**Production flows** — the worker runs them on Lambda; locally, run one directly:

```bash
doppler run -- uv run python -u src/deejay_cog/process_new_files.py
doppler run -- uv run python -u src/deejay_cog/ingest_live_history.py
```

The folder IDs default to the production folders, and a local run ingests
into the development API. `process_new_files.py` therefore refuses to run
outside production unless `CSV_SOURCE_FOLDER_ID` is set in your shell (a
test drop-zone folder): left on the default
it would archive the real drop zone's CSVs into the development API.

**Local-only flows** — run modules directly. These call `post_run_finding(..., production_only=False)`, so their run reports are logged and never sent, whatever is set in your shell:

```bash
doppler run -- uv run python -m deejay_cog.generate_summaries
doppler run -- uv run python -m deejay_cog.update_deejay_set_collection
```

**music_rollback** (a run id as argument; prints what it would undo, and
does it only with `--apply` — see [PIPELINE.md](docs/PIPELINE.md#rolling-a-music-run-back)):

```bash
doppler run -- uv run python -m deejay_cog.music_rollback <run_id>
doppler run -- uv run python -m deejay_cog.music_rollback <run_id> --apply
```

**deduplicate_summary** (spreadsheet IDs as arguments):

```bash
doppler run -- uv run python -u src/deejay_cog/deduplicate_summary.py <spreadsheet_id> [spreadsheet_id ...]
```

---

## Running tests

`tests/unit/` holds the unit tests; `tests/integration/` runs the Lambda
handler end to end on SQS events, with the API stubbed by respx and Drive,
Sheets and Spotify by in-memory fakes. One `uv run pytest` runs both.

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

---

## License

MIT © Kaiano Levine
