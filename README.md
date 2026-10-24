# deejay-cog

Processes DJ set CSV files from Google Drive into Google Sheets (organized by year), can build a master collection spreadsheet and JSON snapshot, generates per-year summary sheets for local validation, and sends sets, live plays and run reports to **api-kaianolevine-com**.

---

## What this processor does

This repository is the backend cog for a Drive-based DJ set pipeline. It reads CSV files (and optionally other files) from a configured Google Drive source folder, normalizes and uploads them as Google Sheets into year-based folders, and can maintain collection and summary artifacts for cross-checks during the PostgreSQL migration.

**Production** runs on AWS Lambda behind its own FIFO SQS queue — see [ADR-006](docs/decisions/ADR-006-lambda-behind-sqs.md). The queue, function and alarm are declared in [`mini-app-polis/infra`](https://github.com/mini-app-polis/infra). **watcher-cog** detects Drive changes and POSTs `/v1/deejay/runs` with a `mode` (`process-new-files` or `ingest-live-history`); api-kaianolevine-com enqueues one message, and `deejay_cog.worker.lambda_handler` runs the matching flow. Nothing runs between triggers.

---

## Flow inventory

### Production (run by the Lambda worker, one flow per queue message)

| Mode | Underlying flow | Script | Notes |
|------|-----------------|--------|--------|
| `process-new-files` | **process-new-csv-files** | `process_new_files.py` | New CSVs → Sheets → archive → API ingest; optional Spotify sync. |
| `ingest-live-history` | **ingest-live-history** | `ingest_live_history.py` | Most recent VirtualDJ `.m3u` from Drive → `POST /v1/live-plays` (not every history file). |

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
| **retag_music.py** | Local-only / WIP tagging pipeline (system binaries required). |
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
Doppler's shared `dev` config — nothing reads a `.env` file, and local
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

**Local-only / WIP flows** — run modules directly. These call `post_run_finding(..., production_only=False)`, so their run reports are logged and never sent, whatever is set in your shell:

```bash
doppler run -- uv run python -m deejay_cog.generate_summaries
doppler run -- uv run python -m deejay_cog.update_deejay_set_collection
doppler run -- uv run python -m deejay_cog.retag_music   # requires ffmpeg + fpcalc on PATH
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
