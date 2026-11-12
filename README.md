# deejay-cog

Processes DJ set CSV files from Google Drive into Google Sheets (organized by year), can build a master collection spreadsheet and JSON snapshot, generates per-year summary sheets for local validation, and sends sets, live plays and run reports to **api-kaianolevine-com**.

---

## What this processor does

This repository is the backend cog for a Drive-based DJ set pipeline. It reads CSV files (and optionally other files) from a configured Google Drive source folder, normalizes and uploads them as Google Sheets into year-based folders, and can maintain collection and summary artifacts for cross-checks during the PostgreSQL migration.

**Production** runs on AWS Lambda behind its own FIFO SQS queue — see [ADR-0006](docs/decisions/ADR-0006-lambda-behind-sqs.md). The queue, function and alarm are declared in [`mini-app-polis/infra`](https://github.com/mini-app-polis/infra). **watcher-cog** detects Drive changes and POSTs `/v1/deejay/runs` with a `mode` (one of the production modes below); api-kaianolevine-com enqueues one message, and `deejay_cog.worker.lambda_handler` runs the matching flow. Nothing runs between triggers except the two library jobs, which watcher-cog sends on their own schedules (declared in `mini-app-polis/infra`): `library-backfill` and `library-reconcile` ([ADR-0003](docs/decisions/ADR-0003-on-demand-no-cadence.md), amended). Either can be run on demand; see [PIPELINE.md](docs/PIPELINE.md#running-a-library-job-on-demand).

**Adding and removing songs:** every case, with what to do and what happens, is in [ADDING-AND-REMOVING-SONGS.md](docs/ADDING-AND-REMOVING-SONGS.md).

**Marking an owner edit:** a file you changed by hand (an intro cut, words censored) is an owner edit, a version with its own catalog entry and plays, never the published song's ([ADR-0008, 2026-11-11](docs/decisions/ADR-0008-catalog-source-of-truth.md#decided-with-the-owner-2026-11-11-owner-edits)). Mark it in the file's tags only, by writing its version in `< >` in the title: `Baby What You Want Me To Do <Intro Cut>`. Identify (for a new file) or the next reconcile (for one already linked) links it to an owner-edit entry; there is no setting on the WCS site. It is not a release label: Clean, Explicit, Radio Edit and an official No Intro Edit stay in `( )` and share the song's entry. Its fingerprint is only a hint at the recording it was cut from, never a reason to hold or relink it, and it is never sent to AcoustID as the official recording; the duplicates step never groups it with the published song's files. See [PIPELINE.md](docs/PIPELINE.md#the-library-jobs).

---

## Flow inventory

### Production (run by the Lambda worker, one flow per queue message)

| Mode | Underlying flow | Script | Notes |
|------|-----------------|--------|--------|
| `process-new-files` | **process-new-csv-files** | `process_new_files.py` | New CSVs → Sheets → archive → API ingest; optional Spotify sync. |
| `ingest-live-history` | **ingest-live-history** | `ingest_live_history.py` | Most recent VirtualDJ `.m3u` from Drive → `POST /v1/live-plays` (not every history file). |
| `retag-music` | **retag-music** | `retag_music_from_catalog.py` | Tags the music upload folder's files from the catalog; a file with no usable title or artist goes to the fingerprint function. |
| `new-music` | **new-music** | `new_music.py` | New music intake: copies each MP3 in the new-music input folder, tags and audio unchanged, into New Music under a name built from its own tags (`{title}_{artist}`) and keeps the original in processed-originals; anything else goes to needs-review. The library steps identify, group, reconcile and sync it from there. Off unless the `flags.deejay_cog.new_music` flag is `dry-run` or `write`. |
| `library-backfill` | **the library job** | `library_nightly.py` | The library job: six steps over the music library (`LIBRARY_ROOT_FOLDER_ID`), in this order, each off unless its `flags.deejay_cog.<step>` flag is `dry-run` or `write`. **Scan** (`library_scan.py`): files added to or removed from the library reach the library registry, metadata only. **Trash** (`library_trash.py`): the copies the owner sent to the trash on the WCS duplicates page go to Drive's trash, never deleted. **Identify** (`library_identify.py`): unidentified files are identified by their tags checked against the fingerprint function, or by the fingerprint alone, and linked to the catalog; only the link is written. FX Music's files link only to fx entries, made from their own tags, and main-library files never do. **Backfill** (`library_backfill.py`): the first linking pass over the library, from a manifest and cursor in `MUSIC_TAGGING_STATE_FOLDER_ID`; it reports `library_done` once every file is linked. **Sync** (`library_sync.py`): edits made in VirtualDJ go up to the catalog, then catalog edits and display-rule changes are written to the files they affect. **Duplicates** (`library_duplicates.py`): files that look like copies of one song are grouped for the duplicates page, different edits and release labels kept on purpose, each library root on its own (FX Music only within a folder; its copies across folders are listed in the run report); never writes music. Every step runs once per library root (the main library, then FX Music when `FX_LIBRARY_ROOT_FOLDER_ID` is set), with its state kept per root; FX Music's files win over the catalog, so no catalog value is written over one. No step renames, moves or deletes a library file except the trash step's move to Drive's trash. |
| `library-reconcile` | **library-reconcile** | `library_reconcile.py` | Library reconcile, its own scheduled mode: linked files compared with the catalog, every difference grouped by kind and sent to the owner's review page on the WCS site. Only files that changed since it last compared them (in Drive, or in their entry's display) are read, up to `LIBRARY_RECONCILE_CHUNK_SIZE` a run. In `write`, a run first settles the files with a difference the owner approved there (catalog wins: written over the file; file wins: sent up to the entry). A file the owner retitled as an owner edit (a version in `< >`) is linked to its owner-edit entry, with no approval needed. FX Music's files, reconciled after the main library's, win: every difference goes up to the file's fx entry with no approval and never to the review page. Off unless the `flags.deejay_cog.library_reconcile` flag is `dry-run` or `write`. |

The **fingerprint function** (`fingerprint.py`) is a second Lambda behind its own queue: it identifies one such file by its sound (AcoustID, MusicBrainz) and tags it, or for a new-music file takes it in with the new-music steps — see [ADR-0007](docs/decisions/ADR-0007-music-tagging-redesign.md). Every flow that writes music goes through `music_safety.py` (modes, the audio check, run records), and `music_rollback.py` undoes a run from its record.

The **library worker** (`library_worker.py`) is a third Lambda behind its own FIFO queue (`deejay-library`), with a longer timeout: the API sends every `library-*` mode there, so a library run that works until its deadline never holds up `process-new-files` or `ingest-live-history` on the shared queue. It runs only `library-*` modes; any other mode it receives fails and dead-letters. The shared worker still accepts every mode.

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
| **library_worker.py** | The library worker: the shared worker's handler, for `library-*` modes only. |
| **new_music.py** | Production new-music intake (the `new-music` mode). |
| **library_nightly.py** | The library job (the `library-backfill` mode): runs the six steps below in order. |
| **library_scan.py**, **library_trash.py**, **library_identify.py**, **library_backfill.py**, **library_sync.py**, **library_duplicates.py** | The library job's steps. |
| **library_reconcile.py** | Production library reconcile (the `library-reconcile` mode). |
| **second_opinion.py** | Breaks a tie when a file's fingerprint and its own tags disagree (AcoustID's other recordings, Spotify's track length). |
| **music_safety.py** | The safety layer every music-writing flow uses: modes, the audio check, run records. |
| **music_rollback.py** | Local CLI: undo a music-tagging run from its run record (dry run unless `--apply`). |
| **library_restore.py**, **restore_bpm.py**, **restore_styling.py**, **fingerprint_check.py**, **link_by_tags.py**, **relink.py** | Local repair CLIs for the library, each a dry run unless `--apply`; see [PIPELINE.md](docs/PIPELINE.md#run-records). |
| **reconcile_admin.py** | Local CLI: review the library reconcile's differences by kind, print the approvals (set on the WCS review page), start a full pass and run it by hand. |
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

**Prerequisites:** Python ≥ 3.14, [uv](https://docs.astral.sh/uv/), and the
[Doppler CLI](https://docs.doppler.com/docs/install-cli). Secrets come from
this repo's own Doppler `dev` config — nothing reads a `.env` file, and local
commands use `dev`. A command that must act on production would say
`--config prd` explicitly (ecosystem-standards CFG-003); none here does.

```bash
brew install gnupg dopplerhq/cli/doppler   # once per machine
doppler login                              # once per machine

git clone git@github.com:mini-app-polis/deejay-cog.git
cd deejay-cog
doppler setup                              # reads doppler.yaml: deejay-cog / dev
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

**reconcile_admin** (working sessions over the library reconcile — see
[PIPELINE.md](docs/PIPELINE.md#working-the-reconcile-from-a-session)):

```bash
doppler run --config prd -- uv run python -m deejay_cog.reconcile_admin review
doppler run --config prd -- uv run python -m deejay_cog.reconcile_admin approvals
doppler run --config prd -- uv run python -m deejay_cog.reconcile_admin full --apply
doppler run --config prd -- uv run python -m deejay_cog.reconcile_admin baseline --apply
doppler run --config prd -- uv run python -m deejay_cog.reconcile_admin run --mode dry-run --chunk 260
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
