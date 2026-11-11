# Configuration reference

How **deejay-cog**'s configuration reaches it, and what each part does when
a value is missing. Every variable, what it does, its default and whether it
is required, is listed in [`.env.example`](../.env.example).

**In production** the Lambda gets its configuration from two places, both
declared in `mini-app-polis/infra`:

- **Secrets and tunable settings** come from SSM Parameter Store, synced
  from Doppler (`mini_app_polis.load_secrets`). They are loaded when the
  package is first imported and **read again at the start of every
  invocation**, so a value changed in Doppler applies to the next run once
  the sync has run — no deploy or cold start. Required ones fail the run
  when missing; optional ones are left unset, and one deleted from Doppler
  is unset at the next run. If SSM cannot be reached on a refresh, the
  values already loaded are kept. Only `SENTRY_DSN_COGS`, read once at import,
  still needs a cold start.
- **Plain settings** are set on the function's environment.

**Locally**, everything comes from this repo's own Doppler `dev` config: run
`doppler setup` once per clone (it reads `doppler.yaml`), then
`doppler run -- …`. No `.env` file is read, and local commands use `dev`: a
command that must act on production would say `--config prd` explicitly
(ecosystem-standards CFG-003), and none in this repository does.
`.env.example` lists the names; `uv run check-doppler-keys` (from
miniapppolis-common-utils) checks that `dev` holds every one.

---

## Environment and API

Every API call resolves the base URL the same way
(`mini_app_polis.environment.api_base_url()`): ingest, live plays, the
Spotify snapshot and the run report. With no URL resolved:

- **process-new-files** counts each set as a failed ingest. The CSV is not
  archived, and the sheet uploaded for it is removed.
- **ingest-live-history** skips.
- **Run reports** are logged, not sent.

---

## Google

The folder IDs default to the production folders in `config.py`; there is
no separate development Drive. Outside production, `process_new_files.py`
refuses to run unless `CSV_SOURCE_FOLDER_ID` is set explicitly, so a local
run cannot archive the production drop zone into the development API.

The **retag-music** worker mode reads two of them:

- `MUSIC_UPLOAD_SOURCE_FOLDER_ID` — the Drive folder whose audio files are
  tagged.
- `MAX_UPLOADS_PER_RUN` — files handled per run (default 200; 0 for no cap).

It needs neither `ffmpeg` nor `fpcalc`. A file with no usable title or
artist is sent to the fingerprint function, one message per file, on the
queue in `DEEJAY_FINGERPRINT_QUEUE_URL` (set on the function by
`mini-app-polis/infra`). Unset, as in a local run, nothing is sent and the
run report names those files instead.

---

## Music tagging safety and new-music

Every flow that writes to the music folders reads a mode of its own from
the API's feature flags (`GET /v1/flags`), at the start of each run, so a
change made in the API applies to the next run with no settings sync. The
modes are not configuration: no environment variable sets one.

Each step has one flag, `flags.deejay_cog.STEP`, whose value is the mode:
`off`, `dry-run` (read, identify and report, write nothing but the would-be
run record) or `write`. It fails closed: an API the run cannot read, a flag
the API does not have, a flag switched off, and an empty or unknown value
(which is logged) are all `off` for that run.

| Flag | Step | Notes |
|------|------|-------|
| `flags.deejay_cog.new_music` | new-music | The fingerprint function uses the stricter of it and the message's mode. |
| `flags.deejay_cog.library_scan` | library-scan | Off, the step reads nothing and sends no report. |
| `flags.deejay_cog.library_trash` | library-trash | `write` moves the files the owner sent to the trash on the WCS duplicates page to Drive's trash (never deleted) and marks them not present. |
| `flags.deejay_cog.library_identify` | library-identify | Off, the step reads nothing and sends no report. The fingerprint function reads it too, and uses the stricter of it and the sender's mode. |
| `flags.deejay_cog.library_backfill` | library-backfill | |
| `flags.deejay_cog.library_sync` | library-sync | Off, the step reads nothing and sends no report. It was kept off until the library was reconciled (ADR-0008); off again, catalog edits stop reaching the files and edits made in VirtualDJ stop going up. |
| `flags.deejay_cog.library_duplicates` | library-duplicates | Groups files that look like copies of one song. `dry-run` lists the groups in the run report; `write` also sends them to the API for the duplicates page. Never writes a music file. |
| `flags.deejay_cog.library_reconcile` | library-reconcile | Off, the step reads nothing and sends no report. `write` applies only what the owner approved on the review page (read from the API), and writes nothing until any approvals are set. Every run sends its differences to the review. |

A flag is changed through the API's protected `PATCH /v1/flags/{name}`
(body `{"enabled": true, "value": "dry-run"}`). Numeric settings (chunk
sizes, time budgets) and folder ids stay as configuration, below.

The **new-music** worker mode reads its mode from
`flags.deejay_cog.new_music`, and:

- `NEW_MUSIC_INPUT_FOLDER_ID` — where new music is dropped.
- `NEW_MUSIC_DESTINATION_FOLDER_ID` — where tagged, renamed copies go.
- `NEW_MUSIC_REVIEW_FOLDER_ID` — where files not identified with
  confidence (and name collisions) go, unchanged.
- `NEW_MUSIC_ORIGINALS_FOLDER_ID` — processed-originals: each identified
  file's original, unchanged.
- `MUSIC_TAGGING_STATE_FOLDER_ID` — where every music-writing flow saves
  its run records, and where `music_rollback` reads them.
- `NEW_MUSIC_MAX_FILES_PER_RUN` — files handled per run (default 25; 0 for
  no cap).

None of the folders has a default. With any of them unset, or two of them
the same folder, the run does nothing and its report (WARN) names the
problem. The mode sends no file to the fingerprint function (ADR-0007,
amended 2026-11-07); a file the catalog does not know gets a catalog entry
from its own tags. watcher-cog
triggers the mode when files land in the input folder; it can also be run
on demand through `POST /v1/deejay/runs` once the API accepts `new-music`.

---

## Library backfill

The **library-backfill** worker mode reads:

| Setting | Default | What it does |
|---------|---------|--------------|
| `LIBRARY_ROOT_FOLDER_ID` | none | The music library's root folder, listed recursively. |
| `MUSIC_TAGGING_STATE_FOLDER_ID` | none | Holds the manifest, the cursor and the run records (shared with new-music). |
| `LIBRARY_BACKFILL_CHUNK_SIZE` | `10` | Files per run at most; a whole number of at least 1. A written file takes about 20 s (download, upload, audio check, record and cursor saved). |
| `LIBRARY_BACKFILL_RESERVE_SECONDS` | `120` | Seconds left for the library job's steps after the backfill: it starts no file with less than this left before the run deadline, and the next run carries on from the cursor. |
| `LIBRARY_BACKFILL_SKIP_FILE_IDS` | empty | Comma-separated Drive file ids to pass over, named in the report: how a file whose audio check fails is passed. |

With either folder unset, the two the same folder, or a chunk size that is
not a whole number of at least 1 (or a reserve that is not a whole number
of seconds), the run does nothing and its report
(WARN) names the problem. The settings are read at the start of each run,
so a change in Doppler applies to the next run. The schedule is
not configured here: what sends the `library-backfill` job on its schedule is
wired outside this repo.

To start the backfill over, move `library-backfill-manifest.json` in the
state folder to the Drive trash; see
[PIPELINE.md](PIPELINE.md#library-backfill).

## Library scan

The same library job first runs the **library-scan** step, which keeps the
library registry in step with what is in the library. It reads
`LIBRARY_ROOT_FOLDER_ID`, `MUSIC_TAGGING_STATE_FOLDER_ID` and
`LIBRARY_BACKFILL_CHUNK_SIZE` as the backfill does, and:

| Setting | Default | What it does |
|---------|---------|--------------|
| `LIBRARY_SCAN_LISTING_SECONDS` | `90` | The most seconds one run spends listing the libraries, so the backfill's chunk still fits in the run; a whole number of at least 1. |
| `NEW_MUSIC_DESTINATION_FOLDER_ID` | none | New Music, the folder new-music intake writes to (above). When set, it is listed as part of the main library, under `LIBRARY_ROOT_FOLDER_ID`'s root. |
| `FX_LIBRARY_ROOT_FOLDER_ID` | none | FX Music, listed recursively as a library root of its own. Unset, there is no FX library. |

There are two libraries
([ADR-0009](decisions/ADR-0009-library-folders.md)): **the main library**,
`LIBRARY_ROOT_FOLDER_ID` and New Music under one root, and **FX Music**.
With only `LIBRARY_ROOT_FOLDER_ID` set, the scan lists that one folder as
before. Any two of `LIBRARY_ROOT_FOLDER_ID`, `NEW_MUSIC_DESTINATION_FOLDER_ID`,
`FX_LIBRARY_ROOT_FOLDER_ID` and `MUSIC_TAGGING_STATE_FOLDER_ID` that name the
same folder are a problem, for the backfill as well as the scan.

A problem with a setting is a WARN in the scan's report; the backfill runs
regardless. See [PIPELINE.md](PIPELINE.md#library-scan).

## Library identify

Between the scan and the backfill's chunk the library job runs the
**library-identify** step: unidentified library files are identified (by
their tags checked against the fingerprint function, else by the
fingerprint alone) and linked to the catalog;
only the link is written to the file. It reads `LIBRARY_ROOT_FOLDER_ID` and
`MUSIC_TAGGING_STATE_FOLDER_ID` as the backfill does, and
`FX_LIBRARY_ROOT_FOLDER_ID` (above): FX Music's files are linked only to fx
entries, made from their own tags, and never fingerprinted. It sends to
`DEEJAY_FINGERPRINT_QUEUE_URL`, and:

| Setting | Default | What it does |
|---------|---------|--------------|
| `LIBRARY_IDENTIFY_CHUNK_SIZE` | `20` | Unidentified files tried per run; a whole number of at least 1. Each is downloaded to read its tags. |

The fingerprint function's confidence threshold is `RETAG_MIN_CONFIDENCE`
(see above). See [PIPELINE.md](PIPELINE.md#library-identify).

## Library reconcile

The **library-reconcile** worker mode, scheduled on its own rather than as
a step of the library job, compares linked library files with the
catalog; a dry run reads only, a write run applies what the owner approved. It reads `LIBRARY_ROOT_FOLDER_ID` and
`MUSIC_TAGGING_STATE_FOLDER_ID` as the backfill does, and:

| Setting | Default | What it does |
|---------|---------|--------------|
| `LIBRARY_RECONCILE_CHUNK_SIZE` | `50` | Changed files compared per run (the rest wait for the next run), and at most as many files with an approved difference settled first; a whole number of at least 1. Each file's tags are read from a ranged download of its start and end (the whole file only when its tag runs past that, or for an approved write), so the chunk is bounded by the worker's timeout. |

See [PIPELINE.md](PIPELINE.md#library-reconcile).

## Library sync

The fifth step of the library job, **library-sync**, writes catalog edits
and display-rule changes to the files they affect. It reads
`LIBRARY_ROOT_FOLDER_ID` and `MUSIC_TAGGING_STATE_FOLDER_ID` as the
backfill does, and:

| Setting | Default | What it does |
|---------|---------|--------------|
| `LIBRARY_SYNC_CHUNK_SIZE` | `50` | Files the API lists as needing a sync taken per run; a whole number of at least 1. Only a file with a field that moved is downloaded and written; the rest are recorded as synced without reading Drive. |
| `LIBRARY_SYNC_EDIT_FLOOD` | `1000` | A backstop: more files changed outside the pipeline than this in one run is a bulk change the value check did not catch: nothing goes up to the catalog and nothing is written; the files keep what they hold, and the run reports it. Below it, a changed field holding a value the pipeline replaced in that file is put back, and every other change goes up (see [PIPELINE.md](PIPELINE.md#library-sync)). A whole number of at least 1. |

See [PIPELINE.md](PIPELINE.md#library-sync).

## Library trash and duplicates

Two more steps of the library job handle duplicate files. Both read
`LIBRARY_ROOT_FOLDER_ID` and `MUSIC_TAGGING_STATE_FOLDER_ID` as the
backfill does, and have no settings of their own: their modes are the
`flags.deejay_cog.library_trash` and `flags.deejay_cog.library_duplicates`
flags (above).

See [PIPELINE.md](PIPELINE.md#library-trash) and
[PIPELINE.md](PIPELINE.md#library-duplicates).

---

## The fingerprint function

`deejay_cog.fingerprint.lambda_handler` is a second Lambda function, behind
its own FIFO queue, run one file at a time. It reads this repo's Doppler
project like the worker, and:

- `ACOUSTID_API_KEY` — required; without it every file fails and its
  message comes back.
- `RETAG_MIN_CONFIDENCE` — the AcoustID score a match must reach before
  the file is written (default 0.90). Below it the file is not changed and
  is named in the run report.
- `DEEJAY_COG_API_KEY` and `GOOGLE_CREDENTIALS_JSON`, as the worker.
- `SPOTIPY_CLIENT_ID`, `SPOTIPY_CLIENT_SECRET` and `SPOTIPY_REFRESH_TOKEN`,
  as the worker — optional: Spotify's search is the second opinion's
  length check when a library file's fingerprint and tags disagree. Without
  them the second opinion rests on AcoustID alone, and a file it does not
  settle is left for the owner.
- For a new-music message: the new-music settings above, in the stricter
  of the message's mode and `flags.deejay_cog.new_music`. The new-music mode
  sends none at present (ADR-0007, amended 2026-11-07).

`fpcalc` comes from the function's Lambda layer, found on `PATH` or at
`/opt/bin/fpcalc`; with neither, every file fails. AcoustID allows three
requests a second: a lookup refused as over that limit is retried twice,
then the file's message comes back to the queue.

---

## Spotify

All three credentials must be set, or every Spotify step is skipped (logged,
not counted as a failure).

A refresh token Spotify refuses (`invalid_grant`, usually because it has
passed its six-month expiry) is reported once per run as
`spotify_token_expired`, with the re-auth link, and the run skips the rest of
its Spotify steps; see [SPOTIFY_SETUP.md](SPOTIFY_SETUP.md#when-the-token-has-expired).

---

## Local-only flows

The columns and column order of summary sheets (`ALLOWED_HEADERS`,
`desiredOrder`) are constants in `config.py`, not environment variables.
