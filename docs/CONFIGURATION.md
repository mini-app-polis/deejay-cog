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

Every flow that writes to the music folders reads a mode of its own, at the
start of each run (so a change in Doppler applies to the next run):
`off` (the default; unset or any unknown value is `off`, and an unknown one
is logged), `dry-run` (read, identify and report, write nothing but the
would-be run record) or `write`.

The **new-music** worker mode reads:

- `NEW_MUSIC_MODE` — `off` | `dry-run` | `write`; default `off`.
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
problem. The fingerprint function reads the same settings for the
new-music files it is sent, so they must reach both functions. watcher-cog
triggers the mode when files land in the input folder; it can also be run
on demand through `POST /v1/deejay/runs` once the API accepts `new-music`.

---

## Library backfill

The **library-backfill** worker mode reads:

| Setting | Default | What it does |
|---------|---------|--------------|
| `LIBRARY_BACKFILL_MODE` | `off` | `off` \| `dry-run` \| `write`; unset or unknown is `off`. |
| `LIBRARY_ROOT_FOLDER_ID` | none | The music library's root folder, listed recursively. |
| `MUSIC_TAGGING_STATE_FOLDER_ID` | none | Holds the manifest, the cursor and the run records (shared with new-music). |
| `LIBRARY_BACKFILL_CHUNK_SIZE` | `10` | Files per run; a whole number of at least 1. |
| `LIBRARY_BACKFILL_SKIP_FILE_IDS` | empty | Comma-separated Drive file ids to pass over, named in the report: how a file whose audio check fails is passed. |

With either folder unset, the two the same folder, or a chunk size that is
not a whole number of at least 1, the run does nothing and its report
(WARN) names the problem. The settings are read at the start of each run,
so a change in Doppler applies to the next night. The nightly trigger is
not configured here: what sends the `library-backfill` job each night is
wired outside this repo.

To start the backfill over, move `library-backfill-manifest.json` in the
state folder to the Drive trash; see
[PIPELINE.md](PIPELINE.md#library-backfill).

## Library scan

The same nightly job first runs the **library-scan** step, which keeps the
library registry in step with what is in the library. It reads
`LIBRARY_ROOT_FOLDER_ID`, `MUSIC_TAGGING_STATE_FOLDER_ID` and
`LIBRARY_BACKFILL_CHUNK_SIZE` as the backfill does, and:

| Setting | Default | What it does |
|---------|---------|--------------|
| `LIBRARY_SCAN_MODE` | `off` | `off` \| `dry-run` \| `write`; unset or unknown is `off`. Off, the step reads nothing and sends no report. |
| `LIBRARY_SCAN_LISTING_SECONDS` | `90` | The most seconds one run spends listing the library, so the backfill's chunk still fits in the run; a whole number of at least 1. |

A problem with a setting is a WARN in the scan's report; the backfill runs
regardless. See [PIPELINE.md](PIPELINE.md#library-scan).

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
- For files the new-music mode sends: the new-music settings above. It
  works in the stricter of the sender's mode and its own `NEW_MUSIC_MODE`.

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
