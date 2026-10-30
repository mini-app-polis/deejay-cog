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

### retag-music system dependencies

retag-music needs two binaries that are not Python packages: `ffmpeg` (audio
decoding) and `fpcalc` (fingerprinting, from chromaprint).

```bash
sudo apt-get install -y ffmpeg libchromaprint-tools   # Ubuntu/Debian
brew install ffmpeg chromaprint                       # macOS
```

No other flow needs them, and the Lambda runtime does not have them.
