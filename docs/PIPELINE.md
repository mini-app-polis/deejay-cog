# Drive ingestion pipeline

How **deejay-cog** turns files dropped in Google Drive into sheets, API
records and Spotify playlists. The decision behind the current shape is
[ADR-006](decisions/ADR-006-lambda-behind-sqs.md).

---

## Trigger path

```
watcher-cog ──POST /v1/deejay/runs {mode}──▶ api-kaianolevine-com
                                                  │ enqueues one message
                                                  ▼
                                         SQS queue `deejay-jobs`
                                                  │ event source mapping
                                                  ▼
                              Lambda `deejay_cog.worker.lambda_handler`
                                                  │ one flow run per message
                                                  ▼
                               process-new-files │ ingest-live-history
```

- **watcher-cog** notices new files in the CSV drop zone or the VirtualDJ
  history folder and asks the API for a run of the matching `mode`.
- **api-kaianolevine-com** validates the mode and enqueues
  `{"type": "deejay.run", "version": 1, "payload": {"mode": "<mode>"}}`. It is
  the queue's only producer.
- **The Lambda** runs one flow per message, under the message id as its run
  id, so a run can be traced from watcher's log to its report.
- Nothing runs between triggers. The queue, function, dead-letter queue and
  alarms are declared in `mini-app-polis/infra` (`cogs.tf`): reserved
  concurrency 1, so two sweeps never race over one folder; a 900 s timeout;
  five receives before a message is dead-lettered.

---

## Flow tiers

### Production (run by the Lambda worker)

| Mode | Flow | Module | What it does |
|------|------|--------|--------------|
| `process-new-files` | **process-new-csv-files** | `process_new_files.py` | Sweeps the CSV drop zone: normalise, upload to Sheets, ingest to the API, archive, sync Spotify. |
| `ingest-live-history` | **ingest-live-history** | `ingest_live_history.py` | Sends the plays in the **most recent** VirtualDJ `.m3u` to `POST /v1/live-plays`. Not the whole history. |

### Local-only (never run by the worker)

| Flow | Module | Role |
|------|--------|------|
| **update-dj-set-collection** | `update_deejay_set_collection.py` | Rebuilds the master collection spreadsheet and JSON snapshot. |
| **generate-summaries** | `generate_summaries.py` | Builds or refreshes per-year summary sheets. |
| **retag-music** | `retag_music.py` | Work in progress: AcoustID/MusicBrainz tagging. Needs `ffmpeg` and `fpcalc`. |

The first two are validation utilities for the PostgreSQL cutover and will be
retired once PostgreSQL is confirmed as the source of truth.

---

## process-new-files, one sweep

A sweep handles whatever is in the drop zone (`CSV_SOURCE_FOLDER_ID`), so a
trigger that arrives after its files were already handled finds nothing.

1. **Clear status prefixes.** `FAILED_`, `possible_duplicate_` and `Copy of `
   are stripped, unless the bare name is already taken, so earlier failures
   are retried.
2. **Per file:**
   - No four-digit year prefix (`2025-…` / `2025_…`) → left in place.
   - Not a CSV → moved into its year folder.
   - CSV whose name already exists in its year folder → renamed
     `possible_duplicate_…`, not imported.
   - Otherwise:
     1. Download and normalise (BOM, `sep=` hint and blank lines dropped).
     2. Upload as a Google Sheet in the year folder. That year's summary sheet
        is deleted so it is rebuilt.
     3. **Ingest** to `POST /v1/ingest`, using the date and venue from the
        filename (`YYYY-MM-DD Venue.csv`).
     4. **Archive** the CSV into the year's `Archive` folder.
     5. **Spotify:** per-set playlist, radio playlist.
3. **Spotify playlist snapshot** pushed to `POST /v1/spotify/playlists`, every
   sweep that has Spotify credentials.
4. **One run report.**

**Ingest before archive.** A CSV leaves the drop zone only once the API has
its set. If the ingest fails (an API error, no base URL, no client), the
sheet just uploaded is deleted and the CSV renamed `FAILED_`; the next run
strips the prefix and imports it from scratch. A filename without a venue,
or a sheet with no tracks, has nothing to send and is archived.

## ingest-live-history

Reads the newest `.m3u` in the VirtualDJ history folder, timestamps each
play in `TIMEZONE` (plays after midnight roll to the next day), and posts
them. Each run re-sends that file's plays; the API deduplicates.

---

## Failure handling

| What failed | What happens |
|-------------|--------------|
| One file (upload, ingest, move) | Contained. Counted, the file is renamed where that helps a retry, the sweep carries on, and the run reports WARN. The message is deleted. |
| The run itself (e.g. the drop zone or the history folder cannot be listed) | The flow raises. The worker reports ERROR and returns the message in `batchItemFailures`, so SQS redelivers it. |
| The deadline | 30 s before the function's timeout the run is stopped (`_deadline.RunOutOfTime`). A sheet uploaded but not yet ingested is deleted, the worker reports ERROR, and the message is redelivered. |
| An unrecognised message (bad JSON, version, type or mode) | Reported once, on its first receive, then returned until it reaches the dead-letter queue, where its own alarm fires. |

Both flows are safe to run again: archived CSVs are no longer in the drop
zone, and re-sent plays are deduplicated by the API.

---

## Run reports

Each production run sends exactly one report to `POST /v1/notify` (Discord),
built with `RunReport` from common-python-utils and version-stamped by
`_pipeline_eval`:

- **SUCCESS** is sent only when the run had something in front of it (files
  in the drop zone, an `.m3u` to read). An idle sweep is logged, not sent.
- **WARN** when anything was counted as an issue: a failed set, a rejected
  ingest, a Spotify failure, a move that did not happen.
- **ERROR** from the worker, when a run raised or a message was unprocessable.

Reports are sent only where an API base URL resolves for the environment
(see [CONFIGURATION.md](CONFIGURATION.md)). Local-only flows never send.

---

## File name prefixes

- **`FAILED_`** — the set failed (upload, or ingest rolled back). Cleared and
  retried by the next sweep.
- **`possible_duplicate_`** — a sheet of that name is already in the year
  folder, or the folder could not be read. Cleared and checked again by the
  next sweep.
- **Spotify** runs only when `SPOTIPY_CLIENT_ID`, `SPOTIPY_CLIENT_SECRET` and
  `SPOTIPY_REFRESH_TOKEN` are all set; otherwise it is skipped, not failed.
