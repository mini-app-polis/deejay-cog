# Drive ingestion pipeline

How **deejay-cog** turns files dropped in Google Drive into sheets, API
records and Spotify playlists. The decision behind the current shape is
[ADR-0006](decisions/ADR-0006-lambda-behind-sqs.md).

---

## Trigger path

```
watcher-cog ──POST /v1/deejay/runs {mode}──▶ api-kaianolevine-com
                                                  │ enqueues one message
                                                  ▼
                                    this cog's SQS FIFO queue
                                                  │ event source mapping
                                                  ▼
                              Lambda `deejay_cog.worker.lambda_handler`
                                                  │ one flow run per message
                                                  ▼
                                the flow for the message's mode
```

- **watcher-cog** notices new files in a watched Drive folder (the CSV drop
  zone, the VirtualDJ history folder, the music upload and new-music input
  folders) and asks the API for a run of the matching `mode`. It also runs
  the library jobs on their schedules (below).
- **api-kaianolevine-com** validates the mode and enqueues
  `{"type": "deejay.run", "version": 1, "payload": {"mode": "<mode>"}}`. It is
  the queue's only producer. A message queued from a request also carries
  `request` (`id`, `requested_at`, and `caller` when the API knows it).
- **The Lambda** runs one flow per message, under the message id as its run
  id, so a run can be traced from watcher's log to its report.
- **library-backfill** and **library-reconcile** are the modes meant to run
  on a schedule ([ADR-0003](decisions/ADR-0003-on-demand-no-cadence.md),
  amended 2026-11-03), each on its own, so the reconcile has a whole run's
  time to write in: `library-backfill` (the
  [library job](#the-library-jobs)) and `library-reconcile`, each several
  times a day.
  watcher-cog's scheduled jobs send them; the schedules are declared in
  `mini-app-polis/infra`. Either can also be run on demand
  ([below](#running-a-library-job-on-demand)).
- Nothing else runs between triggers. The queue, function, dead-letter queue and
  alarms are declared in `mini-app-polis/infra`: a FIFO queue
  with reserved concurrency 1, so two sweeps never race over one folder; a
  600 s timeout; three receives before a message is dead-lettered.

---

## Flow tiers

### Production (run by the Lambda worker)

| Mode | Flow | Module | What it does |
|------|------|--------|--------------|
| `process-new-files` | **process-new-csv-files** | `process_new_files.py` | Sweeps the CSV drop zone: normalise, upload to Sheets, ingest to the API, archive, sync Spotify. |
| `ingest-live-history` | **ingest-live-history** | `ingest_live_history.py` | Sends the plays in the **most recent** VirtualDJ `.m3u` to `POST /v1/live-plays`. Not the whole history. |
| `retag-music` | **retag-music** | `retag_music_from_catalog.py` | Looks each audio file in the music upload folder up in the catalog and writes the catalog's tags over the same Drive file. |
| `new-music` | **new-music** | `new_music.py` | Copies each file in the new-music input folder, unchanged and named `{title}_{artist}` from its own tags, into New Music and keeps the original in processed-originals; the library steps do the rest. Off by default. |
| `library-backfill` | **the library job** | `library_nightly.py` | Six steps over the music library, each with its own flag: scan, trash, identify, backfill, sync, duplicates ([below](#the-library-jobs)). Never renames, moves or deletes a library file (the trash step only moves the copies the owner chose to Drive's trash). |
| `library-reconcile` | **library-reconcile** | `library_reconcile.py` | Settles the differences the owner approved, then compares the next chunk of linked library files with the catalog. |

### Local-only (never run by the worker)

| Flow | Module | Role |
|------|--------|------|
| **update-dj-set-collection** | `update_deejay_set_collection.py` | Rebuilds the master collection spreadsheet and JSON snapshot. |
| **generate-summaries** | `generate_summaries.py` | Builds or refreshes per-year summary sheets. |

Both are validation utilities for the PostgreSQL cutover and will be
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
     5. **Spotify:** the set's own playlist; if that playlist was newly
        created, its tracks also go on the radio playlist.
3. **Repair pass** over recent sets (below).
4. **Spotify playlist snapshot** pushed to `POST /v1/spotify/playlists`, every
   sweep that has Spotify credentials.
5. **One run report.**

**Ingest before archive.** A CSV leaves the drop zone only once the API has
its set. If the ingest fails (an API error, no base URL, no client), the
sheet just uploaded is deleted and the CSV renamed `FAILED_`; the next run
strips the prefix and imports it from scratch. A filename without a venue,
or a sheet with no tracks, has nothing to send and is archived.

**The radio follows the set playlist.** A set's tracks are appended to the
radio playlist only when its own playlist is created — never when an existing
one is refilled — so a set reaches the radio once. If the radio update fails
after the set playlist was created, the run warns and the radio update is not
retried.

### Repair pass

One CSV fans out into a sheet, an API set and a Spotify playlist, and the
archive move is one flag for all of them. So every sweep ends by checking
recent sets against the systems themselves (`repair.py`), with nothing
stored:

| Job | Done when | Repair |
|-----|-----------|--------|
| API set | `GET /v1/sets` lists a set whose `source_file` is the sheet's name | Ingest from the sheet |
| Set playlist | The Spotify account has a playlist of that name | Build it from the sheet; as a new playlist, it feeds the radio |

- **Window:** sheets in the year folders dated within `REPAIR_LOOKBACK_DAYS`
  (183, about six months).
- **Cap:** at most `REPAIR_MAX_PER_RUN` jobs (3) per sweep, newest set first.
  The rest are counted as `repairs_pending` and done by later sweeps.
- **Skipped:** sets imported by this same sweep — their jobs just ran.
- **Reporting:** each repair is an outcome in the run report; a failed repair
  or a failed check is a WARN naming the set, retried next sweep. Neither
  fails the run.
- Playlists are only checked when Spotify credentials are set and the client
  builds.

## ingest-live-history

Reads the newest `.m3u` in the VirtualDJ history folder, timestamps each
play in `TIMEZONE` (plays after midnight roll to the next day), and posts
them. Each run re-sends that file's plays; the API deduplicates.

## retag-music

Lists the audio files in `MUSIC_UPLOAD_SOURCE_FOLDER_ID` and runs the catalog
lookup and tag writer (`music_tags.py`, ADR-0007) on each. A matched file has
its tags written in place; its name, folder and file id do not change, and a
file whose tags already match is not written again.

- **Cap:** at most `MAX_UPLOADS_PER_RUN` files (200; 0 for no cap) a run. The
  rest wait for the next run.
- **Deadline:** checked between files. Past it the run stops with
  `RunOutOfTime` and reports `Stopped early`; the redelivery carries on.
- **Left untouched:** a file the catalog has no song for (`not_in_catalog`). It
  is counted and named in the report.
- **Audio check:** a write whose audio would not stay byte-identical is not
  made, and stops the run (`music_safety.check_audio_unchanged`).
- **Fingerprinting:** a file with no usable title or artist is sent to the
  fingerprint function, one message per file on `DEEJAY_FINGERPRINT_QUEUE_URL`
  (see [CONFIGURATION.md](CONFIGURATION.md#the-fingerprint-function)). With no
  queue configured, as in a local run, it is left untouched and named in the
  report as `needs_fingerprinting` instead.
- **Report:** counts for `scanned`, `matched_in_catalog`, `not_in_catalog`,
  `tagged`, `needing_fingerprinting`, `handed_to_fingerprinting` and `failed`,
  and the untouched files by name. A failed file is an issue, so the run is WARN. One failing file does
  not end the sweep.

## new-music

New music intake ([ADR-0007](decisions/ADR-0007-music-tagging-redesign.md#amended-2026-11-03-owner-two-flows-and-the-safety-rules),
amended [2026-11-11](decisions/ADR-0007-music-tagging-redesign.md#amended-2026-11-11-owner-incoming-music-takes-the-librarys-path)).
It does nothing unless the `flags.deejay_cog.new_music` flag is `dry-run` or `write`, and nothing
unless all five of its folders are set and different (see
[CONFIGURATION.md](CONFIGURATION.md#music-tagging-safety-and-new-music)).

Lists the audio files in `NEW_MUSIC_INPUT_FOLDER_ID`, at most
`NEW_MUSIC_MAX_FILES_PER_RUN` (25; 0 for no cap) a run (see **A batch past
the cap**, below). Intake does not look
a file up, make catalog entries or write tags: it puts the file into New
Music as it came, and the library's steps take it from there (scan,
identify with its fingerprint check, duplicates, reconcile and sync; see
[ADDING-AND-REMOVING-SONGS.md](ADDING-AND-REMOVING-SONGS.md)). Per file:

1. **Not an MP3** → moved, unchanged, to needs-review (the main library
   takes MP3s only).
2. **A byte-identical copy of a library file** → moved, unchanged and
   unrenamed, to needs-review, reported as `needs_review` with the reason
   `exact copy of a library file` and the library file's path. Its md5 (as
   Drive reports it) is checked against the library registry, read once a
   run: a present file not marked for the trash, in the main library (its
   main folder or New Music; FX Music's files never match) with the same
   md5. A registry row with no md5 never matches, and the copy an earlier
   run of this file made in New Music is not counted. If the registry cannot
   be read, the file goes to needs-review with the reason, as the name check
   does. Nothing is trashed or overwritten.
3. **No usable title or artist** → moved, unchanged, to needs-review.
4. **A title tag that looks like an old file name** → moved, unchanged and
   unrenamed, to needs-review, the reason naming the check that found it
   (`new_music.file_name_title`): underscores; a number from 70 to 180 at
   its end (a BPM, as in `… Kahlo 113`); words run together (three
   lowercase letters then a capital or two digits: `BoxingUnderwater`,
   `Kahlo113`; `McDonald` or `M4SONIC` are not); the main artist again
   outside the title's brackets (a song named after its artist aside); or
   three or more brackets of one word each (`Burning (Bridges) (Sistek)
   (Remix) (Ella)`). Named by such a tag, the file would carry the old name
   into the library; a false alarm costs only a review. A dry run moves
   nothing; the record lists the move it would make.
5. **Otherwise** → taken in: copied, tags and audio unchanged, into New
   Music, named from its own tags.

A release label in the file's own title (Clean, Explicit, Radio Edit, an
official No Intro Edit) stays in its title, and so in its name. An owner
edit (a version in `< >`, below) is taken in the same way; the name leaves
out `<` and `>`, as it does every character a file name can't hold
(`Baby What You Want Me To Do Intro Cut_Etta James.mp3`).

**Taking a file in** (write mode):

1. The copy's bytes are the original's, so its tags and audio are unchanged;
   the audio check before and after the upload still holds (ID3v2 at the
   start, ID3v1, APEv2 and an appended ID3v2 at the end are stripped before
   comparing).
2. The file is named `{title}_{artist}.<ext>` with the **main artist
   only**: the artist tag up to a feat./ft./featuring credit, main artists
   split on commas (ADR-0008 decision 9). When a file of that name is
   already in the destination, or anywhere in the library (the API's
   library registry, read once a run), the featured artists are added:
   `{title}_{artist} feat. {featured}.<ext>`. If that is taken too, or there
   are no featured artists to add, nothing is overwritten: the original
   goes to needs-review and the run reports a `name_collision`. If the
   registry cannot be read, the name cannot be checked, so the original
   goes to needs-review as well.
3. The bytes are uploaded as a **new** file under that name into
   `NEW_MUSIC_DESTINATION_FOLDER_ID`, marked with the original's file id.
4. The copy is downloaded again and its audio checked once more.
5. The original, unchanged, is moved to `NEW_MUSIC_ORIGINALS_FOLDER_ID` —
   only if it is still in the input folder and nowhere else.

A copy left by a run that stopped between steps 3 and 5 is found by the
original's id and checked, rather than uploaded twice. Nothing is ever
deleted or overwritten; names are sanitized (no `/ \ : * ? " < > |` or
control characters).

**A batch past the cap** drains run after run. When files are left in the
input folder past `NEW_MUSIC_MAX_FILES_PER_RUN` (`left_past_cap`), a
writing run that moved at least one file asks for the next new-music run
before it ends (`POST /v1/deejay/runs` with `{"mode": "new-music"}`; scope
`deejay.runs.create`), and that run takes the next files, and so on until
the folder is empty, with no gap between them. The request names no file,
so the API claims none: the watcher's claims (`dispatch_claims`, scope
`deejay:new-music`) are left as they are, and no file waits behind the cap
long enough for its claim to be renewed or given up on. The API's queue
collapses an identical request made within its window
(`job_queue.DEDUP_WINDOW_SECONDS`, 60 s) into the first, and a run started
by this cog's own follow-up would ask for the same thing again; such a run
sends its follow-up no sooner than 65 s after the request that started it,
waiting the rest when its deadline allows. The report names the follow-up
(`follow_up_queued`); one the API refuses or still collapses is a WARN
(`follow_up_not_queued`), and the files wait for the watcher's next sweep.
A dry run, or a run that moved no file (every file failed), asks for
nothing; a run stopped at its deadline is delivered again instead.

**Audio check failure** stops the run at once: nothing is written for that
file (after an upload, the bad copy goes to the Drive trash and the original
stays in the input folder), the rest of the folder waits, and an ERROR
finding names the file. The message is not retried.

**Dry run** reads and computes everything and writes nothing to
the music folders. The report names what would happen per file, and the
would-be run record is saved in the state folder as
`new-music-<run id>.dry-run.json`, with the tags, new names and
destinations.

**Report:** counts for `scanned`, `identified` (taken in),
`needs_review`, `collisions`, `failed`,
`no_longer_in_input`, `left_past_cap` (and `not_written_dry_run` in a dry run), with the files
by name. Off, the report says only that the flow is off; a missing setting
is a WARN naming it.

## The library jobs

Two worker modes keep the music library and the catalog in step
([ADR-0008](decisions/ADR-0008-catalog-source-of-truth.md)):

- **`library-backfill`, the library job** (`library_nightly.py`),
  runs six steps in order, each with its own flag, settings and report:
  [library-scan](#library-scan) (files added or removed reach the
  registry), [library-trash](#library-trash) (the copies the owner chose
  on the duplicates page go to Drive's trash),
  [library-identify](#library-identify) (new files are linked to the
  catalog), [library-backfill](#library-backfill) (the first linking pass
  over the library; it reports `library_done` once the whole library is
  linked), [library-sync](#library-sync) (edits made in VirtualDJ go up,
  then catalog edits come down) and
  [library-duplicates](#library-duplicates) (copies of one song grouped
  for the duplicates page). A step that fails is reported and the next
  one still runs; a step due after the run's deadline is skipped and done
  next run.
- **`library-reconcile`** (`library_reconcile.py`) runs on its own,
  several times a day, so a write run has the worker's whole timeout. It
  compares linked files with their entries and writes only what the owner
  approved on the WCS review page.

**They act only on change.** In a library where nothing changed, each step
finds nothing to do and the run reads no music file: the scan lists Drive's
metadata, identify has no unidentified file, the backfill is done, the sync
has no file needing a sync and no edit made in a file, the duplicates are
the groups already open, and the reconcile has no file that changed since
it last compared it. A change is picked up on the next run of the job that
handles it.

**They are quiet when idle.** A step sends its run report only when it
wrote, linked, moved or sent something, found something new (a difference,
a duplicate group, an edit held), stopped early, or hit a problem. A step
with nothing to do logs its counts and sends nothing, so a stable library
sends no reports at all.

**How a file moves through them.** A new file is registered by the scan,
linked by identify (only its `TXXX:CATALOG_ID` is written), and brought in
step with its entry by the reconcile, field by field as the owner approves.
From then on the sync keeps it in step: a catalog edit or display-rule
change is written to the file on the next library job run, and a tag edited in
VirtualDJ goes up to the entry. The sync writes a file only once it has
recorded what the file was last synced with; until then (a file just
linked, written by the reconcile, or restored) it records those values and
writes nothing, so for such a file the reconcile, not the sync, settles a
difference first.

**Owner edits** ([ADR-0008, decided 2026-11-11](decisions/ADR-0008-catalog-source-of-truth.md#decided-with-the-owner-2026-11-11-owner-edits)).
A file the owner changed by hand (an intro cut, words censored) is an owner
edit: not the published recording, and not a release label. It is a
version, like a remix: it has its own catalog entry and plays of its own,
never the published song's entry.

- **How to mark one.** In the file's tags only, by writing its version in
  `< >` in the title (in VirtualDJ, say): `Baby What You Want Me To Do
  <Intro Cut>`, `<Self Censored>`. There is no setting on the WCS site, and
  no search for candidates: an existing owner edit is marked by retitling
  it, and the next reconcile carries it to the catalog. The entry records that
  its version is an owner edit, and its displayed title carries the version
  in `< >`. A file without `< >` behaves as before.
- **How it differs from a release label.** A release label (Clean,
  Explicit, Radio Edit, an official No Intro Edit) is in `( )` or after
  ` - `; its file shares the song's entry and keeps its own label
  (ADR-0008, decision 6). A `< >` version is never read as a release label.
  So the owner's own no-intro cut is `<Intro Cut>`, while a No Intro Edit
  the label released stays `(No Intro Edit)` on the song's entry.
- **What each step does with it.** Identify links it only to an
  owner-edit entry, the catalog's or one made from its title, artist and
  year, never to the published song's; the backfill links it only to an
  owner-edit entry the catalog has, and otherwise leaves it to identify;
  the reconcile carries a file retitled with `< >` up to the catalog by
  linking it to one, with no approval; the sync writes an owner-edit entry's title with its `< >`
  version and never sends a retitle up as an edit to the published entry.
  The **fingerprint** is only a hint: a confident match to the published
  recording is recorded as what the edit was cut from, and no match, or
  another one, is never reported as wrong, held, or used to retitle or
  relink it ([library-identify](#library-identify)). It is never sent to
  **AcoustID** or any other third party as the official recording: the one
  AcoustID lookup only asks what the audio matches, and nothing is
  submitted. The **duplicates** step never groups it with the published
  song's files, even with the same md5; listed beside them, it is kept on
  purpose as an owner edit ([library-duplicates](#library-duplicates)).
  File names leave `<` and `>` out.

### FX Music's formats

Each library root follows a tagging rule profile
([ADR-0010](decisions/ADR-0010-tagging-rule-profiles.md)): the main library
(its folder and New Music) `main`, FX Music `fx`.

- **The main library stays MP3-only.** Its steps list, read and write MP3s
  and their ID3 tags exactly as described below; the catalog wins.
- **FX Music takes every format the shared tagger supports:** MP3, FLAC, Ogg
  Vorbis, Ogg Opus, M4A, WAV and AIFF, listed by their Drive type or name.
  Partial downloads (`.part`, `.crdownload`, `.partial`) and files that are
  not audio (documents, spreadsheets) are ignored. Audio in a format the
  tagger cannot tag (WMA, AAC, MIDI and the like) is named in the scan's
  report (`untaggable`) and left alone; a file whose contents the tagger
  cannot read is left for the owner by identify.
- **Read through the shared tagger.** A file's format is told from its
  contents, not its name. A file other than an MP3 is downloaded whole into
  a temporary local file and read with common-python-utils' `AudioTagger`
  (title, artist, year, genre, BPM, label, comment, and its link).
- **Only the link is written, in the format's own field:** the Vorbis
  comment `CATALOG_ID` (FLAC, Ogg Vorbis, Ogg Opus), the freeform atom
  `----:com.apple.iTunes:CATALOG_ID` (M4A), or `TXXX:CATALOG_ID` in the ID3
  chunk (WAV, AIFF), as on an MP3. Any other write to such a file is refused:
  the file wins, so its own tags are never written over.
- **No conversion, no change to the audio.** A write rewrites the tag block
  only, and follows ADR-0007 as for an MP3: the head revision kept, the
  audio checked before the upload and after it (the bytes outside the tag
  block, taken apart per format: FLAC's STREAMINFO and frames, the Ogg
  packets but the comment header, the M4A `mdat` atoms, the WAV `fmt `/`data`
  and AIFF `COMM`/`SSND` chunks; and mutagen's stream info), the file sent
  back as its own format, and every write in the run record.

### Naming FX Music files whose tags name no song

Identify skips an FX Music file whose tags name no song, and no step fills
them in (the file wins). `python -m deejay_cog.fx_untagged`, run locally
under `doppler run --config prd`, does it from a list the owner approves:

```bash
doppler run --config prd -- uv run python -m deejay_cog.fx_untagged propose --out fx-untagged.csv
doppler run --config prd -- uv run python -m deejay_cog.fx_untagged apply fx-untagged.csv          # what it would do
doppler run --config prd -- uv run python -m deejay_cog.fx_untagged apply fx-untagged.csv --write  # do it
```

`propose` writes one CSV row per such file: `tag` with a title and artist
read from its name ("Artist - Title", or "Title_Artist"; "VA" is not taken
as an artist), `trash` for a copy of another file in the same folder ("…
copy"), or `skip` when the name gives no title or artist. The owner edits
the CSV. `apply --write` then writes each approved title and artist to the
file in its own format (the one write to an FX file's own tags, and only
with his approval), the safe way and recorded as an `fx-untagged` run that
`music_rollback` can undo, and moves each approved `trash` file to Drive's
trash, marking it not present. Nothing is renamed or moved, and a row with
an empty title or artist is never written.

### Running a library job on demand

`POST /v1/deejay/runs` with `{"mode": "library-backfill"}` or
`{"mode": "library-reconcile"}` queues one run, as the schedule does. The
API takes it only from a principal with `deejay.runs.create`; from a
working session, that is the owner console key in the `owner-session`
Doppler project:

```bash
doppler run --project owner-session --config prd -- sh -c 'curl -sS -X POST https://api.kaianolevine.com/v1/deejay/runs -H "Authorization: Bearer $OWNER_CONSOLE_API_KEY" -H "Content-Type: application/json" -d "{\"mode\":\"library-backfill\"}"'
```

What to expect from the queue:

- **One run at a time, in order.** The FIFO queue has one message group,
  so a run queued on demand waits behind the scheduled ones.
- **The same request twice within 5 minutes is one run.** The queue drops
  a message identical to one sent in the last 5 minutes and answers with
  the earlier message's id. The API's `deduplicated` flag is about
  something else (Drive files a watcher already claimed) and stays `false`
  for these requests.
- **A run stopped at the deadline comes back.** Its message is delivered
  again under the same run id, and each step carries on from where it got
  to. After three receives the message goes to the dead-letter queue and
  its alarm fires. A run that cannot finish holds the queue for every run
  behind it until then; to start over, purge the queue
  (`deejay-library-jobs.fifo` for a library run, `deejay-jobs.fifo` for any
  other mode) and, once nothing is running, queue one fresh run.
- **The code a run uses is the code deployed when it starts**, not when
  it was queued. The worker deploys on a release from `main`.

## library-backfill

Retags the files already in the music library from the catalog, in place
([ADR-0007](decisions/ADR-0007-music-tagging-redesign.md#amended-2026-11-03-owner-two-flows-and-the-safety-rules)).
A library file is **never renamed, moved or deleted**: VirtualDJ finds it by
its path. It does nothing unless the `flags.deejay_cog.library_backfill` flag is `dry-run` or
`write`, and nothing unless `LIBRARY_ROOT_FOLDER_ID` and
`MUSIC_TAGGING_STATE_FOLDER_ID` are set and different (see
[CONFIGURATION.md](CONFIGURATION.md#library-backfill)).

**Where it has got to** lives in two JSON files in the state folder, so it
survives redeploys:

- `library-backfill-manifest.json` — the library listed once, recursively
  from `LIBRARY_ROOT_FOLDER_ID` (and, in a manifest started since New Music
  joined the main library, from `NEW_MUSIC_DESTINATION_FOLDER_ID`, its
  files under `/New Music` as the scan registers them), metadata only
  (nothing is downloaded):
  each file's id, name and folder, breadth first, by name within a folder.
  Shortcuts are not followed, and the state folder is left out if it sits
  inside the library. The listing is resumable: the folders still to list
  are kept in the file, saved every 25 folders and when the deadline stops
  the run, and the next run carries on from there. No file is processed
  until the listing is complete. A dry run builds it too (only the state
  folder is written).
- `library-backfill-cursor.json` — the index of the next file in the
  manifest, tied to the manifest's `generation`; the chunk the current run
  is working through; running counts per outcome; and the files that
  failed. Saved only by a `write` run, after each file, and only once that
  file's outcome is saved in the run record.

**Each run** takes the next `LIBRARY_BACKFILL_CHUNK_SIZE` files (10 by
default) after the cursor. The whole library is never rescanned. When the
cursor reaches the end, the run that got there reports `library_done`; every
later run does nothing and sends no report. **To start over** (files added to the library since, or another
root), move `library-backfill-manifest.json` to the Drive trash: the next
run lists the library again and starts from its first file; files whose
tags already match are read but not written. A manifest of another root
than `LIBRARY_ROOT_FOLDER_ID`, or two files of one of these names, stops
the run with a WARN until resolved.

**Each root on its own**
([ADR-0009](decisions/ADR-0009-library-folders.md)). With
`FX_LIBRARY_ROOT_FOLDER_ID` set, FX Music is run after the main library with
a manifest and cursor of its own, `library-backfill-fx-manifest.json` and
`library-backfill-fx-cursor.json` (trash those to start FX Music over), and
its own chunk. An FX file links only to an fx entry: the one the catalog has
for its own title, artist and year, asked for with
`POST /v1/catalog/library/entries`, library `fx` and `preview` set, so the
backfill never makes one (a song the catalog lacks is `not_in_catalog`, left
to identify, which makes its fx entry). A main-library file never links to an
fx entry. As for every file, only the link is written (`TXXX:CATALOG_ID`, or
a non-MP3 file's own field: [FX Music's formats](#fx-musics-formats)): no
catalog value is ever written over an FX file, whose tags win
([ADR-0010](decisions/ADR-0010-tagging-rule-profiles.md)). FX files are
registered under FX Music's root. A root whose state is refused (the WARNs
above) or whose run fails does not stop or change the other: its lines say
why, and a failure is raised once the other root has run, so the run still
fails. An audio mismatch stops the run, whatever the root. The run keeps one
run record. With more than one root, every line of the report starts with
the root, named by the settings naming its folders, and the counts are over
every root. With only `LIBRARY_ROOT_FOLDER_ID` set, the backfill runs and
reports as it always did.

**Per file:**

1. Its id is in `LIBRARY_BACKFILL_SKIP_FILE_IDS` → passed over, named
   (`skipped_by_setting`).
2. Not an MP3 by its listed type or name (in FX Music: not audio in a
   format the shared tagger supports, or a partial download) → passed over,
   not downloaded, named (`not_mp3`).
3. In the trash, or no longer in the folder the manifest lists it in →
   passed over, named (`moved_since`).
4. No usable title or artist → left as it is, named
   (`needs_fingerprinting`). Library files are **not** sent to the
   fingerprint function.
5. Not in the catalog (the same lookup as new-music) → left as it is,
   named (`not_in_catalog`). So is an owner edit (a version in `< >`) the
   catalog answers with the published song's entry: identify takes it in
   on an owner-edit entry.
6. In the catalog and already carrying its tags → not written
   (`already_tagged`). The backfill writes **only the catalog id**
   (`TXXX:CATALOG_ID`): no title, artist, year, genre, BPM, label or
   comment frame changes, even when the file's field is empty, and the
   dry-run report and record show only `catalog_id` changes. So a file
   carrying the catalog id is `already_tagged`, and after a write run a dry
   run of the same chunk shows every written file as `already_tagged`.
7. Otherwise written in place (`music_safety.write_in_place`): no tag
   emptied; the audio of the new bytes byte-identical to the original's;
   the head revision kept forever; the new contents written over the same
   file id; the file downloaded again and its audio checked once more.
   Name, folder and id do not change.

**Registry:** every MP3 the run downloads (steps 4 to 7), in `dry-run` and
`write`, is registered in the API's library registry
(`POST /v1/catalog/library/files`): Drive id, library root, path, size, md5
and duration (of the file as it now is), the catalog entry it links to
(`identified`) or none (`unidentified`), the release label its title carries
(Clean, Explicit, Radio Edit, an official No Intro Edit, Super Clean…;
never an owner edit's `< >` version), and its comment frame as a backup. Registering never changes a music file; a re-run updates
the same row. A registration that fails does not stop the run: the file is
named (`registry_failed`) and, in a `write` run, kept in the cursor's
`registry_retry` list and registered again at the start of the next run,
with its link read from its own `TXXX:CATALOG_ID`.

**Audio check failure** stops the run with an ERROR finding. Before the
write nothing is uploaded; after it, the original bytes are put back. The
cursor stays **on** that file, so the next run starts with it and, if it
fails again, stops again: the backfill does not get past a file it cannot
safely write until the owner has looked. To pass it, add its id to
`LIBRARY_BACKFILL_SKIP_FILE_IDS`. Any other failure on one file (an API
error, a tag that would be emptied) is recorded, named as `failed`, kept
in the cursor's `failed` list, and the run carries on.

**Deadline:** checked between files (and between folders while listing).
Past it the run saves the record, the cursor and the manifest and stops
with `RunOutOfTime`; the redelivery of the same message finishes **that
run's chunk** rather than start a new one, and carries on the same run
record. A write cut off between the upload and its check is checked before
the run stops. Before that, the backfill starts no file with less than
`LIBRARY_BACKFILL_RESERVE_SECONDS` (default 120) left before the run
deadline: it stops cleanly, reported as `paused`, so the sync and
duplicate report after it still run, and the next run carries on from the
cursor. The run record and the cursor are saved after each file even past
the deadline, so a written file is never done twice. A written file takes
about 20 s, so with the function's 600 s timeout a run writes about 15 to
20 files whatever `LIBRARY_BACKFILL_CHUNK_SIZE` says.

**Dry run** writes nothing to any music file and **does not move the
cursor**, so it can be run, reviewed, and the same chunk then written by
switching to `write`. Its would-be record,
`library-backfill-<run id>.dry-run.json`, lists each file's old and new
tags, and the report names each would-be change (`would_write`).

**Report:** counts for `listed`, `scanned`, `matched`, `written`,
`would_write`, `already_tagged`, `not_in_catalog`, `needs_fingerprinting`,
`not_mp3`, `moved_since`, `skipped_by_setting`, `failed`, `remaining`,
`done`, `registered` and `registry_failed`, with the files by name (path under the root). Off, the report says
only that the flow is off; a missing setting is a WARN naming it.

**Undo:** `python -m deejay_cog.music_rollback <run id>` (below) writes
back the revision each written file had before the run. It does not move
the cursor; to retag those files again, start over as above.

## library-scan

The first step of the `library-backfill` library job
([ADR-0008](decisions/ADR-0008-catalog-source-of-truth.md), decisions 5 and
16): files added to and removed from the library reach the API's library
registry. It reads metadata only and **never renames, moves, deletes or
downloads a file**. Off unless the `flags.deejay_cog.library_scan` flag is `dry-run` or `write`
(see [CONFIGURATION.md](CONFIGURATION.md#library-scan)). A scan that fails is
reported as an ERROR finding and the backfill's chunk runs anyway.

**Two libraries**
([ADR-0009](decisions/ADR-0009-library-folders.md)), each listed, compared
and registered on its own:

- **The main library** — `LIBRARY_ROOT_FOLDER_ID` and, when
  `NEW_MUSIC_DESTINATION_FOLDER_ID` is set, New Music. Both folders' files
  are registered under the main folder's id as their library root. A New
  Music file's path is its path under New Music prefixed with `/New Music`
  (`/New Music/Week 1/song.mp3`), which no path in the main folder starts
  with. A file moved between the main folder and New Music, either way, is
  **moved**: it keeps its row, link and plays.
- **FX Music** — `FX_LIBRARY_ROOT_FOLDER_ID`, when set: a library root of
  its own, its id the FX Music folder's. Its files are never left to the
  backfill: the scan registers them, and identify links them.

With only `LIBRARY_ROOT_FOLDER_ID` set, the scan is the one-folder scan it
always was. Only MP3s are compared in the main library; FX Music's
comparison takes every format the shared tagger supports, ignores partial
downloads and files that are not audio, and names audio it cannot tag
(`untaggable`) without registering it ([FX Music's formats](#fx-musics-formats)).

**The listing** is the backfill's kind of listing (breadth first, the state
folder, shortcuts and the other library's folders left out), kept in the
state folder: `library-scan-listing.json` for the main library,
`library-scan-fx-listing.json` for FX Music. A run lists for at most
`LIBRARY_SCAN_LISTING_SECONDS` across both, the main library first; an
unfinished listing is saved and the next run carries on. A listing whose
library's folders have changed since it started is started afresh. Only a
complete listing is compared, and the run after a comparison lists that
library afresh.

**The comparison**, for each library against every registry row under its
root:

- **added** — an MP3 (in FX Music, an audio file in a supported format)
  the registry has never seen is registered
  `unidentified`, with its path. A file the backfill's manifest lists but
  has not reached yet is left to the backfill (`left_for_backfill`), which
  registers it with its link; while the backfill's manifest is still being
  listed, nothing is added (`added_held`).
- **back** — a row marked not present whose file is listed again is
  registered again, its link and status unchanged.
- **moved** — a present row listed under another path keeps its link and
  gets the new path.
- **removed** — a present row whose file is not listed (gone, or in the
  trash) is marked not present. Its row and link stay; the API archives a
  catalog entry left with no plays and no present file, and un-archives it
  when a file links to it again.

**Safety:** a listing with no MP3s (for FX Music, no audio files in a
supported format) while the registry has present files, or
one that would mark more than half of the present files not present,
changes nothing in that library and is a WARN (`refused`, naming the
library's settings): a wrong root or a Drive problem is far likelier than
the library emptying overnight. Each library is judged on its own listing
and rows, so one library's listing never makes the other's files look
removed, and a refusal in one does not stop the other. A dry run compares and
names each would-be change (`would_add`, `would_mark_back`, `would_move`,
`would_remove`) and changes nothing.

**Report:** counts for `listed`, `added`, `back`, `moved`, `removed`,
`left_for_backfill`, `unchanged`, `failed` and `untaggable` across both
libraries, with every file by its path; a refused library's would-be changes are not
counted.

## library-identify

The third step of the `library-backfill` library job
([ADR-0008](decisions/ADR-0008-catalog-source-of-truth.md), decisions 2 and
5): the library files the registry holds as `unidentified` (new to the
library, or ones the backfill could not match) are identified and linked to
their catalog entry; only the link is written to the file, and the
reconcile brings its owned fields in step. Off unless
the `flags.deejay_cog.library_identify` flag is `dry-run` or `write` (see
[CONFIGURATION.md](CONFIGURATION.md#library-identify)).

**Each run** takes the next `LIBRARY_IDENTIFY_CHUNK_SIZE` present,
unidentified files it has not tried before. Every file tried, and its
outcome, is kept in `library-identify-tried.json` in the state folder, so a
file is not fingerprinted again every run; remove an id from it (or trash
it) to try again.

**Per file:**

1. In the trash → left to the scan, named.
2. Its tags (a UTF-8 tag marked Latin-1 is read as UTF-8) are looked up in
   the catalog with the set import's matching. A match is a starting point,
   not the final word
   ([ADR-0007, amended 2026-11-11](decisions/ADR-0007-music-tagging-redesign.md#amended-2026-11-11-owner-incoming-music-takes-the-librarys-path)):
   nothing is linked here. The file is sent to the fingerprint function
   (flow `library-identify`) with the entry its tags found as the one
   expected, and stays unidentified until it has run (note `its tags match
   <artist> - <title> in the catalog; sent to fingerprinting to confirm
   it`). There:
   - a confident AcoustID match (`RETAG_MIN_CONFIDENCE`) to the same song
     (the recording's title and artist agree with the file's, or the
     catalog holds the recording as that very entry) → **linked** (below)
     to that entry, its registry note `identified by its own tags,
     confirmed by the fingerprint (score <score>)`;
   - a confident match to another song → the second opinion (step 5). When
     it backs the tags, the file is linked to the entry its tags found;
     when it backs the fingerprint, to the recording's entry, its note
     saying why (`identified by its fingerprint (…)`); otherwise it is left
     for the owner;
   - no fingerprint result (no match, none confident, a recording
     MusicBrainz does not serve or that has no title or artist, audio
     fpcalc cannot read) → linked by its tags to the entry they found, its
     note `identified by its own tags (<the fingerprint's note>)`: the
     fingerprint did not confirm it.

   A match to an owner-edit entry is the exception: the owner's tags
   identify it, so it is **linked** here and then sent for a hint only
   (below).
3. No usable title or artist, or tags the catalog does not know → sent to
   the fingerprint function (flow `library-identify`). There a confident
   AcoustID match (`RETAG_MIN_CONFIDENCE`) is looked up in the catalog, or
   made a library entry when the catalog has no song for it, and linked.
   No MusicBrainz tags are written. Below the score the file is unchanged
   and stays unidentified for the owner's page. So does a match whose
   recording MusicBrainz refuses (merged or deleted since AcoustID linked
   it), noted with the recording's id, and a file whose audio fpcalc
   cannot read (a damaged or mislabeled file); a MusicBrainz server or
   network error raises instead, and the message is tried again.
4. No fingerprint match, or none confident → the file's own tags make its
   entry when they name a song (an artist other than "Unknown" or
   "Various", a title other than "Untitled", a track number or the file's
   own name copied into the title): linked to
   the catalog's entry for them, or to a library entry made from its title,
   artist and year. Its registry note says so (`identified by its own tags
   (fingerprint: no match)`), marking the link as one no fingerprint
   confirmed.
5. A confident fingerprint match that disagrees with the file's own tags
   gets a second opinion (`second_opinion.py`, ADR-0008, decided
   2026-11-09): when another recording AcoustID links to the same audio is
   the song and version the tags name, the file is linked by its tags;
   otherwise Spotify is searched for each side, and a track that is that
   side's song and version within 3 seconds of the file's length backs it.
   Exactly one side backed → linked by that side, with the reason in its
   registry note. Both, neither, or no Spotify → left for the owner, the
   note saying what each source found. A search that fails (network, token,
   rate limit) is not read as finding nothing: the note says `no second
   opinion: Spotify search failed`, not "neither". A file held for a
   disagreement before the second opinion existed is tried once more, after
   the untried files (marked `second_opinion` in
   `library-identify-tried.json`); so is a file whose note says Spotify
   could not be asked, on each later run until a second opinion is taken.
6. A file left unidentified is registered with its own tags and why
   (`identify_note`): first that its tags did not identify it, then the
   fingerprint's best guess and score. The owner's unidentified-files page
   on the WCS site shows both.

An **owner edit** (a version in `< >`) the catalog has no owner-edit entry
for skips steps 3 to 6: it is linked to an owner-edit entry made from its
title, artist and year (registry note `an owner edit, identified by its own
tags`), never to the published song's entry; the fingerprint never
identifies it. One whose tags name no song (no artist) is left for the
owner with a note saying so. A dry run previews the entry and links nothing
(`would_link_owner_edit`).

**The fingerprint as a hint for an owner edit** (ADR-0008, decided
2026-11-11, decision 4). Once an owner edit is linked (to a new owner-edit
entry or the catalog's), it is sent to the fingerprint function
(`sent_for_cut_from_hint`; in a dry run `would_send_for_cut_from_hint`,
and nothing is sent). There, for a file whose own title has a `< >`
version, a confident match to a recording of its own song (its title
without the version, and its artist) is the published recording it was cut
from: the catalog's entry for it, or the recording's name when the catalog
has none, goes into the file's registry note (`…; fingerprint hint: cut
from <artist> - <title> (catalog entry <id>), score <score>`; its link and
status are sent unchanged) and is named in the fingerprint run's report
(`owner_edit_cut_from`). Nothing else changes. No match, an unconfident
one, another recording, or the edit's own entry gives no hint: the file is
never reported as wrong, held, recorded as unidentified, retitled or
relinked, and gets no second opinion. Nothing is submitted to AcoustID
(its one lookup only asks what the audio matches), MusicBrainz is asked
only for the matched recording's details, and Spotify is not asked. The
entry's `cut_from_id` is not set from here.

**FX Music** ([ADR-0009](decisions/ADR-0009-library-folders.md), decision
3): with `FX_LIBRARY_ROOT_FOLDER_ID` set, its unidentified files are tried
after the main library's, from the same chunk. An FX file links only to an
fx entry, never to a main-library one, even when its tags or fingerprint
match one; and a main-library file never links to an fx entry. The catalog
lookup searches main entries only, so an FX file skips steps 2 to 6: the
catalog is asked for the fx entry of its own title, artist and year
(`POST /v1/catalog/library/entries` with library `fx`), which it finds or
makes, and the file is linked to it (registry note `an FX Music file,
identified by its own tags`). Its tags win over the catalog
([ADR-0010](decisions/ADR-0010-tagging-rule-profiles.md)): it is never
fingerprinted, and only the link is written. It may be in any format the
shared tagger supports, read through it, and its link goes in the format's
own field ([FX Music's formats](#fx-musics-formats)). An FX file whose tags
name no song, or that the tagger cannot read, is left for the owner with a
note saying so. A dry run previews the
entry and links nothing to a new one (`would_make_fx_entry`). A link the
catalog refuses as one to the other library (409 `other_library`) is an
issue (`not_linkable`), never a crash; the run carries on.

**Linking** fills the entry's empty fields from the file
(`POST /v1/catalog/{id}/fill-up`), writes only the link
(`TXXX:CATALOG_ID`) to the same file, and registers it as identified with
when it was synced. Every owned field is left to the reconcile, which
writes the approved kinds when it reaches the file. The
write follows the backfill's safety rules: never renamed, moved or deleted;
head revision kept; audio checked before and after; every write in
`library-identify-<run id>.json`, which `music_rollback` undoes. The comment
and every other frame are left as they are. An audio mismatch stops the run
with an ERROR finding; that file is marked tried and not retried.

**Dry run** looks up and plans, but writes no music, changes nothing in the
catalog or registry, sends nothing to the fingerprint queue and records no
file as tried. Its record is `library-identify-<run id>.dry-run.json`.

**Report:** counts for `waiting`, `fingerprinted_unlinked` (files an
earlier run sent to fingerprinting that are still unidentified: left for the
owner, or not run yet), `tried`, `linked`, `written`, `would_write`,
`filled_up`, `to_fingerprint` (tag matches sent to be confirmed among them,
each named with the entry its tags found), `owner_edits`, `owner_edit_hints`, `fx_entries` (FX files
given a new fx entry, each named), `not_linkable` and `failed`, with every
file by its path.
The fingerprint function's run for a library file is reported under
`library-identify` too, but sent only when it has an issue: a chunk sends
one run per file, so a run that went as expected is logged and kept in its
run record, not posted; an owner edit's cut-from hint is posted, naming
it.

## library-reconcile

Its own worker mode, `library-reconcile`, on its own schedule
([ADR-0008](decisions/ADR-0008-catalog-source-of-truth.md), step 3 and
decision 15): each linked file's owned fields are compared with what its
catalog entry says they should be, and every difference is filed under a
kind for the owner to approve. **A dry run reads only**: no music file,
catalog entry or registry row changes, only its seen record and record in the
state folder. A **write** run applies what the owner approved (below). Off
unless the `flags.deejay_cog.library_reconcile` flag is `dry-run` or `write` (see
[CONFIGURATION.md](CONFIGURATION.md#library-reconcile)).

**Which files: those that changed.** Present, identified registry rows under
the library root. `library-reconcile-seen.json` in the state folder keeps,
for each file, what it was when last compared: when Drive last saw it change
(the scan's listing, `library-scan-listing.json`), when its entry's display
last changed (`changed_at` from `POST /v1/catalog/display`, which a
display-rule change also moves) and the entry it was linked to. A run
compares only the files where one of these moved, or that it never
compared, in the registry's order, up to `LIBRARY_RECONCILE_CHUNK_SIZE`; the
rest wait for the next run (`changed_left`). A file whose comparison failed,
or that stopped the run, is not marked compared, so the next run takes it
again. In a library where nothing changed, a run reads nothing. **A full
pass** (after a change to the reconcile's own rules, the one thing that can
surface differences on unchanged files) is `reconcile_admin full --apply`
(below), which empties the seen record so every file is new to it.

**Approved files first:** a write run first compares the files with an open
difference the owner has approved (read from `library-reconcile-open.json`
and the approvals), up to `LIBRARY_RECONCILE_CHUNK_SIZE` of them, so an
approval settles on the next run. The report counts them as
`approved_first`. An approved difference a write run tried and left open (a
fill-up the catalog refused, a value the API held) is marked `tried` and is
not put first again; its file is compared again when it changes.

**What is compared:** title (with its version), artist, year, genre, BPM and
label in the file's ID3 tags, against `POST /v1/catalog/display`. The
expected title is the entry's title followed by the file's own edit label
from the registry (`Song (No Intro Edit)`). The comment and every
VirtualDJ-owned frame are left out.

**A file retitled as an owner edit** (a version in `< >` while its entry is
the published song's, or another owner edit) is not compared: the retitle is
the owner's choice, and it goes up to the catalog with no approval. A write
run links the file to its owner-edit entry, the one the catalog's matching
finds or one made from its title, artist and year, writing only
`TXXX:CATALOG_ID`, and registers it on that entry (with its release label,
if it has one, and its synced values cleared). The entry it leaves keeps its
plays and its other files. The next run compares the file with its new
entry. A dry run previews the entry and changes nothing
(`would_link_owner_edit`); the report counts `owner_edits`. A `TXXX:CATALOG_ID` naming another
entry than the registry's link is listed under `other` as `catalog_id`, and
a write run corrects it **without an approval**: the row's id is written
over the tag (that frame only). This is how a catalog merge's repointed
files are brought in step, as the merge changes the rows and not the files.
The library-sync step does not compare that tag, so it reads no extra file.

**Tags are read from a ranged download.** Each comparison downloads only
the start of the file (`TAG_HEAD_BYTES` in `music_safety.py`) and its last
few bytes, where an ID3v1 block sits, so the tags read and the differences
found are those of the whole file. The whole file is downloaded only when
its ID3v2 tag runs past the start read, when a range cannot be read (an
empty file), or when an approved difference is written over it: a write and
its audio check are always made on the whole file.

**A kind approval covers every file of that kind.** There is no per-file
exception to it: with `year_later` approved for the file, any file whose
year is older than its entry's sends that year up, even when the entry's
year was set in the editor on purpose. To make one such file follow the
entry, change the entry once the sync has recorded the file's values; the
sync then writes the entry's value to the file before the reconcile sees a
difference. Only `other` differences are approved file by file.

**Kinds:** `case_punctuation`, `featured_moved`, `artist_joiner`,
`version_brackets`, `genre_remapped`, `genre_filled`, `year_filled` (the
file has none), `year_earlier` (the catalog's year is older), `year_later`
(the file's year is older: approved for the file, the older year goes up to
the catalog),
`year_differs` (a year that is not a number), `bpm_rounding` (less than
1.5 BPM), `bpm_differs`, `label_differs`, `fill_up` (a file value for a field
the catalog leaves empty) and `other`.

**Record:** `library-reconcile-<run id>.dry-run.json` in the state folder,
every difference under its kind (path, field, the file's value, the
catalog's); a write run's is `library-reconcile-<run id>.differences.json`.
The report counts each kind, shows a few examples of each, and names every
`other` file. **A held difference is reported once:** one not approved that
an earlier run's report already named with the same values (kept as
`reported` in `library-reconcile-open.json`, below) is left out of the
report's counts and examples, and counted as `held_reported_before`
instead. It is still in the record and still sent to the review. A
difference whose values changed, or a new one, is reported again.

**Review:** every run, dry or write, sends the API the files it compared
and the differences still open on them (`POST
/v1/catalog/library/reconcile/differences`): all of them in a dry run, and
in a write run those it did not write or fill up. For those files they
replace what the API held, so the owner's review page on the WCS site shows
each file's latest. The API leaves out a fill-up its catalog would refuse
(a genre off the vocabulary, a one-character label such as the `E`/`C`
markers): the file keeps that value on purpose and nothing goes up, so the
review page does not list it. A run that cannot send them reports `review_not_sent`
and carries on; the next pass sends them again.

**Approvals** are the owner's, set on the review page; each write run reads
them from the API (`GET /v1/catalog/library/reconcile/approvals`):

```json
{"kinds": ["case_punctuation", "genre_remapped", "fill_up"],
 "files": {"<drive file id>": ["title"]},
 "file_wins_kinds": ["year_later", "bpm_differs"],
 "file_wins_files": {"<drive file id>": ["artist"]}}
```

Every approval says which side wins, so every difference has a way to
close; leaving one unapproved only means it is not decided yet. `kinds`
and `files` approve it **for the catalog**: the catalog's value is written
over the file. `file_wins_kinds` and `file_wins_files` approve it **for the
file**: the file's value goes up to the entry and the file is not written.
`kinds` approves whole kinds (any but `other`); `files` approves `other`
differences one file and field at a time (`title`, `artist`, `year`,
`genre`, `bpm`, `label`). A `catalog_id` difference needs no approval: the
row's id always wins (see "What is compared"). The file's side takes the same
shapes, but not `fill_up` (already the file's value going up), `year_filled`
or `genre_filled` (the file has no value); a kind, or a
file's field, goes one way only. Approvals that cannot be read stop
the run before it compares anything; until any are set, a write run writes
nothing. The approvals given before the API held them were kept in
`library-reconcile-approvals.json` in the state folder; `reconcile_admin
seed` carries that file over once, and nothing reads it after.

**A write run** writes each approved difference with the catalog's value
over the same file, in place, with the backfill's safety rules: never
renamed, moved or deleted; head revision kept; audio checked before and
after; every write in `library-reconcile-<run id>.json`, which
`music_rollback` undoes. The file is then registered as synced. Approved
`fill_up` values go up to the catalog (`POST /v1/catalog/{id}/fill-up`) and
the file is not written. Differences approved for the file go up to the
entry as a catalog revision (`POST /v1/catalog/library/files/edited` with
`keep_file`, whatever the last sync recorded) and the file is not written;
the library sync then carries the value to the entry's other files. One the
API holds (a title or artist that does not split into parts cleanly, a
genre off the vocabulary, a one-character label) stays open on the review
page and is reported under `kept_held` with the reason. Differences not
approved are left and counted (`not_approved`). Genre is written; subgenre is not, as no frame for it is
decided. An audio mismatch stops the run with an ERROR finding; that file
is not marked compared, so the next run takes it again. After a write run,
a full-pass dry run shows no approved difference left.

**Report:** counts for `compared`, `changed_left`, `approved_first`,
`matching`, each kind, and in a write run `written`, `filled_up`,
`kept_from_file` and `not_approved`, with every file by its path. A fill-up
the catalog refused (one a write run tried) is counted as
`fill_up_kept_by_file` and not listed: the file keeps that value on
purpose.

**FX Music: the file wins**
([ADR-0009](decisions/ADR-0009-library-folders.md),
[ADR-0010](decisions/ADR-0010-tagging-rule-profiles.md)). With
`FX_LIBRARY_ROOT_FOLDER_ID` set, FX Music's linked files are reconciled after
the main library's, on their own: those that changed since
`library-reconcile-fx-seen.json` last saw them (Drive's changes from
`library-scan-fx-listing.json`), up to `LIBRARY_RECONCILE_CHUNK_SIZE`. Every
difference with a value in the file goes up to its fx entry
(`POST /v1/catalog/library/files/edited` with `keep_file`), with **no
approval**, and no catalog value is ever written over an FX file. FX
differences never go to the review page or into
`library-reconcile-open.json`. The report names each value sent up, by file
and field (`fx_sent_up`, `path: field 'entry' -> 'file'`), and each value the
API held, with why (`fx_held`: a title or artist that does not split into
parts cleanly, a genre off the vocabulary, a one-character label). A field
the file leaves empty is not sent up and the entry keeps its value
(`fx_not_sent`). FX files in every supported format are compared, read
through the shared tagger ([FX Music's formats](#fx-musics-formats)). A link
naming another entry is corrected as for the main library, that field only
(`TXXX:CATALOG_ID`, or the format's own). The flag applies to FX Music too: a
dry run asks the API what it would take (`preview`), sends nothing up, says
what would go (`would_send_up`, counted as `fx_would_send_up`), and marks no
FX file compared, so the next run compares the same files. A failure in one
library does not stop the other (approvals the API can't return stop only the
main library); a failure is raised once the other has run. With more than one
root, every line of the report names its root and the counts are over both.

**Edits that share an entry at different tempos keep their BPMs.** When the
files linked to one entry hold BPMs 1.5 or more apart (`BPM_ROUNDING`; two
edits of a song cut at different tempos, such as a 3:09 and a 2:53 edit),
a write run settles no BPM difference on any of them, whatever was
approved: the file's BPM is not sent up and the catalog's is not written.
Either would hand one file's tempo to the other through the entry, since
library-sync writes the entry's BPM to every file on it, and the next run
would hand it back. The differences stay open, filed as `bpm_differs`, for
the owner's review; the report names each one an approval would have
settled (`bpm_split_held`, with every file's BPM). library-sync writes
neither BPM ([library-sync](#library-sync)).

**Long-open differences:** `library-reconcile-open.json` in the state folder
keeps when each open difference was first seen; a difference whose values
change starts again. Every run counts those open more than 7 days
(`long_open`), leaving out a refused fill-up (one a write run tried and
the catalog would not take; above). A full pass has had the chance to settle each of them, so it
has no decision, or one that cannot apply. The first run to find any warns
once (`long_open`, with the count by kind), and again only after the count
has been back to zero.

**Approval rules:** what is approved, and why some kinds are held, is in
[ADR-0008, decided 2026-11-07](decisions/ADR-0008-catalog-source-of-truth.md#decided-with-the-owner-2026-11-07-the-first-reconcile-review).
The API's approvals hold the current choices.

### Working the reconcile from a session

`python -m deejay_cog.reconcile_admin`, run locally under
`doppler run --config prd`:

- `review [--since <ISO time>] [--out <path>]` reads every reconcile record
  of differences in the state folder, keeps the latest difference for each
  file and field, and prints them grouped by kind, each kind (and each
  `other` file) marked approved or held by the current approvals. Reads
  only.
- `approvals` prints the approvals the write runs apply, as the API holds
  them. They are changed only on the review page.
- `seed [--apply]` carries `library-reconcile-approvals.json` over to the
  API, checked as a write run read it; sent only with `--apply`. Once the
  API holds approvals it sends nothing and says so.
- `full` says how many files the seen record holds; `full --apply`
  empties it, so the next runs compare every file again, a chunk at a
  time (a full pass).
- `baseline --apply` is the opposite: it records every present, identified
  file as compared as it is now, reading none, so the next runs compare
  only what changes after. For a library just reconciled in full (the
  review page at zero).
- `full` and `baseline` take `--library main|fx|all` (default `all`):
  each library keeps its own seen record (`library-reconcile-seen.json`,
  `library-reconcile-fx-seen.json`), and `all` covers every library set up.
- `run [--mode dry-run|write] [--chunk <n>]` runs the reconcile once,
  locally, writing exactly as the scheduled run does. Without `--mode` it
  runs in the mode `flags.deejay_cog.library_reconcile` sets; `--mode`
  applies to the local run only and leaves the flag as it is. Turn the
  flag off while a local run goes, so the two do not compare the same
  files at once.

## library-sync

The fifth step of the `library-backfill` library job
([ADR-0008](decisions/ADR-0008-catalog-source-of-truth.md), step 4 and
decision 5, edit): catalog edits and display-rule changes reach the files
they affect. Off unless the `flags.deejay_cog.library_sync` flag is
`dry-run` or `write` (see
[CONFIGURATION.md](CONFIGURATION.md#library-sync)); it was kept off until
the library was reconciled, so the reconcile and the owner's approvals
decided the first writes.

A `TXXX:CATALOG_ID` tag that differs from its row's id (the state after a
catalog merge repoints the row) is not the sync's to compare: it reads no
file for that. The reconcile corrects it without approval when it next
compares the file ([library-reconcile](#library-reconcile)).

**Each run** takes the next `LIBRARY_SYNC_CHUNK_SIZE` files the API lists
as needing a sync (`GET /v1/catalog/library/files/needing-sync`): present,
linked files whose entry or the display rules changed after the catalog's
values were last written to them, that never had them, or whose synced
values are not recorded. For each, the entry as the display rules write it
out (`POST /v1/catalog/display`) gives the owned fields: the title with the
file's own release label, artist, year, genre, BPM and label. They are
compared with the values recorded at the file's last sync, and only the
fields that moved are written over the same file, in place
([ADR-0008](decisions/ADR-0008-catalog-source-of-truth.md), 2026-11-09: the
sync writes only what moved). A value the owner held in the reconcile
survives an edit to another field; a field the catalog empties keeps the
file's value; a tag another tool changed is left until its catalog value
moves. A file with no recorded values (linked before this, or just written
by identify or the reconcile, whose registration clears them) gets them
recorded and nothing written, and is not read from Drive; neither is a
file none of whose fields moved. Genre is written; subgenre is not. The
comment and VirtualDJ's frames are left as they are. Then the files are
recorded as synced with their values
(`POST /v1/catalog/library/files/synced`), including those that already
carried them.

**Edits that share an entry at different tempos keep their BPMs.** When a
file's BPM moved and the files linked to its entry hold BPMs 1.5 or more
apart (read from their tags, once per entry per run), the BPM is written to
none of them: each keeps its own, any other moved field is written as
usual, and the report names each (`bpm_split_kept`). The file is recorded
as synced, so the same move is not tried again. The reconcile holds their
BPM differences the same way ([library-reconcile](#library-reconcile)).

**Edits made in a file go up first**
([ADR-0008](decisions/ADR-0008-catalog-source-of-truth.md), 2026-11-09: an
edit made in VirtualDJ goes up). Before the chunk above, each present,
linked file with recorded values that Drive saw change after its last sync
(the scan's listing, `library-scan-listing.json`, keeps when Drive last saw
each file change) is read, and its owned fields are sent to the API
(`POST /v1/catalog/library/files/edited`). Its tags are read from a ranged
download of its start and end, as the reconcile reads them; the whole file
is downloaded only when its ID3v2 tag runs past the start read, or when a
stale field is put back (below). A field the file changed and its
entry did not goes up to the entry as a catalog revision, so the chunk then
carries it to the entry's other files. A field the catalog changed too stays
the catalog's and is written back. A BPM within 1.5 of the recorded one (or
its double or half) is a re-measure and is left; a title or artist that does
not split into parts cleanly, a genre the vocabulary does not place, or a
one-character label is held: the file keeps it and the report names it. A
dry run asks the API what would happen and changes nothing. A file whose
title now names an owner edit its entry is not (a version in `< >`) is not
sent up and is left out of the chunk, so nothing is written over it and it
is not marked synced; the reconcile links it to its owner-edit entry
(`owner_edit_left_for_reconcile`). An owner-edit entry's displayed title
carries its version in `< >`, and the sync writes it so.

**A stale write-back is told from an edit by its value.** A tagger can
write its own stored tags back over the library (VirtualDJ did on
2026-11-09), and those old values must not reach the catalog as edits.
Before a changed file's fields go up, each owned field (title, artist,
year, genre, BPM, label) that differs from the value recorded at its last
sync is checked against the values the pipeline replaced in that file: the
old tags of every in-place write the library flows' run records
(`library-*-<run id>.json` in the state folder) show was done, in any run.
A field that now holds one of them is stale: it is put back, with the value
the file held at its last sync (its Drive revision from then, never the
catalog's), and is not sent up. Every other changed field is an edit and
goes up as above, however many files changed in the run, so a big editing
session goes through. A file with both has the stale field put back and the
edit sent up. A stale field is left as it is, and named in the report
(`stale_write_back_left`), when Drive keeps no revision from the file's last
sync, when the value then was empty, or when it too is one the pipeline
replaced; that file's fields do not go up. The report names every field
put back (`stale write-back put back`; `would_put_back` in a dry run, which
writes nothing) and every edit sent up. A put-back is an in-place write
under the rules below, marked `sync (stale write-back put back)` in the
run's record.

The check cannot tell a deliberate edit back to a value the pipeline once
replaced from a write-back: such an edit is put back too. To set a field to
an older value, change it in the catalog editor; the sync writes it to the
entry's files.

**A bulk change is held.** `LIBRARY_SYNC_EDIT_FLOOD` (default 1000) is a
backstop against a bulk write the value check does not catch: when more
files changed outside the pipeline since their last sync than it allows,
nothing goes up to the catalog and nothing is written: the files keep what
they hold, each is recorded as synced with the values it already had
recorded, and the run ends at WARN with a `bulk_change_held` issue naming
the count. A stale write-back is repaired with
`python -m deejay_cog.library_restore <time> --apply`, which puts each file's
tags back to its last Drive revision before that time. A run record that
marks writes of the catalog's values over files in a bulk change is undone
with `python -m deejay_cog.music_rollback --catalog-put-backs --apply`.

**The edits step leaves the sync step half the run.** Reading files changed
in Drive stops once `EDITS_SHARE` (half) of the run's time is spent; the
files not read are counted as `changed_in_drive_left`, with a notice, and a
later run reads them. The sync step runs on the rest of the time but skips
those files: writing one and marking it synced would hide its unread edit.

**Run records are read once.** The values pipeline writes replaced, by file
and field, are kept in `library-sync-replaced-index.json` in the state
folder with the Drive modified time of each run record read, so a run
downloads only the records that are new or changed since. The index is
saved as records are read, inside the edits step's share: when the share
runs out first, the run keeps its progress, reports `run_records_left`,
judges no file's edits (they are counted as `changed_in_drive_left`) and the
next run reads the rest.

The write follows the backfill's safety rules: never renamed, moved or
deleted; head revision kept; audio checked before and after; every write
in `library-sync-<run id>.json`, which `music_rollback` undoes (and then
has the registry forget the restored files' recorded values, so what it put
back is not read as an edit made in the file). An audio
mismatch stops the run with an ERROR finding; that file is not marked
synced and comes up first next time. A dry run writes no music and marks
nothing synced.

**Each root on its own**
([ADR-0009](decisions/ADR-0009-library-folders.md)). With
`FX_LIBRARY_ROOT_FOLDER_ID` set, FX Music is synced after the main library,
from its own scan listing (`library-scan-fx-listing.json`) and its own
registry rows. **The FX file wins**
([ADR-0010](decisions/ADR-0010-tagging-rule-profiles.md)): an edit made in an
FX file goes up to its fx entry as above (a title retitled with `< >` too: an
FX file's title is its own), but the catalog's values are never written over
an FX file, in any format. FX files other than MP3s are read through the
shared tagger ([FX Music's formats](#fx-musics-formats)). One whose entry
moved keeps what it holds, is recorded as synced
with the values it already had recorded, and is named (`fx_file_wins`); the
reconcile then sends its values up to the entry. A root that fails does not
stop the other; its failure is raised once the other has run. An audio
mismatch or the run's deadline stops the run, whatever the root. With more
than one root, every line of the report names its root and the counts are
over both.

**Report:** counts for `waiting`, `written`, `would_write`,
`already_in_step`, `nothing_moved`, `values_recorded`,
`changed_in_drive_read`, `changed_in_drive_left`, `run_records_read`,
`run_records_sec`, `run_records_left`, `edits_<outcome>`, `marked_synced` and `failed`;
every edit sent up, held or overwritten by the file's path and field; every
file written by its path with the fields written; and how
many files each entry's change reached.

## library-trash

The second step of the `library-backfill` library job
([ADR-0008](decisions/ADR-0008-catalog-source-of-truth.md), decision 7).
Off unless the `flags.deejay_cog.library_trash` flag is `dry-run` or `write`.

When the owner resolves a duplicate group on the WCS duplicates page, the
API marks the group's other copies trash-requested in the library registry.
**Each run** lists the present files under the root marked so
(`GET /v1/catalog/library/files?trash_requested=true`), moves each to
Drive's trash (`files.update`, `trashed=true`; recoverable, never deleted)
and marks it not present (`POST /v1/catalog/library/files/absent`). A file
already in the trash or gone from Drive is only marked not present. No file
the registry does not mark is touched. A dry run lists what it would trash
and changes nothing.

Each library root ([ADR-0009](decisions/ADR-0009-library-folders.md)) is
run on its own, the main library first, under the same flag. A root that
fails does not stop the other; its failure is raised once the other has run.
With more than one root, every line of the report names its root and the
counts are over both.

**Report:** counts for `trash_requested`, `trashed`, `would_trash`,
`already_gone`, `marked_not_present` and `failed`, every file by its path.

## library-duplicates

The last step of the `library-backfill` library job
([ADR-0008](decisions/ADR-0008-catalog-source-of-truth.md), decision 7).
Off unless the `flags.deejay_cog.library_duplicates` flag is `dry-run` or `write`. Read-only: it
never writes, renames, moves or deletes a music file.

**Each run** reads, for each library root on its own, the registry's present
files under that root (leaving out those already marked for the trash) and the catalog's title and artist for
the entries they link to (`POST /v1/catalog/display`), and links files that
share an md5 (exact copies), a catalog entry, a title and artist as the
display rules write them out (case, spacing and punctuation ignored), or a
file name in the same folder (ignoring a copy suffix such as ` (1)`).

**Kept on purpose:** files of one song with a different release label
(Clean, Explicit, Radio Edit, an official No Intro Edit, …) or durations
more than three seconds apart (a shorter cut) are different edits, never duplicates of each
other. Each linked set is split into its edits; every edit with two or
more files is a group, listing the song's other edits marked kept on
purpose with why, so the page can never send them to the trash.

**Every song with two or more files** ([ADR-0007, amended 2026-11-11](decisions/ADR-0007-music-tagging-redesign.md#amended-2026-11-11-owner-the-duplicates-page-decides-each-file):
one entry may keep several files): in the main library, a song whose files are all
different edits, one file each, is still sent as a group, so the page shows
every file the song has. Its first edit's file is offered; the others are
listed beside it marked kept on purpose with why (another release label,
another length, an owner edit), and the page defaults them to their own
version. Nothing is left out because it is a different edit. A group the
owner resolved with every file staying (kept together) is not reopened:
the API skips a reported group with the same key whose files are all files
of the resolved group, so it comes back only when a file joins it. The cog
keeps no memory of its own.

**Owner edits** (ADR-0008, decided 2026-11-11): a file linked to an entry
the catalog marks as an owner edit (`<…>` in its title) is an edit of its
own, never a duplicate of the published song's files, even with the same
md5. Listed in the published song's group, it is kept on purpose as an
owner edit. Files on one owner-edit entry are grouped as copies of it like
any other, the published song's files shown kept beside them. In FX Music
an owner edit is not listed as a copy across folders either.

**Per root** ([ADR-0009](decisions/ADR-0009-library-folders.md), decision
4). Files are grouped only with files of the same root: a file in FX Music
and one in the main library are never duplicates of each other.

- **The main library** (its own folder and New Music, one root): files are
  grouped within and across the two folders, as above. Neither folder is
  preferred; the owner picks the keeper on the duplicates page.
- **FX Music:** files are grouped only within one folder; a different cut
  (another length or edit) is kept on purpose, as above, and a song whose
  files in a folder are all different cuts is not a group. Copies linked
  across FX folders are kept there on purpose (one per role or event), so
  they are never offered as a group to resolve: each set is listed in the
  run report (`fx_across_folders`, with every path) for the owner to decide
  case by case. The duplicates page does not show them.

A group's key is made from its files, so an unchanged library gives the
same groups. `write` sends each root's groups to the API
(`POST /v1/catalog/library/duplicates`), replacing that root's open groups;
a dry run only reports them. A root whose files can't be read or whose
groups can't be sent is reported (`failed`, naming the root), and the other
root still runs.

**Report:** counts for `files` and `groups` over every root (and, in write,
the API's `open_groups`, `added` and `dropped`), and every group with its
reasons and files by path, when the root's groups changed (or in a dry
run). Open groups the same as last run wait on the duplicates page and are
not named again. With more than one root, each root's group count
(`groups_main`, `groups_fx`) is given and every group line starts with the
root, named by the settings naming its folders; with FX Music set,
`fx_across_folders` counts its sets of copies across folders.

**No report when nothing changed.** `library-duplicates-seen.json` in the
state folder keeps a digest of each root's groups (each group's key,
reasons and files, FX Music's sets across folders, and how many groups were
left out) as the last run found them. A run whose groups are the same for
every root, and that the API found nothing added to or dropped from, posts
no report, dry or write: it names no group, is logged with its counts
(`files`, `groups`, `unchanged_since_last_run`) and still sends the groups
to the API in write. A group added, dropped or changed, or a root that
failed, reports as above. A state file that can't be read or saved is
named (`last_run_not_compared`) and the run reports as usual.

### Run records

Every run of a music-writing flow that touches a file leaves a JSON record in
`MUSIC_TAGGING_STATE_FOLDER_ID`, named `<flow>-<run id>.json`; the run id is
the queue message id (a local run makes one up, `local-<time>-<hex>`). A
writing run saves it before and after each step, so a run stopped by the
deadline still leaves it. A redelivery of the same run carries on the same
record: its first save takes over the file the earlier delivery saved and
keeps that delivery's entries ahead of its own, so a run has one record and
a rollback sees every write. Per file it holds the file id, original name, folder, old and new
tags, the action (`deliver`, `review`, `in_place`, `fingerprint`, `skip`),
its state (`planned`, `pending`, `done`, `skipped`, `failed`, `restored`),
the reason for a skip or failure, the Drive revision kept and the MD5 of the
original and written contents (in-place writes), any copy made (id, folder,
name) and any move (from, to).

### Putting the library's tags back to a moment

```bash
doppler run -- uv run python -m deejay_cog.library_restore <time>          # what it would do
doppler run -- uv run python -m deejay_cog.library_restore <time> --apply  # do it
```

`<time>` is an ISO 8601 time with its offset (`2026-11-09T18:30:00-06:00`).
A dry run unless `--apply`. For every library file Drive saw change after
that time, each owned field that now differs from the file's last Drive
revision at or before it gets that revision's value back; a field the old
revision leaves empty is not emptied. Each write is recorded as a
`library-restore` run that `music_rollback` undoes. The registry then
forgets the restored files' synced values, so the sync records them afresh
rather than reading the restore as edits: until it has (a few runs
for a large restore, at `LIBRARY_SYNC_CHUNK_SIZE` files a run), catalog
edits do not reach those files.

### Restoring a file's own BPM

```bash
doppler run -- uv run python -m deejay_cog.restore_bpm          # what it would do
doppler run -- uv run python -m deejay_cog.restore_bpm --apply  # do it
```

A dry run unless `--apply`. It reads every library-reconcile and
library-sync run record, finds each file whose BPM a write moved by 1.5
or more (bpm_differs; rounding is left), and writes back the BPM the file
had before: that field only, and only while the file still holds what
the write left. Each restore is an in-place write recorded as a
`library-bpm-restore` run, which the rollback below can undo. Why BPM
differences are held: ADR-0008, decision 4.

### Undoing fingerprint links to another song

```bash
doppler run --config prd -- uv run python -m deejay_cog.fingerprint_check          # what it would do
doppler run --config prd -- uv run python -m deejay_cog.fingerprint_check --apply  # do it
```

A dry run unless `--apply`. The fingerprint function now links a library
file whose own tags name a song only when the recording AcoustID finds is
that song (the titles without versions or credits, and the main artists,
agree); otherwise the file stays unidentified with a note, for review.
Before that check, a wrong AcoustID link could retitle a file ("Beggin" by
Madcon written as "Don't Worry"). This reads every library-identify run
record, takes each fingerprint write whose old title and artist disagree
with the new ones, rolls its run back (the rollback below) and makes its
registry row unidentified, its link cleared, with a note.

### Linking by their own tags the files the fingerprint found nothing for

```bash
doppler run --config prd -- uv run python -m deejay_cog.link_by_tags          # what it would do
doppler run --config prd -- uv run python -m deejay_cog.link_by_tags --apply  # do it
```

Step 4 above, for the files already on the owner's page: every
unidentified file whose note is the fingerprint's "no match" or "no
confident match" is linked by its own tags, as step 4 does. The dry run
reads the tags the registry holds and downloads nothing; `--apply` reads
each file, links it the library job's way, and records a
`library-link-by-tags` run that `music_rollback` can undo. A file whose
tags name no song is left for the owner.

### Relinking files matched past a version

```bash
doppler run --config prd -- uv run python -m deejay_cog.relink          # what it would do
doppler run --config prd -- uv run python -m deejay_cog.relink --apply  # do it
```

A dry run unless `--apply`. Before the API kept a version that follows an
unbracketed feat. credit ("Starboy ft. Daft Punk (Alex Aiono Cover)"), its
loose match read such a title as the plain song, and the backfill linked
the file to the plain song's entry. This reads every library-backfill run
record, takes each file whose recorded title has that shape, and looks it
up again. A file the lookup now puts under another entry has its
`TXXX:CATALOG_ID` rewritten (that frame only, an in-place write recorded as
a `library-relink` run, which the rollback below can undo) and its registry
row linked to it. A file the catalog has no entry for is made unidentified
in the registry, so library-identify takes it; its link tag is left, since
a tag is never emptied. Files already linked right are left alone.

### Restoring an artist's own styling

```bash
doppler run -- uv run python -m deejay_cog.restore_styling          # what it would do
doppler run -- uv run python -m deejay_cog.restore_styling --apply  # do it
```

The same shape as the BPM restore, for titles and artists: a write that
changed one only in case and punctuation and lost its styling (an
all-caps word, or an all-lowercase value) is undone, that field only, and
only while the file still holds what the write left. A fix of sloppy
casing (`GoldFish` -> `Goldfish`) stays. Recorded as a
`library-styling-restore` run. Why case differences are held: ADR-0008,
decision 6.

### Rolling a music run back

```bash
doppler run -- uv run python -m deejay_cog.music_rollback <run id>          # what it would do
doppler run -- uv run python -m deejay_cog.music_rollback <run id> --apply  # do it
```

A dry run unless `--apply`. It reads the run's record and, last file first:
moves a copy the run made to the Drive **trash** (never deletes it), only
while it is still in that folder with the contents the run wrote; moves an
original back to the folder it came from, only while it is still only where
the run put it; and for tags written in place, writes back the revision the
run kept, only while the file still holds what the run wrote. Anything moved
or changed since is left alone and listed. With `--apply` it saves what it
did as `music-rollback-<run id>-<time>.json` beside the record. A dry run's
record is refused (it wrote nothing). An original moved back into the input
folder is taken in again by the next new-music run: set the
`flags.deejay_cog.new_music` flag to `off` first if that is not wanted.

---

## Failure handling

| What failed | What happens |
|-------------|--------------|
| One file (upload, ingest, move) | Contained. Counted, the file is renamed where that helps a retry, the sweep carries on, and the run reports WARN. The message is deleted. |
| Drive's rate limit (library jobs) | The library jobs' own Drive calls (file metadata, ranged reads, revisions) go through the shared Google retry (`music_safety.drive_call`): a 429, or a 403 whose reason is `userRateLimitExceeded` or `rateLimitExceeded`, is tried again with exponential backoff (1 s doubling to 32 s, six attempts). Any other 403 is not tried again. If library-identify still cannot get through, that file and the rest of its chunk are left untried for the next run and listed as `drive_rate_limited`: the run reports WARN, not `unhandled_exception`. |
| The run itself (e.g. the drop zone or the history folder cannot be listed) | The flow raises. The worker reports ERROR and returns the message in `batchItemFailures`, so SQS redelivers it. |
| The deadline | 30 s before the function's timeout the run is stopped (`_deadline.RunOutOfTime`). One request timeout before that, the shared library stops retrying Google and Spotify calls (`run_deadline`); past it the sweep stops between files rather than count each refusal as a failure, and live history stops rather than report the refused file as unreadable. The repair pass and playlist snapshot, which come after the sets are done, do not fail the run: they stop starting jobs, leave the rest for the next run, and the WARN report says `stopped_at_deadline`. A sheet uploaded but not yet ingested is deleted (outside the deadline, so the delete is not refused), the worker reports ERROR, and the message is redelivered. |
| An unrecognised message (bad JSON, version, type or mode) | Reported once, on its first receive, then returned until it reaches the dead-letter queue, where its own alarm fires. |

Both flows are safe to run again: archived CSVs are no longer in the drop
zone, and re-sent plays are deduplicated by the API.

The library jobs stop between files at the deadline, save where they got
to, and raise `RunOutOfTime`, so the message is delivered again under the
same run id and the run carries on; a step that fails is reported as an
ERROR finding and the next step still runs. See
[Running a library job on demand](#running-a-library-job-on-demand) for
what a run that keeps stopping does to the queue.

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

Every report a queued run sends says who asked for it, when its message
carries a `request`: under **Requested**, the caller (`asked by …`, when the
API knows it) and the request id (`request …`). These are counters, so they
never make a quiet run send or change its severity. A message without a
`request` reports as before. The request is only reported: it changes
neither which run is chosen nor how messages are deduplicated.

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
