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
                               process-new-files │ ingest-live-history
```

- **watcher-cog** notices new files in the CSV drop zone or the VirtualDJ
  history folder and asks the API for a run of the matching `mode`.
- **api-kaianolevine-com** validates the mode and enqueues
  `{"type": "deejay.run", "version": 1, "payload": {"mode": "<mode>"}}`. It is
  the queue's only producer.
- **The Lambda** runs one flow per message, under the message id as its run
  id, so a run can be traced from watcher's log to its report.
- **library-backfill** is the one mode meant to run on a schedule: nightly
  ([ADR-0003](decisions/ADR-0003-on-demand-no-cadence.md), amended
  2026-11-03). What sends its message each night is wired outside this
  repo; it can also be run on demand through `POST /v1/deejay/runs` once
  the API accepts the mode.
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
| `new-music` | **new-music** | `new_music.py` | Identifies each file in the new-music input folder, sends a tagged copy named `{title}_{artist}` to the destination folder and keeps the original in processed-originals. Off by default. |
| `library-backfill` | **library-backfill** | `library_backfill.py` | Retags the music library from the catalog, in place, the next chunk of files each night from a stored position. Never renames, moves or deletes a library file. Off by default. |

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

New music intake ([ADR-0007](decisions/ADR-0007-music-tagging-redesign.md#amended-2026-11-03-owner-two-flows-and-the-safety-rules)).
It does nothing unless `NEW_MUSIC_MODE` is `dry-run` or `write`, and nothing
unless all five of its folders are set and different (see
[CONFIGURATION.md](CONFIGURATION.md#music-tagging-safety-and-new-music)).

Lists the audio files in `NEW_MUSIC_INPUT_FOLDER_ID`, at most
`NEW_MUSIC_MAX_FILES_PER_RUN` (25; 0 for no cap) a run. Per file:

1. **Not an MP3** → moved, unchanged, to needs-review (only ID3 tags are
   written).
2. **Its tags find a catalog entry** (the same lookup as retag-music) →
   identified; the new tags are the catalog's.
3. **Otherwise** (the catalog does not know it, or it has no usable title or
   artist) → sent to the fingerprint function with `"flow": "new-music"` and
   the run's mode, and left where it is. A file with a good title and artist
   that the catalog does not know is fingerprinted too, to confirm it: its
   own tags are not taken on trust. With no queue configured (a local run)
   it is left in the input folder and named as `needs_fingerprinting`.

The fingerprint function, for such a file: checks it is still in the input
folder (otherwise leaves it alone), identifies it by AcoustID and
MusicBrainz, and only when the score reaches `RETAG_MIN_CONFIDENCE` takes it
in with the same steps, tagged from the catalog if the recording is there
and from MusicBrainz's title, artist and year if not. Below the score, or
with no title or artist from MusicBrainz, the file goes to needs-review. It
uses the stricter of the message's mode and its own `NEW_MUSIC_MODE`, so
switching the flow off also stops files already queued.

**Taking an identified file in** (write mode):

1. The new tags are built; a field the file has a value for is never
   emptied, and the audio in the new bytes must be byte-identical to the
   original's (ID3v2 at the start, ID3v1, APEv2 and an appended ID3v2 at
   the end are stripped before comparing).
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
3. The tagged bytes are uploaded as a **new** file under that name into
   `NEW_MUSIC_DESTINATION_FOLDER_ID`, marked with the original's file id.
4. The copy is downloaded again and its audio checked once more.
5. The original, unchanged, is moved to `NEW_MUSIC_ORIGINALS_FOLDER_ID` —
   only if it is still in the input folder and nowhere else.

A copy left by a run that stopped between steps 3 and 5 is found by the
original's id and checked, rather than uploaded twice. Nothing is ever
deleted or overwritten; names are sanitized (no `/ \ : * ? " < > |` or
control characters).

**Audio check failure** stops the run at once: nothing is written for that
file (after an upload, the bad copy goes to the Drive trash and the original
stays in the input folder), the rest of the folder waits, and an ERROR
finding names the file. The message is not retried.

**Dry run** reads, identifies and computes everything and writes nothing to
the music folders. The report names what would happen per file, and the
would-be run record is saved in the state folder as
`new-music-<run id>.dry-run.json`, with old and new tags, new names and
destinations.

**Report:** counts for `scanned`, `identified`, `sent_to_fingerprinting`,
`needs_fingerprinting`, `needs_review`, `collisions`, `failed`,
`no_longer_in_input` (and `not_written_dry_run` in a dry run), with the files
by name. Off, the report says only that the flow is off; a missing setting
is a WARN naming it.

## library-backfill

Retags the files already in the music library from the catalog, in place
([ADR-0007](decisions/ADR-0007-music-tagging-redesign.md#amended-2026-11-03-owner-two-flows-and-the-safety-rules)).
A library file is **never renamed, moved or deleted**: VirtualDJ finds it by
its path. It does nothing unless `LIBRARY_BACKFILL_MODE` is `dry-run` or
`write`, and nothing unless `LIBRARY_ROOT_FOLDER_ID` and
`MUSIC_TAGGING_STATE_FOLDER_ID` are set and different (see
[CONFIGURATION.md](CONFIGURATION.md#library-backfill)).

**Where it has got to** lives in two JSON files in the state folder, so it
survives redeploys:

- `library-backfill-manifest.json` — the library listed once, recursively
  from `LIBRARY_ROOT_FOLDER_ID`, metadata only (nothing is downloaded):
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
cursor reaches the end, every later run reports `library_done` and does
nothing. **To start over** (files added to the library since, or another
root), move `library-backfill-manifest.json` to the Drive trash: the next
run lists the library again and starts from its first file; files whose
tags already match are read but not written. A manifest of another root
than `LIBRARY_ROOT_FOLDER_ID`, or two files of one of these names, stops
the run with a WARN until resolved.

**Per file:**

1. Its id is in `LIBRARY_BACKFILL_SKIP_FILE_IDS` → passed over, named
   (`skipped_by_setting`).
2. Not an MP3 by its listed type or name → passed over, not downloaded,
   named (`not_mp3`).
3. In the trash, or no longer in the folder the manifest lists it in →
   passed over, named (`moved_since`).
4. No usable title or artist → left as it is, named
   (`needs_fingerprinting`). Library files are **not** sent to the
   fingerprint function.
5. Not in the catalog (the same lookup as new-music) → left as it is,
   named (`not_in_catalog`).
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
(Clean, Explicit, Radio Edit, No Intro Edit, Super Clean…), and its comment
frame as a backup. Registering never changes a music file; a re-run updates
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
the run stops. With the function's 600 s timeout and three receives before
the dead-letter queue, a chunk has to fit in about three invocations;
raise `LIBRARY_BACKFILL_CHUNK_SIZE` with that in mind.

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

The first step of the nightly `library-backfill` job
([ADR-0008](decisions/ADR-0008-catalog-source-of-truth.md), decisions 5 and
16): files added to and removed from the library reach the API's library
registry. It reads metadata only and **never renames, moves, deletes or
downloads a file**. Off unless `LIBRARY_SCAN_MODE` is `dry-run` or `write`
(see [CONFIGURATION.md](CONFIGURATION.md#library-scan)). A scan that fails is
reported as an ERROR finding and the backfill's chunk runs anyway.

**The listing** is the backfill's kind of listing (breadth first, the state
folder and shortcuts left out), kept in `library-scan-listing.json` in the
state folder. A run lists for at most `LIBRARY_SCAN_LISTING_SECONDS`; an
unfinished listing is saved and the next night carries on. Only a complete
listing is compared, and the night after a comparison lists afresh.

**The comparison**, against every registry row under the library root:

- **added** — an MP3 the registry has never seen is registered
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

**Safety:** a listing with no MP3s while the registry has present files, or
one that would mark more than half of the present files not present,
changes nothing and is a WARN (`refused`): a wrong root or a Drive problem
is far likelier than the library emptying overnight. A dry run compares and
names each would-be change (`would_add`, `would_mark_back`, `would_move`,
`would_remove`) and changes nothing.

**Report:** counts for `listed`, `added`, `back`, `moved`, `removed`,
`left_for_backfill`, `unchanged` and `failed`, with every file by its path.

## library-identify

The third step of the nightly `library-backfill` job
([ADR-0008](decisions/ADR-0008-catalog-source-of-truth.md), decisions 2 and
5): the library files the registry holds as `unidentified` (new to the
library, or ones the backfill could not match) are identified, linked to
their catalog entry and written with its values. Off unless
`LIBRARY_IDENTIFY_MODE` is `dry-run` or `write` (see
[CONFIGURATION.md](CONFIGURATION.md#library-identify)).

**Each run** takes the next `LIBRARY_IDENTIFY_CHUNK_SIZE` present,
unidentified files it has not tried before. Every file tried, and its
outcome, is kept in `library-identify-tried.json` in the state folder, so a
file is not fingerprinted again every night; remove an id from it (or trash
it) to try again.

**Per file:**

1. In the trash → left to the scan, named.
2. Its tags (a UTF-8 tag marked Latin-1 is read as UTF-8) are looked up in
   the catalog with the set import's matching. A match → **linked** (below).
3. No usable title or artist, or tags the catalog does not know → sent to
   the fingerprint function (flow `library-identify`). There a confident
   AcoustID match (`RETAG_MIN_CONFIDENCE`) is looked up in the catalog, or
   made a library entry when the catalog has no song for it, and linked.
   No MusicBrainz tags are written. Below the score the file is unchanged
   and stays unidentified for the owner's page. Tags alone never make an
   entry: the fingerprint confirms them first.
4. A file left unidentified is registered with its own tags and why
   (`identify_note`): first that its tags did not identify it, then the
   fingerprint's best guess and score. The owner's unidentified-files page
   on the WCS site shows both.

**Linking** fills the entry's empty fields from the file
(`POST /v1/catalog/{id}/fill-up`), writes the catalog's owned fields as the
display rules render them (title with the file's own release label, artist,
year, genre, BPM with one decimal, label, and `TXXX:CATALOG_ID`) over the
same file, and registers it as identified with when it was synced. The
write follows the backfill's safety rules: never renamed, moved or deleted;
head revision kept; audio checked before and after; every write in
`library-identify-<run id>.json`, which `music_rollback` undoes. The comment
and every other frame are left as they are. An audio mismatch stops the run
with an ERROR finding; that file is marked tried and not retried.

**Dry run** looks up and plans, but writes no music, changes nothing in the
catalog or registry, sends nothing to the fingerprint queue and records no
file as tried. Its record is `library-identify-<run id>.dry-run.json`.

**Report:** counts for `waiting`, `tried`, `linked`, `written`,
`would_write`, `filled_up`, `to_fingerprint` and `failed`, with every file
by its path. The fingerprint function's run for a library file is reported
under `library-identify` too.

## library-reconcile

The fifth step of the nightly `library-backfill` job
([ADR-0008](decisions/ADR-0008-catalog-source-of-truth.md), step 3 and
decision 15): each linked file's owned fields are compared with what its
catalog entry says they should be, and every difference is filed under a
kind for the owner to approve. **A dry run reads only**: no music file,
catalog entry or registry row changes, only its cursor and record in the
state folder. A **write** run applies what the owner approved (below). Off
unless `LIBRARY_RECONCILE_MODE` is `dry-run` or `write` (see
[CONFIGURATION.md](CONFIGURATION.md#library-reconcile)).

**Which files:** present, identified registry rows under the library root,
in the registry's order, the next `LIBRARY_RECONCILE_CHUNK_SIZE` after
`library-reconcile-cursor.json` (its own, apart from the backfill's). A dry
run moves this cursor, so repeated runs step through the library; after the
last file the next run starts a new pass.

**What is compared:** title (with its version), artist, year, genre, BPM and
label in the file's ID3 tags, against `POST /v1/catalog/display`. The
expected title is the entry's title followed by the file's own edit label
from the registry (`Song (No Intro Edit)`). The comment and every
VirtualDJ-owned frame are left out. A `TXXX:CATALOG_ID` naming another
entry than the registry's link is listed under `other`.

**Kinds:** `case_punctuation`, `featured_moved`, `artist_joiner`,
`version_brackets`, `genre_remapped`, `genre_filled`, `year_differs`,
`bpm_rounding` (0.5 BPM or less), `bpm_differs`, `label_differs`, `fill_up`
(a file value for a field the catalog leaves empty) and `other`.

**Record:** `library-reconcile-<run id>.dry-run.json` in the state folder,
every difference under its kind (path, field, the file's value, the
catalog's); a write run's is `library-reconcile-<run id>.differences.json`.
The report counts each kind, shows a few examples of each, and names every
`other` file.

**Approvals** are given in working sessions over the dry runs and kept in
`library-reconcile-approvals.json` in the state folder:

```json
{"kinds": ["case_punctuation", "genre_remapped", "fill_up"],
 "files": {"<drive file id>": ["title"]}}
```

`kinds` approves whole kinds (any but `other`); `files` approves `other`
differences one file and field at a time (`title`, `artist`, `year`,
`genre`, `bpm`, `label`, `catalog_id`). A file that does not parse stops
the run, naming the problem; with none, a write run writes nothing.

**A write run** writes each approved difference with the catalog's value
over the same file, in place, with the backfill's safety rules: never
renamed, moved or deleted; head revision kept; audio checked before and
after; every write in `library-reconcile-<run id>.json`, which
`music_rollback` undoes. The file is then registered as synced. Approved
`fill_up` values go up to the catalog (`POST /v1/catalog/{id}/fill-up`) and
the file is not written. Differences not approved are left and counted
(`not_approved`). Genre is written; subgenre is not, as no frame for it is
decided. An audio mismatch stops the run with an ERROR finding and leaves
the cursor on that file. After a write run, a dry run over the same files
shows no approved difference left.

## library-sync

The sixth step of the nightly `library-backfill` job
([ADR-0008](decisions/ADR-0008-catalog-source-of-truth.md), step 4 and
decision 5, edit): catalog edits and display-rule changes reach the files
they affect. Off unless `LIBRARY_SYNC_MODE` is `dry-run` or `write`, and
meant to stay off until the library is reconciled (see
[CONFIGURATION.md](CONFIGURATION.md#library-sync)).

**Each run** takes the next `LIBRARY_SYNC_CHUNK_SIZE` files the API lists
as needing a sync (`GET /v1/catalog/library/files/needing-sync`): present,
linked files whose entry or the display rules changed after the catalog's
values were last written to them, or that never had them. For each, the
entry as the display rules write it out (`POST /v1/catalog/display`) is
written over the same file, in place: the title with the file's own release
label, artist, year, genre, BPM and label, and `TXXX:CATALOG_ID`. A tag
another tool changed gets the catalog's value. Genre is written; subgenre
is not. The comment and VirtualDJ's frames are left as they are. Then the
files are recorded as synced (`POST /v1/catalog/library/files/synced`),
including those that already carried the catalog's values.

The write follows the backfill's safety rules: never renamed, moved or
deleted; head revision kept; audio checked before and after; every write
in `library-sync-<run id>.json`, which `music_rollback` undoes. An audio
mismatch stops the run with an ERROR finding; that file is not marked
synced and comes up first next time. A dry run writes no music and marks
nothing synced.

**Report:** counts for `waiting`, `written`, `would_write`,
`already_in_step`, `marked_synced` and `failed`, every file by its path,
and how many files each entry's change reached.

## library-trash

The second step of the nightly `library-backfill` job
([ADR-0008](decisions/ADR-0008-catalog-source-of-truth.md), decision 7).
Off unless `LIBRARY_TRASH_MODE` is `dry-run` or `write`.

When the owner resolves a duplicate group on the WCS duplicates page, the
API marks the group's other copies trash-requested in the library registry.
**Each run** lists the present files under the root marked so
(`GET /v1/catalog/library/files?trash_requested=true`), moves each to
Drive's trash (`files.update`, `trashed=true`; recoverable, never deleted)
and marks it not present (`POST /v1/catalog/library/files/absent`). A file
already in the trash or gone from Drive is only marked not present. No file
the registry does not mark is touched. A dry run lists what it would trash
and changes nothing.

**Report:** counts for `trash_requested`, `trashed`, `would_trash`,
`already_gone`, `marked_not_present` and `failed`, every file by its path.

## library-duplicates

The last step of the nightly `library-backfill` job
([ADR-0008](decisions/ADR-0008-catalog-source-of-truth.md), decision 7).
Off unless `LIBRARY_DUPLICATES_MODE` is `dry-run` or `write`. Read-only: it
never writes, renames, moves or deletes a music file.

**Each run** reads the registry's present files under the root (leaving out
those already marked for the trash) and the catalog's title and artist for
the entries they link to (`POST /v1/catalog/display`), and links files that
share an md5 (exact copies), a catalog entry, a title and artist as the
display rules write them out (case, spacing and punctuation ignored), or a
file name in the same folder (ignoring a copy suffix such as ` (1)`).

**Kept on purpose:** files of one song with a different release label
(Clean, Explicit, Radio Edit, …) or durations more than three seconds
apart (a No Intro Edit) are different edits, never duplicates of each
other. Each linked set is split into its edits; every edit with two or
more files is a group, listing the song's other edits marked kept on
purpose with why, so the page can never send them to the trash.

A group's key is made from its files, so an unchanged library gives the
same groups. `write` sends them to the API
(`POST /v1/catalog/library/duplicates`), replacing the root's open groups;
a dry run only reports them.

**Report:** counts for `files` and `groups` (and, in write, the API's
`open_groups`, `added` and `dropped`), and every group with its reasons and
files by path.

### Run records

Every run of a music-writing flow that touches a file leaves a JSON record in
`MUSIC_TAGGING_STATE_FOLDER_ID`, named `<flow>-<run id>.json`; the run id is
the queue message id (a local run makes one up, `local-<time>-<hex>`). A
writing run saves it before and after each step, so a run stopped by the
deadline still leaves it (a redelivery of the same run carries on the same
record). Per file it holds the file id, original name, folder, old and new
tags, the action (`deliver`, `review`, `in_place`, `fingerprint`, `skip`),
its state (`planned`, `pending`, `done`, `skipped`, `failed`, `restored`),
the reason for a skip or failure, the Drive revision kept and the MD5 of the
original and written contents (in-place writes), any copy made (id, folder,
name) and any move (from, to).

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
folder is taken in again by the next new-music run: set `NEW_MUSIC_MODE` to
`off` first if that is not wanted.

---

## Failure handling

| What failed | What happens |
|-------------|--------------|
| One file (upload, ingest, move) | Contained. Counted, the file is renamed where that helps a retry, the sweep carries on, and the run reports WARN. The message is deleted. |
| The run itself (e.g. the drop zone or the history folder cannot be listed) | The flow raises. The worker reports ERROR and returns the message in `batchItemFailures`, so SQS redelivers it. |
| The deadline | 30 s before the function's timeout the run is stopped (`_deadline.RunOutOfTime`). One request timeout before that, the shared library stops retrying Google and Spotify calls (`run_deadline`); past it the sweep stops between files rather than count each refusal as a failure, and live history stops rather than report the refused file as unreadable. The repair pass and playlist snapshot, which come after the sets are done, do not fail the run: they stop starting jobs, leave the rest for the next run, and the WARN report says `stopped_at_deadline`. A sheet uploaded but not yet ingested is deleted (outside the deadline, so the delete is not refused), the worker reports ERROR, and the message is redelivered. |
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
