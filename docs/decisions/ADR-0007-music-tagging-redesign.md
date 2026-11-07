# ADR-0007: Tag the Drive music library from the catalog, on Lambda

Date: 2026-10-29

## Status

Accepted. Amended 2026-11-03 (owner): two flows and the safety rules, in
[its section](#amended-2026-11-03-owner-two-flows-and-the-safety-rules).
Amended 2026-11-07 (owner): new music is identified by its own tags,
standardized by the catalog, with no fingerprint, in
[the section at the end](#amended-2026-11-07-owner-new-music-trusts-its-own-tags-standardized-by-the-catalog).

Supersedes, once built, the retag-music parts of
[ADR-0002](./ADR-0002-production-vs-local-flow-tiers.md) (retag-music as a
local-only tier) and [ADR-0006](./ADR-0006-lambda-behind-sqs.md) (retag-music
cannot run on Lambda). Until the replacement is delivered, the existing
`retag_music.py`, its env vars and its docs stay as they are.

## Context

The purpose is to keep the tags of the music in Google Drive consistent:
the same fields, written the same way, on every file, so VirtualDJ, the
play history and the catalog all name a song alike.

`retag_music.py` was written before the API and its database existed. It
downloads each file from `MUSIC_UPLOAD_SOURCE_FOLDER_ID`, fingerprints it
with `fpcalc`, looks the fingerprint up in AcoustID and the recording in
MusicBrainz, writes tags, renames the file `{title}_{artist}`, and uploads
it to `MUSIC_TAGGING_OUTPUT_FOLDER_ID`; a low-confidence match is updated
in place. Its run report is never sent. It needs the `ffmpeg` and `fpcalc`
binaries, which the Lambda runtime does not have, so the worker cannot run
it. It cannot be migrated as it is.

VirtualDJ finds each track by its path in the Drive folder synced to the
DJ machine. Renaming or moving a file breaks that path, and with it the
track's cue points, history and playlists in VirtualDJ.

What exists now that did not then:

- **The catalog.** api-kaianolevine-com keeps every play (`tracks`, from
  set CSVs, and `live_plays`, from VirtualDJ history), and folds them into
  `track_catalog`: one row per song, keyed on normalized title and artist,
  with remix, label, genre, BPM, release year, play count and first and
  last play. `services/song_identity.py` already decides what is a version
  of a song (a remix, an acoustic or live take) and what is only a release
  label (Clean, Radio Edit, a remaster), which is the hard part of naming a
  track consistently.
- **The play history is keyed on the files' own tags.** VirtualDJ's history
  records the title and artist it read from the file, not a path. So a
  file's current tags are already the key that joins it to its plays, and
  the catalog is the source of truth for how a played song is named.

## Decision

### 1. What "consistent" means

- **Tags:** ID3v2.4, written with mutagen (pure Python), from the
  catalog row:
  - `TIT2` title and the remix, from the catalog row's title and remix,
    written in the form VirtualDJ reads back into the same Title and Remix
    columns, so the next play of the file keys to the same catalog row.
    Which frame VirtualDJ reads Remix from is confirmed in the build
    against a file VirtualDJ has tagged;
    *Amended 2026-10-29 (owner):* the remix is written in the title's
    parentheses (`Temple (Niko The Kid Remix)`, the title alone when the
    catalog has no remix). No separate remix frame is written: ID3 has no
    standard one that VirtualDJ shares;
  - `TPE1` artist, as the catalog spells it;
  - `TDRC` release year, `TCON` genre, `TBPM` BPM and `TPUB` label, when
    the catalog has them; a field the catalog lacks is left as it is;
  - `TXXX:CATALOG_ID` the catalog row's id, so a file joins the catalog
    without matching on text again;
  - every other frame left as it is (album, artwork, cue points VirtualDJ
    keeps).
- **Paths: never changed.** A file keeps its name, its folder and its
  Drive file id. Only its tags change, written back over the same Drive
  file (`files.update` with new media). There is no output folder and no
  rename.
- Considered: renaming to `Artist - Title` or the current
  `{title}_{artist}`, and copying tagged files to
  `MUSIC_TAGGING_OUTPUT_FOLDER_ID`. Rejected: both change the path
  VirtualDJ uses to find the track.

### 2. How a track is identified

The catalog is the only source of tag values for a file it knows. Two
stages, each its own function (decision 3):

- **Stage 1, by tags, the way the play history is collected:** a file is
  identified exactly as a play of it is. deejay-cog reads the file's tags
  into the same fields it reads from a VirtualDJ set export (title, remix,
  artist, label, genre, BPM, year) and builds an `IngestTrack` with the
  same `build_ingest_tracks` the set import uses. The API resolves it to a
  catalog row with the same matching the ingest's reconciliation uses
  (`display_title`, the catalog keys, the version fallback in
  `services/reconciliation.py`), moved into one function both call. Neither
  side gets a second copy of the rules, so a file and its plays can never
  disagree about which song it is.
  - A hit: the file's tags are written from the catalog row.
  - No hit, but the file has a title and artist: the file is not changed.
    It has not been played, so the catalog has nothing better; it is
    counted in the run report.
  - No usable title or artist: handed to stage 2.
- **Stage 2, by fingerprint:** `fpcalc` fingerprints the file, and
  AcoustID and MusicBrainz give a recording. Only when the AcoustID score
  reaches `RETAG_MIN_CONFIDENCE`:
  - the recording's title and artist go through the same `IngestTrack`
    and catalog lookup as stage 1, and a hit is written as in stage 1;
  - a miss is written with MusicBrainz's title, artist and year, so the
    file at least carries usable tags and joins the catalog once played.
  Below the score, the file is not changed and is listed in the run
  report to be tagged by hand. (Today's flow updates low-confidence files
  in place; that ends.)
- Considered: **confirming every match with Spotify** for album, year and
  ISRC. Rejected: the catalog already holds what consistency needs, and a
  second source would bring its own spelling.
- Considered: **AcoustID first for every file**, as today. Rejected: the
  play history already keys on the tags, most files are tagged well, and
  fingerprinting every file is slow and is what needs native binaries.

### 3. How it runs and reports on Lambda

- **Stage 1:** a `retag-music` mode in `deejay_cog.worker.MODES`, on the
  existing `deejay-jobs.fifo` queue (ADR-0006), on the managed Python
  runtime. Triggered two ways, both already in place for the other modes:
  watcher-cog when files land in `MUSIC_UPLOAD_SOURCE_FOLDER_ID`, and on
  demand through `POST /v1/deejay/runs`. No schedule (ADR-0003).
- **Stage 2: a separate function in deejay-cog**, its own
  `deejay-fingerprint-jobs.fifo` queue, sharing stage 1's catalog lookup
  and tag writer. Stage 1 sends one message per file with no usable tags.
  Kept separate so only this function carries a native binary, and so its
  longer per-file time and the AcoustID rate limit (3 requests a second)
  don't hold up stage 1; its concurrency is held at 1.
  - Considered: one function with the binary in it. Rejected: every
    `deejay-cog` mode would then carry it, for a step most runs skip.
  - Considered: a separate cog repository. Rejected: it would duplicate
    or extract the tag writer for one caller.
- Both stages: at most `MAX_UPLOADS_PER_RUN` files a run, stopping cleanly
  before the function's deadline; what is left is picked up by the next
  run. Neither deletes, moves nor renames a file.
- Each stage sends its own run report through `run_report` to the API, as
  every other mode does: counts (scanned, matched in catalog, not in
  catalog, matched by fingerprint, tagged, handed to stage 2, left
  untouched) and the untouched files by name.
- API changes: a catalog lookup that takes `IngestTrack`s, resolves each
  with the reconciliation's matching, records no play, and returns the
  matching row (or none) with the fields decision 1 writes. The existing `GET /v1/catalog` filters by substring
  and does not return label, so it is not that lookup. `retag-music` joins
  the modes `POST /v1/deejay/runs` accepts. Both go in the contract in
  common-python-utils. No new tables.

### 4. ffmpeg and fpcalc

- Stage 1 needs neither: reading and writing ID3 frames is pure Python, so
  its function stays a zip on the managed runtime.
- **Stage 2: a Lambda layer** with chromaprint's static `fpcalc` release,
  which decodes audio itself. The `cog-worker` module in
  mini-app-polis/infra builds zip functions only; this adds a `layers`
  input to it and nothing else. No code here calls `ffmpeg` directly;
  `retag_music.py` lists it only for pyacoustid's decoding, which the
  static `fpcalc` does itself, so `ffmpeg` is not in the layer.
- Considered: a container-image function. Kept as the fallback if the
  layer passes the 250 MB unzipped limit; it needs the `cog-worker` module
  to build images and an ECR repository.

## Consequences

- The flow joins the production modes; ADR-0002's local-only tier for it
  ends.
- Paths never change, so VirtualDJ keeps every track's cues, history and
  playlists. VirtualDJ shows the new tags after it rescans the files.
- Every played song's files carry the catalog's spelling, version and
  release data. Unplayed files keep their tags until first played;
  untagged files are identified by fingerprint when the match is
  confident, and surfaced when it is not.
- A changed title or artist changes how the next play of that track is
  recorded; `TXXX:CATALOG_ID` keeps it joined to its catalog row.
- Tag consistency now depends on the catalog's normalization and
  `song_identity.py`; a change there changes what the next run writes.
- `MUSIC_TAGGING_OUTPUT_FOLDER_ID` is no longer used.
- Building it touches:
  - common-python-utils: the lookup's contract and client method, and the
    `retag-music` mode;
  - api-kaianolevine-com: the reconciliation's matching moved into one
    function, and the lookup route on it;
  - deejay-cog: the tag reader, lookup and in-place writer; the
    `retag-music` mode and its run report; the stage 2 function;
  - infra: the `layers` input to `cog-worker`, the fpcalc layer, the stage
    2 function and its queue, and the source folder for watcher-cog;
  - watcher-cog: a watcher on the source folder;
  - deejay-cog, last: removing the old flow: `retag_music.py` and
     `tests/unit/test_retag_music.py` once their lookup lives in stage 2,
     `RETAG_MAX_CANDIDATES` and `MUSIC_TAGGING_OUTPUT_FOLDER_ID` from
     `.env.example`, and the retag-music notes in `README.md`,
     `docs/CONFIGURATION.md`, `docs/PIPELINE.md`, ADR-0001, ADR-0002,
     ADR-0004, ADR-0006 and `src/deejay_cog/_pipeline_eval.py`.
- Out of scope: anything beyond the tags of the Drive music library.

## Amended 2026-11-03 (owner): two flows and the safety rules

### Context

Music reaches the Drive library in two ways, and they need different
handling. Files dropped in a new-music folder are mostly new official
releases: not yet played, so not yet in the play history or the catalog,
and not yet anywhere VirtualDJ has a path, cue point or history for. Files
already in the library number in the thousands and are what VirtualDJ
plays from. And these flows write to the music the owner DJs from, so a
mistake must not be able to damage a file, and any run must be possible to
review before it acts and to undo after.

### Decision

**Two flows.**

- **New-music intake** (the `new-music` worker mode, on the same queue as
  the others, triggered by watcher-cog when files land in the input folder
  and on demand through `POST /v1/deejay/runs`). Each file is identified by
  its tags in the catalog (decision 2, stage 1); otherwise it goes to the
  fingerprint function (stage 2), whose message now says which flow sent
  it. A file with a good title and artist that the catalog does not know is
  fingerprinted to confirm it, not accepted on its own tags: new releases
  are exactly the files the catalog cannot vouch for. On a confident
  identification (the AcoustID score reaching `RETAG_MIN_CONFIDENCE`, or a
  catalog match), the file is tagged by decision 1's rules and **renamed
  `{title}_{artist}`**: its tagged bytes are uploaded as a new file into the
  destination folder, and the original is moved, unchanged, to a
  processed-originals folder. A file that cannot be identified with
  confidence, or whose new name is already taken in the destination, is
  moved unchanged to a needs-review folder. **This is the only flow that may
  rename**: these files are not in the library yet, so no VirtualDJ path
  refers to them.
- **Library backfill** (the `library-backfill` worker mode). Files already
  in the library are **never renamed, moved or deleted** — decision 1's
  reasoning stands: renaming or moving a library file breaks the path
  VirtualDJ finds it by, and with it its cue points, history and
  playlists. They are only retagged from the catalog, in place, in nightly
  chunks with a stored position so each night resumes where the last
  stopped; the chunk starts at 10 files and is raised to 200 by the owner
  once the results are trusted. A nightly schedule is a change to
  [ADR-0003](./ADR-0003-on-demand-no-cadence.md)'s on-demand stance,
  recorded there. As built:
  - The position is a manifest of the library, listed once (metadata
    only, resumable across runs) and kept in
    `MUSIC_TAGGING_STATE_FOLDER_ID`, and a cursor into it; the library is
    never rescanned to find the next chunk. Trashing the manifest is how
    the backfill is started over.
  - Only stage 1 applies: a library file with no usable title or artist
    is named in the report as needing fingerprinting and **not** sent to
    the fingerprint function, whose stage would write MusicBrainz's tags;
    a file the catalog does not know is left as it is (decision 2).
  - The backfill writes only the link: `TXXX:CATALOG_ID`. No title,
    artist, year, genre, BPM, label or comment frame changes, even when
    the file's field is empty, and every other frame is left as it is. A
    file already carrying the id is not written, so a dry run after a write
    run shows every written file as already tagged. Catalog values reach
    the file later, once the catalog is clean
    ([ADR-0008](./ADR-0008-catalog-source-of-truth.md)); new-music intake
    still writes the catalog's values.
  - A dry run does not move the cursor, so the chunk it showed is the one
    the next write run writes.
  - An audio mismatch leaves the cursor on that file, so the next run
    stops on it again rather than pass it; the owner passes it
    deliberately by listing its id in `LIBRARY_BACKFILL_SKIP_FILE_IDS`.

**Safety rules, for every flow that writes music** (`music_safety.py`):

- **A mode per flow**, read from the environment at the start of each run:
  `off`, `dry-run` or `write`. The default is `off`, and so is an unset or
  unknown value (logged). Off, the flow does nothing and reports that it is
  off. A dry run reads, identifies and computes everything, writes nothing
  to the music folders, and reports per file what it would do (old and new
  tags, new name, destination). New-music reads `NEW_MUSIC_MODE`; the
  backfill reads `LIBRARY_BACKFILL_MODE`. The fingerprint function uses the stricter of
  the sender's mode and its own.
- **The audio is never altered.** Before new tag bytes are uploaded, their
  audio — the bytes left once any ID3v2 tag (with its footer) at the start
  and any ID3v1, APEv2 or appended ID3v2 tag at the end are removed — must
  be byte-identical to the original's. After the write the file is
  downloaded and checked again. A mismatch before the write means nothing
  is uploaded; after it, the original is restored (in place: the kept
  revision's bytes; for a copy: the copy goes to the trash and the original
  never moved). Either way the file counts as failed and the run stops, with
  an ERROR finding.
- **No tag is emptied.** A field the source has no value for keeps the
  file's own.
- **Nothing is deleted or overwritten.** The only removal is Drive's
  trash, and only by rollback. A file is moved only from the folder the
  flow works on, and only while it is in that folder alone.
- **Every write is recorded.** Each run keeps a JSON run record in
  `MUSIC_TAGGING_STATE_FOLDER_ID`, named by flow and run id: per file its
  id, original name, old and new tags, the Drive revision kept, any copy
  made (id, folder, name) and any move (from, to). A writing run saves it
  before and after each step, so a run stopped by the deadline still
  leaves it; a dry run saves the would-be record, marked as one.
- **Every run can be undone.** `python -m deejay_cog.music_rollback <run id>`,
  run locally under Doppler, reads a run's record and undoes it: for
  new-music, the copy to the trash and the original back to the input
  folder; for in-place writes, the kept revision's contents back. It is a
  dry run unless given `--apply`, and leaves alone anything moved or changed
  since the run. It is a local command rather than a worker mode because
  `POST /v1/deejay/runs` carries a mode and no parameters, so it cannot be
  given a run id.
- **A missing required folder setting** means the flow reports it and does
  nothing.

### Consequences

- Decision 1's "paths: never changed" holds for the library; new-music is
  the one flow that names files, and only before they reach the library.
- Decision 3's "neither deletes, moves nor renames a file" holds for
  retag-music and the backfill; new-music copies, and moves its own input
  files to processed-originals or needs-review.
- Until the backfill replaces it, retag-music (and the fingerprint
  function's retag path) has the audio check before its write, which stops
  the run on a mismatch, but none of the other rules.
- A new release AcoustID does not know yet scores low and goes to
  needs-review, to be tagged by hand.
- The new-music settings must reach the fingerprint function as well as the
  worker, and `new-music` must join the modes the API contract accepts.

## Amended 2026-11-07 (owner): new music trusts its own tags, standardized by the catalog

### Context

The first new-music dry run (2026-11-07) identified 19 of 25 files from the
catalog and sent the other 6 to the fingerprint function, which failed: no
AcoustID application key had been set up. The owner decided AcoustID is not
needed at this phase: the music coming in is trusted to be tagged with
what it is. He also wants every file's tags standardized the same way,
whichever way it was identified.

### Decision

- A new-music file the catalog does not know is identified by **its own
  tags**. They are not written as they are: the API makes the song's
  catalog entry from them (`POST /v1/catalog/library/entries`, an entry
  with no plays), which runs the catalog's own parsing (main and featured
  artists, the version in brackets), its genre vocabulary and the display
  rules. The file is then tagged and named from that entry, exactly as a
  catalog match is. Adding music to the library adds it to the catalog
  (ADR-0008, decision 1).
- A dry run asks the same endpoint for a **preview**: the entry it would
  make, written out by the same rules, with nothing saved.
- A file with **no usable title or artist** goes to needs-review.
- **No new-music file is fingerprinted.** AcoustID is left for a later
  phase; the fingerprint function keeps its new-music path, but nothing
  sends to it, and library-identify still uses the function as before.

### Consequences

- A file whose tags are wrong becomes a wrong catalog entry. The report
  names every new entry (`would_create_entry` in a dry run,
  `created_entry` in write), so the dry run is where a bad tag is caught;
  an entry is fixed afterwards in the catalog editor, and the sync writes
  the fix to the file.
- An entry is made before the file's name is checked, so a file that then
  goes to needs-review (a name collision) still leaves its song in the
  catalog. It has no plays and no present file, as any library entry can.
- The 2026-11-03 amendment's "a new release AcoustID does not know yet
  scores low and goes to needs-review" no longer applies to new music.
