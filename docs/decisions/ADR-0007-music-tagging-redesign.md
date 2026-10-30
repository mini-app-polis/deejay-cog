# ADR-0007: Tag the Drive music library from the catalog, on Lambda

Date: 2026-10-29

## Status

Accepted

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
