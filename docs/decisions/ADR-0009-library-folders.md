# ADR-0009: The library spans more than one Drive folder

Date: 2026-11-11

## Status

Accepted (owner, 2026-11-11). Builds on
[ADR-0008](./ADR-0008-catalog-source-of-truth.md), which describes the
library as one Drive folder; this ADR changes that, and nothing else in
ADR-0008. The [ADR-0007](./ADR-0007-music-tagging-redesign.md) safety rules
hold for every folder named here. How each folder's files are tagged is a
separate decision and is not settled here.

## Context

The library job manages one Drive folder, `LIBRARY_ROOT_FOLDER_ID`, listed
recursively. Every step (scan, trash, identify, backfill, sync, duplicates,
reconcile) reads that one root. The file registry already keys each file by
its Drive id and records its `library_root_id` and its path under that root,
so more than one root fits the data model. A file whose path changes under
the same root is already treated as moved, keeping its row and catalog link.

The owner keeps music in two more Drive folders, both siblings of the main
library folder (at the same level, not inside it):

- **New Music** holds the latest music. It is where new-music intake writes
  the songs it processes (`NEW_MUSIC_DESTINATION_FOLDER_ID`). Later the
  owner moves files from it into the main library folder in VirtualDJ.
- **FX Music** holds fx and bumper music for events: walk-ons, bows, awards,
  MC intros, rotation music and sound clips. Many files are the owner's own
  cuts, and a song may be in FX Music as well as in the main library. FX
  Music keeps copies of one file in several role and event folders on
  purpose. Event-specific folders are expected to move under it.

## Decision

### 1. Two libraries: the main library and FX Music

- **The main library** is the main library folder plus New Music. The two
  folders are one library; the line between them is only a logical one.
  Both are listed under the same `library_root_id`, the main library
  folder's id, so a file moved from New Music into the main folder (or
  back) keeps its Drive id, registry row, catalog link and plays, and is
  reported as moved, never as removed and added. Files in New Music are
  registered with their path under New Music, prefixed so it can't clash
  with a path in the main folder.
- **FX Music** is its own library root, with its own `library_root_id` (the
  FX Music folder's id), listed recursively like the main library.

Each folder is named by its own setting. `LIBRARY_ROOT_FOLDER_ID` keeps its
meaning. New Music is the folder new-music intake already writes to; FX
Music gets a setting of its own. With only `LIBRARY_ROOT_FOLDER_ID` set,
every step behaves exactly as today.

### 2. Every step runs on both libraries

Scan, trash, identify, backfill, sync, duplicates and reconcile run on the
main library (both of its folders) and on FX Music. Each step keeps its own
feature flag; a flag that is off leaves the step off for every root. A
safety refusal (such as the scan refusing when most present files would
disappear) is judged per root, so one root's listing can't make files of the
other look removed.

### 3. FX Music has entries of its own in the catalog

FX files are in the catalog, but never on the main library's song entries.
Identify must not link an FX file to an entry that has main-library files,
even when the fingerprint matches; an FX file gets an FX entry of its own.
In the same way, a main-library file never links to an FX entry. The
catalog has to be able to tell an FX entry from a main-library entry to
enforce this.

### 4. Duplicates

- **The main library:** duplicates are found within each of its folders and
  across the two, as they are today. Which copy to keep stays the owner's
  choice on the duplicates page; neither folder is preferred.
- **FX Music:** copies of one file inside the same folder are duplicates to
  remove, unless they are different cuts (a different length or edit).
  Copies in different FX folders are reported for the owner to decide case
  by case, never offered as duplicates to remove by default.
- A file in FX Music and a file in the main library are never duplicates of
  each other.

## Consequences

- Moving music from New Music into the main library is a move, so plays and
  catalog links survive VirtualDJ reorganizing the folders.
- Steps that read `settings.root` read a list of roots, each with the
  folders it covers; the backfill's manifest, the scan's listing and the
  reconcile's state are kept per root.
- The catalog gains a way to keep FX entries apart from main-library
  entries, and identify and the duplicates step have to respect it.
- The scan and backfill list only MP3s today. FX Music holds m4a, wav and
  opus files too; until the tagging rules for other formats are decided,
  those files are left alone.
- docs/CONFIGURATION.md and docs/PIPELINE.md describe one library folder and
  need to describe both libraries.
