# ADR-0010: Each library is tagged by a named rule profile

Date: 2026-11-11

## Status

Accepted (owner, 2026-11-11). Builds on
[ADR-0009](./ADR-0009-library-folders.md), which left how each library's
files are tagged to a separate decision; this ADR is that decision. The
[ADR-0007](./ADR-0007-music-tagging-redesign.md) safety rules and
[ADR-0008](./ADR-0008-catalog-source-of-truth.md) hold for the main library
unchanged.

## Context

ADR-0009 makes the library two libraries: the main library (the main folder
and New Music) and FX Music, the fx and bumper music for events. The main
library is tagged as ADR-0008 decides: the catalog is the source of truth,
the sync writes the catalog's displayed values over the owned fields, and
only MP3 files are tagged.

FX Music is different. Many of its files are the owner's own cuts, its
folders are roles (Walk On / Judges, Bow, Award, MC Intro) and events, and
it holds audio in more formats than MP3. The owner wants FX files tagged by
additional or different rules from the main library's.

api-deejaytools also takes uploads in many formats, and deejaytools is
expected later to become the place where FX music for specific events is
shared between users, with access control. The rules worked out for the
owner's own FX clean-up should be the first rules deejaytools uses, so
nothing is built twice.

## Decision

### 1. Rule profiles, shared in common-python-utils

Tagging rules are named **rule profiles**: `main`, `fx`, and later others
(such as one for routine cuts). A profile says which fields are written to
files, which display rules write them out, and whether the file or the
catalog wins when they differ. The profile model, and the code that applies
it, live in common-python-utils, next to the tag reading and writing, so
deejay-cog and api-deejaytools share both.

Each app chooses which profile applies to what. deejay-cog maps its library
folders to profiles in its own configuration: the main library (the main
folder and New Music) uses `main`, FX Music uses `fx`. deejaytools will map
event uploads to profiles in its own data.

The `main` profile is the main library's rules as they are today (ADR-0008):
the catalog wins, and the owned fields are written.

### 2. Every audio format the shared tagger supports, and no conversion

Tag reading and writing for more formats than MP3 is built once, in
common-python-utils, next to the existing MP3 tagging. A file in a format
the shared tagger supports is tagged; a file it cannot tag is reported and
left alone. A file's audio format is never converted. Partial downloads
(`.part`) and files that are not audio (documents, spreadsheets) are
ignored.

The main library stays MP3-only until a decision says otherwise; the `fx`
profile covers every supported format.

### 3. The fx profile: the file wins

For FX Music the file's tags are the source of truth: the catalog takes its
values from an FX file's tags, never the reverse. FX files keep their own
catalog entries (ADR-0009).

### 4. Not decided yet: role and event tags

The owner leans towards two FX tags, neither settled:

- a **role** tag taken from the file's folder (such as Walk On / Judges,
  Bow, Award, MC Intro), written to a field VirtualDJ shows, such as
  Grouping, while the genre keeps the song's real genre;
- an **event**, either as a tag from the event folder's name (such as
  `2025.11 DCSX`), or by generating VirtualDJ playlist files per event and
  role, which would also reduce the copies kept in several folders.

Until they are decided, the `fx` profile writes neither. Event features that
deejaytools will own later are not built here; the owner's personal rules
stay simple.

## Consequences

- Multi-format tagging and the profile model are built first, in
  common-python-utils; deejay-cog's folder-to-profile configuration and the
  `fx` profile's behaviour follow and depend on them.
- FX Music's files can be in formats other than MP3, so the FX scan lists
  every format the shared tagger supports, while the main library's listing
  stays MP3-only.
- Because FX files win over the catalog, the reconcile and sync treat a
  difference on an FX file as the file's value going up to its fx entry,
  and the sync never writes catalog values over an FX file.
- api-deejaytools can adopt the same profiles and tagger when it takes FX
  uploads, without a second implementation.
- The role and event tags need a decision before the `fx` profile writes
  them.

## Amended 2026-11-11 (owner): the reconcile over FX Music

### Context

Decision 3 makes an FX file's tags the source of truth, and the
consequences say the reconcile treats an FX difference as the file's value
going up to its fx entry. The reconcile's approvals are by kind and cover
every file, and the review page exists to choose which side wins. Running
the reconcile once per library root (ADR-0009) needed a rule for FX files
so that an approval for the catalog never writes catalog values over one.

### Decision

1. **Every FX difference goes up to the file's fx entry, with no
   approval.** The file wins, so there is nothing to choose: FX differences
   are not filed for review and never appear on the review page, and the
   approvals apply to the main library only.
2. **Nothing changes silently.** The run report lists each value sent up,
   by file and field.
3. **The reconcile's flag applies to FX too.** In a dry run nothing is
   sent up and the report says what would be, so the first FX pass is read
   before it writes.
4. **FX files are real recordings, kept apart only for duplicates.** Each
   has its own fx entry; the two libraries are separate so that duplicates
   are never matched, grouped or resolved across them. An fx entry does not
   link to the main library's entry for the same recording.

### Consequences

- The reconcile, backfill and sync run once per root; for FX the backfill
  and sync never write catalog values over a file, and the reconcile only
  sends values up.
- A wrong tag on an FX file reaches its fx entry. Plays never land on an fx
  entry and the main library never links to one, so the mistake stays with
  that file, and is fixed in the file's tags.
