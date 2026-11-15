# ADR-0010: Each library is tagged by a named rule profile

Date: 2026-11-11

## Status

Accepted (owner, 2026-11-11); decision 4, the role and event tags, was
[decided on 2026-11-14](#amended-2026-11-14-owner-fx-roles-and-events).
Builds on
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

*Decided 2026-11-14 ([below](#amended-2026-11-14-owner-fx-roles-and-events)):
a role tag at the start of the comment, and events as generated VirtualDJ
playlists, not a tag.*

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
  them. (Decided 2026-11-14, below.)

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

## Amended 2026-11-14 (owner): FX roles and events

### Context

Decision 4 left the role and event tags as leanings. Many FX files already
carry a role the owner wrote by hand at the start of their comment, with
more `<…>` parts after it, such as `<FX><Walk On><Competitor Routine>`.
The FX folders are still being reorganized: the event-specific folders
(`EventSpecific`) are expected to move under FX Music (ADR-0009).

### Decision

1. **An FX file's role comes from its folder and goes in its comment.**
   Four folders name a role:

   | Folder           | Role         |
   |------------------|--------------|
   | `_WalkOn`        | `Walk On`    |
   | `_BowMusic`      | `Bow`        |
   | `_MCIntro`       | `MC Intro`   |
   | `_RotationMusic` | `Rotation`   |

   The role is the one of the nearest of these folders the file is under
   (so a role folder inside an event folder counts too). A file under none
   of them gets no role.
2. **It is written as `<FX><Role>` at the start of the comment, and the
   rest of the comment is kept.** The owner's hand-added `<…>` parts stay
   after it, as he wrote them; they are never rewritten, reordered or
   removed. In full:
   - a comment that already starts with `<FX><Role>` for the file's role
     is left as it is;
   - a comment that starts with `<FX>` followed by another role is left as
     it is and reported, for the owner to settle (the file may be in the
     wrong folder, or the role was set by hand on purpose);
   - a comment that starts with `<FX>` and no role gets `<Role>` after its
     `<FX>`;
   - any other comment gets `<FX><Role>` in front of it (a space between
     them when the comment does not start with `<`), and an empty comment
     becomes `<FX><Role>`.
3. **This is the one part of an FX file's comment the system writes.** The
   comment is still not a profile field: it never goes up to the catalog,
   and the reconcile and sync neither compare nor write it. The main
   library's comment stays VirtualDJ's and is never written (ADR-0008,
   decided 2026-11-05, decision 5).
4. **The role is written by a reviewed command, the safe way.** A dry run
   lists every change; the write follows ADR-0007 (revision pinned, audio
   checked, every write in a run record that `music_rollback` can undo), in
   every format the shared tagger supports. Running it again changes only
   files that are new or have moved since, so it is run again after the
   folders are reorganized.
5. **Events are generated VirtualDJ playlists, not a tag.** No event tag
   is written to any file. For each event folder under FX Music, one
   playlist is generated per role found in it, listing the event's files
   for that role, so the event's music can be played from the playlists
   rather than from more copies of the files. Generating them writes
   playlist files only, never an audio file or its tags.
6. **The personal rules stay simple.** Sharing an event's music between
   users, access control, and events as data stay with deejaytools
   (decision 1's context) and are not built here.

### Consequences

- The role rule (the comment prefix, given a role) is shared in
  common-python-utils beside the `fx` profile, so api-deejaytools can use
  it; which folder names which role is deejay-cog's configuration, as
  decision 1 has it.
- The build is three agent tasks, each small and in one repository: the
  shared role rule in common-python-utils; the reviewed role command in
  deejay-cog; and the event playlists in deejay-cog.
- The event playlists find events under FX Music, so they have nothing to
  list until `EventSpecific` has moved under it; the role tag does not
  wait for the move, but is run again after it.
- A file in two role folders is two files (ADR-0009), and each copy gets
  its own folder's role.
