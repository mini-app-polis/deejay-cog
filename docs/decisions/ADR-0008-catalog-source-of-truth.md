# ADR-0008: The catalog is the source of truth for the music library

Date: 2026-11-05

## Status

Accepted (owner, 2026-11-05). Builds on
[ADR-0007](./ADR-0007-music-tagging-redesign.md): its safety rules hold for
every write described here. Some decisions wait on the owner; they are listed
under [Open decisions](#open-decisions).

## Context

ADR-0007 tags library files from the catalog, and its library backfill fills
only the fields a file leaves empty. Reviewing the first backfill dry runs
(2026-11-05) showed two things:

- **The catalog only knows what has been played.** It is built from play
  history, so a large share of the library (about 40% of the first 100 files)
  has no entry at all, and nothing records which files the library holds.
- **The catalog is not yet consistent.** Genre has no fixed vocabulary (the
  same music appears as `R&B`, `R&B/Soul`, `Contemporary R&B` and `Soul`),
  year and genre are often empty, some songs have more than one entry, and
  featured artists are written sometimes in the title and sometimes in the
  artist.

The owner's goal is to manage the music he has through the catalog: the
catalog holds how each song is written, the files follow it, and music is
added to and removed from the library as it is today.

## Decision

### 1. The library is managed in Drive; the catalog follows

Music is added to and removed from the library by putting files into, or
taking them out of, the library folder in Drive (or through the new-music
intake, which delivers into it). The system never deletes a library file and,
as in ADR-0007, never renames or moves one. The catalog observes the library
and follows it.

### 2. The catalog registers the library's files

api-kaianolevine-com keeps a library registry: one row per Drive file in the
library, with its Drive id, path, size, checksum, duration, the catalog entry
it is linked to, whether it is present, and when it was first and last seen.
A catalog entry is *in the library* while at least one present file links to
it. A file carries its link as `TXXX:CATALOG_ID` (ADR-0007), so the link
survives a re-scan.

A song in the library that has never been played gets a catalog entry of its
own, marked as coming from the library, with no plays. Play counts, stats and
summaries count plays only, so these entries do not appear in them.

### 3. The catalog owns how a song is written

For the fields it owns, the catalog's value is the one every linked file
carries: title (with its version), artist, year, genre, BPM and label. What
VirtualDJ owns is never written: cue points, comments, ratings, play counts,
stems, and the file's name and path.

### 4. Four flows keep files and catalog in step

- **Add.** A new file in the library (watcher-cog sees the Drive change) is
  identified (tags, then fingerprint, as in ADR-0007), linked to its catalog
  entry or given a new library entry, registered, and written with the
  catalog's values.
- **Remove.** A file gone from the library, or in the trash, is marked not
  present. Its catalog entry keeps its play history. An entry left with no
  plays and no present files is archived, never deleted.
- **Edit.** A change to an entry's owned fields is recorded with when it
  changed. A sync run writes the change to every present file linked to that
  entry that has not had it yet. Catalog edits are the only way owned fields
  change once the library is reconciled.
- **Fill up.** When a file has a value for a field the catalog leaves empty,
  the value goes up to the catalog. A value the catalog already has is kept,
  as the set import already does.

### 5. Getting there, in order

1. **Link.** The library backfill writes only `TXXX:CATALOG_ID` and registers
   each file it reaches. Nothing VirtualDJ shows changes. Every later step
   needs the link.
2. **Set the standard and clean the catalog.** The owner fixes the open
   decisions below; the catalog's entries are brought to that standard
   (genres mapped, duplicate entries merged, titles and artists normalized).
3. **Reconcile.** A dry run lists, for every linked file, each owned field
   where file and catalog differ, grouped by kind of change (case only, a
   genre mapping, a different year, BPM precision, and so on), plus the
   values that would go up to the catalog. The owner approves kinds of
   change, not files; the approved kinds are written with the catalog
   winning. A dry run keeps its own position, so repeated dry runs step
   through the whole library.
4. **Steady state.** From then on the four flows run.

Duplicate files are found and handled separately, with the owner's approval
for each kind of duplicate (a no-intro edit, or a clean and an explicit copy,
share a catalog entry but are kept).

## Open decisions

Waiting on the owner before step 2:

1. The genre vocabulary: a short fixed list and what maps to each.
2. How featured artists are written: in the title (`Song (feat. X)`) or in
   the artist (`A feat. X`).
3. BPM precision: whole numbers, or one decimal.
4. Whether the catalog owns anything beyond title, artist, year, genre, BPM
   and label (for example key).

## Consequences

- The catalog becomes the one place a song's tags are corrected, and a
  correction reaches every copy of the song.
- The library registry gives the catalog a view it lacks today: what music
  the owner has, not only what he has played.
- api-kaianolevine-com takes on a registry, library entries with no plays,
  change tracking on entries, and a wider catalog edit; watcher-cog watches
  the library folder; deejay-cog gains link, reconcile and sync runs.
- Every visible change to the library is reviewed before it is written:
  first as kinds of change in step 3, then as catalog edits.
- Until step 3 is done, files and catalog can disagree; the link-only write
  in step 1 does not make that worse.
