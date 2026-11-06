# ADR-0008: The catalog is the source of truth for the music library

Date: 2026-11-05

## Status

Accepted (owner, 2026-11-05). Builds on
[ADR-0007](./ADR-0007-music-tagging-redesign.md): its safety rules hold for
every write described here. The standards the catalog is cleaned to are
[decided with the owner](#decided-with-the-owner-2026-11-05).

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

The library is one Drive folder, `LIBRARY_ROOT_FOLDER_ID`, with everything
under it. Other folders may be added later, so the registry records which
library root each file is under rather than assuming one.

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

A library entry is created only from a confident identification. A file
without usable tags is identified by fingerprint first (the fingerprint
function of ADR-0007, which looks the recording up in MusicBrainz); if that
is not confident either, the file is listed for the owner to identify in the
catalog editor, and no entry is made from it until he does.

### 3. The catalog owns how a song is written

For the fields it owns, the catalog's value is the one every linked file
carries: title (with its version), artist, year, genre, BPM and label. What
VirtualDJ owns is never written: cue points, comments, ratings, play counts,
stems, and the file's name and path.

### 4. The catalog stores a song's parts; display rules write them out

An entry holds the parts of a song separately: the title, the version (a
remix, an acoustic or live take, a session), the main artist, and the
featured artists, in order. The title and artist a file carries are not
stored; they are written out from the parts by display rules (for example
where featured artists go, how a version is bracketed, how BPM is rounded).
The rules are configuration, one set for the whole catalog.

So a change to how songs are shown is a change to a rule, not an edit to
every entry: the next sync run writes the new display to every linked file.
A change to one song is an edit to its parts. Anything that reads the
catalog (stats, summaries, the collection) uses the same rules.

### 5. Four flows keep files and catalog in step

- **Add.** A new file in the library (watcher-cog sees the Drive change) is
  identified (tags, then fingerprint, as in ADR-0007), linked to its catalog
  entry or given a new library entry, registered, and written with the
  catalog's values.
- **Remove.** A file gone from the library, or in the trash, is marked not
  present. Its catalog entry keeps its play history. An entry left with no
  plays and no present files is archived, never deleted.
- **Edit.** A change to an entry's parts, or to a display rule, is recorded
  with when it changed. A sync run writes the result to every present file
  linked to an entry it affects that has not had it yet. Catalog edits and
  rule changes are the only way owned fields change once the library is
  reconciled. Sync runs nightly: edits made during the day reach the files
  overnight. A tag edited in a file by another tool (VirtualDJ can write
  tags back) is not read back, and the next sync for that entry writes the
  catalog's value over it.
- **Fill up.** When a file has a value for a field the catalog leaves empty,
  the value goes up to the catalog. A value the catalog already has is kept,
  as the set import already does.

### 6. Getting there, in order

1. **Link.** The library backfill writes only `TXXX:CATALOG_ID` and registers
   each file it reaches. Nothing VirtualDJ shows changes. Every later step
   needs the link.
2. **Set the standard and clean the catalog.** To the standards decided
   below, the catalog's entries are split into their parts
   (version and featured artists out of the title and artist), genres are
   mapped, and duplicate entries are merged.
3. **Reconcile.** A dry run lists, for every linked file, each owned field
   where file and catalog differ, grouped by kind of change (case only, a
   genre mapping, a different year, BPM precision, and so on), plus the
   values that would go up to the catalog. The owner approves kinds of
   change, not files, by going through each dry run in a working session;
   the approved kinds are saved where the write run reads them (an
   approvals file in the music-tagging state folder), and the write run
   applies only those, with the catalog winning. A dry run keeps its own position, so repeated dry runs step
   through the whole library.
4. **Steady state.** From then on the four flows run.

### 7. Where editing and clean-up happen

- **Editing the catalog** happens on the WCS website (wcs.kaianolevine.com):
  an owner-only page to edit an entry's parts and the display rules, through
  the API's catalog edit.
- **Duplicates** are removed by the owner in Drive. The catalog only helps
  find them: a report groups the files that look like copies of one song
  (same catalog entry, checksum or tags), leaving apart the ones that are
  kept on purpose (a no-intro edit, or a clean and an explicit copy, share a
  catalog entry but are different files). A file the owner deletes goes
  through the remove flow like any other.

## Decided with the owner, 2026-11-05

1. **Genre is two-level:** about a dozen broad genres, each with optional
   subgenres (for example R&B/Soul > Contemporary R&B). Every existing
   catalog genre maps onto one; the mapping is reviewed with the owner
   before the clean-up applies it.
2. **Missing genres are filled** from the library files' own tags (mapped to
   the vocabulary), then from an outside lookup (Spotify artist genres), and
   what is left is listed for the owner to set in the editor.
3. **First display rules** (changeable later; a change reaches every file on
   the next sync):
   - Featured artists go in the artist: `Pink Sweat$ feat. Kehlani`.
   - Several featured artists, and several main artists, are joined with
     commas: `Kelly Clarkson, Dolly Parton`; `Wale feat. Usher, Rick Ross,
     Big Sean`.
   - A version is shown in parentheses: `Attention (Lash Remix)`.
   - BPM has one decimal: `95.5`.
   - Capitalisation is kept as the artist styles it (`6 foot deep`,
     `ILLENIUM`); no automatic case changes.
4. **Owned fields** are title (with version), artist, year, genre and
   subgenre, BPM and label. Nothing else is pushed to files.
5. **The comment field is preserved:** never written, and each file's
   comment is recorded in the library registry when the file is registered,
   as a backup.
6. **Each file keeps its own edit label.** Clean, explicit, radio-edit and
   no-intro copies share a catalog entry, and the registry records each
   file's edit label; its displayed title is the song's title followed by
   that label in parentheses (`Baby What You Want Me To Do (No Intro
   Edit)`), and a file with no label shows the song's title alone.

## Consequences

- The catalog becomes the one place a song's tags are corrected, and a
  correction reaches every copy of the song.
- How songs are shown can be changed for the whole library at once, by
  changing a display rule, without editing entries.
- Splitting entries into parts is the largest piece of the clean-up: the
  version and featured artists have to be pulled out of titles and artists
  that today mix them, with a review of what the split proposes.
- The library registry gives the catalog a view it lacks today: what music
  the owner has, not only what he has played.
- api-kaianolevine-com takes on a registry, library entries with no plays,
  structured song parts with display rules, change tracking on entries and
  rules, and a wider catalog edit; watcher-cog watches
  the library folder; deejay-cog gains link, reconcile and sync runs.
- VirtualDJ reloads tags it finds changed, so a sync shows up there without
  re-adding tracks.
- Every visible change to the library is reviewed before it is written:
  first as kinds of change in step 3, then as catalog edits.
- Until step 3 is done, files and catalog can disagree; the link-only write
  in step 1 does not make that worse.
