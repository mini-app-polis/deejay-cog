# ADR-0008: The catalog is the source of truth for the music library

Date: 2026-11-05

## Status

Accepted (owner, 2026-11-05). Builds on
[ADR-0007](./ADR-0007-music-tagging-redesign.md): its safety rules hold for
every write described here. The standards the catalog is cleaned to are
[decided with the owner](#decided-with-the-owner-2026-11-05); the rules
for approving the reconcile's differences were
[decided in its first review](#decided-with-the-owner-2026-11-07-the-first-reconcile-review);
how an owner's own edits are marked was
[decided on 2026-11-11](#decided-with-the-owner-2026-11-11-owner-edits).

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
intake, which delivers into it). As in ADR-0007, the system never renames or
moves a library file, and it removes one only when the owner picks the keeper
of a duplicate group on the WCS site (decision 7), and then only to Drive's
trash. The catalog observes the library
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

A library entry is created only from a confident identification
(amended 2026-11-09, below: a file the fingerprint finds nothing for is
identified by its own tags). A file
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
  entry or given a new library entry, and registered. Linking writes only
  the link (`TXXX:CATALOG_ID`); the reconcile writes the owned fields, the
  approved kinds only, when it reaches the file.
- **Remove.** A file gone from the library, or in the trash, is marked not
  present. Its catalog entry keeps its play history. An entry left with no
  plays and no present files is archived, never deleted.
- **Edit.** A change to an entry's parts, or to a display rule, is recorded
  with when it changed. A sync run writes the result to every present file
  linked to an entry it affects that has not had it yet. Catalog edits and
  rule changes are the only way owned fields change once the library is
  reconciled. Sync runs nightly: edits made during the day reach the files
  overnight (several times a day since 2026-11-10, a stable library is
  quiet). A sync writes only the fields whose catalog value moved since
  the file's last sync (2026-11-09, the sync writes only what moved). A tag
  edited in a file in VirtualDJ goes up to the catalog (2026-11-09, an edit
  made in VirtualDJ goes up) and from there to the song's other files.
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
   the approved kinds are saved where the write run reads them (at first
   an approvals file in the music-tagging state folder; since 2026-11-07
   the API, set on the owner's review page on the WCS site, which every run
   sends its differences to), and the write run
   applies only those, with the catalog winning. A dry run keeps its own position, so repeated dry runs step
   through the whole library.
4. **Steady state.** From then on the four flows run.

### 7. Where editing and clean-up happen

- **Editing the catalog** happens on the WCS website (wcs.kaianolevine.com):
  an owner-only page to edit an entry's parts and the display rules, through
  the API's catalog edit.
- **Duplicates** are handled on the WCS site too. A report groups the files
  that look like copies of one song (same catalog entry, checksum or tags),
  leaving apart the ones that are kept on purpose (a no-intro edit, or a
  clean and an explicit copy, share a catalog entry but are different
  files). On a duplicates page the owner picks the keeper of a group; the
  catalog merges the group's entries and links (keeping every play), and the
  other copies are moved to Drive's trash, where they stay recoverable. A
  file the owner deletes in Drive himself goes through the remove flow like
  any other.
- **Signing in** to the editor and the duplicates page uses the ecosystem's
  existing identity sign-in, with an owner-only scope for catalog writes.

## Decided with the owner, 2026-11-05

1. **Genre is two-level:** thirteen broad genres (Pop, R&B/Soul, Hip-Hop,
   Dance/Electronic, Alternative/Indie, Rock, Blues, Country, Folk &
   Acoustic, Jazz, Funk/Disco, Latin & World, Soundtrack & Seasonal), each
   with optional subgenres (for example R&B/Soul > Soul). Every existing
   catalog genre value maps onto one, by a mapping agreed with the owner;
   values that are not genres (Miscellaneous, Other) are cleared, and
   Instrumental moves to the version.
2. **Missing genres are filled** from the library files' own tags (mapped
   the same way), and what is left is listed for the owner to set in the
   editor. An outside lookup is left for a later iteration, once every
   song's identity is managed.
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
7. **Every distinct recording is its own entry.** A version (remix, acoustic,
   live, session, instrumental, extended or club mix, cover) has an entry and
   plays of its own; only release labels (clean, explicit, radio edit, no
   intro edit, original mix, remastered and the like) share the song's entry.
8. **Garbled tags are repaired on read:** a tag written in UTF-8 but marked
   as Latin-1 is read as UTF-8, and the file's tags are rewritten properly at
   its next catalog write.
9. **New-music filenames** stay `{title}_{artist}` with the main artist only;
   featured artists are added only when the name would otherwise clash with
   a file already in the library.
10. **Testing is up front, not by rollback.** Each write mode is proven by
    its tests and a dry run before it writes; after a first write run, a
    dry run of the same chunk must show every written file as already
    tagged with every other frame unchanged.
11. **Display rules live in the database** and are edited on the WCS site;
    a rule change is tracked like an entry edit.
12. **Splitting entries into parts:** clear patterns (feat., ft., featuring;
    version words such as Remix, Acoustic, Live) split automatically and are
    approved as one kind; anything ambiguous is reviewed entry by entry.
13. **Merging duplicate entries:** the most-played entry's values win, its
    empty fields fill from the others, and every merge is reviewed.
14. **Matching keeps every past spelling.** Each entry remembers every title
    and artist it has been matched under, so set imports match the current
    display or any earlier spelling, and nothing that matched stops matching.
15. **Reconcile groups differences into kinds:** case or punctuation
    only; featured artist moved; artist joiner; version brackets; genre
    remapped; genre filled where the file is empty; year (split by which
    year is older, from 2026-11-07); BPM rounding only (0.5 or less apart);
    BPM differs; label differs; a file value going up to an empty catalog
    field; anything else, reviewed file by file. Once a kind is approved,
    the catalog wins (for year, see 2026-11-07, decision 3).
16. **Adds and removes are noticed nightly,** in the same nightly run as the
    sync, by listing the library's metadata (no downloads) and comparing it
    with the registry.

## Decided with the owner, 2026-11-07 (the first reconcile review)

1. **A kind is approved only when the catalog is right for every difference
   reviewed under it.** An approved kind covers the whole library, files not
   yet reviewed included. Where the catalog is wrong, the entry is fixed in
   the catalog editor and the kind waits; a fix made later reaches the files
   through the sync.
2. **Genre is for consistency, not correctness.** The catalog's broad genre
   wins even where a song's placement is arguable, so every file carries one
   of the thirteen. Genre (and year) will later come from an outside source,
   into the catalog and from there to the files.
3. **The older year is kept.** A file with no year takes the catalog's; where
   both have one, the older wins: the catalog's is written when it is older,
   and the file keeps its own when that is older (the reconcile's
   year_filled, year_earlier and year_later kinds; only the first two are
   approved).
4. **The catalog wins for BPM rounding only** (amended 2026-11-07; the
   rounding window widened from 0.5 to under 1.5 BPM on 2026-11-08). A real
   difference (bpm_differs, 1.5 BPM or more apart) is held: the catalog
   can keep one BPM for a song whose files are versions at different
   tempos (a 90 BPM edit and the 102 BPM original), so its BPM written
   over every file was wrong. The writes made before the hold are undone
   with `python -m deejay_cog.restore_bpm`.
5. **Nothing inconsistent goes up to the catalog.** A value filled up from a
   file is taken only when it fits the catalog's standards: a genre the
   vocabulary places, and a label longer than one character (the clean and
   explicit markers some files carry are not labels).
6. **Case is held until the catalog keeps the artist's styling** (decision 3
   of 2026-11-05). The catalog's values from play history flatten all-caps
   names and remixer names (`FRENSHIP`, `RAC Remix`) and hold a few typos;
   those entries are fixed in the editor before case differences are
   approved. Approved for a while on 2026-11-07, case_punctuation flattened
   styling the catalog still lacked (`WSTRN`, `i'm so tired...`) and is
   held again; `python -m deejay_cog.restore_styling` undoes those writes.
7. **The artist joiner is held** until an act whose name contains "&" (Jack
   & Jack) is stored as one artist, not two.
8. **Anything else is approved file by file**, and a file is left out when
   approving it would lose a version or a credit, or write a value the
   catalog has wrong; its entry is fixed instead.
9. **Reviews run through `python -m deejay_cog.reconcile_admin`**, which
   groups the differences by kind and records the approvals in the state
   folder. (Superseded the same day: the approvals moved to the API and
   are set on the owner's review page on the WCS site; `reconcile_admin`
   now reads them and no longer records them.)

## Decided with the owner, 2026-11-10 (every difference closes)

1. **The goal is that every file and its entry match.** A difference left
   unapproved only means it is not decided yet; it is never the way to keep
   the file's value.
2. **An approval says which side wins.** Approved for the catalog, its value
   is written over the file (as before). Approved for the file, the file's
   value goes up to the entry as a catalog revision, the file is not
   written, and the sync carries the value to the entry's other files.
   Either way the two then match and the difference closes. Kinds and
   `other` differences can each be approved either way, one way at a time.
3. **Keeping the file's value now goes through the catalog.** Where the
   2026-11-07 decisions keep the file's value (the older year, decision 3;
   a real BPM difference, decision 4), the difference is approved for the
   file instead of being left open, so the catalog takes that value.
4. **A value the catalog cannot take stays open.** The file's side uses the
   same holds as an edit made in VirtualDJ (a title or artist that does not
   split into parts cleanly, a genre off the vocabulary, a one-character
   label): such a difference stays on the review page with the reason, for
   the entry to be fixed in the editor.
5. **A fill-up the catalog refuses is the one difference that never
   closes.** Under decision 5 of 2026-11-07 the file keeps such a value
   (the `E`/`C` clean and explicit markers, a genre off the vocabulary)
   and nothing goes up; the owner is fine with the review page not
   listing it, as long as the file keeps it (2026-11-09).

## Decided with the owner, 2026-11-10 (an approval settles on the next run)

1. **An approved difference is settled on the next reconcile run**, not
   when the run's cursor next reaches its file. Each write run first takes
   the files with an approved difference still open, up to its chunk
   size, then carries on with its cursor. A full pass over the library
   takes many runs, so an approval could otherwise wait most of a day.
2. **An approved difference a run tried and could not settle** (a refused
   fill-up, a value the API held) is not taken first again; the cursor
   still reaches it, and it does not count as open too long.

## Decided with the owner, 2026-11-10 (a stable library is quiet)

1. **A library that did not change does no work and sends nothing.** Left
   alone for months, the library jobs should read as stable: every step
   acts only on what changed, and a step with nothing to do logs its counts
   and sends no report.
2. **The reconcile compares only files that changed** since it last
   compared them: in Drive, or in their entry's display (an entry edit or
   a display-rule change). It no longer walks the whole library over and
   over. A full pass is started by hand, after a change to the reconcile's
   own rules.
3. **Changes resolve within the hour.** With idle runs this cheap, the
   library job runs several times a day rather than nightly, so an edit
   made in VirtualDJ or in the catalog editor reaches the other side the
   same hour.
4. **Refused fill-ups are not reported as work.** A fill-up the catalog
   refuses stays the file's (2026-11-07, decision 5), so the reports count
   it apart and do not list it.

## Decided with the owner, 2026-11-09 (files the fingerprint finds nothing for)

1. **A file the fingerprint finds nothing for is identified by its own
   tags.** AcoustID has no match for routine edits, covers and small
   artists, so the fingerprint never confirms those files, and nothing
   contradicts their tags. When AcoustID has no match, or none it is
   confident of, and the file's title and artist name a song (not
   "Unknown", "Various Artists", "Untitled" or a track number), the file is
   linked to the catalog's entry for them or to a library entry made from
   its tags.
2. **The link stays marked as unconfirmed.** Its registry note reads
   `identified by its own tags (<the fingerprint's note>)`, not the
   comment: the comment is VirtualDJ's (decision 3).
3. **A disagreement still goes to the owner.** A file the fingerprint names
   as another song is not linked by its tags. (Amended below: a second
   source decides first.)

## Decided with the owner, 2026-11-09 (a second opinion on a disagreement)

1. **Neither the fingerprint nor the tags wins by itself.** AcoustID's best
   recording is often another song, another version of the file's song, or
   the right song spelled or credited differently; and a file's tags can be
   wrong too (a remix tagged with the remixer as its artist, a credit left as
   "Various Artist"). Trusting either side every time throws away what the
   other catches.
2. **A second source decides, then the owner.** First, the other recordings
   AcoustID links to the same audio: when one of them is the song and
   version the tags name, the file is linked by its tags. Otherwise Spotify
   is searched for each side, and a track that is that side's song and
   version within 3 seconds of the file's own length backs it; when exactly
   one side is backed, the file is linked by that side. Both backed, neither,
   or no Spotify: the file stays for the owner, with what each source said
   in its note. A paid second fingerprint service is not used.
3. **Files held before this are tried once more.** A file already left for
   review because its fingerprint disagreed with its tags is sent to the
   fingerprint function again, once, after the untried files.

## Decided with the owner, 2026-11-09 (the sync writes only what moved)

1. **A sync writes only the fields whose catalog value moved.** Each file's
   owned fields are recorded, as the display rules wrote them out, when a
   sync reaches it (the registry's synced values). The next sync compares
   the entry's display with them and writes only the fields that differ, so
   a value the owner held in the reconcile (a BPM, a year, an artist's
   styling) survives an edit to another field of its entry. An edit to the
   held field itself is the owner's decision, and it is written.
2. **A file with no recorded values is recorded, not written.** Every file
   linked before this, and a file identify or the reconcile has just
   written (their registration clears the values), gets the entry's current
   values recorded and nothing written: the reconcile, with the owner's
   approvals, is what brings a file in step with its entry.
3. **The sync's mode stays off until the library is reconciled**, as
   before.

## Decided with the owner, 2026-11-09 (an edit made in VirtualDJ goes up)

1. **An edit made in a file reaches the catalog with no review.** The owner
   edits tags in VirtualDJ and wants that to cost him nothing more. Each
   night, before the catalog's changes come down, a file Drive saw change
   after its last sync is read. A field it holds differently from the value
   recorded at that sync, while its entry still shows the recorded value,
   goes up to the entry as a catalog revision, and the sync carries it to
   the entry's other files.
2. **When both changed, the catalog wins**, and the report names the edit
   it wrote over.
3. **What is not an edit, or cannot be taken as one, stays in the file and
   is named in the report:** a BPM within 1.5 of the recorded one, or of
   its double or half (a player re-measuring the track); a title or artist
   that does not split into the catalog's parts cleanly (an "&" between
   artists, a "with" credit); a genre the vocabulary does not place; a
   one-character label (an explicit or clean marker). A field the file
   empties is not an edit either.
4. **A rollback is not an edit.** Undoing a write puts back the file's
   earlier tags, so the rollback has the registry forget the values the
   file was synced with, and the next sync records them afresh.
5. **It runs with the sync**, under the sync's mode, and only for files
   whose values a sync has recorded: off until the library is reconciled.

### Amended 2026-11-13 (a title or artist edit waits for the owner)

Supersedes decision 1 above for the title and artist only. On 2026-11-13
VirtualDJ, whose write-tags option an update had turned back on, wrote
titles made from file names (`1973 James Blunt` for `1973_James
Blunt.mp3`) into about a hundred new files, and the sync carried a dozen of
them into the catalog, and one into another file of its entry.

1. **A title or artist a main-library file changed does not go up by
   itself.** The sync sends the recorded value in its place and names the
   change in its report. The file then differs from its entry, and the
   reconcile files that difference for the owner like any other: file wins
   sends it up, catalog wins writes the entry's value back.
2. **Genre, year, BPM and label edits still go up with no review**, as
   decided above.
3. **FX Music's files still win** (ADR-0010), and a retitle as an owner
   edit (`< >`) is still the reconcile's (decided 2026-11-11).
4. **Only a change made since the file's last sync is an edit.** The values
   the sync records for a file never synced are its entry's, not read from
   the file, so a field the file already held differently then (checked
   against its Drive revision at that sync) is not an edit and is left to
   the reconcile.

## Decided with the owner, 2026-11-10 (a stale write-back is told by its value)

1. **A changed field is stale when it holds a value the pipeline replaced
   in that file**, as any library run record's old tags show for a done,
   in-place write, however long ago. A stale field is put back with what
   the file held at its last sync, never the catalog's value, and is not
   sent up; every other change is an edit and goes up, however many files
   changed, so a large editing session in VirtualDJ is not blocked.
2. **The count limit stays only as a backstop**, `LIBRARY_SYNC_EDIT_FLOOD`
   at 1000 by default: past it the whole change is held, as before.
3. **A stale field with no safe value to put back is left and named**: no
   Drive revision from the last sync, an empty value then, or one the
   pipeline also replaced. That file's fields do not go up.

## Decided with the owner, 2026-11-09 (the steps' modes are feature flags)

1. **Each step's mode lives in the API's feature flags**, not in a setting.
   Each step has one flag, `flags.deejay_cog.STEP` (`new_music`,
   `library_scan`, `library_trash`, `library_identify`, `library_backfill`,
   `library_sync`, `library_duplicates` and `library_reconcile`), whose
   value is `off`, `dry-run` or `write`. A run reads its step's flag when
   it starts, so a change made in the API takes effect on the next run with
   no settings sync. The `*_MODE` settings are gone.
2. **It fails closed.** An API the run cannot read, a flag the API does not
   have, a flag switched off, and an empty or unknown value are all `off`
   for that run; an unknown value is logged and never turns writing on.
3. **The fingerprint function still runs in the stricter** of the message's
   mode and the flag (`library_identify` for a library file, `new_music`
   for a new-music file).
4. **Numbers and folders stay configuration.** Chunk sizes, time budgets
   and folder ids are still settings; only the modes moved.
5. **A local reconcile run's `--mode` is for that run only**; it does not
   change the flag.

## Decided with the owner, 2026-11-11 (owner edits)

A file the owner changed by hand (an intro cut, words censored) is an
owner edit: not the published recording, and not a release label.

1. **An owner edit is a version.** Like a remix, it has its own catalog
   entry and plays of its own, and its version part (for example
   `Intro Cut`, `Self Censored`) is written into the title by the display
   rules. This amends decision 7 of 2026-11-05 for owner edits: an
   owner's no-intro cut no longer shares the song's entry. Official
   release labels (clean, explicit, radio edit, and an official no-intro
   edit) still share it, as decision 6 says.
2. **Its brackets mark it.** The display rules have an owner-edit pair,
   `<` and `>`, beside the version pair: `Baby What You Want Me To Do
   <Intro Cut>`. The entry records that its version is an owner edit, and
   a `<…>` version read from a file's title means the same. Filenames keep
   leaving brackets out, so `<` and `>` never reach a file name.
3. **The owner sets it in the file's tags only,** by writing the version in
   `< >` in the title. Reconcile and intake pick it up, and the catalog
   follows: an owner-edit entry is made, or the file is linked to an
   existing one. There is no separate setting on the WCS site.
4. **The fingerprint is a hint for an owner edit.** It still runs. A match
   to a published recording is recorded as the original the edit was cut
   from and shown on the review pages; no match, or a different one, is
   expected and is never reported as wrong, held, or used to retitle or
   relink the file. An owner edit is never sent to AcoustID or any other
   third party as the official recording.
5. **Existing owner edits are marked by hand.** There is no candidate
   search: the owner retitles each one in its tags, and the next reconcile
   carries it to the catalog. Routine cuts are identified another way and
   are not covered here.

   *Amended 2026-11-12:* the owner wants no hand work in VirtualDJ, so
   candidates are proposed from the files (`deejay_cog.owner_edits`: a name
   or title that suggests an owner edit) and the titles written by a
   reviewed command. A file is marked only once the owner approves it on
   the list; the pattern alone never marks one.

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
