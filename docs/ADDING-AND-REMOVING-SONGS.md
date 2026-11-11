# Adding and removing songs

How a song gets into the main library, and out of it, without losing a
catalog link or a play. Every file takes the same path the library's own
files took to reach their current state
([ADR-0007, amended 2026-11-11](decisions/ADR-0007-music-tagging-redesign.md#amended-2026-11-11-owner-incoming-music-takes-the-librarys-path)):
its own tags are the starting point, and every one of the library's checks
verifies them. Nothing is overwritten or deleted; a file leaves the library
only through Drive's trash. FX Music has rules of its own
([ADR-0010](decisions/ADR-0010-tagging-rule-profiles.md)); this page is
about the main library (its own folder and New Music,
[ADR-0009](decisions/ADR-0009-library-folders.md)).

## Adding songs

**Do:** put the files in the new-music input folder
(`NEW_MUSIC_INPUT_FOLDER_ID`). That is the way in. A file put straight
into a library folder (from VirtualDJ, say) takes the same path from step 2
on, so it is safe too.

**The path every file takes:**

1. **new-music** checks the file can be taken in and puts a copy into New
   Music, its tags and audio unchanged, named from its own tags; the
   original goes to the originals folder.
2. **library-scan** registers it, unidentified.
3. **library-identify** identifies it from its tags and checks that against
   the fingerprint, then links it to its catalog entry, or makes the entry
   from its tags when the catalog has no song for it.
4. **library-duplicates** groups it with any copy the library already has.
5. **library-reconcile** files every difference between its tags and its
   entry for review on the reconcile page.
6. **library-sync** writes the catalog's values to it once the
   differences are settled.

**What happens to each file:**

| The file is | Outcome | What you do |
| --- | --- | --- |
| A song the catalog has, with no file in the library yet | Linked to the song's entry; its differences with the entry wait on the reconcile page; the sync then writes the catalog's values | Review the differences on the reconcile page |
| A song the catalog does not have | Its entry is made from its tags, standardized by the catalog's parsing, genre vocabulary and display rules, with no plays | Check the new entry in the run report; fix it in the catalog editor if needed, and the sync writes the fix to the file |
| The same song, same release, as a file the library already has | Taken into New Music and grouped with the existing file on the duplicates page; neither is trashed until you choose | On the duplicates page, keep the one you want; the other goes to Drive's trash |
| A better copy meant to replace an existing file | The same: grouped with the existing file | Keep the new copy on the duplicates page; the old one goes to Drive's trash, and the entry keeps its plays and links |
| A different release of a song the library has (Clean, Explicit, Radio Edit, an official No Intro Edit) | A second file on the song's entry, with its own release label; never grouped as a duplicate | Nothing |
| A remix or other version of a song the library has | An entry of its own | Nothing |
| Your own cut (a version in `< >` in its title) | An owner-edit entry of its own, never the published song's ([ADR-0008, 2026-11-11](decisions/ADR-0008-catalog-source-of-truth.md)) | Nothing |
| Tagged differently from its catalog entry | Its differences wait on the reconcile page | Approve the catalog's value or the file's, kind by kind or file by file |
| One whose fingerprint disagrees with its tags | Given a second opinion; when that does not settle it, left unidentified with what each source found | Identify it on the unidentified-files page |
| Identified by nothing (no catalog match, no confident fingerprint, tags that name no song) | Left unidentified, with why | Identify it on the unidentified-files page |
| Not an MP3 | Moved, unchanged, to needs-review | Nothing more: the main library takes MP3s only, and no file is converted |
| Missing a usable title or artist | Moved, unchanged, to needs-review | Fix its tags and put it in the input folder again |

## Removing songs

| You want to remove | Do | What happens |
| --- | --- | --- |
| A copy of a song (a duplicate) | On the duplicates page, keep the copy you want | library-trash moves the others to Drive's trash and the registry marks them not present; the entry and its plays stay with the kept file |
| A file, replaced by a better one | Add the better one as above, then keep it on the duplicates page | As above: the old file goes to Drive's trash; the entry keeps its plays and links |
| A whole song | Move its file to Drive's trash | The next scan marks it not present. Its catalog entry stays, with its plays and any other files, even when it has none left |

A file in Drive's trash can be restored from there; the next scan finds it
present again, still linked to its entry.

## Where to look

- The run reports of each step name every file and what was done.
- The reconcile page holds every difference waiting for you.
- The duplicates page holds every group of copies waiting for you.
- The unidentified-files page holds every file no check could identify.
- [PIPELINE.md](PIPELINE.md) describes each step in full.
