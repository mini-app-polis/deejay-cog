"""The ``library-identify`` step of the library job: identify the
library files the registry holds as unidentified and link each confident
one to its catalog entry, writing only the link to the file; the reconcile
brings its owned fields in step (ADR-0008, decisions 2 and 5).

The library scan registers a file new to the library as ``unidentified``,
and the backfill does the same for a file it could not match. Each run
this step takes the next ``LIBRARY_IDENTIFY_CHUNK_SIZE`` of them (default
20) that it has not tried before:

1. **By its tags.** The file's tags are read (a tag written in UTF-8 but
   marked Latin-1 is read as UTF-8) and looked up in the catalog with the
   set import's own matching. A match is a starting point, not the final
   word (ADR-0007, amended 2026-11-11): the file is sent to the fingerprint
   function with the entry its tags found as the one expected, and nothing
   is linked here. There a confident match to the same song links it to
   that entry; a confident match to another song gets the second opinion
   (:mod:`deejay_cog.second_opinion`); and no fingerprint result (no
   match, none confident, audio it cannot read) links it to that entry by
   its tags, the registry note saying the fingerprint did not confirm it.
2. **By its fingerprint** otherwise: a file with no usable title or artist,
   or one whose tags the catalog does not know, goes to the fingerprint
   function's queue (flow ``library-identify``). There a confident
   AcoustID match is looked up in the catalog, or made a library entry
   (``POST /v1/catalog/library/entries``) when the catalog has no song for
   it, and then linked as below. A file still unidentified is registered
   with its own tags and why (``identify_note``: the tags' miss, then the
   fingerprint's best guess and score), for the owner's page.
3. **By its own tags** when the fingerprint finds nothing: a file
   AcoustID has no match for, or none it is confident of, is made a library
   entry from its own title, artist and year (:func:`link_by_own_tags`),
   when they name a song (an artist that is not "Unknown" or "Various", a
   title that is not "Untitled" or a track number). Nothing contradicts
   them, and the fingerprint never will. A file the fingerprint names as
   another song stays for the owner (ADR-0008, decided 2026-11-09).

**A file the owner unlinked** (:mod:`deejay_cog.unlink`; its note starts
``UNLINKED_BY_OWNER``) is never taken here: the identification it had was
wrong, and its fingerprint would link it back. It is counted
(``left_for_owner``) and waits for the owner's unidentified-files page.

**An owner edit** (a version in ``< >`` in its title, "Song <Intro Cut>": a
cut the owner made himself) is linked only to an owner-edit entry, never to
the published song's: by its tags when the catalog has one, otherwise to an
owner-edit entry made from its own title, artist and year. Its tags are the
owner's choice, so the fingerprint never identifies it: once it is linked it
is sent to the fingerprint function for a hint only, the published
recording it was cut from, which changes nothing else (ADR-0008, decided
2026-11-11, decision 4).

**Each library root** (ADR-0009): the main library's unidentified files are
tried first, then FX Music's. **An FX Music file** links only to an fx
entry, never to a main-library one, however well its tags or fingerprint
match it, and a main-library file never links to an fx entry (ADR-0009,
decision 3). The catalog lookup searches main entries only, so an FX file
is not looked up or fingerprinted: the catalog is asked for the fx entry of
its own title and artist (``POST /v1/catalog/library/entries`` with library
``fx``), which it finds or makes. Its tags win over the catalog (ADR-0010),
so, as for any file, only the link is written to it. An FX file may be in
any format the shared tagger supports; it is read through that tagger, and
its link is written in its format's own field (:mod:`deejay_cog.audio_formats`).
An FX file whose tags name no song, or that the tagger cannot tag, is left
for the owner. A link the catalog refuses as one to
the other library (``OtherLibraryError``) is reported as not linkable, and
the run carries on.

**Linking a file** (:func:`link_and_write`): the file's values go up to the
catalog's empty fields (``POST /v1/catalog/{id}/fill-up``); only the link
(``TXXX:CATALOG_ID``) is written to the file in place, and every owned field
is left to the reconcile, which writes the approved kinds when it reaches
the file (ADR-0008); and the file is registered as identified, with its link, edit label, comment backup and
when it was synced. The write follows ADR-0007: never renamed, moved or
deleted, the head revision kept, the audio checked before and after, and
every write in the run record (``library-identify-<run id>.json``), which
``python -m deejay_cog.music_rollback <run id>`` undoes.

**Tried once.** Every file this step has tried, and with what outcome, is in
``library-identify-tried.json`` in the state folder, so a file is not
fingerprinted again every run. To try a file again, remove its id from
that file (or trash the file to try them all). The one exception: a file
sent to fingerprinting and left for review because its fingerprint
disagreed with its tags, and not marked ``second_opinion``, is tried once
more, after the untried files, so the fingerprint function's second opinion
(:mod:`deejay_cog.second_opinion`) is taken on it; it is then marked, and
not tried again, unless its note says Spotify could not be asked (no client,
or a failed search): the second opinion was not taken, so it is tried again
on later runs, after the untried files, until it is. A file skipped because
its own tags named no song (an FX Music file or an owner edit, outcomes
:data:`TAGS_MISSED`) is tried again once its md5 in the registry changes,
so tags the owner fixes (by hand or with :mod:`deejay_cog.fx_untagged`) are
taken in on the next run after the scan sees the new file.

**Safety:** ``flags.deejay_cog.library_identify`` is ``off`` unless ``dry-run`` or
``write``. A dry run reads and looks up, and names what it would link,
write and send, but writes no music, changes nothing in the catalog or
registry, sends nothing to the fingerprint queue, and does not record the
files as tried, so the same files are looked at again.
"""

from __future__ import annotations

import os
import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import monotonic
from typing import Any, Protocol, cast

from mini_app_polis import logger as logger_mod
from mini_app_polis.api import OtherLibraryError
from mini_app_polis.api.contract import (
    CatalogDisplay,
    CatalogDisplayRequest,
    CatalogFillUp,
    CatalogFillUpResult,
    CatalogLookupRequest,
    CatalogMatch,
    IngestTrack,
    LibraryEntryCreate,
    LibraryEntryCreated,
    LibraryFile,
    LibraryFileRegister,
    LibraryFileTags,
)
from mini_app_polis.deadline import without_run_deadline
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI

from deejay_cog._pipeline_eval import REPO, post_run_finding, run_report
from deejay_cog._shared_deadline import shared_deadline_passed
from deejay_cog._version import __version__
from deejay_cog.api_client import api_client
from deejay_cog.audio_formats import untaggable
from deejay_cog.fingerprint_jobs import FingerprintJobs, fingerprint_jobs_from_env
from deejay_cog.library_backfill import (
    BackfillSettings,
    LibraryRoot,
    StateFile,
    StateProblem,
)
from deejay_cog.music_safety import (
    AudioChanged,
    FlowMode,
    RunRecord,
    TagEmptied,
    file_meta,
    flow_mode,
    is_rate_limited,
    local_run_id,
    md5,
    write_in_place,
)
from deejay_cog.music_tags import (
    comment_text,
    duration_secs,
    edit_label,
    ingest_track,
    is_owner_edit,
    owner_edit_fits,
    read_tag_row,
    write_owned_fields,
)
from deejay_cog.second_opinion import NO_SPOTIFY

log = logger_mod.get_logger()

FLOW_NAME = "library-identify"

#: The feature flag holding this step's mode: off | dry-run | write; a
#: missing flag or an unknown value is off.
MODE_FLAG = "flags.deejay_cog.library_identify"

#: Unidentified files tried per run; a whole number of at least 1.
CHUNK_SIZE_VAR = "LIBRARY_IDENTIFY_CHUNK_SIZE"
DEFAULT_CHUNK_SIZE = 20

TRIED_NAME = f"{FLOW_NAME}-tried.json"

#: The tried entry's mark for a file sent again for a second opinion.
SECOND_OPINION = "second_opinion"

#: What a file's note says when its fingerprint disagreed with its tags:
#: the fingerprint function's wording, and a rolled-back link's.
_DISAGREED = ("disagrees with the file's own tags", "the file says")

#: Registry rows asked for per page.
REGISTRY_PAGE = 500


class Catalog(Protocol):
    """The parts of the API client identifying and linking a file calls."""

    def list_library_files(self, **kwargs: Any) -> list[LibraryFile]:
        """One page of registered files."""
        ...

    def lookup_catalog(
        self, payload: CatalogLookupRequest
    ) -> list[CatalogMatch | None]:
        """The catalog entry each track is a play of."""
        ...

    def read_catalog_display(
        self, payload: CatalogDisplayRequest
    ) -> list[CatalogDisplay | None]:
        """Each entry's owned fields as a file should carry them."""
        ...

    def fill_up_catalog_entry(
        self, catalog_id: str | uuid.UUID, payload: CatalogFillUp
    ) -> CatalogFillUpResult:
        """Fill the entry's empty fields from a file."""
        ...

    def register_library_file(self, payload: LibraryFileRegister) -> LibraryFile:
        """Register a library file, or bring its row up to date."""
        ...

    def create_library_entry(self, payload: LibraryEntryCreate) -> LibraryEntryCreated:
        """The song's entry, made from the library when the catalog lacks it."""
        ...


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def chunk_size_setting() -> int | str:
    """The chunk size as the worker logs it: the number, or the raw text
    when it will not parse."""
    raw = os.getenv(CHUNK_SIZE_VAR, "").strip()
    if not raw:
        return DEFAULT_CHUNK_SIZE
    try:
        return int(raw)
    except ValueError:
        return raw


# ---------------------------------------------------------------------------
# Linking one file
# ---------------------------------------------------------------------------


def fill_up_values(tags: dict[str, str]) -> CatalogFillUp | None:
    """The file's year, genre, BPM and label, for the catalog's empty
    fields; None when it has none of them."""
    values: dict[str, Any] = {}
    year = tags.get("year", "")
    if year.isdigit() and 1000 <= int(year) <= 9999:
        values["release_year"] = int(year)
    if tags.get("genre"):
        values["genre"] = tags["genre"]
    try:
        bpm = float(tags.get("bpm") or 0)
    except ValueError:
        bpm = 0.0
    if bpm > 0:
        values["bpm"] = bpm
    if tags.get("label"):
        values["label"] = tags["label"]
    return CatalogFillUp(**values) if values else None


@dataclass
class Linked:
    """What linking one file did, or in a dry run would do."""

    catalog_id: str
    written: bool
    would_write: bool
    filled: list[str] = field(default_factory=list)


#: How the registry marks a file linked by its own tags, no fingerprint
#: having confirmed them.
TAGS_NOTE = "identified by its own tags"

#: How the registry marks a file whose tag match the fingerprint confirmed.
CONFIRMED_NOTE = "identified by its own tags, confirmed by the fingerprint"

#: How the registry marks an owner edit linked by its own tags.
OWNER_EDIT_NOTE = "an owner edit, identified by its own tags"

#: How the registry marks an FX Music file linked to its fx entry.
FX_NOTE = "an FX Music file, identified by its own tags"

#: An artist that names no one.
_NO_ARTIST = re.compile(r"^(?:unknown(?: artist)?|various(?: artists)?|va|n/?a)$", re.I)

#: A title that names no song.
_NO_TITLE = re.compile(r"^(?:untitled|unknown|track\s*\d*|\d+)$", re.I)

#: Outcomes that wait on the owner fixing the file's own tags: the file is
#: tried again once it changes (:func:`_tags_changed_since`), not only once.
TAGS_MISSED = frozenset({"fx_unnamed", "owner_edit_unnamed"})


def _squashed(text: str) -> str:
    return re.sub(r"[\W_]+", "", text).lower()


def _title_is_file_name(title: str, name: str) -> bool:
    """Whether ``title`` is only the file's name with its separators gone
    ("FindAWayBayje112 Bayje 112" for ``FindAWayBayje112_Bayje_112.mp3``):
    a tagger that copied the name, not the song's title. A one-part name
    ("Happy.mp3") can be the title itself, so only a name of several parts
    counts."""
    stem = name.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    return "_" in stem and _squashed(title) == _squashed(stem)


#: A tempo named in a title as an edit ("Hush (93 Edit)"), or at the end of a
#: file name ("Hush_TheMaras_93.mp3").
_TITLE_BPM_EDIT = re.compile(r"\b(\d{2,3}(?:\.\d+)?)\s+Edit\b", re.I)
_NAME_BPM = re.compile(r"_(\d{2,3}(?:\.\d+)?)$")


def _bpm_disagrees(tags: dict[str, str], name: str) -> bool:
    """Whether a title naming its tempo as an edit ("93 Edit") has that
    tempo disagree with the BPM tag or with the tempo the file name ends in,
    so the entry's name and its filled BPM would come from different values."""
    named = _TITLE_BPM_EDIT.search(str(tags.get("title") or ""))
    if named is None:
        return False
    stem = name.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    in_name = _NAME_BPM.search(stem)
    try:
        tag = float(tags.get("bpm") or 0)
    except ValueError:
        tag = 0.0
    others = [float(in_name.group(1))] if in_name else []
    if tag > 0:
        others.append(tag)
    return any(round(other) != round(float(named.group(1))) for other in others)


def usable_track(tags: dict[str, str], name: str = "") -> IngestTrack | None:
    """The file's own tags as a track to make a library entry from, or None
    when they do not name a song: no title or artist, an artist such as
    "Unknown" or "Various Artists", a title such as "Untitled" or
    "Track 3", a title that is only the file's ``name``, or a title naming a
    tempo ("93 Edit") that the BPM tag or the file name contradicts (held for
    review, never named or filled from either)."""
    track = ingest_track(tags)
    if track is None:
        return None
    artist = str(tags.get("artist") or "").strip()
    title = str(tags.get("title") or "").strip()
    if _NO_ARTIST.match(artist) or _NO_TITLE.match(title):
        return None
    if name and _title_is_file_name(title, name):
        return None
    if _bpm_disagrees(tags, name):
        return None
    return track


def _release_year(value: str | None) -> int | None:
    year = str(value or "").strip()[:4]
    return int(year) if len(year) == 4 and year.isdigit() else None


@dataclass(frozen=True)
class TagsLink:
    """A file linked by its own tags; in a dry run, what would be done."""

    #: The entry linked to; empty in a dry run that would make one.
    catalog_id: str
    entry_created: bool
    linked: Linked | None


def link_by_own_tags(
    drive: Any,
    api: Any,
    record: RunRecord,
    *,
    file_id: str,
    name: str,
    root: str,
    path: str,
    data: bytes,
    why: str,
    catalog_id: str | None = None,
) -> TagsLink | None:
    """Link a file the fingerprint could not identify by its own tags: to
    the catalog's entry for them, or to a library entry made from its title,
    artist and year. None when its tags do not name a song
    (:func:`usable_track`); the file is then left as it is.

    ``catalog_id``, when given, is the entry the file's tags already found
    (a tag match sent to the fingerprint to confirm): the file is linked to
    it, with no second lookup.

    ``why`` is what the fingerprint found (its note). The registry row keeps
    it as ``identified by its own tags (<why>)``, so a link no fingerprint
    confirmed stays marked as one.

    A dry run (``record.mode``) looks up but makes no entry and links
    nothing; one that would make an entry returns an empty ``catalog_id``.
    """
    created = False
    if catalog_id is None:
        tags = read_tag_row(data)
        track = usable_track(tags, name)
        if track is None:
            return None
        match = api.lookup_catalog(CatalogLookupRequest(tracks=[track]))[0]
        if match is not None:
            catalog_id = str(match.id)
        elif record.mode.writes:
            made = api.create_library_entry(
                LibraryEntryCreate(
                    title=tags["title"].strip(),
                    artist=tags["artist"].strip(),
                    release_year=_release_year(tags.get("year")),
                )
            )
            catalog_id, created = str(made.entry.id), made.created
        else:
            return TagsLink("", False, None)
    linked = link_and_write(
        drive,
        api,
        record,
        file_id=file_id,
        name=name,
        root=root,
        path=path,
        data=data,
        catalog_id=catalog_id,
        identified_by=f"own tags ({why})",
        note=f"{TAGS_NOTE} ({why})",
    )
    return TagsLink(catalog_id, created, linked)


def record_unidentified(
    api: Any, *, file_id: str, root: str, path: str, data: bytes, note: str
) -> None:
    """Keep the file's own tags and why it is still unidentified in the
    registry, for the owner's unidentified-files page. A failure here is
    logged and does not stop the run."""
    tags = {key: value or None for key, value in read_tag_row(data).items()}
    try:
        api.register_library_file(
            LibraryFileRegister(
                drive_file_id=file_id,
                library_root_id=root,
                path=path,
                identification_status="unidentified",
                tags=LibraryFileTags(**tags),
                identify_note=note,
            )
        )
    except Exception:  # noqa: BLE001 — the note is not the identification
        log.exception("library-identify: could not record why %s is unidentified", path)


def link_and_write(
    drive: Any,
    api: Catalog,
    record: RunRecord,
    *,
    file_id: str,
    name: str,
    root: str,
    path: str,
    data: bytes,
    catalog_id: str,
    identified_by: str,
    note: str | None = None,
) -> Linked:
    """Link one identified library file to ``catalog_id``: fill the entry's
    empty fields from the file, write only the link (``TXXX:CATALOG_ID``) to
    the file in place, and register the file as identified, with ``note`` as its
    ``identify_note`` when one is given (otherwise the row keeps its own).

    A dry run (``record.mode``) reads the entry and plans the write, but
    calls nothing that changes the catalog, the registry or the file.
    Raises :class:`AudioChanged` when the audio would not stay
    byte-identical (the file is unchanged, or restored).
    """
    writes = record.mode.writes
    tags = read_tag_row(data)
    filled: list[str] = []
    fill = fill_up_values(tags)
    if writes and fill is not None:
        filled = api.fill_up_catalog_entry(catalog_id, fill).filled
    label = edit_label(tags["title"])
    tagged = write_owned_fields(data, {"catalog_id": catalog_id})
    written = would_write = False
    current = data
    if tagged is not None:
        entry = write_in_place(
            drive,
            record,
            file_id=file_id,
            name=name,
            original=data,
            tagged=tagged,
            identified_by=identified_by,
        )
        written = entry.state == "done"
        would_write = entry.state == "planned"
        if written:
            current = tagged
    if writes:
        row: dict[str, Any] = {
            "drive_file_id": file_id,
            "library_root_id": root,
            "path": path,
            "size_bytes": len(current),
            "md5": md5(current),
            "duration_secs": duration_secs(current),
            "identification_status": "identified",
            "catalog_id": uuid.UUID(catalog_id),
            "edit_label": label,
            "comment_backup": comment_text(data) or None,
            "synced_at": datetime.now(UTC),
        }
        # Sent only when given: the API keeps a field a registration leaves out.
        if note is not None:
            row["identify_note"] = note
        api.register_library_file(LibraryFileRegister(**row))
    return Linked(catalog_id, written, would_write, filled)


# ---------------------------------------------------------------------------
# One run
# ---------------------------------------------------------------------------


@dataclass
class IdentifySummary:
    """What one run did, or in a dry run would do, file by file."""

    mode: FlowMode
    problems: list[str] = field(default_factory=list)
    record_name: str = ""
    waiting: int = 0
    #: Files sent to fingerprinting by an earlier run and still unidentified:
    #: the fingerprint function left them for the owner, or has not run yet.
    fingerprinted_unlinked: int = 0
    tried: int = 0
    linked: list[str] = field(default_factory=list)
    written: list[str] = field(default_factory=list)
    would_write: list[str] = field(default_factory=list)
    filled: list[str] = field(default_factory=list)
    to_fingerprint: list[str] = field(default_factory=list)
    #: Owner edits linked to an owner-edit entry made from their tags (or,
    #: in a dry run, that would be).
    owner_edits: list[str] = field(default_factory=list)
    #: Owner edits sent to the fingerprint function for a cut-from hint (or,
    #: in a dry run, that would be).
    owner_edit_hints: list[str] = field(default_factory=list)
    #: FX Music files given a new fx entry (or, in a dry run, that would be).
    fx_entries: list[str] = field(default_factory=list)
    #: Files the catalog refused to link because the entry is of the other
    #: library (main or fx).
    not_linkable: list[str] = field(default_factory=list)
    no_queue: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    #: Unidentified files the owner unlinked by hand: left for the owner.
    left_for_owner: int = 0
    #: Files left for the next run because Drive's rate limit outlasted the
    #: backoff (:func:`deejay_cog.music_safety.drive_call`).
    rate_limited: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    aborted: str = ""


#: How the note of a file the owner unlinked by hand starts
#: (:mod:`deejay_cog.unlink`): such a file is never identified again here,
#: since the identification it had was wrong. The owner links it on the
#: unidentified-files page.
UNLINKED_BY_OWNER = "unlinked by the owner: "


def unlinked_by_owner(row: LibraryFile) -> bool:
    """Whether the owner unlinked ``row`` by hand."""
    return (row.identify_note or "").startswith(UNLINKED_BY_OWNER)


def _unidentified(api: Catalog, root: str) -> list[LibraryFile]:
    rows: list[LibraryFile] = []
    offset = 0
    while True:
        page = api.list_library_files(
            library_root_id=root,
            identification_status="unidentified",
            present=True,
            limit=REGISTRY_PAGE,
            offset=offset,
        )
        rows.extend(page)
        if len(page) < REGISTRY_PAGE:
            return rows
        offset += REGISTRY_PAGE


class Identify:
    """One run over the next chunk of untried, unidentified files."""

    def __init__(
        self,
        drive: Any,
        api: Catalog,
        settings: BackfillSettings,
        record: RunRecord,
        summary: IdentifySummary,
        jobs: FingerprintJobs | None,
        tried: dict[str, Any],
        again: frozenset[str] = frozenset(),
    ) -> None:
        self.drive = drive
        self.api = api
        self.settings = settings
        self.record = record
        self.summary = summary
        self.jobs = jobs
        self.tried = tried
        self.again = again
        #: The library root of the file being tried; set per file.
        self.root: LibraryRoot = settings.roots[0]

    def _mark(self, row: LibraryFile, outcome: str) -> None:
        entry: dict[str, Any] = {"outcome": outcome, "at": _now()}
        if outcome in TAGS_MISSED:
            # Kept so a later run can tell the owner has since fixed the tags.
            entry["md5"] = row.md5
        if row.drive_file_id in self.again:
            entry[SECOND_OPINION] = True
        self.tried[row.drive_file_id] = entry

    def one(self, row: LibraryFile) -> None:
        """Try one file. Raises :class:`AudioChanged` (the run stops)."""
        summary = self.summary
        meta = file_meta(self.drive, row.drive_file_id)
        if meta.get("trashed"):
            summary.skipped.append(f"{row.path} (in the trash; the scan will mark it)")
            return
        name = str(meta.get("name") or row.path.rsplit("/", 1)[-1])
        data: bytes = self.drive.download_file_bytes(row.drive_file_id).data
        if self.root.key == "fx":
            self._fx(row, name, data)
            return
        tags = read_tag_row(data)
        track = ingest_track(tags)
        match = None
        if track is not None:
            match = self.api.lookup_catalog(CatalogLookupRequest(tracks=[track]))[0]
        if match is not None and not owner_edit_fits(tags["title"], match):
            match = None
        # A main-library file never links to an fx entry (ADR-0009).
        if match is not None and match.library != "main":
            match = None
        if match is None and is_owner_edit(tags["title"]):
            self._owner_edit(row, name, data)
            return
        if match is None:
            self._fingerprint(
                row,
                name,
                "no usable title or artist" if track is None else "not in the catalog",
                data,
            )
            return
        if not match.owner_edit:
            # A tag match is a starting point, not the final word: the
            # fingerprint checks it before anything is linked (ADR-0007,
            # amended 2026-11-11).
            self._fingerprint(row, name, "", data, expected=match)
            return
        # An owner-edit entry: the owner's tags identify it (ADR-0008,
        # decided 2026-11-11), and the fingerprint is asked for a hint only.
        linked = link_and_write(
            self.drive,
            self.api,
            self.record,
            file_id=row.drive_file_id,
            name=name,
            root=self.root.root_id,
            path=row.path,
            data=data,
            catalog_id=str(match.id),
            identified_by="tags + catalog",
        )
        label = f"{row.path} -> {match.artist} - {match.title}"
        summary.linked.append(label)
        if linked.written:
            summary.written.append(row.path)
        if linked.would_write:
            summary.would_write.append(row.path)
        if linked.filled:
            summary.filled.append(f"{row.path} ({', '.join(linked.filled)})")
        self._mark(row, "linked")
        self._hint(row, name)

    def _owner_edit(self, row: LibraryFile, name: str, data: bytes) -> None:
        """Take in an owner edit the catalog has no owner-edit entry for: an
        entry is made from its own tags (the API reads the ``< >`` version
        as an owner edit) and the file is linked to it. Its tags are the
        owner's own choice, so the fingerprint never identifies it; once
        linked, it is sent for a cut-from hint (:meth:`_hint`)."""
        summary = self.summary
        tags = read_tag_row(data)
        if usable_track(tags, name) is None:
            note = "an owner edit whose tags do not name a song: set its artist"
            summary.skipped.append(f"{row.path} ({note})")
            if self.mode_writes:
                record_unidentified(
                    self.api,
                    file_id=row.drive_file_id,
                    root=self.root.root_id,
                    path=row.path,
                    data=data,
                    note=note,
                )
                self._mark(row, "owner_edit_unnamed")
            return
        made = self.api.create_library_entry(
            LibraryEntryCreate(
                title=tags["title"].strip(),
                artist=tags["artist"].strip(),
                release_year=_release_year(tags.get("year")),
                preview=not self.mode_writes,
            )
        )
        if made.entry.library != "main":
            summary.not_linkable.append(
                f"{row.path} (the catalog answered with an {made.entry.library} "
                "entry; nothing linked)"
            )
            if self.mode_writes:
                self._mark(row, "other_library")
            return
        if not made.entry.owner_edit:
            summary.failed.append(
                f"{row.path} (the catalog did not take {tags['title']!r} as an "
                "owner edit; nothing linked)"
            )
            if self.mode_writes:
                self._mark(row, "owner_edit_refused")
            return
        entry = made.entry
        if not self.mode_writes:
            summary.owner_edits.append(f"{row.path} -> {entry.artist} - {entry.title}")
            return
        linked = link_and_write(
            self.drive,
            self.api,
            self.record,
            file_id=row.drive_file_id,
            name=name,
            root=self.root.root_id,
            path=row.path,
            data=data,
            catalog_id=str(entry.id),
            identified_by="own tags (owner edit)",
            note=OWNER_EDIT_NOTE,
        )
        label = f"{row.path} -> {entry.artist} - {entry.title}"
        summary.linked.append(label)
        summary.owner_edits.append(label)
        if linked.written:
            summary.written.append(row.path)
        if linked.filled:
            summary.filled.append(f"{row.path} ({', '.join(linked.filled)})")
        self._mark(row, "linked")
        self._hint(row, name)

    def _hint(self, row: LibraryFile, name: str) -> None:
        """Send a linked owner edit to the fingerprint function for a hint
        only: the published recording it was cut from, recorded there and
        nothing else (ADR-0008, decided 2026-11-11, decision 4). A dry run
        sends nothing; the hint is not the link, so a missing queue is
        named and the file stays linked."""
        summary = self.summary
        if not self.mode_writes:
            summary.owner_edit_hints.append(row.path)
            return
        if self.jobs is None:
            summary.no_queue.append(f"{row.path} (owner edit: no cut-from hint)")
            return
        self.jobs.send(
            row.drive_file_id,
            name,
            flow="library-identify",
            mode=self.record.mode.value,  # type: ignore[arg-type]
            library_root_id=self.root.root_id,
            path=row.path,
        )
        summary.owner_edit_hints.append(row.path)

    def _fx(self, row: LibraryFile, name: str, data: bytes) -> None:
        """Take in an FX Music file: it links only to an fx entry, the one
        the catalog has for its own title and artist or one made from them
        (``POST /v1/catalog/library/entries`` with library ``fx``), never
        to a main-library entry, however well its tags or its fingerprint
        match one (ADR-0009, decision 3). Its tags win over the catalog
        (ADR-0010), so it is not looked up or fingerprinted, and only the
        link is written to it."""
        summary = self.summary
        tags = read_tag_row(data)
        why_not = untaggable(data)
        if why_not is not None or usable_track(tags, name) is None:
            note = (
                f"an FX Music file the shared tagger cannot tag: {why_not}"
                if why_not is not None
                else "an FX Music file whose tags do not name a song: set its title and artist"
            )
            summary.skipped.append(f"{row.path} ({note})")
            if self.mode_writes:
                record_unidentified(
                    self.api,
                    file_id=row.drive_file_id,
                    root=self.root.root_id,
                    path=row.path,
                    data=data,
                    note=note,
                )
                self._mark(row, "fx_unnamed" if why_not is None else "fx_untaggable")
            return
        made = self.api.create_library_entry(
            LibraryEntryCreate(
                title=tags["title"].strip(),
                artist=tags["artist"].strip(),
                release_year=_release_year(tags.get("year")),
                preview=not self.mode_writes,
                library="fx",
            )
        )
        entry = made.entry
        label = f"{row.path} -> {entry.artist} - {entry.title}"
        if entry.library != "fx":
            summary.not_linkable.append(
                f"{row.path} (the catalog answered with a main-library entry for "
                "an FX Music file; nothing linked)"
            )
            if self.mode_writes:
                self._mark(row, "other_library")
            return
        if made.created or made.previewed:
            summary.fx_entries.append(label)
        if made.previewed:
            # A dry run that would make the entry has no real id to link to.
            return
        linked = link_and_write(
            self.drive,
            self.api,
            self.record,
            file_id=row.drive_file_id,
            name=name,
            root=self.root.root_id,
            path=row.path,
            data=data,
            catalog_id=str(entry.id),
            identified_by="own tags (FX Music)",
            note=FX_NOTE,
        )
        summary.linked.append(label)
        if linked.written:
            summary.written.append(row.path)
        if linked.would_write:
            summary.would_write.append(row.path)
        if linked.filled:
            summary.filled.append(f"{row.path} ({', '.join(linked.filled)})")
        if self.mode_writes:
            self._mark(row, "linked")

    def _fingerprint(
        self,
        row: LibraryFile,
        name: str,
        why: str,
        data: bytes,
        *,
        expected: CatalogMatch | None = None,
    ) -> None:
        """Send the file to the fingerprint function. ``expected`` is the
        entry its tags found, for the function to check before it links the
        file (ADR-0007, amended 2026-11-11); without one, the tags did not
        identify the file (``why``)."""
        summary = self.summary
        if expected is None:
            note = f"its tags did not identify it ({why}); sent to fingerprinting"
        else:
            song = f"{expected.artist} - {expected.title}"
            why = f"its tags match {song}; the fingerprint is to confirm it"
            note = f"its tags match {song} in the catalog; sent to fingerprinting to confirm it"
        if not self.mode_writes:
            summary.to_fingerprint.append(f"{row.path} ({why})")
            return
        record_unidentified(
            self.api,
            file_id=row.drive_file_id,
            root=self.root.root_id,
            path=row.path,
            data=data,
            note=note,
        )
        if self.jobs is None:
            summary.no_queue.append(f"{row.path} ({why})")
            return
        self.jobs.send(
            row.drive_file_id,
            name,
            flow="library-identify",
            mode=self.record.mode.value,  # type: ignore[arg-type]
            library_root_id=self.root.root_id,
            path=row.path,
            expected_catalog_id=str(expected.id) if expected is not None else None,
        )
        summary.to_fingerprint.append(f"{row.path} ({why})")
        self._mark(row, "sent_to_fingerprint")

    @property
    def mode_writes(self) -> bool:
        """Whether this run writes (``write``), rather than only planning."""
        return self.record.mode.writes


def _settings() -> tuple[BackfillSettings | None, int, list[str]]:
    settings, problems = BackfillSettings.from_env()
    size = chunk_size_setting()
    if not isinstance(size, int) or size < 1:
        problems.append(
            f"{CHUNK_SIZE_VAR}={size!r} is not a whole number of at least 1"
        )
        size = 0
    return settings, size, problems


def _identify(
    drive: Any,
    api: Catalog,
    settings: BackfillSettings,
    chunk_size: int,
    run_id: str,
    jobs: FingerprintJobs | None,
    summary: IdentifySummary,
) -> None:
    tried_store = StateFile(drive, settings.state, TRIED_NAME)
    try:
        tried_doc = tried_store.load() or {"flow": FLOW_NAME, "files": {}}
    except StateProblem as exc:
        summary.problems.append(str(exc))
        return
    tried: dict[str, Any] = dict(tried_doc.get("files") or {})
    # Every root's files (ADR-0009): the main library's, then FX Music's.
    root_of: dict[str, LibraryRoot] = {}
    unidentified: list[LibraryFile] = []
    for root in settings.roots:
        for row in _unidentified(api, root.root_id):
            if unlinked_by_owner(row):
                summary.left_for_owner += 1
                continue
            root_of[row.drive_file_id] = root
            unidentified.append(row)
    again = [
        r
        for r in unidentified
        if _second_opinion_due(r, tried) or _tags_changed_since(r, tried)
    ]
    waiting = [r for r in unidentified if r.drive_file_id not in tried] + again
    summary.waiting = len(waiting)
    summary.fingerprinted_unlinked = sum(
        1
        for r in unidentified
        if (tried.get(r.drive_file_id) or {}).get("outcome") == "sent_to_fingerprint"
    )
    chunk = waiting[:chunk_size]
    if not chunk:
        return
    record = RunRecord(
        FLOW_NAME,
        run_id,
        summary.mode,
        stage=f"{len(chunk)} of {len(waiting)} unidentified files",
        folders={"root": settings.root, "state": settings.state},
    ).attach(drive, settings.state)
    summary.record_name = record.file_name
    work = Identify(
        drive,
        api,
        settings,
        record,
        summary,
        jobs,
        tried,
        frozenset(r.drive_file_id for r in again),
    )
    status = "complete"
    try:
        for index, row in enumerate(chunk):
            if shared_deadline_passed():
                status = "stopped"
                break
            summary.tried += 1
            work.root = root_of[row.drive_file_id]
            try:
                work.one(row)
            except OtherLibraryError as exc:
                # A backstop: the entry is checked before linking, so this is
                # the catalog refusing a link the step thought was its own
                # library's. The file is not linked; any link already
                # written is in the run record, for music_rollback.
                summary.not_linkable.append(f"{row.path} ({exc})")
                work._mark(row, "other_library")
            except AudioChanged as exc:
                summary.aborted = f"{row.path}: {exc}"
                work._mark(row, "audio_changed")
                status = "aborted"
                break
            except TagEmptied as exc:
                summary.failed.append(f"{row.path} ({exc})")
            except Exception as exc:  # noqa: BLE001 — one file must not end the run
                if shared_deadline_passed():
                    status = "stopped"
                    break
                if is_rate_limited(exc):
                    # Drive still says slow down after the backoff: this
                    # file and the rest of the chunk wait for the next run
                    # (none is marked tried).
                    log.warning("library-identify: Drive rate limit on %s", row.path)
                    summary.rate_limited.extend(r.path for r in chunk[index:])
                    status = "stopped"
                    break
                log.exception("library-identify: failed on %s", row.path)
                summary.failed.append(f"{row.path} ({type(exc).__name__}: {exc})")
    finally:
        with without_run_deadline():
            if record.entries:
                record.finish(status)
            if summary.mode.writes:
                tried_store.save(
                    {"flow": FLOW_NAME, "updated_at": _now(), "files": tried}
                )
    if summary.aborted:
        _report_abort(run_id, summary)


def _tags_changed_since(row: LibraryFile, tried: dict[str, Any]) -> bool:
    """Whether a file skipped because its own tags named no song
    (:data:`TAGS_MISSED`) has changed since, so its new tags are worth
    reading: the registry's md5 differs from the one it had when tried.
    An entry from before the md5 was kept is taken as changed, once: the
    retry keeps the md5, so it is not taken again unless the file changes."""
    entry = tried.get(row.drive_file_id) or {}
    if entry.get("outcome") not in TAGS_MISSED:
        return False
    if "md5" not in entry:
        return True
    return row.md5 is not None and row.md5 != entry["md5"]


def _second_opinion_due(row: LibraryFile, tried: dict[str, Any]) -> bool:
    """Whether a file left for review because its fingerprint disagreed
    with its tags has yet to be tried with a second opinion: never tried
    for one, or tried when Spotify could not be asked."""
    entry = tried.get(row.drive_file_id) or {}
    note = row.identify_note or ""
    return (
        entry.get("outcome") == "sent_to_fingerprint"
        and any(phrase in note for phrase in _DISAGREED)
        and (not entry.get(SECOND_OPINION) or NO_SPOTIFY in note)
    )


def _report_abort(run_id: str, summary: IdentifySummary) -> None:
    try:
        post_run_finding(
            FLOW_NAME,
            "ERROR",
            "library-identify stopped: a file's audio would not stay "
            f"byte-identical ({summary.aborted}). The file is unchanged (or "
            f"restored) and will not be tried again; see {summary.record_name}.",
            run_id=run_id,
        )
    except Exception:  # noqa: BLE001 — the notification is not the job
        log.exception("library-identify: could not report the stop")


def run_identify(
    *,
    run_id: str | None,
    mode: FlowMode,
    drive: Any | None = None,
    api: Catalog | None = None,
    jobs: FingerprintJobs | None = None,
) -> IdentifySummary:
    """One run: the mode, settings, the next chunk, the record and report."""
    started_at = monotonic()
    run_id = run_id or local_run_id()
    summary = IdentifySummary(mode)
    if mode is FlowMode.OFF:
        return summary
    with run_report(
        FLOW_NAME,
        repo=REPO,
        notable=False,
        run_id=run_id,
        version=__version__,
        list_all=True,
    ) as report:
        try:
            settings, chunk_size, problems = _settings()
            if settings is None or problems:
                summary.problems.extend(problems)
                return summary
            if drive is None:
                drive = GoogleAPI.from_env().drive
            client = api or cast(
                Catalog, api_client(base_url=api_base_url().strip() or None)
            )
            if jobs is None:
                jobs = fingerprint_jobs_from_env()
            _identify(drive, client, settings, chunk_size, run_id, jobs, summary)
        finally:
            _record(report, summary, monotonic() - started_at)
    return summary


def library_identify(*, run_id: str | None = None) -> IdentifySummary:
    """Identify the next chunk of unidentified library files and report."""
    return run_identify(run_id=run_id, mode=flow_mode(MODE_FLAG))


def _record(report: Any, summary: IdentifySummary, duration_sec: float) -> None:
    report.duration_sec = duration_sec
    for problem in summary.problems:
        report.issue("setting", problem)
    if summary.problems:
        return
    if summary.mode is FlowMode.DRY_RUN:
        report.notice(
            "dry_run",
            f"{MODE_FLAG} is dry-run: no music, catalog or registry value changed, "
            f"nothing was sent to fingerprinting; record {summary.record_name or '(none)'}",
        )
    report.count("waiting", summary.waiting)
    report.count("fingerprinted_unlinked", summary.fingerprinted_unlinked)
    report.count("left_for_owner", summary.left_for_owner)
    report.count("tried", summary.tried)
    report.count("linked", len(summary.linked))
    report.count("written", len(summary.written))
    report.count("would_write", len(summary.would_write))
    report.count("filled_up", len(summary.filled))
    report.count("to_fingerprint", len(summary.to_fingerprint))
    report.count("owner_edits", len(summary.owner_edits))
    report.count("owner_edit_hints", len(summary.owner_edit_hints))
    report.count("fx_entries", len(summary.fx_entries))
    report.count("not_linkable", len(summary.not_linkable))
    report.count("failed", len(summary.failed))
    writes = summary.mode.writes
    for item in summary.linked:
        if writes:
            report.updated("library file linked", item)
        else:
            report.notice("would_link", item)
    for item in summary.written:
        report.created("tags written", item)
    for item in summary.would_write:
        report.notice("would_write", item)
    for item in summary.filled:
        report.updated("catalog filled up", item)
    for item in summary.owner_edits:
        report.notice("owner_edit" if writes else "would_link_owner_edit", item)
    for item in summary.owner_edit_hints:
        report.notice(
            "sent_for_cut_from_hint" if writes else "would_send_for_cut_from_hint",
            item,
        )
    for item in summary.fx_entries:
        if writes:
            report.created("fx entry made", item)
        else:
            report.notice("would_make_fx_entry", item)
    for item in summary.not_linkable:
        report.issue("not_linkable", item)
    for item in summary.to_fingerprint:
        report.notice("sent_to_fingerprint" if writes else "would_fingerprint", item)
    for item in summary.no_queue:
        report.issue("no_fingerprint_queue", item)
    for item in summary.skipped:
        report.notice("skipped", item)
    for item in summary.failed:
        report.issue("failed", item)
    for item in summary.rate_limited:
        report.issue("drive_rate_limited", item)
    if summary.aborted:
        report.issue("audio_changed_run_stopped", summary.aborted)
    report.ok(len(summary.linked))
