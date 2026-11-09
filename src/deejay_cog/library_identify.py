"""The ``library-identify`` step of the nightly library run: identify the
library files the registry holds as unidentified, link each confident one
to its catalog entry, and write the catalog's values to it (ADR-0008,
decisions 2 and 5).

The library scan registers a file new to the library as ``unidentified``,
and the backfill does the same for a file it could not match. Each night
this step takes the next ``LIBRARY_IDENTIFY_CHUNK_SIZE`` of them (default
20) that it has not tried before:

1. **By its tags.** The file's tags are read (a tag written in UTF-8 but
   marked Latin-1 is read as UTF-8) and looked up in the catalog with the
   set import's own matching. A match is a confident identification.
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

**Linking a file** (:func:`link_and_write`): the file's values go up to the
catalog's empty fields (``POST /v1/catalog/{id}/fill-up``); the catalog's
owned fields, as the display rules write them out, are written to the file
in place, with the file's own release label kept in its title; and the file
is registered as identified, with its link, edit label, comment backup and
when it was synced. The write follows ADR-0007: never renamed, moved or
deleted, the head revision kept, the audio checked before and after, and
every write in the run record (``library-identify-<run id>.json``), which
``python -m deejay_cog.music_rollback <run id>`` undoes.

**Tried once.** Every file this step has tried, and with what outcome, is in
``library-identify-tried.json`` in the state folder, so a file is not
fingerprinted again every night. To try a file again, remove its id from
that file (or trash the file to try them all).

**Safety:** ``LIBRARY_IDENTIFY_MODE`` is ``off`` unless ``dry-run`` or
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
from mini_app_polis.api.contract import (
    CatalogDisplay,
    CatalogDisplayRequest,
    CatalogFillUp,
    CatalogFillUpResult,
    CatalogLookupRequest,
    CatalogMatch,
    IngestTrack,
    LibraryEntryCreate,
    LibraryFile,
    LibraryFileRegister,
    LibraryFileTags,
)
from mini_app_polis.deadline import without_run_deadline
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI
from mini_app_polis.pipeline_status import run_report

from deejay_cog._pipeline_eval import REPO, post_run_finding
from deejay_cog._shared_deadline import shared_deadline_passed
from deejay_cog._version import __version__
from deejay_cog.api_client import api_client
from deejay_cog.fingerprint_jobs import FingerprintJobs, fingerprint_jobs_from_env
from deejay_cog.library_backfill import BackfillSettings, StateFile, StateProblem
from deejay_cog.music_safety import (
    AudioChanged,
    FlowMode,
    RunRecord,
    TagEmptied,
    file_meta,
    flow_mode,
    local_run_id,
    md5,
    write_in_place,
)
from deejay_cog.music_tags import (
    comment_text,
    duration_secs,
    edit_label,
    ingest_track,
    read_tag_row,
    write_display_tags,
)

log = logger_mod.get_logger()

FLOW_NAME = "library-identify"

#: off | dry-run | write; unset or unknown is off.
MODE_VAR = "LIBRARY_IDENTIFY_MODE"

#: Unidentified files tried per run; a whole number of at least 1.
CHUNK_SIZE_VAR = "LIBRARY_IDENTIFY_CHUNK_SIZE"
DEFAULT_CHUNK_SIZE = 20

TRIED_NAME = f"{FLOW_NAME}-tried.json"

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

#: An artist that names no one.
_NO_ARTIST = re.compile(r"^(?:unknown(?: artist)?|various(?: artists)?|va|n/?a)$", re.I)

#: A title that names no song.
_NO_TITLE = re.compile(r"^(?:untitled|unknown|track\s*\d*|\d+)$", re.I)


def usable_track(tags: dict[str, str]) -> IngestTrack | None:
    """The file's own tags as a track to make a library entry from, or None
    when they do not name a song: no title or artist, an artist such as
    "Unknown" or "Various Artists", or a title such as "Untitled" or
    "Track 3"."""
    track = ingest_track(tags)
    if track is None:
        return None
    artist = str(tags.get("artist") or "").strip()
    title = str(tags.get("title") or "").strip()
    if _NO_ARTIST.match(artist) or _NO_TITLE.match(title):
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
) -> TagsLink | None:
    """Link a file the fingerprint could not identify by its own tags: to
    the catalog's entry for them, or to a library entry made from its title,
    artist and year. None when its tags do not name a song
    (:func:`usable_track`); the file is then left as it is.

    ``why`` is what the fingerprint found (its note). The registry row keeps
    it as ``identified by its own tags (<why>)``, so a link no fingerprint
    confirmed stays marked as one.

    A dry run (``record.mode``) looks up but makes no entry and links
    nothing; one that would make an entry returns an empty ``catalog_id``.
    """
    tags = read_tag_row(data)
    track = usable_track(tags)
    if track is None:
        return None
    match = api.lookup_catalog(CatalogLookupRequest(tracks=[track]))[0]
    created = False
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
    empty fields from the file, write the catalog's values to the file in
    place, and register the file as identified, with ``note`` as its
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
    [display] = api.read_catalog_display(
        CatalogDisplayRequest(ids=[uuid.UUID(catalog_id)])
    )
    if display is None:
        raise LookupError(f"catalog entry {catalog_id} is gone")
    label = edit_label(tags["title"])
    tagged = write_display_tags(data, display, label=label)
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
    no_queue: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    aborted: str = ""


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
    ) -> None:
        self.drive = drive
        self.api = api
        self.settings = settings
        self.record = record
        self.summary = summary
        self.jobs = jobs
        self.tried = tried

    def _mark(self, row: LibraryFile, outcome: str) -> None:
        self.tried[row.drive_file_id] = {"outcome": outcome, "at": _now()}

    def one(self, row: LibraryFile) -> None:
        """Try one file. Raises :class:`AudioChanged` (the run stops)."""
        summary = self.summary
        meta = file_meta(self.drive, row.drive_file_id)
        if meta.get("trashed"):
            summary.skipped.append(f"{row.path} (in the trash; the scan will mark it)")
            return
        name = str(meta.get("name") or row.path.rsplit("/", 1)[-1])
        data: bytes = self.drive.download_file_bytes(row.drive_file_id).data
        track = ingest_track(read_tag_row(data))
        match = None
        if track is not None:
            match = self.api.lookup_catalog(CatalogLookupRequest(tracks=[track]))[0]
        if match is None:
            self._fingerprint(
                row,
                name,
                "no usable title or artist" if track is None else "not in the catalog",
                data,
            )
            return
        linked = link_and_write(
            self.drive,
            self.api,
            self.record,
            file_id=row.drive_file_id,
            name=name,
            root=self.settings.root,
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

    def _fingerprint(self, row: LibraryFile, name: str, why: str, data: bytes) -> None:
        summary = self.summary
        if not self.mode_writes:
            summary.to_fingerprint.append(f"{row.path} ({why})")
            return
        record_unidentified(
            self.api,
            file_id=row.drive_file_id,
            root=self.settings.root,
            path=row.path,
            data=data,
            note=f"its tags did not identify it ({why}); sent to fingerprinting",
        )
        if self.jobs is None:
            summary.no_queue.append(f"{row.path} ({why})")
            return
        self.jobs.send(
            row.drive_file_id,
            name,
            flow="library-identify",
            mode=self.record.mode.value,  # type: ignore[arg-type]
            library_root_id=self.settings.root,
            path=row.path,
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
    unidentified = _unidentified(api, settings.root)
    waiting = [r for r in unidentified if r.drive_file_id not in tried]
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
    work = Identify(drive, api, settings, record, summary, jobs, tried)
    status = "complete"
    try:
        for row in chunk:
            if shared_deadline_passed():
                status = "stopped"
                break
            summary.tried += 1
            try:
                work.one(row)
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
        notable=True,
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
    return run_identify(run_id=run_id, mode=flow_mode(MODE_VAR))


def _record(report: Any, summary: IdentifySummary, duration_sec: float) -> None:
    report.duration_sec = duration_sec
    for problem in summary.problems:
        report.issue("setting", problem)
    if summary.problems:
        return
    if summary.mode is FlowMode.DRY_RUN:
        report.notice(
            "dry_run",
            f"{MODE_VAR} is dry-run: no music, catalog or registry value changed, "
            f"nothing was sent to fingerprinting; record {summary.record_name or '(none)'}",
        )
    report.count("waiting", summary.waiting)
    report.count("fingerprinted_unlinked", summary.fingerprinted_unlinked)
    report.count("tried", summary.tried)
    report.count("linked", len(summary.linked))
    report.count("written", len(summary.written))
    report.count("would_write", len(summary.would_write))
    report.count("filled_up", len(summary.filled))
    report.count("to_fingerprint", len(summary.to_fingerprint))
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
    for item in summary.to_fingerprint:
        report.notice("sent_to_fingerprint" if writes else "would_fingerprint", item)
    for item in summary.no_queue:
        report.issue("no_fingerprint_queue", item)
    for item in summary.skipped:
        report.notice("skipped", item)
    for item in summary.failed:
        report.issue("failed", item)
    if summary.aborted:
        report.issue("audio_changed_run_stopped", summary.aborted)
    report.ok(len(summary.linked))
