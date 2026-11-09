"""The ``library-sync`` step of the nightly library run: write catalog
edits and display-rule changes to the library files they affect (ADR-0008,
step 4 and decision 5, edit).

Once the library is reconciled, the catalog is where a song's owned fields
change: an edit on the WCS site, an edit made in a file (below), or a
change to the display rules. The API
lists the present, linked files whose entry or rules changed after the
catalog's values were last written to them, or that never had them written
(``GET /v1/catalog/library/files/needing-sync``). Each night this step takes
the next ``LIBRARY_SYNC_CHUNK_SIZE`` of them (default 50) and, for each:

1. reads its entry as the display rules write it out
   (``POST /v1/catalog/display``): the owned fields, the title with the
   file's own release label, artist, year, genre, BPM and label;
2. compares them with the values recorded at the file's last sync (its
   ``synced``) and writes, over the same file and in place, only the fields
   whose value moved (ADR-0008, 2026-11-09:
   the sync writes only what moved). A field that did not move is
   never written, so a value the file keeps on purpose (a BPM, year or
   spelling the owner held in the reconcile) survives an edit to another
   field of its entry. A field the catalog empties keeps the file's value;
3. records the file as synced (``POST /v1/catalog/library/files/synced``)
   with the values it now records, whether a write was needed, the file
   already said so, or nothing moved.

A file with no recorded values (never synced since the change, or
registered again by identify or the reconcile, which clears them) gets the
entry's current values recorded and nothing written: the reconcile, not the
sync, is what brings a file in step with its entry. Neither that nor a file
whose fields did not move reads the file from Drive.

**Edits made in a file go up first.** Before the catalog's changes come
down, a file changed in Drive after its last sync (the scan's listing has
when Drive last saw each file change) is read, and its owned fields are sent
to the API (``POST /v1/catalog/library/files/edited``; ADR-0008, 2026-11-09:
an edit made in VirtualDJ goes up). A field the file changed and the catalog
did not goes up to its entry, and the sync below then carries it to the
entry's other files; one the catalog changed too stays the catalog's and is
written back; a BPM a player re-measured is left; a title or artist that
does not split into the catalog's parts cleanly, and the like, is held and
named in the report. A file with no recorded values is not read.

The write follows ADR-0007: never renamed, moved or deleted, the head
revision kept, the audio checked before and after, and every write in the
run record (``library-sync-<run id>.json``), which
``python -m deejay_cog.music_rollback <run id>`` undoes. The comment and
every frame VirtualDJ owns are never written. Genre is written; subgenre
is not, until a frame for it is decided.

**The report** says how many files each entry's change reached, so an edit
or a rule change can be followed to the files it touched.

**Safety:** ``LIBRARY_SYNC_MODE`` is ``off`` unless ``dry-run`` or
``write``, and stays off until the library is reconciled: before then the
reconcile, with the owner's approvals, decides what is written. A dry run
reads and plans, writes no music, and records nothing as synced, so the
same files come up again. An audio mismatch stops the run with an ERROR
finding; that file is not marked synced, so it comes up again first.
"""

from __future__ import annotations

import os
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import monotonic
from typing import Any, Protocol, cast

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import (
    CATALOG_DISPLAY_MAX_IDS,
    LIBRARY_ABSENT_MAX_FILES,
    CatalogDisplay,
    CatalogDisplayRequest,
    LibraryFile,
    LibraryFileEdited,
    LibraryFileEditResult,
    LibraryFilesSynced,
    LibraryFilesSyncedResult,
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
from deejay_cog.library_backfill import BackfillSettings, StateFile, StateProblem
from deejay_cog.library_scan import LISTING_NAME as SCAN_LISTING_NAME
from deejay_cog.music_safety import (
    AudioChanged,
    FlowMode,
    RunRecord,
    TagEmptied,
    file_meta,
    flow_mode,
    local_run_id,
    write_in_place,
)
from deejay_cog.music_tags import display_fields, read_tag_row, write_owned_fields

log = logger_mod.get_logger()

FLOW_NAME = "library-sync"

#: off | dry-run | write; unset or unknown is off.
MODE_VAR = "LIBRARY_SYNC_MODE"

#: Files written per run; a whole number of at least 1.
CHUNK_SIZE_VAR = "LIBRARY_SYNC_CHUNK_SIZE"
DEFAULT_CHUNK_SIZE = 50

#: Registry rows asked for per page when looking for files changed in Drive.
REGISTRY_PAGE = 500


class Catalog(Protocol):
    """The parts of the API client the sync calls."""

    def list_library_files_needing_sync(self, **kwargs: Any) -> list[LibraryFile]:
        """One page of the files a sync has not caught up with."""
        ...

    def read_catalog_display(
        self, payload: CatalogDisplayRequest
    ) -> list[CatalogDisplay | None]:
        """Each entry's owned fields as a file should carry them."""
        ...

    def mark_library_files_synced(
        self, payload: LibraryFilesSynced
    ) -> LibraryFilesSyncedResult:
        """Record that files now carry the catalog's values."""
        ...

    def list_library_files(self, **kwargs: Any) -> list[LibraryFile]:
        """One page of registered files."""
        ...

    def report_library_file_edited(
        self, payload: LibraryFileEdited
    ) -> LibraryFileEditResult:
        """Send up the fields a file changed outside the pipeline."""
        ...


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


@dataclass
class SyncSummary:
    """What one run wrote, or in a dry run would write, file by file."""

    mode: FlowMode
    problems: list[str] = field(default_factory=list)
    record_name: str = ""
    waiting: int = 0
    written: list[str] = field(default_factory=list)
    would_write: list[str] = field(default_factory=list)
    already: list[str] = field(default_factory=list)
    #: Files with no recorded values: recorded now, nothing written.
    recorded: list[str] = field(default_factory=list)
    #: Files whose entry changed but none of whose fields moved.
    unmoved: list[str] = field(default_factory=list)
    #: Files read because they changed in Drive after their last sync.
    edits_read: int = 0
    #: Each field a file changed, by outcome: "path: field 'a' -> 'b'".
    edits: dict[str, list[str]] = field(default_factory=dict)
    synced: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    #: Files written (or that would be) per entry, as the entry is shown.
    reached: Counter[str] = field(default_factory=Counter)
    aborted: str = ""
    stopped: str = ""


def _settings() -> tuple[BackfillSettings | None, int, list[str]]:
    settings, problems = BackfillSettings.from_env()
    size = chunk_size_setting()
    if not isinstance(size, int) or size < 1:
        problems.append(
            f"{CHUNK_SIZE_VAR}={size!r} is not a whole number of at least 1"
        )
        size = 0
    return settings, size, problems


def _displays(api: Catalog, rows: list[LibraryFile]) -> dict[uuid.UUID, CatalogDisplay]:
    ids = list(dict.fromkeys(r.catalog_id for r in rows if r.catalog_id))
    found: dict[uuid.UUID, CatalogDisplay] = {}
    for start in range(0, len(ids), CATALOG_DISPLAY_MAX_IDS):
        batch = ids[start : start + CATALOG_DISPLAY_MAX_IDS]
        for display in api.read_catalog_display(CatalogDisplayRequest(ids=batch)):
            if display is not None:
                found[display.id] = display
    return found


def _moved(now: dict[str, str | None], synced: LibraryFileTags) -> dict[str, str]:
    """The fields whose value moved since the last sync. A field the
    catalog now leaves empty is not one: the file keeps its value."""
    return {
        name: value
        for name, value in now.items()
        if value and value != getattr(synced, name)
    }


def _sync_one(
    drive: Any,
    record: RunRecord,
    row: LibraryFile,
    display: CatalogDisplay,
    summary: SyncSummary,
) -> LibraryFileTags | None:
    """Write one file's moved fields from its entry. The values to record
    when it now carries them (written, already so, or nothing moved), None
    when not. Raises :class:`AudioChanged`."""
    now = display_fields(display, label=row.edit_label)
    values = LibraryFileTags(**now)
    if row.synced is None:
        summary.recorded.append(row.path)
        return values
    moved = _moved(now, row.synced)
    if not moved:
        summary.unmoved.append(row.path)
        return values
    meta = file_meta(drive, row.drive_file_id)
    if meta.get("trashed"):
        summary.failed.append(f"{row.path} (in the trash; the scan will mark it)")
        return None
    name = str(meta.get("name") or row.path.rsplit("/", 1)[-1])
    data: bytes = drive.download_file_bytes(row.drive_file_id).data
    tagged = write_owned_fields(data, moved)
    if tagged is None:
        summary.already.append(row.path)
        return values
    entry = write_in_place(
        drive,
        record,
        file_id=row.drive_file_id,
        name=name,
        original=data,
        tagged=tagged,
        identified_by="sync (catalog wins on the fields that moved)",
        head_revision=meta.get("headRevisionId"),
    )
    summary.reached[f"{display.artist} - {display.title}"] += 1
    label = f"{row.path} ({', '.join(sorted(moved))})"
    if entry.state == "done":
        summary.written.append(label)
        return values
    summary.would_write.append(label)
    return None


def _mark(api: Catalog, done: dict[str, LibraryFileTags]) -> None:
    when = datetime.now(UTC)
    ids = list(done)
    for start in range(0, len(ids), LIBRARY_ABSENT_MAX_FILES):
        batch = ids[start : start + LIBRARY_ABSENT_MAX_FILES]
        api.mark_library_files_synced(
            LibraryFilesSynced(
                drive_file_ids=batch,
                synced_at=when,
                values={i: done[i] for i in batch},
            )
        )


def _changed_since_sync(drive: Any, settings: BackfillSettings) -> dict[str, datetime]:
    """When Drive last saw each listed file change, from the scan's listing."""
    listing = StateFile(drive, settings.state, SCAN_LISTING_NAME).load() or {}
    changed: dict[str, datetime] = {}
    for item in listing.get("files") or []:
        when = item.get("modified")
        if when:
            changed[str(item["id"])] = datetime.fromisoformat(str(when))
    return changed


def _present_files(api: Catalog) -> list[LibraryFile]:
    """Every present registry row."""
    rows: list[LibraryFile] = []
    while True:
        page = api.list_library_files(
            present=True, limit=REGISTRY_PAGE, offset=len(rows)
        )
        rows.extend(page)
        if len(page) < REGISTRY_PAGE:
            return rows


def _edit_line(row: LibraryFile, edit: Any) -> str:
    why = f" ({edit.why})" if edit.why else ""
    return f"{row.path}: {edit.field} {edit.synced!r} -> {edit.file!r}{why}"


def _edits(
    drive: Any, api: Catalog, settings: BackfillSettings, summary: SyncSummary
) -> None:
    """Send up what files changed outside the pipeline since their last sync."""
    try:
        changed = _changed_since_sync(drive, settings)
    except StateProblem as exc:
        summary.failed.append(f"edits made in files not read: {exc}")
        return
    if not changed:
        return
    for row in _present_files(api):
        when = changed.get(row.drive_file_id)
        if (
            when is None
            or row.synced is None
            or row.last_synced is None
            or row.identification_status != "identified"
            or when <= row.last_synced
        ):
            continue
        if shared_deadline_passed():
            summary.stopped = f"run deadline reached before reading {row.path}"
            return
        summary.edits_read += 1
        try:
            data: bytes = drive.download_file_bytes(row.drive_file_id).data
            tags = {k: v or None for k, v in read_tag_row(data).items()}
            result = api.report_library_file_edited(
                LibraryFileEdited(
                    drive_file_id=row.drive_file_id,
                    tags=LibraryFileTags(**tags),
                    preview=not summary.mode.writes,
                )
            )
        except Exception as exc:  # noqa: BLE001 — one file must not end the run
            if shared_deadline_passed():
                summary.stopped = f"run deadline reached reading {row.path}"
                return
            log.exception("library-sync: reading edits in %s failed", row.path)
            summary.failed.append(
                f"{row.path} (edits not read: {type(exc).__name__}: {exc})"
            )
            continue
        for edit in result.fields:
            summary.edits.setdefault(edit.outcome, []).append(_edit_line(row, edit))


def _sync(
    drive: Any,
    api: Catalog,
    settings: BackfillSettings,
    chunk_size: int,
    run_id: str,
    summary: SyncSummary,
) -> None:
    _edits(drive, api, settings, summary)
    if summary.stopped:
        return
    rows = api.list_library_files_needing_sync(
        library_root_id=settings.root, limit=chunk_size, offset=0
    )
    summary.waiting = len(rows)
    if not rows:
        return
    displays = _displays(api, rows)
    record = RunRecord(
        FLOW_NAME,
        run_id,
        summary.mode,
        stage=f"{len(rows)} files needing a sync",
        folders={"root": settings.root, "state": settings.state},
    ).attach(drive, settings.state)
    summary.record_name = record.file_name
    done: dict[str, LibraryFileTags] = {}
    status = "complete"
    try:
        for row in rows:
            if shared_deadline_passed():
                summary.stopped = f"run deadline reached before {row.path}"
                status = "stopped"
                break
            display = displays.get(row.catalog_id) if row.catalog_id else None
            if display is None:
                summary.failed.append(
                    f"{row.path} (its catalog entry {row.catalog_id} is gone)"
                )
                continue
            try:
                values = _sync_one(drive, record, row, display, summary)
                if values is not None:
                    done[row.drive_file_id] = values
            except AudioChanged as exc:
                summary.aborted = f"{row.path}: {exc}"
                status = "aborted"
                break
            except TagEmptied as exc:
                summary.failed.append(f"{row.path} ({exc})")
            except Exception as exc:  # noqa: BLE001 — one file must not end the run
                if shared_deadline_passed():
                    summary.stopped = f"run deadline reached reading {row.path}"
                    status = "stopped"
                    break
                log.exception("library-sync: failed on %s", row.path)
                summary.failed.append(f"{row.path} ({type(exc).__name__}: {exc})")
    finally:
        with without_run_deadline():
            if record.entries:
                record.finish(status)
            if summary.mode.writes and done:
                _mark(api, done)
                summary.synced = list(done)
    if summary.aborted:
        _report_abort(run_id, summary)


def _report_abort(run_id: str, summary: SyncSummary) -> None:
    try:
        post_run_finding(
            FLOW_NAME,
            "ERROR",
            "library-sync stopped: a file's audio would not stay "
            f"byte-identical ({summary.aborted}). The file is unchanged (or "
            "restored) and not marked synced, so it comes up again first.",
            run_id=run_id,
        )
    except Exception:  # noqa: BLE001 — the notification is not the job
        log.exception("library-sync: could not report the stop")


def run_sync(
    *,
    run_id: str | None,
    mode: FlowMode,
    drive: Any | None = None,
    api: Catalog | None = None,
) -> SyncSummary:
    """One run: the mode, settings, the next chunk, the record and report."""
    started_at = monotonic()
    run_id = run_id or local_run_id()
    summary = SyncSummary(mode)
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
            _sync(drive, client, settings, chunk_size, run_id, summary)
        finally:
            _record(report, summary, monotonic() - started_at)
    return summary


def library_sync(*, run_id: str | None = None) -> SyncSummary:
    """Write catalog changes to the next chunk of linked files and report."""
    return run_sync(run_id=run_id, mode=flow_mode(MODE_VAR))


def _record(report: Any, summary: SyncSummary, duration_sec: float) -> None:
    report.duration_sec = duration_sec
    for problem in summary.problems:
        report.issue("setting", problem)
    if summary.problems:
        return
    if summary.mode is FlowMode.DRY_RUN:
        report.notice(
            "dry_run",
            f"{MODE_VAR} is dry-run: no music was written and nothing was "
            f"marked synced; record {summary.record_name or '(none)'}",
        )
    if summary.stopped:
        report.notice("stopped", summary.stopped)
    report.count("waiting", summary.waiting)
    report.count("written", len(summary.written))
    report.count("would_write", len(summary.would_write))
    report.count("already_in_step", len(summary.already))
    report.count("nothing_moved", len(summary.unmoved))
    report.count("values_recorded", len(summary.recorded))
    report.count("changed_in_drive_read", summary.edits_read)
    for outcome, lines in sorted(summary.edits.items()):
        report.count(f"edits_{outcome}", len(lines))
    for line in summary.edits.get("sent_up", []):
        if summary.mode.writes:
            report.updated("edit made in a file sent up to the catalog", line)
        else:
            report.notice("would_send_up", line)
    for line in summary.edits.get("overwritten", []):
        report.notice("edit_overwritten_by_the_catalog", line)
    for line in summary.edits.get("held", []):
        report.notice("edit_held", line)
    report.count("marked_synced", len(summary.synced))
    report.count("failed", len(summary.failed))
    for shown, files in sorted(summary.reached.items()):
        noun = "file" if files == 1 else "files"
        if summary.mode.writes:
            report.updated("entry written to files", f"{shown}: {files} {noun}")
        else:
            report.notice("would_reach", f"{shown}: {files} {noun}")
    for path in summary.written:
        report.created("tags written", path)
    for path in summary.would_write:
        report.notice("would_write", path)
    for item in summary.failed:
        report.issue("failed", item)
    if summary.aborted:
        report.issue("audio_changed_run_stopped", summary.aborted)
    report.ok(len(summary.written))
