"""The ``library-trash`` step of the nightly library run: move the files the
owner sent to the trash on the WCS duplicates page to Drive's trash
(ADR-0008, decision 7).

Resolving a duplicate group on the WCS site keeps the file the owner picks
and marks the group's other copies trash-requested in the library registry.
Each night this step lists the present files under the library root that
are marked so (``GET /v1/catalog/library/files?trash_requested=true``) and,
for each:

1. moves it to Drive's trash (``files.update`` with ``trashed=true``): it is
   recoverable from the trash, and never deleted outright;
2. marks it not present in the registry
   (``POST /v1/catalog/library/files/absent``);
3. lists it in the run report.

A file already in the trash, or gone from Drive, is only marked not
present. No other file is touched: the step trashes nothing the registry
does not mark, and the registry is marked only through the owner's choice.

**Safety:** ``flags.deejay_cog.library_trash`` is ``off`` unless ``dry-run`` or
``write``. A dry run lists the files it would trash and changes nothing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from time import monotonic
from typing import Any, Protocol, cast

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import (
    LIBRARY_ABSENT_MAX_FILES,
    LibraryFile,
    LibraryFilesAbsentRequest,
    LibraryFilesAbsentResult,
)
from mini_app_polis.deadline import without_run_deadline
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI
from mini_app_polis.pipeline_status import run_report

from deejay_cog._pipeline_eval import REPO
from deejay_cog._shared_deadline import shared_deadline_passed
from deejay_cog._version import __version__
from deejay_cog.api_client import api_client
from deejay_cog.library_backfill import BackfillSettings
from deejay_cog.music_safety import FlowMode, file_meta, flow_mode, local_run_id

log = logger_mod.get_logger()

FLOW_NAME = "library-trash"

#: The feature flag holding this step's mode: off | dry-run | write; a
#: missing flag or an unknown value is off.
MODE_FLAG = "flags.deejay_cog.library_trash"

#: Registry rows read per page.
PAGE_SIZE = 500


class Catalog(Protocol):
    """The parts of the API client the step calls."""

    def list_library_files(self, **kwargs: Any) -> list[LibraryFile]:
        """One page of registered files."""
        ...

    def mark_library_files_absent(
        self, payload: LibraryFilesAbsentRequest
    ) -> LibraryFilesAbsentResult:
        """Mark files gone from the library not present."""
        ...


@dataclass
class TrashSummary:
    """What one run trashed, or in a dry run would trash."""

    mode: FlowMode
    problems: list[str] = field(default_factory=list)
    requested: int = 0
    trashed: list[str] = field(default_factory=list)
    would_trash: list[str] = field(default_factory=list)
    already_gone: list[str] = field(default_factory=list)
    marked_absent: int = 0
    failed: list[str] = field(default_factory=list)
    stopped: str = ""


def _requested(api: Catalog, root: str) -> list[LibraryFile]:
    rows: list[LibraryFile] = []
    offset = 0
    while True:
        page = api.list_library_files(
            library_root_id=root,
            present=True,
            trash_requested=True,
            limit=PAGE_SIZE,
            offset=offset,
        )
        # Only rows the registry itself says are marked: never trust the
        # filter alone to decide what goes to the trash.
        rows.extend(r for r in page if r.trash_requested and r.present)
        if len(page) < PAGE_SIZE:
            return rows
        offset += PAGE_SIZE


def _gone(exc: Exception) -> bool:
    """Drive answered 404: the file is gone."""
    return getattr(getattr(exc, "resp", None), "status", None) == 404


def _trash(drive: Any, api: Catalog, root: str, summary: TrashSummary) -> None:
    rows = _requested(api, root)
    summary.requested = len(rows)
    absent: list[str] = []
    try:
        for row in rows:
            if shared_deadline_passed():
                summary.stopped = f"run deadline reached before {row.path}"
                break
            try:
                meta = file_meta(drive, row.drive_file_id)
            except Exception as exc:  # noqa: BLE001 — one file must not end the run
                if not _gone(exc):
                    summary.failed.append(f"{row.path} ({type(exc).__name__}: {exc})")
                    continue
                meta = {"trashed": True}
            if meta.get("trashed"):
                summary.already_gone.append(row.path)
                absent.append(row.drive_file_id)
                continue
            if not summary.mode.writes:
                summary.would_trash.append(row.path)
                continue
            try:
                drive.trash_file(row.drive_file_id)
            except Exception as exc:  # noqa: BLE001 — one file must not end the run
                log.exception("library-trash: could not trash %s", row.path)
                summary.failed.append(f"{row.path} ({type(exc).__name__}: {exc})")
                continue
            summary.trashed.append(row.path)
            absent.append(row.drive_file_id)
    finally:
        if summary.mode.writes and absent:
            with without_run_deadline():
                for start in range(0, len(absent), LIBRARY_ABSENT_MAX_FILES):
                    batch = absent[start : start + LIBRARY_ABSENT_MAX_FILES]
                    result = api.mark_library_files_absent(
                        LibraryFilesAbsentRequest(drive_file_ids=batch)
                    )
                    summary.marked_absent += result.marked


def run_trash(
    *,
    run_id: str | None,
    mode: FlowMode,
    drive: Any | None = None,
    api: Catalog | None = None,
) -> TrashSummary:
    """One run: the mode, settings, the trash-requested files and report."""
    started_at = monotonic()
    run_id = run_id or local_run_id()
    summary = TrashSummary(mode)
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
            settings, problems = BackfillSettings.from_env()
            if settings is None:
                summary.problems.extend(problems)
                return summary
            if drive is None:
                drive = GoogleAPI.from_env().drive
            client = api or cast(
                Catalog, api_client(base_url=api_base_url().strip() or None)
            )
            _trash(drive, client, settings.root, summary)
        finally:
            _record(report, summary, monotonic() - started_at)
    return summary


def library_trash(*, run_id: str | None = None) -> TrashSummary:
    """Move the trash-requested library files to Drive's trash and report."""
    return run_trash(run_id=run_id, mode=flow_mode(MODE_FLAG))


def _record(report: Any, summary: TrashSummary, duration_sec: float) -> None:
    report.duration_sec = duration_sec
    for problem in summary.problems:
        report.issue("setting", problem)
    if summary.problems:
        return
    if summary.mode is FlowMode.DRY_RUN:
        report.notice(
            "dry_run", f"{MODE_FLAG} is dry-run: nothing was trashed or marked"
        )
    if summary.stopped:
        report.notice("stopped", summary.stopped)
    report.count("trash_requested", summary.requested)
    report.count("trashed", len(summary.trashed))
    report.count("would_trash", len(summary.would_trash))
    report.count("already_gone", len(summary.already_gone))
    report.count("marked_not_present", summary.marked_absent)
    report.count("failed", len(summary.failed))
    for path in summary.trashed:
        report.updated("moved to Drive's trash", path)
    for path in summary.would_trash:
        report.notice("would_trash", path)
    for path in summary.already_gone:
        report.notice("already_gone", path)
    for item in summary.failed:
        report.issue("failed", item)
    report.ok(len(summary.trashed))
