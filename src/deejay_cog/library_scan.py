"""The ``library-scan`` step of the nightly library run: notice files added
to and removed from the library, and keep the library registry in step
(ADR-0008, decisions 5 and 16).

Each night, before the backfill's chunk, the library is listed again,
metadata only (no downloads), and compared with the API's library registry:

- **Added.** An MP3 the registry has never seen is registered as
  ``unidentified``, with its path. Identifying it (tags, then fingerprint)
  is a later step. A file the backfill's manifest lists but has not reached
  yet is left to the backfill, which registers it with its link; and while
  the backfill's manifest is still being listed, no file is added.
- **Back.** A file the registry has as not present that is listed again is
  registered again with its link and status unchanged, at its listed path.
- **Moved.** A present file listed under a different path keeps its link
  and gets the new path.
- **Removed.** A present file no longer listed, or in the trash, is marked
  not present (``POST /v1/catalog/library/files/absent``). Its row and link
  stay; the API archives an entry left with no plays and no present file.

**Never** renames, moves, deletes or downloads a file: only the registry
changes.

**Position.** The listing is the backfill's resumable listing, kept in
``library-scan-listing.json`` in ``MUSIC_TAGGING_STATE_FOLDER_ID``. A run
lists for at most ``LIBRARY_SCAN_LISTING_SECONDS`` (default 90), so the
backfill's chunk still fits in the run, and the next night carries on where
it stopped. Once a listing is complete it is compared, and the next night
starts a new one.

**Safety:** ``LIBRARY_SCAN_MODE`` is ``off`` unless ``dry-run`` or
``write``. A dry run lists and compares, and reports what it would add and
remove, but changes nothing in the registry. A listing with no MP3s, or one
that would mark more than half of the present files not present, changes
nothing and is reported as an issue: it is far likelier to be a wrong root
or a Drive outage than the library emptying overnight.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import monotonic
from typing import Any, Protocol, cast

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import (
    LIBRARY_ABSENT_MAX_FILES,
    LibraryFile,
    LibraryFileRegister,
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
from deejay_cog.library_backfill import (
    CURSOR_NAME,
    MANIFEST_NAME,
    ROOT_FOLDER_VAR,
    BackfillSettings,
    StateFile,
    StateProblem,
    is_mp3_listing,
    list_one_folder,
    new_manifest,
)
from deejay_cog.music_safety import FlowMode, flow_mode, local_run_id

log = logger_mod.get_logger()

FLOW_NAME = "library-scan"

#: off | dry-run | write; unset or unknown is off.
MODE_VAR = "LIBRARY_SCAN_MODE"

#: The most seconds one run spends listing; a whole number of at least 1.
LISTING_SECONDS_VAR = "LIBRARY_SCAN_LISTING_SECONDS"
DEFAULT_LISTING_SECONDS = 90

LISTING_NAME = f"{FLOW_NAME}-listing.json"

#: Registry rows asked for per page.
REGISTRY_PAGE = 500

#: How many folders are listed between saves of a listing in progress.
LISTING_SAVE_EVERY = 25


class Registry(Protocol):
    """The part of the API client the scan calls."""

    def list_library_files(self, **kwargs: Any) -> list[LibraryFile]:
        """One page of registered files."""
        ...

    def register_library_file(self, payload: LibraryFileRegister) -> LibraryFile:
        """Register a library file, or bring its row up to date."""
        ...

    def mark_library_files_absent(
        self, payload: LibraryFilesAbsentRequest
    ) -> LibraryFilesAbsentResult:
        """Mark files gone from the library not present."""
        ...


def listing_seconds_setting() -> int | str:
    """The listing budget as the worker logs it: the number, or the raw
    text when it will not parse."""
    raw = os.getenv(LISTING_SECONDS_VAR, "").strip()
    if not raw:
        return DEFAULT_LISTING_SECONDS
    try:
        return int(raw)
    except ValueError:
        return raw


def _listing_seconds() -> tuple[int, str | None]:
    value = listing_seconds_setting()
    if isinstance(value, int) and value >= 1:
        return value, None
    return 0, f"{LISTING_SECONDS_VAR}={value!r} is not a whole number of at least 1"


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@dataclass
class ScanSummary:
    """What one scan did, or in a dry run would do."""

    mode: FlowMode
    problems: list[str] = field(default_factory=list)
    refused: str = ""
    listed: int = 0
    listing_complete: bool = False
    listing_pending_folders: int = 0
    compared: bool = False
    added_held: str = ""
    added: list[str] = field(default_factory=list)
    back: list[str] = field(default_factory=list)
    moved: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    left_for_backfill: int = 0
    unchanged: int = 0
    failed: list[str] = field(default_factory=list)


def _path(listing: dict[str, Any], item: dict[str, Any]) -> str:
    folder = listing["folders"].get(item["folder_id"], "")
    return f"{folder}/{item['name']}" if folder else str(item["name"])


def _registry_rows(api: Registry, root: str) -> dict[str, LibraryFile]:
    """Every row registered under ``root``, present or not, by Drive id."""
    rows: dict[str, LibraryFile] = {}
    offset = 0
    while True:
        page = api.list_library_files(
            library_root_id=root, limit=REGISTRY_PAGE, offset=offset
        )
        for row in page:
            rows[row.drive_file_id] = row
        if len(page) < REGISTRY_PAGE:
            return rows
        offset += REGISTRY_PAGE


def _pending_backfill(drive: Any, state: str) -> set[str] | None:
    """The ids the backfill's manifest lists but its cursor has not reached;
    None while the backfill's manifest is still being listed."""
    manifest = StateFile(drive, state, MANIFEST_NAME).load()
    if manifest is None or not manifest.get("complete"):
        return None
    cursor = StateFile(drive, state, CURSOR_NAME).load() or {}
    position = 0
    if cursor.get("generation") == manifest.get("generation"):
        position = int(cursor.get("position", 0))
    return {str(item["id"]) for item in manifest["files"][position:]}


def _list(
    drive: Any,
    store: StateFile,
    listing: dict[str, Any],
    *,
    exclude: set[str],
    budget: float,
) -> bool:
    """List folders until none are left or the budget is spent; save the
    listing either way. True when the listing is complete."""
    started = monotonic()
    listed = 0
    while listing["pending_folders"]:
        if shared_deadline_passed() or monotonic() - started >= budget:
            with without_run_deadline():
                store.save(listing)
            return False
        list_one_folder(drive, listing, exclude=exclude)
        listed += 1
        if listed % LISTING_SAVE_EVERY == 0 and listing["pending_folders"]:
            store.save(listing)
    listing["complete"] = True
    listing["completed_at"] = _now()
    store.save(listing)
    return True


def compare(
    listing: dict[str, Any],
    rows: dict[str, LibraryFile],
    pending: set[str] | None,
    root: str,
    summary: ScanSummary,
) -> tuple[list[LibraryFileRegister], list[str]]:
    """What the registry needs: registrations, and Drive ids gone.

    Names each file under its outcome in ``summary``.
    """
    registrations: list[LibraryFileRegister] = []
    listed: set[str] = set()
    for item in listing["files"]:
        if not is_mp3_listing(item):
            continue
        file_id = str(item["id"])
        listed.add(file_id)
        path = _path(listing, item)
        row = rows.get(file_id)
        if row is None:
            if pending is None:
                summary.added_held = (
                    f"the backfill's {MANIFEST_NAME} is still being listed"
                )
                continue
            if file_id in pending:
                summary.left_for_backfill += 1
                continue
            registrations.append(
                LibraryFileRegister(
                    drive_file_id=file_id,
                    library_root_id=root,
                    path=path,
                    identification_status="unidentified",
                )
            )
            summary.added.append(path)
            continue
        if row.present and row.path == path:
            summary.unchanged += 1
            continue
        # Its link and status as they are; only where it is changes.
        registrations.append(
            LibraryFileRegister(
                drive_file_id=file_id,
                library_root_id=root,
                path=path,
                identification_status=row.identification_status,
                catalog_id=row.catalog_id,
            )
        )
        if row.present:
            summary.moved.append(f"{row.path} -> {path}")
        else:
            summary.back.append(path)
    gone = [
        file_id
        for file_id, row in rows.items()
        if row.present and file_id not in listed
    ]
    summary.removed.extend(rows[file_id].path for file_id in gone)
    return registrations, gone


def _refusal(
    listing: dict[str, Any], rows: dict[str, LibraryFile], gone: list[str]
) -> str:
    """Why the comparison must not be applied, or "" when it may."""
    mp3s = sum(1 for item in listing["files"] if is_mp3_listing(item))
    present = sum(1 for row in rows.values() if row.present)
    if mp3s == 0 and present:
        return (
            f"the listing of {ROOT_FOLDER_VAR} found no MP3s but the registry has "
            f"{present} present; nothing was changed"
        )
    if present and len(gone) * 2 > present:
        return (
            f"{len(gone)} of {present} present files would be marked not present; "
            "nothing was changed (check the root and Drive, then trash "
            f"{LISTING_NAME} to list again)"
        )
    return ""


def _apply(
    api: Registry,
    registrations: list[LibraryFileRegister],
    gone: list[str],
    summary: ScanSummary,
) -> None:
    for payload in registrations:
        try:
            api.register_library_file(payload)
        except Exception as exc:  # noqa: BLE001 — one file must not end the scan
            if shared_deadline_passed():
                raise
            log.warning("library-scan: registering %s failed: %s", payload.path, exc)
            summary.failed.append(f"{payload.path} ({type(exc).__name__}: {exc})")
    for start in range(0, len(gone), LIBRARY_ABSENT_MAX_FILES):
        batch = gone[start : start + LIBRARY_ABSENT_MAX_FILES]
        try:
            api.mark_library_files_absent(
                LibraryFilesAbsentRequest(drive_file_ids=batch)
            )
        except Exception as exc:  # noqa: BLE001 — reported; the next night retries
            if shared_deadline_passed():
                raise
            log.warning(
                "library-scan: marking %d files absent failed: %s", len(batch), exc
            )
            summary.failed.append(
                f"marking {len(batch)} files not present ({type(exc).__name__}: {exc})"
            )


def _scan(
    drive: Any,
    api: Registry | None,
    settings: BackfillSettings,
    budget: int,
    summary: ScanSummary,
) -> None:
    store = StateFile(drive, settings.state, LISTING_NAME)
    try:
        listing = store.load()
    except StateProblem as exc:
        summary.problems.append(str(exc))
        return
    if (
        listing is None
        or listing.get("compared_at")
        or listing.get("root_folder_id") != settings.root
    ):
        listing = {**new_manifest(settings.root), "flow": FLOW_NAME}
    before = len(listing["files"])
    complete = _list(drive, store, listing, exclude={settings.state}, budget=budget)
    summary.listed = len(listing["files"]) - before
    summary.listing_pending_folders = len(listing["pending_folders"])
    if not complete:
        return
    summary.listing_complete = True

    try:
        pending = _pending_backfill(drive, settings.state)
    except StateProblem as exc:
        summary.problems.append(str(exc))
        return
    client = api or cast(Registry, api_client(base_url=api_base_url().strip() or None))
    rows = _registry_rows(client, settings.root)
    registrations, gone = compare(listing, rows, pending, settings.root, summary)
    summary.refused = _refusal(listing, rows, gone)
    if not summary.refused:
        summary.compared = True
        if summary.mode.writes:
            _apply(client, registrations, gone, summary)
    # Compared (or refused): the next night lists the library afresh.
    listing["compared_at"] = _now()
    store.save(listing)


def run_scan(
    *,
    run_id: str | None,
    mode: FlowMode,
    drive: Any | None = None,
    api: Registry | None = None,
) -> ScanSummary:
    """One scan: the mode, settings, listing, comparison and report."""
    started_at = monotonic()
    run_id = run_id or local_run_id()
    summary = ScanSummary(mode)
    if mode is FlowMode.OFF:
        # No report either: the backfill's report already says what ran.
        return summary
    with run_report(
        FLOW_NAME,
        repo=REPO,
        notable=True,
        run_id=run_id,
        version=__version__,
        list_all=True,  # every added and removed file is named
    ) as report:
        try:
            settings, problems = BackfillSettings.from_env()
            budget, budget_problem = _listing_seconds()
            if budget_problem:
                problems.append(budget_problem)
            if settings is None or problems:
                summary.problems.extend(problems)
                return summary
            if drive is None:
                drive = GoogleAPI.from_env().drive
            _scan(drive, api, settings, budget, summary)
        finally:
            _record(report, summary, monotonic() - started_at)
    return summary


def library_scan(*, run_id: str | None = None) -> ScanSummary:
    """Scan the library for added and removed files and report the run."""
    return run_scan(run_id=run_id, mode=flow_mode(MODE_VAR))


def _record(report: Any, summary: ScanSummary, duration_sec: float) -> None:
    report.duration_sec = duration_sec
    for problem in summary.problems:
        report.issue("setting", problem)
    if summary.problems:
        return
    if summary.mode is FlowMode.DRY_RUN:
        report.notice("dry_run", f"{MODE_VAR} is dry-run: the registry was not changed")
    if not summary.listing_complete:
        report.notice(
            "listing_in_progress",
            f"{LISTING_NAME}: {summary.listed} files listed this run, "
            f"{summary.listing_pending_folders} folders still to list",
        )
    if summary.refused:
        report.issue("refused", summary.refused)
    if summary.added_held:
        report.notice("added_held", f"no file added: {summary.added_held}")
    report.count("listed", summary.listed)
    report.count("added", len(summary.added))
    report.count("back", len(summary.back))
    report.count("moved", len(summary.moved))
    report.count("removed", len(summary.removed) if summary.compared else 0)
    report.count("left_for_backfill", summary.left_for_backfill)
    report.count("unchanged", summary.unchanged)
    report.count("failed", len(summary.failed))
    if not summary.compared:
        return
    writes = summary.mode.writes
    for path in summary.added:
        if writes:
            report.created("library file", path)
        else:
            report.notice("would_add", path)
    for path in summary.back:
        if writes:
            report.updated("library file back", path)
        else:
            report.notice("would_mark_back", path)
    for move in summary.moved:
        if writes:
            report.updated("library file moved", move)
        else:
            report.notice("would_move", move)
    for path in summary.removed:
        if writes:
            report.removed("library file", path)
        else:
            report.notice("would_remove", path)
    for failure in summary.failed:
        report.issue("failed", failure)
    report.ok(len(summary.added) + len(summary.back) + len(summary.moved))
