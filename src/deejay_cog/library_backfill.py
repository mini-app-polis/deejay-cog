"""The ``library-backfill`` worker mode: retag the music library from the
catalog, in place, a chunk a night (ADR-0007, amended 2026-11-03).

The library under ``LIBRARY_ROOT_FOLDER_ID`` is what VirtualDJ plays from,
by path. So a library file is **never renamed, moved or deleted**: only its
tags are rewritten from the catalog, over the same Drive file (same name,
folder and file id), through :func:`deejay_cog.music_safety.write_in_place`.

**Position.** The library is thousands of files, so it is never rescanned to
find where to carry on. Two JSON files in ``MUSIC_TAGGING_STATE_FOLDER_ID``
hold the position:

- ``library-backfill-manifest.json`` — the library listed once, recursively,
  metadata only (no downloads): every file's id, name and folder, in a fixed
  order (breadth first from the root; within a folder by name, then id).
  The listing is resumable: the folders still to list are kept in the
  manifest, saved every few folders and when the run stops, so a listing cut
  off by the deadline carries on where it stopped. Until it is complete no
  file is processed. Built in ``dry-run`` too: listing reads nothing but
  metadata, and only the state folder is written.
- ``library-backfill-cursor.json`` — how far through that manifest the
  backfill has got (the next file's index), tied to the manifest it counts
  through by the manifest's ``generation``. Saved only by a ``write`` run,
  after each file's outcome is in the run record, so the cursor never moves
  past a file whose outcome is not recorded.

Each run takes the next ``LIBRARY_BACKFILL_CHUNK_SIZE`` files (default 10)
after the cursor. A redelivery of a run stopped by the deadline (the same
queue message id) finishes that run's chunk rather than start a new one.
Once the cursor reaches the end, every run reports the library done and does
nothing more. **To start over** (files added to the library since, or a new
root), move ``library-backfill-manifest.json`` to the Drive trash: the next
run lists the library again and, the generation having changed, starts from
the first file.

**Per file:** not an MP3 (by its listed type or name) → skipped, named. No
longer in the folder the manifest has it in, or in the trash → skipped,
named. No usable title or artist → skipped and named as needing
fingerprinting (library files are **not** sent to the fingerprint function:
that writes MusicBrainz tags). Not in the catalog → skipped, named. In the
catalog and already carrying its tags → nothing written. Otherwise written
in place, with the head revision kept forever, the audio checked before and
after, and the original restored if the check after fails.

**Safety:** ``LIBRARY_BACKFILL_MODE`` is ``off`` unless ``dry-run`` or
``write``. A dry run writes nothing to any music file, does not move the
cursor (so the same chunk can be reviewed and then written) and saves the
would-be run record. An audio mismatch stops the run with an ERROR finding
and leaves the cursor **on** that file: the next run tries it again first
(a mismatch found before the write writes nothing), so the backfill does not
pass a file it could not safely write until the owner has looked. To pass
one, put its id in ``LIBRARY_BACKFILL_SKIP_FILE_IDS``. Every write run's
record is undone by ``python -m deejay_cog.music_rollback <run id>``.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from time import monotonic
from typing import Any

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import CatalogLookupRequest
from mini_app_polis.deadline import without_run_deadline
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI
from mini_app_polis.lambda_worker import RunOutOfTime
from mini_app_polis.pipeline_status import run_report

from deejay_cog._pipeline_eval import REPO, post_run_finding
from deejay_cog._shared_deadline import shared_deadline_passed
from deejay_cog._version import __version__
from deejay_cog.api_client import api_client
from deejay_cog.music_safety import (
    MP3_MIME_TYPE,
    RECORD_MIME_TYPE,
    STATE_FOLDER_VAR,
    AudioChanged,
    FlowMode,
    RecordEntry,
    RunRecord,
    TagEmptied,
    check_audio_unchanged,
    file_meta,
    flow_mode,
    local_run_id,
    tag_snapshot,
    update_contents,
    write_in_place,
)
from deejay_cog.music_tags import (
    CatalogLookup,
    ingest_track,
    read_tag_row,
    write_catalog_tags,
)

log = logger_mod.get_logger()

FLOW_NAME = "library-backfill"

#: off | dry-run | write; unset or unknown is off.
MODE_VAR = "LIBRARY_BACKFILL_MODE"

#: The library's root folder, walked recursively.
ROOT_FOLDER_VAR = "LIBRARY_ROOT_FOLDER_ID"

#: Files per run; a whole number of at least 1.
CHUNK_SIZE_VAR = "LIBRARY_BACKFILL_CHUNK_SIZE"
DEFAULT_CHUNK_SIZE = 10

#: Comma-separated Drive file ids the backfill passes over (named in the
#: report): the way past a file whose audio check fails.
SKIP_IDS_VAR = "LIBRARY_BACKFILL_SKIP_FILE_IDS"

MANIFEST_NAME = f"{FLOW_NAME}-manifest.json"
CURSOR_NAME = f"{FLOW_NAME}-cursor.json"

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
SHORTCUT_MIME_TYPE = "application/vnd.google-apps.shortcut"

#: How many folders are listed between saves of a manifest in progress.
LISTING_SAVE_EVERY = 25

#: The outcomes a file can have, as the report and the cursor count them.
OUTCOMES = (
    "written",
    "would_write",
    "already_tagged",
    "not_in_catalog",
    "needs_fingerprinting",
    "not_mp3",
    "moved_since",
    "skipped_by_setting",
    "failed",
)


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BackfillSettings:
    root: str
    state: str
    chunk_size: int
    skip_ids: frozenset[str]

    @classmethod
    def from_env(cls) -> tuple[BackfillSettings | None, list[str]]:
        """The settings, or None and why not."""
        root = os.getenv(ROOT_FOLDER_VAR, "").strip()
        state = os.getenv(STATE_FOLDER_VAR, "").strip()
        problems = [
            f"{var} is not set"
            for var, value in ((ROOT_FOLDER_VAR, root), (STATE_FOLDER_VAR, state))
            if not value
        ]
        if root and root == state:
            problems.append(
                f"{ROOT_FOLDER_VAR} and {STATE_FOLDER_VAR} must be different folders"
            )
        raw = os.getenv(CHUNK_SIZE_VAR, "").strip()
        chunk_size = DEFAULT_CHUNK_SIZE
        if raw:
            try:
                chunk_size = int(raw)
            except ValueError:
                chunk_size = 0
            if chunk_size < 1:
                problems.append(
                    f"{CHUNK_SIZE_VAR}={raw!r} is not a whole number of at least 1"
                )
        skip_ids = frozenset(
            part.strip()
            for part in os.getenv(SKIP_IDS_VAR, "").split(",")
            if part.strip()
        )
        if problems:
            return None, problems
        return cls(root, state, chunk_size, skip_ids), []


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
# State files in the state folder
# ---------------------------------------------------------------------------


class StateProblem(Exception):  # noqa: N818 — named for what it is
    """The state folder holds something the backfill will not guess about."""


@dataclass
class StateFile:
    """One JSON file in the state folder: found by exact name, created on
    the first save, written over in place after that."""

    drive: Any
    folder: str
    name: str
    file_id: str | None = None

    def load(self) -> dict[str, Any] | None:
        files = self.drive.list_files(
            parent_id=self.folder, name_contains=self.name, include_folders=False
        )
        matches = [f for f in files if str(f.name) == self.name]
        if len(matches) > 1:
            raise StateProblem(
                f"{len(matches)} files named {self.name} in the state folder; "
                "trash all but one"
            )
        if not matches:
            return None
        self.file_id = str(matches[0].id)
        raw = self.drive.download_file_bytes(self.file_id).data
        try:
            data = json.loads(bytes(raw).decode("utf-8"))
        except ValueError as exc:
            raise StateProblem(f"{self.name} is not valid JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise StateProblem(f"{self.name} is not a JSON object")
        return data

    def save(self, data: dict[str, Any]) -> None:
        content = json.dumps(data, indent=1).encode()
        if self.file_id is None:
            self.file_id = self.drive.upload_bytes(
                parent_id=self.folder,
                filename=self.name,
                content=content,
                mime_type=RECORD_MIME_TYPE,
            )
            return
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as handle:
            handle.write(content)
            path = handle.name
        try:
            self.drive.update_file(self.file_id, path, mime_type=RECORD_MIME_TYPE)
        finally:
            os.unlink(path)


def new_manifest(root: str) -> dict[str, Any]:
    """A manifest about to be listed: only the root is queued."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return {
        "flow": FLOW_NAME,
        "version": 1,
        "generation": f"{stamp}-{os.urandom(3).hex()}",
        "root_folder_id": root,
        "started_at": _now(),
        "completed_at": None,
        "complete": False,
        "pending_folders": [{"id": root, "path": ""}],
        "visited_folders": [],
        "folders": {root: ""},
        "shortcuts_skipped": 0,
        "files": [],
    }


def _sort_key(item: Any) -> tuple[str, str]:
    return (str(getattr(item, "name", "")).casefold(), str(getattr(item, "id", "")))


def list_one_folder(drive: Any, manifest: dict[str, Any], *, exclude: set[str]) -> None:
    """List the next queued folder into ``manifest``: its files appended in
    name order, its subfolders queued in name order. Only metadata is read."""
    folder = manifest["pending_folders"][0]
    folder_id, path = str(folder["id"]), str(folder["path"])
    children = sorted(
        drive.list_files(parent_id=folder_id, trashed=False, include_folders=True),
        key=_sort_key,
    )
    visited = set(manifest["visited_folders"])
    queued = {str(f["id"]) for f in manifest["pending_folders"]}
    for child in children:
        child_id = str(child.id)
        mime_type = str(getattr(child, "mime_type", "") or "")
        if mime_type == FOLDER_MIME_TYPE:
            if child_id in visited or child_id in queued or child_id in exclude:
                continue
            child_path = f"{path}/{child.name}" if path else str(child.name)
            manifest["pending_folders"].append({"id": child_id, "path": child_path})
            manifest["folders"][child_id] = child_path
            queued.add(child_id)
        elif mime_type == SHORTCUT_MIME_TYPE:
            # A shortcut points elsewhere, maybe outside the library.
            manifest["shortcuts_skipped"] += 1
        else:
            manifest["files"].append(
                {
                    "id": child_id,
                    "name": str(child.name),
                    "folder_id": folder_id,
                    "mime_type": mime_type,
                }
            )
    manifest["pending_folders"].pop(0)
    manifest["visited_folders"].append(folder_id)


def build_manifest(
    drive: Any, store: StateFile, manifest: dict[str, Any], *, exclude: set[str]
) -> None:
    """List folders until none are left, saving every few and at the end.

    Past the deadline it saves what it has and raises ``RunOutOfTime``; the
    next run carries on from the saved queue.
    """
    listed = 0
    while manifest["pending_folders"]:
        if shared_deadline_passed():
            with without_run_deadline():
                store.save(manifest)
            raise RunOutOfTime(
                f"run deadline reached listing the library "
                f"({len(manifest['files'])} files so far)"
            )
        list_one_folder(drive, manifest, exclude=exclude)
        listed += 1
        if listed % LISTING_SAVE_EVERY == 0 and manifest["pending_folders"]:
            store.save(manifest)
    manifest["complete"] = True
    manifest["completed_at"] = _now()
    store.save(manifest)


def new_cursor(generation: str, total: int) -> dict[str, Any]:
    return {
        "flow": FLOW_NAME,
        "version": 1,
        "generation": generation,
        "position": 0,
        "total": total,
        "run": None,
        "counts": dict.fromkeys(OUTCOMES, 0),
        "failed": [],
        "done_at": None,
        "updated_at": _now(),
    }


# ---------------------------------------------------------------------------
# One run
# ---------------------------------------------------------------------------


@dataclass
class BackfillSummary:
    """What one run did, or in a dry run would do, file by file."""

    mode: FlowMode
    record_name: str = ""
    problems: list[str] = field(default_factory=list)
    listed: int = 0
    listing_finished: bool = False
    listing_pending_folders: int = 0
    total: int = 0
    start: int = 0
    position: int = 0
    scanned: int = 0
    outcomes: dict[str, list[str]] = field(
        default_factory=lambda: {outcome: [] for outcome in OUTCOMES}
    )
    restarted: bool = False
    done: bool = False
    aborted: str = ""

    def add(self, outcome: str, item: str) -> None:
        self.outcomes[outcome].append(item)

    @property
    def remaining(self) -> int:
        return max(self.total - self.position, 0)

    @property
    def matched(self) -> int:
        return sum(
            len(self.outcomes[o]) for o in ("written", "would_write", "already_tagged")
        )


def is_mp3_listing(item: dict[str, Any]) -> bool:
    """Whether a listed file is an MP3, by its type or name: only ID3 tags
    are written."""
    return item.get("mime_type") == MP3_MIME_TYPE or str(
        item.get("name", "")
    ).lower().endswith(".mp3")


class Backfill:
    """One run over one chunk of the manifest."""

    def __init__(
        self,
        drive: Any,
        api: CatalogLookup,
        settings: BackfillSettings,
        record: RunRecord,
        summary: BackfillSummary,
        manifest: dict[str, Any],
    ) -> None:
        self.drive = drive
        self.api = api
        self.settings = settings
        self.record = record
        self.summary = summary
        self.manifest = manifest

    @property
    def mode(self) -> FlowMode:
        return self.record.mode

    def label(self, item: dict[str, Any], name: str | None = None) -> str:
        path = self.manifest["folders"].get(item["folder_id"], "")
        name = name or str(item["name"])
        return f"{path}/{name}" if path else name

    def _skip(
        self, item: dict[str, Any], outcome: str, reason: str, name: str | None = None
    ) -> None:
        self.record.add(
            RecordEntry(
                file_id=str(item["id"]),
                original_name=name or str(item["name"]),
                action="skip",
                state="skipped",
                folder=str(item["folder_id"]),
                reason=reason,
            )
        )
        label = self.label(item, name)
        self.summary.add(
            outcome, f"{label} ({reason})" if outcome == "failed" else label
        )

    def one(self, item: dict[str, Any]) -> str:
        """Process one listed file; return its outcome. Raises
        :class:`AudioChanged` (the run stops) and lets anything unexpected
        out to :func:`_process_chunk`, which records it as failed."""
        file_id = str(item["id"])
        if file_id in self.settings.skip_ids:
            self._skip(item, "skipped_by_setting", f"listed in {SKIP_IDS_VAR}")
            return "skipped_by_setting"
        if not is_mp3_listing(item):
            self._skip(item, "not_mp3", "not an MP3: only ID3 tags are written")
            return "not_mp3"
        meta = file_meta(self.drive, file_id)
        name = str(meta.get("name") or item["name"])
        if meta.get("trashed") or item["folder_id"] not in (meta.get("parents") or []):
            self._skip(item, "moved_since", "no longer where the manifest has it", name)
            return "moved_since"

        data: bytes = self.drive.download_file_bytes(file_id).data
        track = ingest_track(read_tag_row(data))
        if track is None:
            self._skip(item, "needs_fingerprinting", "no usable title or artist", name)
            return "needs_fingerprinting"
        match = self.api.lookup_catalog(CatalogLookupRequest(tracks=[track]))[0]
        if match is None:
            self._skip(item, "not_in_catalog", "not in the catalog", name)
            return "not_in_catalog"
        tagged = write_catalog_tags(data, match)
        if tagged is None:
            self.record.add(
                RecordEntry(
                    file_id=file_id,
                    original_name=name,
                    action="skip",
                    state="skipped",
                    folder=str(item["folder_id"]),
                    reason="already tagged from the catalog",
                    identified_by="catalog",
                    old_tags=tag_snapshot(data),
                )
            )
            self.summary.add("already_tagged", self.label(item, name))
            return "already_tagged"

        entries_before = len(self.record.entries)
        try:
            entry = write_in_place(
                self.drive,
                self.record,
                file_id=file_id,
                name=name,
                original=data,
                tagged=tagged,
                identified_by="catalog",
                folder=str(item["folder_id"]),
            )
        except AudioChanged:
            raise
        except Exception:
            self._check_unfinished_write(entries_before, file_id, name, data)
            raise
        outcome = "written" if entry.state == "done" else "would_write"
        label = self.label(item, name)
        if outcome == "would_write":
            changes = ", ".join(
                f"{k}: {entry.old_tags.get(k, '')!r} -> {v!r}"
                for k, v in entry.new_tags.items()
                if entry.old_tags.get(k, "") != v
            )
            label = f"{label} ({changes})"
        self.summary.add(outcome, label)
        return outcome

    def _check_unfinished_write(
        self, entries_before: int, file_id: str, name: str, original: bytes
    ) -> None:
        """After a write that failed part-way: if the new bytes were already
        uploaded but not yet checked, check them now (past the deadline if
        need be), and put the original back on a mismatch."""
        if len(self.record.entries) <= entries_before:
            return
        entry = self.record.entries[-1]
        if entry.state != "pending" or not entry.written_md5:
            return
        with without_run_deadline():
            written = self.drive.download_file_bytes(file_id).data
            try:
                check_audio_unchanged(original, written)
            except AudioChanged as exc:
                update_contents(self.drive, file_id, name, original)
                entry.state = "restored"
                entry.error = f"audio changed after the write; original restored: {exc}"
                self.record.checkpoint()
                raise
        entry.state = "done"


def _process_chunk(
    work: Backfill,
    cursor: dict[str, Any],
    cursor_store: StateFile,
    end: int,
) -> None:
    """Process manifest files from the cursor up to ``end``.

    In a writing run the record is saved, and only then the cursor moved
    past the file, after every file. Raises ``RunOutOfTime`` between files
    once the deadline has passed, and :class:`AudioChanged` the moment a
    file's audio check fails, leaving the cursor on that file.
    """
    summary = work.summary
    files = work.manifest["files"]
    writes = work.mode.writes
    index = summary.start
    while index < end:
        item = files[index]
        if shared_deadline_passed():
            raise RunOutOfTime(f"run deadline reached before {item['name']}")
        summary.scanned += 1
        entries_before = len(work.record.entries)
        try:
            outcome = work.one(item)
        except AudioChanged as exc:
            # The cursor stays on this file: the run stops here.
            _record_failure(work, item, entries_before, f"audio check: {exc}")
            work.record.checkpoint()
            raise
        except TagEmptied as exc:
            outcome = "failed"
            _record_failure(work, item, entries_before, str(exc))
        except Exception as exc:  # noqa: BLE001 — one file must not end the run
            if shared_deadline_passed():
                raise RunOutOfTime(
                    f"run deadline reached processing {item['name']}"
                ) from exc
            log.exception("library-backfill: failed on %s", item["name"])
            outcome = "failed"
            _record_failure(work, item, entries_before, f"{type(exc).__name__}: {exc}")
        index += 1
        if writes:
            # The record first: the cursor never passes an unrecorded file.
            work.record.checkpoint()
            cursor["position"] = index
            cursor["counts"][outcome] = cursor["counts"].get(outcome, 0) + 1
            if outcome == "failed":
                cursor["failed"].append(
                    {
                        "file_id": str(item["id"]),
                        "name": str(item["name"]),
                        "run_id": work.record.run_id,
                    }
                )
            cursor["updated_at"] = _now()
            cursor_store.save(cursor)
            summary.position = index


def _record_failure(
    work: Backfill, item: dict[str, Any], entries_before: int, reason: str
) -> None:
    if len(work.record.entries) > entries_before:
        entry = work.record.entries[-1]
        if entry.state not in ("done", "restored"):
            entry.state = "failed"
        entry.error = entry.error or reason
        work.summary.add("failed", f"{work.label(item)} ({reason})")
    else:
        work._skip(item, "failed", reason)


def run_backfill(
    *,
    run_id: str | None,
    mode: FlowMode,
    drive: Any | None = None,
    api: CatalogLookup | None = None,
) -> BackfillSummary:
    """One run: the mode, settings, manifest, cursor, record and report."""
    started_at = monotonic()
    run_id = run_id or local_run_id()
    summary = BackfillSummary(mode)
    with run_report(
        FLOW_NAME,
        repo=REPO,
        notable=True,
        run_id=run_id,
        version=__version__,
    ) as report:
        try:
            if mode is FlowMode.OFF:
                return summary
            settings, problems = BackfillSettings.from_env()
            if settings is None:
                summary.problems.extend(problems)
                return summary
            if drive is None:
                drive = GoogleAPI.from_env().drive
            _run(drive, api, settings, run_id, summary)
            if summary.aborted:
                _report_abort(run_id, summary)
        finally:
            _record(report, summary, monotonic() - started_at)
    return summary


def _run(
    drive: Any,
    api: CatalogLookup | None,
    settings: BackfillSettings,
    run_id: str,
    summary: BackfillSummary,
) -> None:
    mode = summary.mode
    manifest_store = StateFile(drive, settings.state, MANIFEST_NAME)
    cursor_store = StateFile(drive, settings.state, CURSOR_NAME)
    try:
        manifest = manifest_store.load()
        cursor = cursor_store.load()
    except StateProblem as exc:
        summary.problems.append(str(exc))
        return

    if manifest is not None and manifest.get("root_folder_id") != settings.root:
        summary.problems.append(
            f"{MANIFEST_NAME} lists root {manifest.get('root_folder_id')}, not "
            f"{ROOT_FOLDER_VAR}={settings.root}; trash it to list the new root"
        )
        return
    if manifest is None:
        manifest = new_manifest(settings.root)
    if not manifest.get("complete"):
        before = len(manifest["files"])
        try:
            build_manifest(drive, manifest_store, manifest, exclude={settings.state})
        finally:
            summary.listed = len(manifest["files"]) - before
            summary.listing_pending_folders = len(manifest["pending_folders"])
        summary.listing_finished = True

    files = manifest["files"]
    summary.total = len(files)
    generation = str(manifest["generation"])
    if cursor is None or cursor.get("generation") != generation:
        summary.restarted = cursor is not None
        cursor = new_cursor(generation, len(files))
    start = int(cursor.get("position", 0))
    summary.start = summary.position = start
    if start >= len(files):
        summary.done = True
        if mode.writes and not cursor.get("done_at"):
            cursor["done_at"] = _now()
            cursor_store.save(cursor)
        return

    run = cursor.get("run") or {}
    if run.get("run_id") == run_id and int(run.get("chunk_end", 0)) > start:
        # A redelivery of a run the deadline stopped: finish its chunk.
        end = min(int(run["chunk_end"]), len(files))
    else:
        end = min(start + settings.chunk_size, len(files))
    if mode.writes:
        cursor["run"] = {"run_id": run_id, "chunk_end": end}
        cursor["total"] = len(files)
        cursor["updated_at"] = _now()
        cursor_store.save(cursor)

    record = RunRecord(
        FLOW_NAME,
        run_id,
        mode,
        stage=f"files {start + 1}-{end} of {len(files)} (manifest {generation})",
        folders={"root": settings.root, "state": settings.state},
    ).attach(drive, settings.state)
    try:
        _resume_record(drive, settings.state, record)
    except StateProblem as exc:
        summary.problems.append(str(exc))
        return
    summary.record_name = record.file_name
    if api is None:
        api = api_client(base_url=api_base_url().strip() or None)
    work = Backfill(drive, api, settings, record, summary, manifest)
    status = "failed"
    try:
        _process_chunk(work, cursor, cursor_store, end)
        status = "complete"
    except AudioChanged as exc:
        summary.aborted = str(exc)
        status = "aborted"
    except RunOutOfTime:
        status = "stopped"
        raise
    finally:
        if record.entries:
            with without_run_deadline():
                record.finish(status)
    if mode.writes and summary.position >= len(files):
        summary.done = True
        cursor["done_at"] = _now()
        cursor_store.save(cursor)


def _resume_record(drive: Any, state_folder: str, record: RunRecord) -> None:
    """Carry on the record a redelivered run (same run id) already saved,
    so one run keeps one record, as rollback expects."""
    existing = StateFile(drive, state_folder, record.file_name)
    saved = existing.load()
    if saved is None:
        return
    names = {f.name for f in fields(RecordEntry)}
    record.entries = [
        RecordEntry(**{k: v for k, v in entry.items() if k in names})
        for entry in saved.get("entries") or []
    ]
    record.started_at = str(saved.get("started_at") or record.started_at)
    record._file_id = existing.file_id


def library_backfill(*, run_id: str | None = None) -> BackfillSummary:
    """Backfill the next chunk of the library and report the run.

    ``run_id`` is the queue message id when the Lambda worker runs this.
    """
    return run_backfill(run_id=run_id, mode=flow_mode(MODE_VAR))


def _report_abort(run_id: str, summary: BackfillSummary) -> None:
    try:
        post_run_finding(
            FLOW_NAME,
            "ERROR",
            "library-backfill stopped: a file's audio would not stay "
            f"byte-identical ({summary.aborted}). The file is unchanged (or "
            f"restored); the cursor stays on it; see {summary.record_name}.",
            run_id=run_id,
        )
    except Exception:  # noqa: BLE001 — the notification is not the job
        log.exception("library-backfill: could not report the stop")


def _record(report: Any, summary: BackfillSummary, duration_sec: float) -> None:
    report.duration_sec = duration_sec
    if summary.mode is FlowMode.OFF:
        report.notice("off", f"{MODE_VAR} is off: nothing was read or written")
        return
    for problem in summary.problems:
        report.issue("setting", problem)
    if summary.problems:
        return
    dry = summary.mode is FlowMode.DRY_RUN
    if dry:
        report.notice(
            "dry_run",
            f"{MODE_VAR} is dry-run: no music was written and the cursor did "
            f"not move; would-be record {summary.record_name or '(none)'}",
        )
    if summary.listing_finished:
        report.notice(
            "manifest_built",
            f"{MANIFEST_NAME}: {summary.total} files listed",
        )
    elif summary.listing_pending_folders:
        report.notice(
            "manifest_in_progress",
            f"{MANIFEST_NAME}: {summary.listed} files listed this run, "
            f"{summary.listing_pending_folders} folders still to list",
        )
    if summary.restarted:
        report.notice(
            "manifest_rebuilt", "a new manifest: starting from its first file"
        )
    if summary.done:
        report.notice(
            "library_done",
            f"all {summary.total} files in the manifest are processed; trash "
            f"{MANIFEST_NAME} in the state folder to start over",
        )
    report.ok(len(summary.outcomes["written"]))
    report.count("listed", summary.listed)
    report.count("scanned", summary.scanned)
    report.count("matched", summary.matched)
    for outcome in OUTCOMES:
        report.count(outcome, len(summary.outcomes[outcome]))
    report.count("remaining", summary.remaining)
    report.count("done", 1 if summary.done else 0)
    for name in summary.outcomes["written"]:
        report.created("tags written", name)
    for outcome in (
        "would_write",
        "not_in_catalog",
        "needs_fingerprinting",
        "not_mp3",
        "moved_since",
        "skipped_by_setting",
    ):
        for name in summary.outcomes[outcome]:
            report.notice(outcome, name)
    for name in summary.outcomes["failed"]:
        report.issue("failed", name)
    if summary.aborted:
        report.issue("audio_changed_run_stopped", summary.aborted)


if __name__ == "__main__":
    library_backfill()
