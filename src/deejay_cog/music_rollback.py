"""Undo a music-tagging run from its run record. Run locally:

    doppler run -- uv run python -m deejay_cog.music_rollback <run-id>
    doppler run -- uv run python -m deejay_cog.music_rollback <run-id> --apply

Without ``--apply`` it is a dry run: it reads the run's record from
``MUSIC_TAGGING_STATE_FOLDER_ID`` (``<flow>-<run id>.json``), looks at each
file as it is now, and prints what it would do. With ``--apply`` it does it,
and saves what it did there too, as ``music-rollback-<run id>-<time>.json``.

Per file, last first:

- **A copy the run made** (new-music) is moved to the Drive trash, never
  deleted, and only while it is still in the folder the run put it in with
  the contents the run wrote. A copy moved or changed since is left alone
  and said so.
- **A move the run made** (an original to processed-originals or
  needs-review) is moved back to the folder it came from, only while it is
  still where the run put it.
- **Tags written in place** (library backfill) are undone by writing back
  the Drive revision the run kept, only while the file still holds what the
  run wrote.

An original moved back into the new-music input folder is taken in again by
the next new-music run; set ``NEW_MUSIC_MODE`` to ``off`` first to stop that.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from mini_app_polis import logger as logger_mod
from mini_app_polis.google import GoogleAPI

from deejay_cog.music_safety import (
    RECORD_MIME_TYPE,
    SOURCE_PROPERTY,
    STATE_FOLDER_VAR,
    file_meta,
    parse_record,
    revision_bytes,
    update_contents,
)

log = logger_mod.get_logger()

ROLLBACK_FLOW = "music-rollback"


@dataclass
class Step:
    """One thing the rollback does, or why it does not."""

    file_name: str
    what: str
    action: Callable[[], None] | None = None
    skipped: str = ""
    result: str = ""

    @property
    def line(self) -> str:
        """The step as one line of the rollback plan."""
        if self.skipped:
            return f"skip  {self.file_name}: {self.what} — {self.skipped}"
        return f"do    {self.file_name}: {self.what}"


class RollbackError(RuntimeError):
    """The run's record cannot be found or is not one to undo."""


def find_record(drive: Any, state_folder: str, run_id: str) -> tuple[str, str]:
    """The id and name of the run's record. A dry run's record is never
    one to undo, since that run wrote nothing."""
    run_id = run_id.strip()
    if not run_id:
        raise RollbackError("no run id given")
    files = drive.list_files(
        parent_id=state_folder, name_contains=run_id, include_folders=False
    )
    matches = [f for f in files if str(f.name).endswith(f"-{run_id}.json")]
    if not matches:
        dry = [f for f in files if str(f.name).endswith(f"-{run_id}.dry-run.json")]
        if dry:
            raise RollbackError(
                f"run {run_id} was a dry run ({dry[0].name}): it wrote nothing"
            )
        raise RollbackError(f"no run record for {run_id} in the state folder")
    if len(matches) > 1:
        names = ", ".join(sorted(str(f.name) for f in matches))
        raise RollbackError(f"more than one record for {run_id}: {names}")
    return str(matches[0].id), str(matches[0].name)


def _parents(meta: dict[str, Any]) -> list[str]:
    return list(meta.get("parents") or [])


def _trash_copy_step(
    drive: Any, entry: dict[str, Any], copy: dict[str, Any], name: str
) -> Step:
    copy_id = str(copy.get("id") or "")
    what = f"trash the copy {copy.get('name') or copy_id} ({copy_id})"
    if copy.get("trashed"):
        return Step(name, what, skipped="the run already trashed it")
    meta = file_meta(drive, copy_id)
    if meta.get("trashed"):
        return Step(name, what, skipped="already in the trash")
    if copy.get("folder_id") not in _parents(meta):
        return Step(name, what, skipped="moved since the run; left alone")
    written = entry.get("written_md5")
    if written and meta.get("md5Checksum") and meta["md5Checksum"] != written:
        return Step(name, what, skipped="changed since the run; left alone")
    return Step(name, what, action=lambda: drive.trash_file(copy_id))


def _move_back_step(
    drive: Any, file_id: str, name: str, moved_to: str, moved_from: str
) -> Step:
    what = f"move the original back from {moved_to} to {moved_from}"
    meta = file_meta(drive, file_id)
    parents = _parents(meta)
    if meta.get("trashed"):
        return Step(name, what, skipped="it is in the trash; left alone")
    if moved_to not in parents:
        if moved_from in parents:
            return Step(name, what, skipped="already back")
        return Step(name, what, skipped="moved since the run; left alone")
    if parents != [moved_to]:
        # Drive's move takes a file out of every folder it is in.
        return Step(name, what, skipped="also in another folder now; left alone")
    return Step(
        name,
        what,
        action=lambda: drive.move_file(file_id, new_parent_id=moved_from),
    )


def _restore_revision_step(drive: Any, entry: dict[str, Any], name: str) -> Step:
    file_id = str(entry["file_id"])
    revision = entry.get("revision_kept")
    what = f"restore revision {revision} over the tags the run wrote"
    if not revision:
        return Step(name, what, skipped="the run kept no revision (it wrote nothing)")
    if entry.get("state") == "restored":
        return Step(name, what, skipped="the run already restored it")
    meta = file_meta(drive, file_id)
    written = entry.get("written_md5")
    if not written:
        return Step(name, what, skipped="the run had not written the file")
    if meta.get("md5Checksum") != written:
        return Step(name, what, skipped="changed since the run; left alone")

    def restore() -> None:
        update_contents(drive, file_id, name, revision_bytes(drive, file_id, revision))

    return Step(name, what, action=restore)


def plan_rollback(drive: Any, record: dict[str, Any]) -> list[Step]:
    """What undoing ``record`` takes, file by file, last first."""
    if record.get("dry_run"):
        raise RollbackError("a dry run's record: that run wrote nothing")
    folders = record.get("folders") or {}
    steps: list[Step] = []
    for entry in reversed(record.get("entries") or []):
        action = entry.get("action")
        file_id = str(entry.get("file_id") or "")
        name = str(entry.get("original_name") or file_id)
        if entry.get("state") == "planned":
            continue
        if action == "in_place":
            steps.append(_restore_revision_step(drive, entry, name))
            continue
        if action not in ("deliver", "review"):
            continue
        if action == "deliver":
            copy = entry.get("copy")
            if not copy and folders.get("destination"):
                # A run stopped between the upload and its record: find the
                # copy by the source it names.
                found = drive.find_files_by_app_property(
                    folders["destination"], key=SOURCE_PROPERTY, value=file_id
                )
                if found:
                    copy = {"id": found[0], "folder_id": folders["destination"]}
            if copy:
                steps.append(_trash_copy_step(drive, entry, copy, name))
        move = entry.get("move")
        moved_to = (move or {}).get("to") or folders.get(
            "originals" if action == "deliver" else "review"
        )
        moved_from = (move or {}).get("from") or folders.get("input")
        if moved_to and moved_from:
            steps.append(_move_back_step(drive, file_id, name, moved_to, moved_from))
    return steps


def rollback(
    drive: Any, state_folder: str, run_id: str, *, apply: bool
) -> tuple[str, list[Step]]:
    """Plan, and with ``apply`` carry out, the undoing of one run. Returns
    the record's name and the steps, each with its result."""
    record_id, record_name = find_record(drive, state_folder, run_id)
    record = parse_record(drive.download_file_bytes(record_id).data)
    steps = plan_rollback(drive, record)
    if not apply:
        return record_name, steps
    for step in steps:
        if step.action is None:
            step.result = "skipped"
            continue
        try:
            step.action()
            step.result = "done"
        except Exception as exc:  # noqa: BLE001 — one file must not stop the rest
            log.exception("music-rollback: %s failed", step.what)
            step.result = f"failed: {type(exc).__name__}: {exc}"
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report = {
        "flow": ROLLBACK_FLOW,
        "run_id": run_id,
        "record": record_name,
        "at": stamp,
        "steps": [
            {
                "file": s.file_name,
                "what": s.what,
                "skipped": s.skipped,
                "result": s.result,
            }
            for s in steps
        ],
    }
    drive.upload_bytes(
        parent_id=state_folder,
        filename=f"{ROLLBACK_FLOW}-{run_id}-{stamp}.json",
        content=json.dumps(report, indent=2).encode(),
        mime_type=RECORD_MIME_TYPE,
    )
    return record_name, steps


def main(argv: list[str] | None = None) -> int:
    """Plan the undo of one run from its record, and carry it out with
    ``--apply``. Returns the exit status."""
    parser = argparse.ArgumentParser(
        prog="python -m deejay_cog.music_rollback",
        description="Undo a music-tagging run from its run record.",
    )
    parser.add_argument("run_id", help="the run id the record is named by")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="carry it out (without this, only print what would be done)",
    )
    args = parser.parse_args(argv)

    state_folder = os.getenv(STATE_FOLDER_VAR, "").strip()
    if not state_folder:
        print(f"{STATE_FOLDER_VAR} is not set; nothing done.", file=sys.stderr)
        return 2
    drive = GoogleAPI.from_env().drive
    try:
        record_name, steps = rollback(
            drive, state_folder, args.run_id, apply=args.apply
        )
    except RollbackError as exc:
        print(f"{exc}; nothing done.", file=sys.stderr)
        return 2
    print(
        f"{'Rolling back' if args.apply else 'Dry run of rolling back'} {record_name}"
    )
    for step in steps:
        print(step.line + (f"  [{step.result}]" if step.result else ""))
    if not steps:
        print("Nothing to undo.")
    if not args.apply:
        print("Nothing was changed. Run again with --apply to carry it out.")
    return 1 if any(s.result.startswith("failed") for s in steps) else 0


if __name__ == "__main__":
    sys.exit(main())
