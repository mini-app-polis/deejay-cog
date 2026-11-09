"""Put back a file's own BPM where a library write replaced it with a
different tempo. Run locally:

    doppler run -- uv run python -m deejay_cog.restore_bpm
    doppler run -- uv run python -m deejay_cog.restore_bpm --apply

The catalog can hold one BPM for a song whose files are different
versions at different tempos (a 90 BPM edit and the 102 BPM original), so
writing its BPM over every file of the song was wrong. ``bpm_differs``
(``BPM_ROUNDING`` or more apart) is held for that reason; this undoes the
writes made before it was.

It reads every library-reconcile and library-sync run record in
``MUSIC_TAGGING_STATE_FOLDER_ID``, finds each file whose BPM a write moved
by ``BPM_ROUNDING`` or more, and writes back the BPM the file had before
its first such write: that one field, nothing else. A file whose BPM has
changed since the last such write is left alone and said so. Rounding
(``bpm_rounding``) is not undone.

Without ``--apply`` it is a dry run that prints what it would do. With it,
each write is made the safe way (:func:`write_in_place`: revision pinned,
audio checked) and recorded as a ``library-bpm-restore`` run, which
``deejay_cog.music_rollback`` can undo.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any

from mini_app_polis import logger as logger_mod
from mini_app_polis.google import GoogleAPI

from deejay_cog.library_reconcile import BPM_ROUNDING
from deejay_cog.music_safety import (
    STATE_FOLDER_VAR,
    FlowMode,
    RunRecord,
    local_run_id,
    tag_snapshot,
    write_in_place,
)
from deejay_cog.music_tags import write_owned_fields

log = logger_mod.get_logger()

FLOW_NAME = "library-bpm-restore"

#: The flows that write catalog values over library files in place.
WRITING_FLOWS = ("library-reconcile", "library-sync")


def _bpm(text: str) -> float | None:
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


@dataclass
class Restore:
    """One file whose BPM a write moved to a different tempo."""

    file_id: str
    name: str
    before: str  # the file's own BPM, before the first such write
    written: str  # the BPM the last such write left
    result: str = ""


def tempo_writes(records: list[dict[str, Any]]) -> list[Restore]:
    """The files whose BPM a done in-place write moved by ``BPM_ROUNDING``
    or more, oldest write first: the BPM before the first, and the
    BPM after the last."""
    found: dict[str, Restore] = {}
    ordered = sorted(records, key=lambda r: str(r.get("started_at", "")))
    for record in ordered:
        for entry in record.get("entries") or []:
            if entry.get("action") != "in_place" or entry.get("state") != "done":
                continue
            old = str((entry.get("old_tags") or {}).get("bpm", ""))
            new = str((entry.get("new_tags") or {}).get("bpm", ""))
            old_n, new_n = _bpm(old), _bpm(new)
            file_id = str(entry.get("file_id", ""))
            if file_id in found:
                # A later write over a file already moved: what it left
                # is what is checked before restoring.
                if new != old:
                    found[file_id].written = new
                continue
            if old_n is None or new_n is None or abs(old_n - new_n) < BPM_ROUNDING:
                continue
            found[file_id] = Restore(
                file_id=file_id,
                name=str(entry.get("original_name", file_id)),
                before=old,
                written=new,
            )
    return list(found.values())


def read_run_records(drive: Any, state_folder: str) -> list[dict[str, Any]]:
    """Every writing run record of the flows in :data:`WRITING_FLOWS`."""
    out: list[dict[str, Any]] = []
    for flow in WRITING_FLOWS:
        files = drive.list_files(
            parent_id=state_folder, name_contains=f"{flow}-", include_folders=False
        )
        for f in files:
            name = str(f.name)
            if not name.startswith(f"{flow}-") or not name.endswith(".json"):
                continue
            if name.endswith(".dry-run.json"):
                continue
            raw = drive.download_file_bytes(str(f.id)).data
            try:
                data = json.loads(bytes(raw).decode("utf-8"))
            except ValueError:
                continue
            # Records of differences and approvals share the prefix; a run
            # record is the one with entries.
            if (
                isinstance(data, dict)
                and data.get("flow") == flow
                and "entries" in data
            ):
                out.append(data)
    return out


def restore(
    drive: Any, state_folder: str, *, apply: bool, run_id: str | None = None
) -> tuple[str, list[Restore]]:
    """Plan, and with ``apply`` make, each restore. Returns the run record's
    name and the restores, each with its result."""
    restores = tempo_writes(read_run_records(drive, state_folder))
    mode = FlowMode.WRITE if apply else FlowMode.DRY_RUN
    record = RunRecord(FLOW_NAME, run_id or local_run_id(), mode).attach(
        drive, state_folder
    )
    status = "complete"
    for r in restores:
        try:
            data = drive.download_file_bytes(r.file_id).data
            now = tag_snapshot(data).get("bpm", "")
            if now != r.written:
                r.result = f"left alone: its BPM is {now!r} now, not {r.written!r}"
                continue
            tagged = write_owned_fields(data, {"bpm": r.before})
            if tagged is None:
                r.result = "left alone: already holds its own BPM"
                continue
            entry = write_in_place(
                drive,
                record,
                file_id=r.file_id,
                name=r.name,
                original=data,
                tagged=tagged,
                identified_by="bpm restore (bpm_differs held)",
            )
            r.result = "restored" if entry.state == "done" else "would restore"
        except Exception as exc:  # noqa: BLE001 — one file must not stop the rest
            log.exception("restore-bpm: failed on %s", r.name)
            r.result = f"failed: {type(exc).__name__}: {exc}"
            status = "failed"
    if record.entries:
        record.finish(status)
    return record.file_name, restores


def main(argv: list[str] | None = None) -> int:
    """Print, and with ``--apply`` make, the restores. Returns the exit
    status."""
    parser = argparse.ArgumentParser(
        prog="python -m deejay_cog.restore_bpm",
        description="Put back files' own BPM where a library write changed the tempo.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write the restores (without this, only print what would be done)",
    )
    args = parser.parse_args(argv)
    state_folder = os.getenv(STATE_FOLDER_VAR, "").strip()
    if not state_folder:
        print(f"{STATE_FOLDER_VAR} is not set; nothing done.", file=sys.stderr)
        return 2
    drive = GoogleAPI.from_env().drive
    record_name, restores = restore(drive, state_folder, apply=args.apply)
    print(
        f"{'Restoring' if args.apply else 'Dry run of restoring'} BPM: {len(restores)} files"
    )
    for r in restores:
        print(f"{r.name}: {r.written} -> {r.before}  [{r.result}]")
    if args.apply and any(r.result == "restored" for r in restores):
        print(f"Recorded as {record_name}; music_rollback can undo it.")
    if not args.apply:
        print("Nothing was changed. Run again with --apply to carry it out.")
    return 1 if any(r.result.startswith("failed") for r in restores) else 0


if __name__ == "__main__":
    sys.exit(main())
