"""Put back an artist's own styling where a library write flattened it.
Run locally:

    doppler run -- uv run python -m deejay_cog.restore_styling
    doppler run -- uv run python -m deejay_cog.restore_styling --apply

The catalog's values from play history can flatten deliberate styling: an
all-caps name (``WSTRN`` -> ``Wstrn``) or an all-lowercase title
(``i'm so tired...`` -> ``I'm So Tired``). Reconcile runs that applied
``case_punctuation`` wrote those over the files (ADR-0008, decision 6); this
undoes the writes that lost styling.

It reads the same run records as :mod:`deejay_cog.restore_bpm`, finds each
title or artist a write changed only in case and punctuation, and restores
the file's own value where the write lost styling: a word of two or more
capitals that the write took out of all capitals, or an all-lowercase value
the write gave capitals. A write that only corrected sloppy casing
(``GoldFish`` -> ``Goldfish``) or brought in the catalog's own styling
(``If We Never Met`` -> ``if we never met``) is left as it is. Only the
flattened field is written, and only while the file still holds what the
write left.

Without ``--apply`` it is a dry run. With it, each write is made the safe
way and recorded as a ``library-styling-restore`` run, which
``deejay_cog.music_rollback`` can undo.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from typing import Any

from mini_app_polis import logger as logger_mod
from mini_app_polis.google import GoogleAPI

from deejay_cog.music_safety import (
    STATE_FOLDER_VAR,
    FlowMode,
    RunRecord,
    local_run_id,
    tag_snapshot,
    write_in_place,
)
from deejay_cog.music_tags import write_owned_fields
from deejay_cog.restore_bpm import read_run_records

log = logger_mod.get_logger()

FLOW_NAME = "library-styling-restore"

#: The fields whose styling an artist chooses.
STYLED_FIELDS = ("title", "artist")

_NOT_WORD = re.compile(r"[\W_]+", re.UNICODE)
_CAPS_WORD = re.compile(r"\b[^\W\d_]{2,}\b", re.UNICODE)


def _fold(text: str) -> str:
    return _NOT_WORD.sub("", text).casefold()


def _caps_words(text: str) -> set[str]:
    return {w.casefold() for w in _CAPS_WORD.findall(text) if w.isupper()}


def _all_lower(text: str) -> bool:
    return any(c.isalpha() for c in text) and text == text.lower()


def flattened(before: str, after: str) -> bool:
    """Whether ``after`` is ``before`` in other case and punctuation only,
    with the styling ``before`` chose lost."""
    if not before or before == after or _fold(before) != _fold(after):
        return False
    if _caps_words(before) - _caps_words(after):
        return True
    return _all_lower(before) and not _all_lower(after)


@dataclass
class Restore:
    """One field of one file whose styling a write flattened."""

    file_id: str
    name: str
    field: str
    before: str  # the file's own value, before the first write to the field
    written: str  # the value the last write left
    result: str = ""


def styling_writes(records: list[dict[str, Any]]) -> list[Restore]:
    """The title and artist values a done in-place write flattened, with
    the value before the first write and after the last."""
    found: dict[tuple[str, str], Restore] = {}
    ordered = sorted(records, key=lambda r: str(r.get("started_at", "")))
    for record in ordered:
        for entry in record.get("entries") or []:
            if entry.get("action") != "in_place" or entry.get("state") != "done":
                continue
            file_id = str(entry.get("file_id", ""))
            for field in STYLED_FIELDS:
                old = str((entry.get("old_tags") or {}).get(field, ""))
                new = str((entry.get("new_tags") or {}).get(field, ""))
                key = (file_id, field)
                if key in found:
                    if new != old:
                        found[key].written = new
                    continue
                if flattened(old, new):
                    found[key] = Restore(
                        file_id=file_id,
                        name=str(entry.get("original_name", file_id)),
                        field=field,
                        before=old,
                        written=new,
                    )
    return list(found.values())


def restore(
    drive: Any, state_folder: str, *, apply: bool, run_id: str | None = None
) -> tuple[str, list[Restore]]:
    """Plan, and with ``apply`` make, each restore, a file's flattened
    fields in one write. Returns the run record's name and the restores."""
    restores = styling_writes(read_run_records(drive, state_folder))
    by_file: dict[str, list[Restore]] = {}
    for r in restores:
        by_file.setdefault(r.file_id, []).append(r)
    mode = FlowMode.WRITE if apply else FlowMode.DRY_RUN
    record = RunRecord(FLOW_NAME, run_id or local_run_id(), mode).attach(
        drive, state_folder
    )
    status = "complete"
    for file_id, fields in by_file.items():
        try:
            data = drive.download_file_bytes(file_id).data
            now = tag_snapshot(data)
            values: dict[str, str] = {}
            for r in fields:
                if now.get(r.field, "") != r.written:
                    r.result = (
                        f"left alone: its {r.field} is {now.get(r.field, '')!r} now"
                    )
                else:
                    values[r.field] = r.before
            if not values:
                continue
            tagged = write_owned_fields(data, values)
            if tagged is None:
                for r in fields:
                    r.result = r.result or "left alone: already holds its own value"
                continue
            entry = write_in_place(
                drive,
                record,
                file_id=file_id,
                name=fields[0].name,
                original=data,
                tagged=tagged,
                identified_by="styling restore (case_punctuation held)",
            )
            done = "restored" if entry.state == "done" else "would restore"
            for r in fields:
                r.result = r.result or done
        except Exception as exc:  # noqa: BLE001 — one file must not stop the rest
            log.exception("restore-styling: failed on %s", fields[0].name)
            for r in fields:
                r.result = f"failed: {type(exc).__name__}: {exc}"
            status = "failed"
    if record.entries:
        record.finish(status)
    return record.file_name, restores


def main(argv: list[str] | None = None) -> int:
    """Print, and with ``--apply`` make, the restores. Returns the exit
    status."""
    parser = argparse.ArgumentParser(
        prog="python -m deejay_cog.restore_styling",
        description="Put back a title or artist's styling where a library write flattened it.",
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
    files = len({r.file_id for r in restores})
    verb = "Restoring" if args.apply else "Dry run of restoring"
    print(f"{verb} styling: {len(restores)} fields in {files} files")
    for r in restores:
        print(f"{r.name}: {r.field} {r.written!r} -> {r.before!r}  [{r.result}]")
    if args.apply and any(r.result == "restored" for r in restores):
        print(f"Recorded as {record_name}; music_rollback can undo it.")
    if not args.apply:
        print("Nothing was changed. Run again with --apply to carry it out.")
    return 1 if any(r.result.startswith("failed") for r in restores) else 0


if __name__ == "__main__":
    sys.exit(main())
