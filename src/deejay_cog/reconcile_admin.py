"""Work the library reconcile from a working session (ADR-0008, step 3):
review its differences, read the owner's approvals, carry the old approvals
file over to the API, move its cursor and run it by hand. Run locally
against production:

    doppler run --config prd -- uv run python -m deejay_cog.reconcile_admin review
    doppler run --config prd -- uv run python -m deejay_cog.reconcile_admin approvals
    doppler run --config prd -- uv run python -m deejay_cog.reconcile_admin seed [--apply]
    doppler run --config prd -- uv run python -m deejay_cog.reconcile_admin cursor \\
        [--position 0 --apply]
    doppler run --config prd -- uv run python -m deejay_cog.reconcile_admin run \\
        [--mode dry-run|write] [--chunk 260]

**review** reads every reconcile record in ``MUSIC_TAGGING_STATE_FOLDER_ID``
(dry runs and write runs' differences) saved since ``--since`` (an ISO time;
all of them without it), keeps the latest difference for each file and
field, and prints them grouped by kind, each marked approved or held by the
current approvals. ``--out PATH`` also writes it to a file. It reads only.
The owner's review page on the WCS site shows the same differences, as
each run reports them to the API.

**approvals** prints the approvals the write runs apply, as the API holds
them. They are the owner's and are set on the review page: this command
cannot change them. **seed** carries ``library-reconcile-approvals.json``
(the approvals given before the API held them) over to the API, checked the
way a write run read it; without ``--apply`` it is only printed. Once the API
holds approvals it sends nothing and says so (the API would refuse it too).

**cursor** prints the reconcile's cursor; ``--position N --apply`` moves it
(0 starts the next run from the first identified file).

**run** runs the reconcile once, here, with the environment's settings or
the ``--mode`` and ``--chunk`` given, and prints what it did. When the mode
comes out off (the scheduled reconcile turned off, and no ``--mode``), it
runs nothing and says so. A write run
here writes exactly as the scheduled one does; run it while the scheduled
reconcile is off (``LIBRARY_RECONCILE_MODE=off``), so the two do not move
the cursor at once.

**cursor** changes only the cursor file, and **seed** only the API's
approvals, once; **review** and **approvals** read only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from mini_app_polis.api.contract import ReconcileApprovalsSet
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI

from deejay_cog.api_client import api_client
from deejay_cog.library_backfill import StateFile, StateProblem
from deejay_cog.library_reconcile import (
    APPROVALS_NAME,
    CHUNK_SIZE_VAR,
    CURSOR_NAME,
    FLOW_NAME,
    KINDS,
    MODE_VAR,
    Approvals,
    from_api,
    library_reconcile,
    parse_approvals,
)
from deejay_cog.music_safety import STATE_FOLDER_VAR, FlowMode, flow_mode

PROG = "python -m deejay_cog.reconcile_admin"

#: The record names a reconcile run saves its differences under.
RECORD_SUFFIXES = (".dry-run.json", ".differences.json")


class AdminError(Exception):
    """A command that cannot go ahead; the message says why."""


# ---------------------------------------------------------------------------
# Review
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Item:
    """One difference as the latest record holding it has it."""

    kind: str
    path: str
    drive_file_id: str
    field: str
    file_value: str
    catalog_value: str
    saved_at: str

    @property
    def name(self) -> str:
        """The file's name, without the folders it is in."""
        return self.path.rsplit("/", 1)[-1]


def is_record(name: str) -> bool:
    """Whether ``name`` is a reconcile run's record of its differences."""
    return name.startswith(f"{FLOW_NAME}-") and name.endswith(RECORD_SUFFIXES)


def latest_items(records: Sequence[dict[str, Any]], since: str = "") -> list[Item]:
    """The differences in ``records`` saved at or after ``since``, the latest
    record winning for each file and field."""
    latest: dict[tuple[str, str], Item] = {}
    kept = [r for r in records if str(r.get("saved_at", "")) >= since]
    for record in sorted(kept, key=lambda r: str(r.get("saved_at", ""))):
        for kind, diffs in (record.get("differences") or {}).items():
            for diff in diffs:
                item = Item(
                    kind=kind,
                    path=str(diff["path"]),
                    drive_file_id=str(diff["drive_file_id"]),
                    field=str(diff["field"]),
                    file_value=str(diff["file"]),
                    catalog_value=str(diff["catalog"]),
                    saved_at=str(record.get("saved_at", "")),
                )
                latest[(item.drive_file_id, item.field)] = item
    return list(latest.values())


def _approved(item: Item, approvals: Approvals) -> bool:
    if item.kind == "other":
        return item.field in approvals.files.get(item.drive_file_id, frozenset())
    return item.kind in approvals.kinds


def review_text(items: Sequence[Item], approvals: Approvals, since: str = "") -> str:
    """The differences grouped by kind, each marked approved or held."""
    files = {i.drive_file_id for i in items}
    lines = [
        f"# Reconcile review{f' since {since}' if since else ''}",
        f"{len(items)} differences in {len(files)} files",
        "",
    ]
    for kind in KINDS:
        found = sorted(
            (i for i in items if i.kind == kind), key=lambda i: (i.name, i.field)
        )
        if not found:
            continue
        if kind == "other":
            state = "approved file by file"
        else:
            state = "approved" if kind in approvals.kinds else "held"
        lines.append(f"## {kind} ({len(found)}, {state})")
        for i in found:
            mark = ""
            if kind == "other":
                mark = " [approved]" if _approved(i, approvals) else " [held]"
            lines.append(
                f"- {i.name} [{i.drive_file_id}] {i.field}: "
                f"{i.file_value!r} -> {i.catalog_value!r}{mark}"
            )
        lines.append("")
    return "\n".join(lines)


def read_records(drive: Any, state: str) -> list[dict[str, Any]]:
    """Every reconcile record of differences in the state folder."""
    files = drive.list_files(
        parent_id=state, name_contains=f"{FLOW_NAME}-", include_folders=False
    )
    out = []
    for f in files:
        if not is_record(str(f.name)):
            continue
        raw = drive.download_file_bytes(str(f.id)).data
        out.append(json.loads(bytes(raw).decode("utf-8")))
    return out


# ---------------------------------------------------------------------------
# Approvals and cursor
# ---------------------------------------------------------------------------


def old_approvals(drive: Any, state: str) -> dict[str, Any] | None:
    """What ``library-reconcile-approvals.json`` holds, checked the way a
    write run read it; None when there is no such file. Raises
    :class:`AdminError` when it does not parse."""
    data = StateFile(drive, state, APPROVALS_NAME).load()
    if data is None:
        return None
    _, problems = parse_approvals(data)
    if problems:
        raise AdminError("; ".join(problems))
    return {
        "kinds": list(data.get("kinds") or []),
        "files": dict(data.get("files") or {}),
    }


def moved_cursor(cursor: dict[str, Any] | None, position: int) -> dict[str, Any]:
    """The cursor at ``position``; everything else in it is kept but the
    file it last compared, so the next run starts at ``position``."""
    if position < 0:
        raise AdminError("--position must be 0 or more")
    if cursor is None:
        raise AdminError(f"no {CURSOR_NAME} in the state folder: nothing to move")
    return {
        **{key: value for key, value in cursor.items() if key != "after"},
        "position": position,
        "updated_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------------------
# The command line
# ---------------------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROG, description="Work the library reconcile from a working session."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    review = sub.add_parser("review", help="every difference, grouped by kind")
    review.add_argument("--since", default="", help="only records saved since (ISO)")
    review.add_argument("--out", help="also write the review to this file")

    sub.add_parser("approvals", help="print the approvals the write runs apply")

    seed = sub.add_parser("seed", help="carry the old approvals file over to the API")
    seed.add_argument("--apply", action="store_true", help="send it")

    cursor = sub.add_parser("cursor", help="print or move the cursor")
    cursor.add_argument("--position", type=int, help="move it here (with --apply)")
    cursor.add_argument("--apply", action="store_true", help="save the move")

    run = sub.add_parser("run", help="run the reconcile once, here")
    run.add_argument("--mode", choices=["dry-run", "write"])
    run.add_argument("--chunk", type=int, help="files to compare")
    return parser


def _state_folder() -> str:
    state = os.getenv(STATE_FOLDER_VAR, "").strip()
    if not state:
        raise AdminError(f"{STATE_FOLDER_VAR} is not set")
    return state


def _api() -> Any:
    return api_client(base_url=api_base_url().strip() or None)


def _approvals_cmd(api: Any) -> int:
    approvals = api.read_reconcile_approvals()
    print(json.dumps(approvals.model_dump(mode="json"), indent=1))
    if approvals.updated_at is None:
        print("No approvals are set yet: a write run writes nothing.")
    return 0


def _seed_cmd(args: argparse.Namespace, api: Any, drive: Any, state: str) -> int:
    data = old_approvals(drive, state)
    if data is None:
        raise AdminError(f"no {APPROVALS_NAME} in the state folder: nothing to seed")
    print(json.dumps(data, indent=1))
    held = api.read_reconcile_approvals()
    if held.updated_at is not None:
        raise AdminError(
            f"the API already holds approvals (saved {held.updated_at:%Y-%m-%d %H:%M} "
            f"UTC by {held.updated_by}): they are changed only on the review page"
        )
    if not args.apply:
        print("Nothing was sent. Run again with --apply to send it.")
        return 0
    try:
        seeded = api.seed_reconcile_approvals(
            ReconcileApprovalsSet.model_validate(data)
        )
    except Exception as exc:  # noqa: BLE001 — the API says why (409: already set)
        raise AdminError(f"the API refused the seed: {exc}") from exc
    print(
        f"Seeded the API's approvals: {len(seeded.kinds)} kinds, "
        f"{len(seeded.files)} files."
    )
    return 0


def _cursor_cmd(args: argparse.Namespace, drive: Any, state: str) -> int:
    store = StateFile(drive, state, CURSOR_NAME)
    cursor = store.load()
    print(json.dumps(cursor, indent=1))
    if args.position is None:
        return 0
    moved = moved_cursor(cursor, args.position)
    if not args.apply:
        print(f"Would move the cursor to {args.position}. Run again with --apply.")
        return 0
    store.save(moved)
    print(f"Moved the cursor to {args.position}.")
    return 0


def _run_cmd(args: argparse.Namespace) -> int:
    if args.mode:
        os.environ[MODE_VAR] = args.mode
    if args.chunk is not None:
        os.environ[CHUNK_SIZE_VAR] = str(args.chunk)
    if flow_mode(MODE_VAR) is FlowMode.OFF:
        raise AdminError(f"{MODE_VAR} is off here: give --mode dry-run or --mode write")
    s = library_reconcile()
    print(
        f"{s.record_name or 'no record'}: mode {s.mode.value}, files "
        f"{s.start + 1}-{s.position}, {s.compared} compared, {s.matching} matching, "
        f"{len(s.written)} written, {len(s.filled)} filled up, "
        f"{s.not_approved} not approved, {len(s.failed)} failed"
    )
    for line in (*s.problems, *s.failed):
        print(f"  {line}")
    if s.aborted:
        print(f"  stopped: {s.aborted}")
    return 1 if s.problems or s.aborted else 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run one command. Returns the exit status."""
    args = _parser().parse_args(argv)
    try:
        if args.command == "run":
            return _run_cmd(args)
        if args.command == "approvals":
            return _approvals_cmd(_api())
        state = _state_folder()
        drive = GoogleAPI.from_env().drive
        if args.command == "seed":
            return _seed_cmd(args, _api(), drive, state)
        if args.command == "review":
            approvals = from_api(_api().read_reconcile_approvals()) or Approvals()
            text = review_text(
                latest_items(read_records(drive, state), args.since),
                approvals,
                args.since,
            )
            print(text)
            if args.out:
                with open(args.out, "w", encoding="utf-8") as handle:
                    handle.write(text + "\n")
            return 0
        return _cursor_cmd(args, drive, state)
    except (AdminError, StateProblem) as exc:
        print(f"{PROG}: {exc}; nothing changed.", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
