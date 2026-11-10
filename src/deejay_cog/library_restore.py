"""Put the library's tags back to what each file held at a moment. Run locally:

    doppler run -- uv run python -m deejay_cog.library_restore <time>
    doppler run -- uv run python -m deejay_cog.library_restore <time> --apply

``<time>`` is an ISO 8601 time with its offset, such as
``2026-11-09T18:30:00-06:00``. Every file in the library folder that Drive
saw change after it is read; the owned tag fields (title, artist, year,
genre, BPM, label) that now differ from the file's last Drive revision saved
at or before that time get that revision's values back. A field the old
revision leaves empty is not emptied. The comment, the audio and every other
frame stay as they are.

Without ``--apply`` it is a dry run: it prints each field it would put back
and changes nothing. With ``--apply`` each write is the safe in-place write
(revision pinned, audio checked before and after) in a run record
(``library-restore-<run id>.json``), which ``music_rollback`` undoes; the
library registry then forgets the values those files were last synced with,
so the library sync records them afresh instead of reading the restore as an
edit made in the file.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime
from typing import Any

from mini_app_polis import logger as logger_mod
from mini_app_polis.google import GoogleAPI

from deejay_cog.library_backfill import ROOT_FOLDER_VAR, StateFile
from deejay_cog.library_scan import LISTING_NAME
from deejay_cog.music_rollback import forget_synced_values
from deejay_cog.music_safety import (
    STATE_FOLDER_VAR,
    FlowMode,
    RunRecord,
    file_meta,
    local_run_id,
    revision_at,
    revision_bytes,
    tag_snapshot,
    write_in_place,
)
from deejay_cog.music_tags import write_owned_fields

log = logger_mod.get_logger()

FLOW_NAME = "library-restore"

#: The owned tag fields put back (never the catalog link).
FIELDS = ("title", "artist", "year", "genre", "bpm", "label")


def changed_since(drive: Any, state: str, root: str, when: datetime) -> list[dict]:
    """The library's files Drive saw change after ``when``, in every folder
    the last scan listed: ``{"id", "name", "path"}`` each."""
    listing = StateFile(drive, state, LISTING_NAME).load() or {}
    folders: dict[str, str] = dict(listing.get("folders") or {})
    folders.setdefault(root, "")
    stamp = when.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    found: list[dict] = []
    for folder_id, folder_path in folders.items():
        token: str | None = None
        while True:
            page = (
                drive.service.files()
                .list(
                    q=(
                        f"'{folder_id}' in parents and trashed = false and "
                        f"modifiedTime > '{stamp}' and "
                        "mimeType != 'application/vnd.google-apps.folder'"
                    ),
                    fields="nextPageToken, files(id, name)",
                    pageSize=1000,
                    pageToken=token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )
            for f in page.get("files") or []:
                name = str(f["name"])
                if name.lower().endswith(".mp3"):
                    path = f"{folder_path}/{name}" if folder_path else name
                    found.append({"id": str(f["id"]), "name": name, "path": path})
            token = page.get("nextPageToken")
            if not token:
                break
    return sorted(found, key=lambda f: f["path"].lower())


def fields_to_restore(before: dict[str, str], now: dict[str, str]) -> dict[str, str]:
    """The owned fields that differ from ``before``, with ``before``'s
    values; a field ``before`` leaves empty is left."""
    return {
        f: before[f]
        for f in FIELDS
        if before.get(f) and before.get(f) != (now.get(f) or "")
    }


def restore(
    drive: Any, state: str, root: str, when: datetime, *, apply: bool
) -> tuple[list[str], list[str]]:
    """Plan, and with ``apply`` make, the restore, printing each line as it
    goes. Returns the lines and the Drive ids of the files written."""
    mode = FlowMode.WRITE if apply else FlowMode.DRY_RUN
    record = RunRecord(
        FLOW_NAME,
        local_run_id(),
        mode,
        stage=f"tags put back to what each file held at {when.isoformat()}",
        folders={"root": root, "state": state},
    ).attach(drive, state)
    lines: list[str] = []
    written: list[str] = []

    def say(line: str) -> None:
        lines.append(line)
        print(line, flush=True)

    status = "complete"
    try:
        for f in changed_since(drive, state, root, when):
            try:
                rev = revision_at(drive, f["id"], when)
                if rev is None:
                    say(f"left  {f['path']}: Drive keeps no revision that old")
                    continue
                before = tag_snapshot(revision_bytes(drive, f["id"], rev))
                meta = file_meta(drive, f["id"])
                data: bytes = drive.download_file_bytes(f["id"]).data
                now = tag_snapshot(data)
                back = fields_to_restore(before, now)
                if not back:
                    continue
                for name in sorted(back):
                    say(
                        f"{'put  ' if apply else 'would'} {f['path']}: {name} "
                        f"{now.get(name)!r} -> {back[name]!r}"
                    )
                tagged = write_owned_fields(data, back)
                if tagged is None:
                    continue
                entry = write_in_place(
                    drive,
                    record,
                    file_id=f["id"],
                    name=f["name"],
                    original=data,
                    tagged=tagged,
                    identified_by=f"restore (tags as at {when.isoformat()})",
                    head_revision=meta.get("headRevisionId"),
                )
                if entry.state == "done":
                    written.append(f["id"])
            except Exception as exc:  # noqa: BLE001 — one file must not stop the rest
                log.exception("library-restore: %s failed", f["path"])
                say(f"FAIL  {f['path']}: {type(exc).__name__}: {exc}")
    except BaseException:
        status = "stopped"
        raise
    finally:
        if record.entries:
            record.finish(status)
    return lines, written


def main(argv: list[str] | None = None) -> int:
    """Print the plan; with ``--apply``, carry it out."""
    parser = argparse.ArgumentParser(
        prog="python -m deejay_cog.library_restore",
        description="Put the library's tags back to what each file held at a time.",
    )
    parser.add_argument("time", help="ISO 8601 time with offset")
    parser.add_argument("--apply", action="store_true", help="write (default: dry run)")
    args = parser.parse_args(argv)
    try:
        when = datetime.fromisoformat(args.time)
    except ValueError:
        print(f"not an ISO 8601 time: {args.time}", file=sys.stderr)
        return 2
    if when.tzinfo is None:
        print("give the time with its offset, e.g. -06:00", file=sys.stderr)
        return 2
    state = os.getenv(STATE_FOLDER_VAR, "").strip()
    root = os.getenv(ROOT_FOLDER_VAR, "").strip()
    if not state or not root:
        print(f"{STATE_FOLDER_VAR} and {ROOT_FOLDER_VAR} must be set", file=sys.stderr)
        return 2
    drive = GoogleAPI.from_env().drive
    lines, written = restore(drive, state, root, when, apply=args.apply)
    files = len({ln.split(":")[0] for ln in lines if ln.startswith(("put", "would"))})
    print(
        f"{'Put back' if args.apply else 'Would put back'} "
        f"{sum(ln.startswith(('put', 'would')) for ln in lines)} fields in {files} files."
    )
    if written:
        forget_synced_values(written)
        print(f"The registry forgot the synced values of {len(written)} files.")
    if not args.apply:
        print("Nothing was changed. Run again with --apply to carry it out.")
    return 1 if any(ln.startswith("FAIL") for ln in lines) else 0


if __name__ == "__main__":
    sys.exit(main())
