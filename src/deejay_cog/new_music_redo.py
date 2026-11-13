"""Send the New Music copies intake named from an old file name back through it.

Before intake learned to send a file whose title tag is an old file name to
needs-review (:func:`deejay_cog.new_music.file_name_title`), it named its
copies from such tags: ``CandyCleanKahlo113 Kahlo 113_Kah-lo.mp3``. Library
files are never renamed, so this lists those copies for the owner to check,
and then sends each approved one back through intake, which now names it
properly or sends it to needs-review. Run locally, against production:

    doppler run --config prd -- uv run python -m deejay_cog.new_music_redo propose --out new-music-redo.csv
    doppler run --config prd -- uv run python -m deejay_cog.new_music_redo apply new-music-redo.csv
    doppler run --config prd -- uv run python -m deejay_cog.new_music_redo apply new-music-redo.csv --write

``propose`` reads every writing new-music run record in
``MUSIC_TAGGING_STATE_FOLDER_ID`` and writes a CSV with one row per copy
intake delivered whose title tag the check finds looking like a file name,
with the ``check`` that found it, an ``action`` of ``redo`` and the run it
came from. The owner changes an action to ``skip`` to leave a file as it
is. ``apply`` reads it back and, without ``--write``, prints what it would
do and changes nothing. With ``--write``, for each ``redo`` row:

- **The original is still in processed-originals**: the copy is moved to
  Drive's trash, the registry marks it absent at once (so intake does not
  refuse the original as a byte-identical copy of it), and the original is
  moved back to the new-music input folder.
- **The original is gone**: the copy itself is moved to the input folder
  and marked absent.

The library's steps write tags into a copy once it is in New Music, so a
copy whose tags changed since its run is still sent back, as long as its
audio is the original's. A copy moved since its run, or whose audio
differs, is left alone, and so is its original; each is said so. Nothing
is deleted or renamed, and what was done is saved to the state folder as
``new-music-redo-<time>.json``. The next new-music run takes the files in.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import LibraryFilesAbsentRequest
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI
from pydantic import BaseModel, ConfigDict, Field, field_validator

from deejay_cog.api_client import api_client
from deejay_cog.music_rollback import Step, _move_back_step
from deejay_cog.music_safety import (
    RECORD_MIME_TYPE,
    STATE_FOLDER_VAR,
    AudioChanged,
    check_audio_unchanged,
    file_meta,
    parse_record,
)
from deejay_cog.new_music import FLOW_NAME as NEW_MUSIC_FLOW
from deejay_cog.new_music import file_name_title

log = logger_mod.get_logger()

FLOW_NAME = "new-music-redo"


class Api(Protocol):
    """The part of the API client this uses."""

    def mark_library_files_absent(self, payload: LibraryFilesAbsentRequest) -> Any:
        """Mark registry rows absent."""
        ...


@dataclass
class Row:
    """One delivered copy in the proposal, as the CSV holds it."""

    action: str
    copy_name: str
    copy_id: str
    file_id: str
    check: str
    title: str
    artist: str
    run_id: str
    started_at: str


COLUMNS = [f.name for f in fields(Row)]


class CsvRow(BaseModel):
    """A line of the CSV as read back after review. The owner may have edited
    it, so the action must be one this command carries out; a column that is
    missing or empty reads as "", and other columns are ignored."""

    model_config = ConfigDict(extra="ignore")

    action: Literal["redo", "skip"] = Field(
        description='"redo" sends the file back through intake, "skip" leaves it.'
    )
    copy_name: str = Field(default="", description="The name intake gave the copy.")
    copy_id: str = Field(default="", description="The copy's Drive id.")
    file_id: str = Field(default="", description="The original's Drive id.")
    check: str = Field(
        default="", description="Which check found the title a file name."
    )
    title: str = Field(default="", description="The copy's title tag.")
    artist: str = Field(default="", description="The copy's artist tag.")
    run_id: str = Field(default="", description="The new-music run that made the copy.")
    started_at: str = Field(default="", description="When that run started.")

    @field_validator("*", mode="before")
    @classmethod
    def _cell(cls, value: object) -> object:
        """A cell as text with its surrounding spaces removed; empty for none."""
        if value is None:
            return ""
        return value.strip() if isinstance(value, str) else value


def new_music_records(drive: Any, state_folder: str) -> list[dict[str, Any]]:
    """Every writing new-music run record in the state folder, oldest first."""
    prefix = f"{NEW_MUSIC_FLOW}-"
    files = drive.list_files(
        parent_id=state_folder, name_contains=prefix, include_folders=False
    )
    records: list[dict[str, Any]] = []
    for f in files:
        name = str(f.name)
        if not name.startswith(prefix) or not name.endswith(".json"):
            continue
        if name.endswith(".dry-run.json") or name.startswith(f"{FLOW_NAME}-"):
            continue
        try:
            record = parse_record(drive.download_file_bytes(str(f.id)).data)
        except ValueError:
            continue
        if record.get("flow") == NEW_MUSIC_FLOW and not record.get("dry_run"):
            records.append(record)
    return sorted(records, key=lambda r: str(r.get("started_at") or ""))


def propose(drive: Any, state_folder: str) -> list[Row]:
    """One row for each delivered copy named from a title tag that looks like
    an old file name, the latest delivery of each original."""
    found: dict[str, Row] = {}
    for record in new_music_records(drive, state_folder):
        for entry in record.get("entries") or []:
            copy = entry.get("copy") or {}
            if entry.get("action") != "deliver" or entry.get("state") != "done":
                continue
            if not copy.get("id") or copy.get("trashed"):
                continue
            tags = entry.get("new_tags") or {}
            title = str(tags.get("title") or "").strip()
            artist = str(tags.get("artist") or "").strip()
            check = file_name_title(title, artist)
            if check is None:
                continue
            file_id = str(entry.get("file_id") or "")
            found[file_id] = Row(
                "redo",
                str(copy.get("name") or entry.get("new_name") or ""),
                str(copy["id"]),
                file_id,
                check,
                title,
                artist,
                str(record.get("run_id") or ""),
                str(record.get("started_at") or ""),
            )
    return sorted(found.values(), key=lambda r: r.copy_name.lower())


def write_csv(path: str, rows: list[Row]) -> None:
    """Write the proposal to ``path`` as a CSV for the owner to review."""
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.__dict__)


def read_csv(path: str) -> list[Row]:
    """The reviewed proposal at ``path``, each line checked through CsvRow."""
    with open(path, newline="", encoding="utf-8") as handle:
        return [
            Row(**CsvRow.model_validate(r).model_dump()) for r in csv.DictReader(handle)
        ]


def _entries(drive: Any, state_folder: str) -> dict[str, tuple[dict, dict]]:
    """Each original's latest delivery entry, with its run's folders."""
    out: dict[str, tuple[dict, dict]] = {}
    for record in new_music_records(drive, state_folder):
        for entry in record.get("entries") or []:
            if entry.get("action") == "deliver" and entry.get("state") == "done":
                out[str(entry.get("file_id") or "")] = (
                    entry,
                    record.get("folders") or {},
                )
    return out


def _copy_left(drive: Any, row: Row, entry: dict, destination: str) -> str:
    """Why the copy must be left alone, or "" when it may be sent back: it is
    still in the folder its run put it in and still carries the audio its
    run wrote. Tags may differ: the library's steps write them."""
    copy = file_meta(drive, row.copy_id)
    if copy.get("trashed"):
        return "the copy is in the trash"
    if (copy.get("parents") or []) != [destination]:
        return "the copy was moved since the run"
    written = entry.get("written_md5")
    if written and copy.get("md5Checksum") == written:
        return ""
    source = row.file_id
    if file_meta(drive, source).get("trashed"):
        return ""  # nothing to compare with; the copy is all there is
    try:
        check_audio_unchanged(
            drive.download_file_bytes(source).data,
            drive.download_file_bytes(row.copy_id).data,
        )
    except AudioChanged as exc:
        return f"the copy's audio is not the original's ({exc})"
    return ""


def _steps(drive: Any, row: Row, entry: dict, folders: dict) -> list[Step]:
    """What sending one copy back takes: trash the copy and move the original
    back, or, with the original gone, move the copy itself to the input. A
    copy left alone leaves its original where it is."""
    name = row.copy_name or row.copy_id
    move = entry.get("move") or {}
    originals = move.get("to") or folders.get("originals") or ""
    input_folder = move.get("from") or folders.get("input") or ""
    destination = (entry.get("copy") or {}).get("folder_id") or folders.get(
        "destination", ""
    )
    original = file_meta(drive, row.file_id)
    have_original = not original.get("trashed") and originals in (
        original.get("parents") or []
    )
    trash = f"trash the copy {name} ({row.copy_id})"
    back = "move the copy to the input folder (its original is gone)"
    left = _copy_left(drive, row, entry, destination)
    if left:
        steps = [Step(name, trash if have_original else back, skipped=left)]
        if have_original:
            steps.append(
                Step(name, "move the original back", skipped="its copy was left alone")
            )
        return steps
    if not have_original:
        return [
            Step(
                name,
                back,
                action=lambda: drive.move_file(row.copy_id, new_parent_id=input_folder),
            )
        ]
    return [
        Step(name, trash, action=lambda: drive.trash_file(row.copy_id)),
        _move_back_step(drive, row.file_id, name, originals, input_folder),
    ]


def apply(
    drive: Any, api: Api, state_folder: str, rows: list[Row], *, write: bool
) -> list[str]:
    """Carry out the approved rows. Returns one result line per step."""
    entries = _entries(drive, state_folder)
    lines: list[str] = []
    report: list[dict[str, str]] = []
    for row in rows:
        if row.action != "redo":
            lines.append(f"skip  {row.copy_name}: skipped")
            continue
        found = entries.get(row.file_id)
        if found is None or (found[0].get("copy") or {}).get("id") != row.copy_id:
            lines.append(
                f"skip  {row.copy_name}: no delivery of this copy in a run record"
            )
            continue
        try:
            steps = _steps(drive, row, *found)
        except Exception as exc:  # noqa: BLE001 — one file must not stop the rest
            log.exception("new-music-redo: could not plan %s", row.copy_name)
            lines.append(f"fail  {row.copy_name}: {type(exc).__name__}: {exc}")
            continue
        if write:
            _carry_out(api, row, steps)
        for step in steps:
            lines.append(step.line + (f"  [{step.result}]" if step.result else ""))
            report.append(
                {
                    "file": step.file_name,
                    "what": step.what,
                    "skipped": step.skipped,
                    "result": step.result,
                }
            )
    if write and report:
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        drive.upload_bytes(
            parent_id=state_folder,
            filename=f"{FLOW_NAME}-{stamp}.json",
            content=json.dumps(
                {"flow": FLOW_NAME, "at": stamp, "steps": report}, indent=2
            ).encode(),
            mime_type=RECORD_MIME_TYPE,
        )
    return lines


def _carry_out(api: Api, row: Row, steps: list[Step]) -> None:
    """Run one row's steps in order, marking the copy absent as soon as it has
    left New Music; a step that fails or is skipped stops the rest."""
    for step in steps:
        if step.action is None:
            step.result = "skipped"
            return
        try:
            step.action()
            step.result = "done"
            if step is steps[0]:
                api.mark_library_files_absent(
                    LibraryFilesAbsentRequest(drive_file_ids=[row.copy_id])
                )
        except Exception as exc:  # noqa: BLE001 — one file must not stop the rest
            log.exception("new-music-redo: %s failed", step.what)
            step.result = f"failed: {type(exc).__name__}: {exc}"
            return


def main(argv: list[str] | None = None) -> int:
    """Propose, or apply, sending the junk-named copies back through intake."""
    parser = argparse.ArgumentParser(
        prog="python -m deejay_cog.new_music_redo",
        description="Send New Music copies named from an old file name back through intake.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    prop = sub.add_parser("propose", help="write the list of copies to send back")
    prop.add_argument("--out", required=True, help="the CSV to write")
    app = sub.add_parser("apply", help="carry out an approved proposal CSV")
    app.add_argument("csv", help="the approved CSV")
    app.add_argument(
        "--write",
        action="store_true",
        help="make the changes (without this, only print what would be done)",
    )
    args = parser.parse_args(argv)
    state_folder = os.getenv(STATE_FOLDER_VAR, "").strip()
    if not state_folder:
        print(f"{STATE_FOLDER_VAR} must be set; nothing done.", file=sys.stderr)
        return 2
    drive = GoogleAPI.from_env().drive
    if args.command == "propose":
        rows = propose(drive, state_folder)
        write_csv(args.out, rows)
        print(f"Wrote {len(rows)} copies to {args.out}.")
        print("Nothing was changed. Check the CSV, then run apply on it.")
        return 0
    api = api_client(base_url=api_base_url().strip() or None)
    lines = apply(drive, api, state_folder, read_csv(args.csv), write=args.write)
    print("\n".join(lines) or "Nothing to do.")
    if not args.write:
        print("Nothing was changed. Run again with --write to carry it out.")
    return (
        1 if any("[failed" in line or line.startswith("fail ") for line in lines) else 0
    )


if __name__ == "__main__":
    sys.exit(main())
