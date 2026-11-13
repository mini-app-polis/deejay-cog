"""Retitle the library's existing owner edits with their version in ``< >``.

An owner edit (a cut the owner made by hand: an intro cut, words censored, a
routine cut) is marked by its version in ``< >`` in the file's title, and
reconcile then moves it to an owner-edit entry of its own (ADR-0008,
2026-11-11). Files cut before that rule carry an ordinary label, or none.
This lists the main library's files whose name or title suggests an owner
edit, with the title each would take, for the owner to check, and then
writes the titles he approved. Run locally, against production:

    doppler run --config prd -- uv run python -m deejay_cog.owner_edits propose --out owner-edits.csv
    doppler run --config prd -- uv run python -m deejay_cog.owner_edits apply owner-edits.csv
    doppler run --config prd -- uv run python -m deejay_cog.owner_edits apply owner-edits.csv --write

``propose`` writes a CSV with an ``action`` (``retitle`` or ``skip``), the
proposed ``title`` and a ``note``. A name that says so plainly (no intro,
an intro cut, censored, a routine or team cut) is proposed as ``retitle``.
A bare "Edit" or "Edited", which is also an official release label, is
proposed as ``skip`` for the owner to decide. The owner edits the CSV:
he changes an action, a title, or adds a file the list missed. A file
becomes an owner edit only by his approval, never by the pattern alone.

``apply`` without ``--write`` prints what it would do and changes nothing.
With ``--write`` each ``retitle`` row's title is written to the file's tags
(any format the shared tagger supports, the safe way: revision pinned,
audio checked), recorded as an ``owner-edits`` run that
``deejay_cog.music_rollback`` can undo. The next library reconcile moves
each to an owner-edit entry, keeping its plays. Files are never renamed,
and FX Music is left alone (its own rules apply there).
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from dataclasses import dataclass, fields
from pathlib import PurePosixPath
from typing import Any, Protocol

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import LibraryFile
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI
from mini_app_polis.mp3.tag import AudioTagger

from deejay_cog import audio_formats
from deejay_cog.api_client import api_client
from deejay_cog.library_backfill import FX_ROOT_FOLDER_VAR
from deejay_cog.music_safety import (
    STATE_FOLDER_VAR,
    FlowMode,
    RunRecord,
    local_run_id,
    write_in_place,
)
from deejay_cog.music_tags import is_owner_edit, read_tag_row

log = logger_mod.get_logger()

FLOW_NAME = "owner-edits"

#: Registry rows are read a page at a time.
PAGE = 500


@dataclass(frozen=True)
class Hint:
    """One sign in a name or title that a file is an owner edit."""

    pattern: re.Pattern[str]
    version: str
    plain: bool  # proposed as retitle (True) or left for the owner (False)


#: Checked in order; the first that matches names the version. "Edit" and
#: "Edited" come last and are not plain: they are official labels too.
HINTS = (
    Hint(
        re.compile(r"no[\s_-]*intro|intro[\s_-]*(?:cut|edit)", re.I), "Intro Cut", True
    ),
    Hint(re.compile(r"self[\s_-]*censor|censored", re.I), "Self Censored", True),
    Hint(re.compile(r"routine|team[\s_-]*cut|\bteam\b", re.I), "Routine Cut", True),
    # "Edited" as a word, or run together after a lower-case letter only when
    # capitalised ("SongEdited"), so a word like "Credit" doesn't match. A
    # "Radio Edit" is an official release, never an owner edit.
    Hint(
        re.compile(
            r"(?<![Rr]adio )(?<![Rr]adio)\b[Ee]dit(?:ed)?\b|(?<=[a-z])(?<!radio)Edit(?:ed)?\b"
        ),
        "Edit",
        False,
    ),
)

#: A bracketed part or a trailing " - part" naming the edit, dropped from the
#: title before the ``< >`` version goes on.
_PART = re.compile(r"\s*[\[(]([^\])]*)[\])]|\s+-\s+(.*)$")


class Api(Protocol):
    """The part of the API client this uses."""

    def list_library_files(
        self,
        *,
        present: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[LibraryFile]:
        """One page of registered library files."""
        ...


@dataclass
class Row:
    """One file in the proposal, as the CSV holds it."""

    action: str
    path: str
    file_id: str
    title: str
    current_title: str
    note: str


COLUMNS = [f.name for f in fields(Row)]


def hint_for(text: str) -> Hint | None:
    """The first sign of an owner edit in ``text``, or None."""
    for hint in HINTS:
        if hint.pattern.search(text):
            return hint
    return None


def owner_edit_title(title: str, version: str) -> str:
    """``title`` with the parts naming the edit dropped and ``version`` put
    on in ``< >``: "Song (No Intro Edit)" -> "Song <Intro Cut>"."""

    hint = next((h for h in HINTS if h.version == version), None)

    def drop(match: re.Match[str]) -> str:
        # Only the part naming this edit goes; an official label such as
        # "(Radio Edit)" stays.
        part = match.group(1) if match.group(1) is not None else match.group(2)
        named = hint is not None and hint.pattern.search(part or "")
        return "" if named and hint_for(part or "") is hint else match.group(0)

    base = " ".join(_PART.sub(drop, title).split())
    return f"{base} <{version}>" if base else ""


def propose_row(row: LibraryFile, title: str) -> Row | None:
    """The proposal for one file, or None when nothing suggests an owner
    edit or it is one already."""
    if is_owner_edit(title):
        return None
    stem = PurePosixPath(row.path).stem
    hint = hint_for(title) or hint_for(stem)
    if hint is None:
        return None
    new = owner_edit_title(title or stem, hint.version)
    if hint.plain:
        return Row("retitle", row.path, row.drive_file_id, new, title, "")
    return Row(
        "skip",
        row.path,
        row.drive_file_id,
        new,
        title,
        '"Edit"/"Edited" is also an official label: set retitle if this one is yours',
    )


def main_rows(api: Api, fx_root: str) -> list[LibraryFile]:
    """Every present main-library registry row (FX Music left out)."""
    rows: list[LibraryFile] = []
    offset = 0
    while True:
        page = api.list_library_files(present=True, limit=PAGE, offset=offset)
        rows.extend(
            r for r in page if r.library_root_id != fx_root and not r.trash_requested
        )
        if len(page) < PAGE:
            return rows
        offset += PAGE


def propose(drive: Any, api: Api, fx_root: str) -> list[Row]:
    """One row per main-library file whose name or title suggests an owner
    edit. Only the files whose name already does, or whose registry label
    is an edit label, are downloaded to read the title."""
    out: list[Row] = []
    for row in sorted(main_rows(api, fx_root), key=lambda r: r.path.lower()):
        label = row.edit_label or ""
        if hint_for(PurePosixPath(row.path).stem) is None and hint_for(label) is None:
            continue
        data: bytes = drive.download_file_bytes(row.drive_file_id).data
        if audio_formats.untaggable(data) is not None:
            continue
        proposal = propose_row(row, str(read_tag_row(data).get("title") or "").strip())
        if proposal is not None:
            out.append(proposal)
    return out


def write_csv(path: str, rows: list[Row]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.__dict__)


def read_csv(path: str) -> list[Row]:
    with open(path, newline="", encoding="utf-8") as handle:
        return [
            Row(**{c: (r.get(c) or "").strip() for c in COLUMNS})
            for r in csv.DictReader(handle)
        ]


def retitled(data: bytes, title: str) -> bytes:
    """The file's bytes with ``title`` written, in its own format, every
    other tag left as it is."""
    fmt = audio_formats.other_format(data)
    if fmt is None:
        fmt = audio_formats.AudioFormat.MP3
    if not isinstance(fmt, audio_formats.AudioFormat):
        raise audio_formats.NotTaggable(f"{fmt.reason.value} ({fmt.detail})")
    with audio_formats.local_copy(data, fmt) as local:
        AudioTagger().write(local, {"title": title})
        with open(local, "rb") as handle:
            return handle.read()


def apply(
    drive: Any,
    state_folder: str,
    rows: list[Row],
    *,
    write: bool,
    run_id: str | None = None,
) -> tuple[str, list[str]]:
    """Write the approved titles. Returns the run record's name and one
    result line per row."""
    mode = FlowMode.WRITE if write else FlowMode.DRY_RUN
    record = RunRecord(FLOW_NAME, run_id or local_run_id(), mode).attach(
        drive, state_folder
    )
    results: list[str] = []
    status = "complete"
    for row in rows:
        try:
            results.append(_one(drive, record, row, write=write))
        except Exception as exc:  # noqa: BLE001 — one file must not stop the rest
            log.exception("owner-edits: failed on %s", row.path)
            results.append(f"{row.path}: failed: {type(exc).__name__}: {exc}")
            status = "failed"
    if record.entries:
        record.finish(status)
    return record.file_name, results


def _one(drive: Any, record: RunRecord, row: Row, *, write: bool) -> str:
    if row.action != "retitle":
        return f"{row.path}: skipped"
    if not is_owner_edit(row.title):
        return f"{row.path}: skipped: the title has no version in < >"
    data: bytes = drive.download_file_bytes(row.file_id).data
    tagged = retitled(data, row.title)
    entry = write_in_place(
        drive,
        record,
        file_id=row.file_id,
        name=row.path,
        original=data,
        tagged=tagged,
        identified_by="owner-edits (owner-approved retitle)",
    )
    if not write:
        return f"{row.path}: would retitle to {row.title}"
    if entry.state != "done":
        return f"{row.path}: not written: {entry.state}"
    return f"{row.path}: retitled to {row.title}"


def main(argv: list[str] | None = None) -> int:
    """Propose, or apply, the owner-edit retitles."""
    parser = argparse.ArgumentParser(
        prog="python -m deejay_cog.owner_edits",
        description="Retitle the library's existing owner edits with < >.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    prop = sub.add_parser("propose", help="write the proposal CSV")
    prop.add_argument("--out", required=True, help="the CSV to write")
    app = sub.add_parser("apply", help="carry out an approved proposal CSV")
    app.add_argument("csv", help="the approved CSV")
    app.add_argument(
        "--write",
        action="store_true",
        help="make the changes (without this, only print what would be done)",
    )
    args = parser.parse_args(argv)
    fx_root = os.getenv(FX_ROOT_FOLDER_VAR, "").strip()
    state_folder = os.getenv(STATE_FOLDER_VAR, "").strip()
    if not state_folder:
        print(f"{STATE_FOLDER_VAR} must be set; nothing done.", file=sys.stderr)
        return 2
    drive = GoogleAPI.from_env().drive
    if args.command == "propose":
        api = api_client(base_url=api_base_url().strip() or None)
        rows = propose(drive, api, fx_root)
        write_csv(args.out, rows)
        retitle = sum(r.action == "retitle" for r in rows)
        print(
            f"Wrote {len(rows)} files to {args.out}: {retitle} to retitle, "
            f"{len(rows) - retitle} for you to decide."
        )
        print("Nothing was changed. Check the CSV, then run apply on it.")
        return 0
    record_name, results = apply(
        drive, state_folder, read_csv(args.csv), write=args.write
    )
    print("\n".join(results))
    if args.write:
        print(f"Recorded as {record_name}; music_rollback can undo the writes.")
    else:
        print("Nothing was changed. Run again with --write to carry it out.")
    return 1 if any(": failed:" in r for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
