"""Name, or trash, the FX Music files whose tags name no song.

library-identify skips an FX Music file whose tags do not name a song ("set
its title and artist"), and FX Music's tags win over the catalog, so nothing
fills them in. This proposes a title and artist for each such file from its
name, or proposes trashing it when it is a copy of another file in the same
folder, for the owner to check, and then writes what he approved. Run
locally, against production:

    doppler run --config prd -- uv run python -m deejay_cog.fx_untagged propose --out fx-untagged.csv
    doppler run --config prd -- uv run python -m deejay_cog.fx_untagged apply fx-untagged.csv
    doppler run --config prd -- uv run python -m deejay_cog.fx_untagged apply fx-untagged.csv --write

``propose`` reads every present FX Music file the registry has as not
identified and writes a CSV, one row per file whose tags name no song, with
an ``action`` of ``tag``, ``trash`` or ``skip``, the proposed ``title`` and
``artist``, and a ``note`` on how they were found. The owner edits the
CSV: he corrects a title or artist, fills in what is blank, or changes an
action. ``apply`` reads it back and, without ``--write``, prints what it
would do and changes nothing. With ``--write``:

- **tag**: the title and artist are written to the file's tags, in any
  format the shared tagger supports, the safe way (:func:`write_in_place`:
  revision pinned, audio checked). The write is recorded as an
  ``fx-untagged`` run, which ``deejay_cog.music_rollback`` can undo. The
  next library-identify then takes the file in.
- **trash**: the file is moved to Drive's trash and the registry marks it
  absent. It can be restored from Drive's trash.
- **skip**: nothing is done.

A file is never renamed or moved, and a row with an empty title or artist
is never written.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from dataclasses import dataclass, fields
from pathlib import PurePosixPath
from typing import Any, Literal, Protocol

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import LibraryFile, LibraryFilesAbsentRequest
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI
from mini_app_polis.mp3.tag import AudioTagger
from pydantic import BaseModel, ConfigDict, Field, field_validator

from deejay_cog import audio_formats
from deejay_cog.api_client import api_client
from deejay_cog.library_backfill import FX_ROOT_FOLDER_VAR
from deejay_cog.library_identify import usable_track
from deejay_cog.music_safety import (
    STATE_FOLDER_VAR,
    FlowMode,
    RunRecord,
    local_run_id,
    write_in_place,
)
from deejay_cog.music_tags import read_tag_row

log = logger_mod.get_logger()

FLOW_NAME = "fx-untagged"

#: Registry rows are read a page at a time.
PAGE = 500

#: A leading track number: "01 - ", "02", "3. ".
_TRACK_NUMBER = re.compile(r"^\d{1,3}(?:\s*[-.]\s*|\s+|(?=[A-Z]))")

#: An artist the identify step refuses, so a name's "VA - Footloose" gives
#: no artist.
_NO_ARTIST = re.compile(r"^(?:unknown(?: artist)?|various(?: artists)?|va|n/?a)$", re.I)

#: " copy", " copy 2", "(copy)" at the end of a name.
_COPY = re.compile(r"[\s_-]*\(?copy(?:\s*\d+)?\)?$", re.I)

#: The boundary in a run-together name: "GoodMood" -> "Good Mood".
_CAMEL = re.compile(r"(?<=[a-z])(?=[A-Z])")


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

    def mark_library_files_absent(self, payload: LibraryFilesAbsentRequest) -> Any:
        """Mark registry rows absent."""
        ...


@dataclass
class Row:
    """One file in the proposal, as the CSV holds it."""

    action: str
    path: str
    file_id: str
    title: str
    artist: str
    current_title: str
    current_artist: str
    note: str


COLUMNS = [f.name for f in fields(Row)]


class CsvRow(BaseModel):
    """A line of the CSV as read back after review. The owner may have edited
    it, so the action must be one this command carries out; a column that is
    missing or empty reads as "", and other columns are ignored."""

    model_config = ConfigDict(extra="ignore")

    action: Literal["tag", "trash", "skip"] = Field(
        description='"tag" writes the title and artist, "trash" moves the copy to the trash, "skip" leaves the file.'
    )
    path: str = Field(
        default="", description="The file's path under its root, as listed."
    )
    file_id: str = Field(default="", description="The file's Drive id.")
    title: str = Field(default="", description="The title to write.")
    artist: str = Field(default="", description="The artist to write.")
    current_title: str = Field(
        default="", description="The title the file has now, for the owner to compare."
    )
    current_artist: str = Field(
        default="", description="The artist the file has now, for the owner to compare."
    )
    note: str = Field(default="", description="Why the line was proposed as it was.")

    @field_validator("*", mode="before")
    @classmethod
    def _cell(cls, value: object) -> object:
        """A cell as text with its surrounding spaces removed; empty for none."""
        if value is None:
            return ""
        return value.strip() if isinstance(value, str) else value


def _stem(path: str) -> str:
    return PurePosixPath(path).stem.strip()


def _spaced(text: str) -> str:
    """Underscores and run-together words as words, spaces tidied."""
    text = _CAMEL.sub(" ", text.replace("_", " "))
    return " ".join(text.split())


def name_guess(path: str) -> tuple[str, str, str]:
    """A title and artist read from a file's name, and how: "Artist -
    Title", or "Title_Artist" as the old library named files. A part that
    can't be told is left empty."""
    stem = _COPY.sub("", _stem(path))
    stem = _TRACK_NUMBER.sub("", stem).strip() or stem
    if " - " in stem:
        artist, title = (part.strip() for part in stem.split(" - ", 1))
        if _NO_ARTIST.match(artist):
            return title, "", f'from the name; "{artist}" is not an artist, so set one'
        return title, artist, 'from the name ("Artist - Title")'
    parts = [p for p in stem.split("_") if p]
    if len(parts) >= 2 and not parts[1].isdigit():
        return _spaced(parts[0]), _spaced(parts[1]), 'from the name ("Title_Artist")'
    return _spaced(stem), "", "from the name; no artist in it, so set one"


def copy_of(path: str, siblings: set[str]) -> str:
    """The file in the same folder this one is a copy of, by name, or ""."""
    stem = _stem(path)
    base = _COPY.sub("", stem).strip()
    if base == stem or not base:
        return ""
    folder = str(PurePosixPath(path).parent)
    suffix = PurePosixPath(path).suffix
    for other in sorted(siblings):
        if other == path or str(PurePosixPath(other).parent) != folder:
            continue
        if _stem(other).lower() == base.lower() or other.lower() == (
            f"{folder}/{base}{suffix}".lower()
        ):
            return other
    return ""


def propose_row(row: LibraryFile, tags: dict[str, str], siblings: set[str]) -> Row:
    """The proposal for one file whose tags name no song."""
    current_title = str(tags.get("title") or "").strip()
    current_artist = str(tags.get("artist") or "").strip()
    original = copy_of(row.path, siblings)
    if original:
        return Row(
            "trash",
            row.path,
            row.drive_file_id,
            "",
            "",
            current_title,
            current_artist,
            f"a copy of {original} in the same folder",
        )
    title, artist, note = name_guess(row.path)
    if current_artist and not _NO_ARTIST.match(current_artist):
        artist, note = current_artist, f"{note}; artist kept from its tags"
    action = "tag" if title and artist else "skip"
    return Row(
        action,
        row.path,
        row.drive_file_id,
        title,
        artist,
        current_title,
        current_artist,
        note,
    )


def fx_rows(api: Api, fx_root: str) -> list[LibraryFile]:
    """Every present FX Music registry row."""
    rows: list[LibraryFile] = []
    offset = 0
    while True:
        page = api.list_library_files(present=True, limit=PAGE, offset=offset)
        rows.extend(r for r in page if r.library_root_id == fx_root)
        if len(page) < PAGE:
            return rows
        offset += PAGE


def propose(drive: Any, api: Api, fx_root: str) -> list[Row]:
    """One row for each FX Music file whose tags name no song."""
    rows = fx_rows(api, fx_root)
    siblings = {r.path for r in rows}
    out: list[Row] = []
    for row in sorted(rows, key=lambda r: r.path.lower()):
        if row.identification_status == "identified" or row.trash_requested:
            continue
        data: bytes = drive.download_file_bytes(row.drive_file_id).data
        if audio_formats.untaggable(data) is not None:
            continue
        tags = read_tag_row(data)
        if usable_track(tags, row.path) is not None:
            continue
        out.append(propose_row(row, tags, siblings))
    return out


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


def retitled(data: bytes, title: str, artist: str) -> bytes:
    """The file's bytes with ``title`` and ``artist`` written, in its own
    format, every other tag left as it is."""
    fmt = audio_formats.other_format(data)
    if fmt is None:
        fmt = audio_formats.AudioFormat.MP3
    if not isinstance(fmt, audio_formats.AudioFormat):
        raise audio_formats.NotTaggable(f"{fmt.reason.value} ({fmt.detail})")
    with audio_formats.local_copy(data, fmt) as local:
        AudioTagger().write(local, {"title": title, "artist": artist})
        with open(local, "rb") as handle:
            return handle.read()


def apply(
    drive: Any,
    api: Api,
    state_folder: str,
    rows: list[Row],
    *,
    write: bool,
    run_id: str | None = None,
) -> tuple[str, list[str]]:
    """Carry out the approved rows. Returns the run record's name and one
    result line per row."""
    mode = FlowMode.WRITE if write else FlowMode.DRY_RUN
    record = RunRecord(FLOW_NAME, run_id or local_run_id(), mode).attach(
        drive, state_folder
    )
    results: list[str] = []
    trashed: list[str] = []
    status = "complete"
    for row in rows:
        try:
            results.append(_one(drive, record, row, write=write, trashed=trashed))
        except Exception as exc:  # noqa: BLE001 — one file must not stop the rest
            log.exception("fx-untagged: failed on %s", row.path)
            results.append(f"{row.path}: failed: {type(exc).__name__}: {exc}")
            status = "failed"
    if trashed:
        api.mark_library_files_absent(LibraryFilesAbsentRequest(drive_file_ids=trashed))
    if record.entries:
        record.finish(status)
    return record.file_name, results


def _one(
    drive: Any, record: RunRecord, row: Row, *, write: bool, trashed: list[str]
) -> str:
    if row.action == "trash":
        if not write:
            return f"{row.path}: would move to Drive's trash"
        drive.trash_file(row.file_id)
        trashed.append(row.file_id)
        return f"{row.path}: moved to Drive's trash"
    if row.action != "tag":
        return f"{row.path}: skipped"
    if not row.title or not row.artist:
        return f"{row.path}: skipped: the title or artist is empty"
    data: bytes = drive.download_file_bytes(row.file_id).data
    tagged = retitled(data, row.title, row.artist)
    entry = write_in_place(
        drive,
        record,
        file_id=row.file_id,
        name=row.path,
        original=data,
        tagged=tagged,
        identified_by="fx-untagged (owner-approved title and artist)",
    )
    if not write:
        return f"{row.path}: would set {row.artist} - {row.title}"
    if entry.state != "done":
        return f"{row.path}: not written: {entry.state}"
    return f"{row.path}: set {row.artist} - {row.title}"


def main(argv: list[str] | None = None) -> int:
    """Propose, or apply, the FX Music titles and trashes."""
    parser = argparse.ArgumentParser(
        prog="python -m deejay_cog.fx_untagged",
        description="Name, or trash, the FX Music files whose tags name no song.",
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
    if not fx_root or not state_folder:
        print(
            f"{FX_ROOT_FOLDER_VAR} and {STATE_FOLDER_VAR} must be set; nothing done.",
            file=sys.stderr,
        )
        return 2
    drive = GoogleAPI.from_env().drive
    api = api_client(base_url=api_base_url().strip() or None)
    if args.command == "propose":
        rows = propose(drive, api, fx_root)
        write_csv(args.out, rows)
        counts = {a: sum(r.action == a for r in rows) for a in ("tag", "trash", "skip")}
        print(
            f"Wrote {len(rows)} files to {args.out}: {counts['tag']} to tag, "
            f"{counts['trash']} to trash, {counts['skip']} needing a title or artist."
        )
        print("Nothing was changed. Check the CSV, then run apply on it.")
        return 0
    record_name, results = apply(
        drive, api, state_folder, read_csv(args.csv), write=args.write
    )
    print("\n".join(results))
    if args.write:
        print(f"Recorded as {record_name}; music_rollback can undo the tag writes.")
    else:
        print("Nothing was changed. Run again with --write to carry it out.")
    return 1 if any(": failed:" in r for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
