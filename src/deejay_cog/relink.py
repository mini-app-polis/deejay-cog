"""Link again the library files the catalog lookup matched past a version.

Until the API's fix of Nov 8 2026, the lookup's loose match dropped a
version that followed an unbracketed feat. credit: "Starboy ft. Daft Punk
(Alex Aiono Cover)" matched the plain "Starboy", so the backfill linked the
cover to the original's entry and wrote that link into the file. Run
locally, against production:

    doppler run --config prd -- uv run python -m deejay_cog.relink
    doppler run --config prd -- uv run python -m deejay_cog.relink --apply

It reads every library-backfill run record in
``MUSIC_TAGGING_STATE_FOLDER_ID`` and takes each file whose title, as the
record has it, carries an unbracketed feat./ft./featuring credit with a
bracketed or " - " part after it. Each one is read again from Drive and
looked up again, and its link (``TXXX:CATALOG_ID``) compared with the
answer:

- **same entry**: left as it is;
- **another entry**: the file's link is rewritten to it, and the registry
  row links it;
- **no entry**: the registry row is made unidentified, so library-identify
  takes the file and writes its own link. The file's old link tag is left
  (a tag is never emptied); only the registry's link is read.

Only ``TXXX:CATALOG_ID`` is written; every other frame is left, and the
file is never renamed or moved. The next reconcile compares a re-linked file
with its new entry. Without ``--apply`` it is a dry run that prints what it
would do. With it, each write is made the safe way (:func:`write_in_place`:
revision pinned, audio checked) and recorded as a ``library-relink`` run,
which ``deejay_cog.music_rollback`` can undo (the file's bytes; the
registry row is put right by running this again).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import (
    CatalogLookupRequest,
    CatalogMatch,
    LibraryFile,
    LibraryFileRegister,
)
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI

from deejay_cog.api_client import api_client
from deejay_cog.music_safety import (
    STATE_FOLDER_VAR,
    FlowMode,
    RunRecord,
    local_run_id,
    write_in_place,
)
from deejay_cog.music_tags import (
    ingest_track,
    linked_catalog_id,
    read_tag_row,
    write_owned_fields,
)

log = logger_mod.get_logger()

FLOW_NAME = "library-relink"

#: The flow whose links the lookup's old loose match could have got wrong.
LINKING_FLOW = "library-backfill"

#: An unbracketed feat. credit with a bracketed or " - " part after it: the
#: titles the old loose match read as the plain song.
CREDIT_THEN_PART = re.compile(
    r"\s(?:feat\.?|ft\.?|featuring)\s[^\[()\]]*?(?:\s*[\[(]|\s+-\s+)", re.IGNORECASE
)

#: Registry rows are read a page at a time.
PAGE = 500


class Api(Protocol):
    """The part of the API client this uses."""

    def lookup_catalog(
        self, payload: CatalogLookupRequest
    ) -> list[CatalogMatch | None]:
        """Each track's catalog entry, or None (``POST /v1/catalog/lookup``)."""
        ...

    def register_library_file(self, payload: LibraryFileRegister) -> LibraryFile:
        """Register a library file, or bring its registry row up to date."""
        ...

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
class Relink:
    """One file to look up again."""

    file_id: str
    name: str
    title: str
    linked: str = ""
    found: str = ""
    result: str = ""


def candidates(records: list[dict[str, Any]]) -> list[Relink]:
    """Each file a backfill record names, once, whose recorded title the old
    loose match could have read as the plain song."""
    found: dict[str, Relink] = {}
    for record in records:
        for entry in record.get("entries") or []:
            file_id = str(entry.get("file_id", ""))
            title = str((entry.get("old_tags") or {}).get("title", ""))
            if not file_id or file_id in found or not CREDIT_THEN_PART.search(title):
                continue
            found[file_id] = Relink(
                file_id=file_id,
                name=str(entry.get("original_name", file_id)),
                title=title,
            )
    return sorted(found.values(), key=lambda r: r.name.lower())


def read_run_records(drive: Any, state_folder: str) -> list[dict[str, Any]]:
    """Every run record of :data:`LINKING_FLOW` (write runs only)."""
    out: list[dict[str, Any]] = []
    files = drive.list_files(
        parent_id=state_folder, name_contains=f"{LINKING_FLOW}-", include_folders=False
    )
    for f in files:
        name = str(f.name)
        if not name.startswith(f"{LINKING_FLOW}-") or not name.endswith(".json"):
            continue
        if name.endswith(".dry-run.json"):
            continue
        raw = drive.download_file_bytes(str(f.id)).data
        try:
            data = json.loads(bytes(raw).decode("utf-8"))
        except ValueError:
            continue
        if (
            isinstance(data, dict)
            and data.get("flow") == LINKING_FLOW
            and "entries" in data
        ):
            out.append(data)
    return out


def registry_rows(api: Api) -> dict[str, LibraryFile]:
    """Every present registry row, by Drive file id."""
    rows: dict[str, LibraryFile] = {}
    offset = 0
    while True:
        page = api.list_library_files(present=True, limit=PAGE, offset=offset)
        rows.update({row.drive_file_id: row for row in page})
        if len(page) < PAGE:
            return rows
        offset += PAGE


def relink(
    drive: Any,
    api: Api,
    state_folder: str,
    *,
    apply: bool,
    run_id: str | None = None,
    path: str = "",
) -> tuple[str, list[Relink]]:
    """Plan, and with ``apply`` make, each relink. Returns the run record's
    name and the files, each with its result. With ``path``, the files are
    instead the present registry rows whose path contains it (case
    ignored), whatever their title."""
    if path:
        rows = registry_rows(api)
        needle = path.lower()
        planned = sorted(
            (
                Relink(file_id=row.drive_file_id, name=row.path, title="")
                for row in rows.values()
                if needle in row.path.lower()
            ),
            key=lambda r: r.name.lower(),
        )
    else:
        planned = candidates(read_run_records(drive, state_folder))
        rows = registry_rows(api) if planned else {}
    mode = FlowMode.WRITE if apply else FlowMode.DRY_RUN
    record = RunRecord(FLOW_NAME, run_id or local_run_id(), mode).attach(
        drive, state_folder
    )
    status = "complete"
    for r in planned:
        try:
            _one(drive, api, record, rows, r, apply=apply)
        except Exception as exc:  # noqa: BLE001 — one file must not stop the rest
            log.exception("relink: failed on %s", r.name)
            r.result = f"failed: {type(exc).__name__}: {exc}"
            status = "failed"
    if record.entries:
        record.finish(status)
    return record.file_name, planned


def _one(
    drive: Any,
    api: Api,
    record: RunRecord,
    rows: dict[str, LibraryFile],
    r: Relink,
    *,
    apply: bool,
) -> None:
    row = rows.get(r.file_id)
    if row is None:
        r.result = "left alone: not a present file in the registry"
        return
    data: bytes = drive.download_file_bytes(r.file_id).data
    r.linked = linked_catalog_id(data)
    tags = read_tag_row(data)
    r.title = r.title or tags["title"]
    track = ingest_track(tags)
    if track is None:
        r.result = "left alone: no usable title or artist now"
        return
    match = api.lookup_catalog(CatalogLookupRequest(tracks=[track]))[0]
    r.found = str(match.id) if match else ""
    if r.found == r.linked:
        r.result = "unchanged: already linked to the entry the lookup finds"
        return
    if match is None:
        # The file's link tag stays: emptying a tag is refused everywhere
        # (check_no_emptied_tags), and only the registry's link is read. The
        # row goes unidentified, so library-identify takes the file and
        # writes its own link.
        if not apply:
            r.result = "would unlink in the registry (left for identify)"
            return
        _register(api, row, None)
        r.result = "unlinked in the registry: left for identify"
        return
    tagged = write_owned_fields(data, {"catalog_id": r.found})
    if tagged is None:
        r.result = "unchanged: the file already holds that link"
        return
    entry = write_in_place(
        drive,
        record,
        file_id=r.file_id,
        name=r.name,
        original=data,
        tagged=tagged,
        identified_by="relink (version after a feat. credit)",
    )
    if not apply:
        r.result = "would link to another entry"
        return
    if entry.state != "done":
        r.result = f"not written: {entry.state}"
        return
    _register(api, row, match.id)
    r.result = "linked to another entry"


def _register(api: Api, row: LibraryFile, catalog_id: uuid.UUID | None) -> None:
    """The registry row, linked to ``catalog_id`` or made unidentified."""
    api.register_library_file(
        LibraryFileRegister(
            drive_file_id=row.drive_file_id,
            library_root_id=row.library_root_id,
            path=row.path,
            identification_status="identified" if catalog_id else "unidentified",
            catalog_id=catalog_id,
        )
    )


def main(argv: list[str] | None = None) -> int:
    """Print, and with ``--apply`` make, the relinks. Returns the exit status."""
    parser = argparse.ArgumentParser(
        prog="python -m deejay_cog.relink",
        description=(
            "Look up again the library files whose title has a version after a "
            "feat. credit, and fix their catalog link."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write the relinks (without this, only print what would be done)",
    )
    parser.add_argument(
        "--path",
        default="",
        help=(
            "look up again the files whose registry path contains this text, "
            "instead of the titles the backfill records name"
        ),
    )
    args = parser.parse_args(argv)
    state_folder = os.getenv(STATE_FOLDER_VAR, "").strip()
    if not state_folder:
        print(f"{STATE_FOLDER_VAR} is not set; nothing done.", file=sys.stderr)
        return 2
    drive = GoogleAPI.from_env().drive
    api = api_client(base_url=api_base_url().strip() or None)
    record_name, files = relink(
        drive, api, state_folder, apply=args.apply, path=args.path
    )
    print(
        f"{'Relinking' if args.apply else 'Dry run of relinking'}: "
        f"{len(files)} files with a version after a feat. credit"
    )
    for r in files:
        print(
            f"{r.name} ({r.title!r}): {r.linked or '-'} -> {r.found or '-'}  [{r.result}]"
        )
    if args.apply and any(r.result.startswith("linked") for r in files):
        print(f"Recorded as {record_name}; music_rollback can undo the file writes.")
    if not args.apply:
        print("Nothing was changed. Run again with --apply to carry it out.")
    return 1 if any(r.result.startswith("failed") for r in files) else 0


if __name__ == "__main__":
    sys.exit(main())
