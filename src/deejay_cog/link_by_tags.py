"""Link by their own tags the library files the fingerprint found nothing for.

A library file whose tags the catalog does not know goes to the fingerprint
function, and one AcoustID has no match for (or none it is confident of)
is linked by its own tags when they name a song
(:func:`deejay_cog.library_identify.link_by_own_tags`). This does the same
for the files already unidentified with the fingerprint's "no match" note.
Run locally, against production:

    doppler run --config prd -- uv run python -m deejay_cog.link_by_tags
    doppler run --config prd -- uv run python -m deejay_cog.link_by_tags --apply

It takes every present, unidentified registry row whose note is the
fingerprint's "no match" or "no confident match". A file whose tags name a
song is linked to the catalog's entry for them, or to a library entry made
from its title, artist and year, and its registry row keeps the mark
``identified by its own tags (<the fingerprint's note>)``. A file whose
tags name no song (no title or artist, "Unknown", "Various Artists",
"Track 3") is left for the owner.

Without ``--apply`` it is a dry run: it reads the tags the registry holds,
looks each up, and prints what it would do, downloading nothing. With it,
each file is read from Drive and linked the library job's way (the catalog's
values written in place, revision pinned, audio checked), recorded as a
``library-link-by-tags`` run that ``deejay_cog.music_rollback`` can undo
(the file's bytes; the entry and the registry row stay).
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Any

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import CatalogLookupRequest, LibraryFile
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI

from deejay_cog.api_client import api_client
from deejay_cog.library_identify import link_by_own_tags, usable_track
from deejay_cog.music_safety import (
    STATE_FOLDER_VAR,
    FlowMode,
    RunRecord,
    file_meta,
    local_run_id,
)

log = logger_mod.get_logger()

FLOW_NAME = "library-link-by-tags"

#: The notes the fingerprint leaves on a file it found nothing for.
NO_FINGERPRINT = ("fingerprint: no match", "fingerprint: no confident match")

#: Registry rows are read a page at a time.
PAGE = 500


@dataclass
class Candidate:
    """One file the fingerprint found nothing for."""

    row: LibraryFile
    song: str = ""
    result: str = ""


def candidates(api: Any) -> list[Candidate]:
    """Every present, unidentified file whose note says the fingerprint
    found no match, by path."""
    rows: list[LibraryFile] = []
    offset = 0
    while True:
        page = api.list_library_files(
            identification_status="unidentified",
            present=True,
            limit=PAGE,
            offset=offset,
        )
        rows.extend(page)
        if len(page) < PAGE:
            break
        offset += PAGE
    found = [
        Candidate(row)
        for row in rows
        if (row.identify_note or "").startswith(NO_FINGERPRINT)
    ]
    return sorted(found, key=lambda c: c.row.path.lower())


def _registry_tags(row: LibraryFile) -> dict[str, str]:
    tags = row.tags.model_dump() if row.tags is not None else {}
    return {key: str(value or "") for key, value in tags.items()}


def plan(api: Any, found: list[Candidate]) -> None:
    """What each file would get, from the tags the registry holds."""
    for c in found:
        tags = _registry_tags(c.row)
        track = usable_track(tags, c.row.path)
        if track is None:
            c.result = "left: its tags name no song"
            continue
        c.song = f"{tags['artist']} - {tags['title']}"
        match = api.lookup_catalog(CatalogLookupRequest(tracks=[track]))[0]
        if match is None:
            c.result = "would make a library entry"
        else:
            c.result = "would link to the catalog's entry"
            c.song += f" -> {match.artist} - {match.title}"


def apply(drive: Any, api: Any, state_folder: str, found: list[Candidate]) -> str:
    """Link each file by the tags it holds now. Returns the run record's name."""
    record = RunRecord(FLOW_NAME, local_run_id(), FlowMode.WRITE).attach(
        drive, state_folder
    )
    status = "complete"
    for c in found:
        try:
            _one(drive, api, record, c)
        except Exception as exc:  # noqa: BLE001 — one file must not stop the rest
            log.exception("link-by-tags: failed on %s", c.row.path)
            c.result = f"failed: {type(exc).__name__}: {exc}"
            status = "failed"
    if record.entries:
        record.finish(status)
    return record.file_name


def _one(drive: Any, api: Any, record: RunRecord, c: Candidate) -> None:
    row = c.row
    meta = file_meta(drive, row.drive_file_id)
    if meta.get("trashed"):
        c.result = "left: in the trash (the scan will mark it)"
        return
    data: bytes = drive.download_file_bytes(row.drive_file_id).data
    by_tags = link_by_own_tags(
        drive,
        api,
        record,
        file_id=row.drive_file_id,
        name=str(meta.get("name") or row.path.rsplit("/", 1)[-1]),
        root=row.library_root_id,
        path=row.path,
        data=data,
        why=row.identify_note or "",
    )
    if by_tags is None or by_tags.linked is None:
        c.result = "left: its tags name no song"
        return
    c.result = (
        "linked to a new library entry"
        if by_tags.entry_created
        else "linked to the catalog's entry"
    )
    if by_tags.linked.written:
        c.result += "; tags written"


def main(argv: list[str] | None = None) -> int:
    """Print, and with ``--apply`` make, the links. Returns the exit status."""
    parser = argparse.ArgumentParser(
        prog="python -m deejay_cog.link_by_tags",
        description=(
            "Link by their own tags the library files the fingerprint found "
            "no match for."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="link them (without this, only print what would be done)",
    )
    args = parser.parse_args(argv)
    state_folder = os.getenv(STATE_FOLDER_VAR, "").strip()
    if not state_folder:
        print(f"{STATE_FOLDER_VAR} is not set; nothing done.", file=sys.stderr)
        return 2
    api = api_client(base_url=api_base_url().strip() or None)
    found = candidates(api)
    record_name = ""
    if args.apply:
        record_name = apply(GoogleAPI.from_env().drive, api, state_folder, found)
    else:
        plan(api, found)
    print(
        f"{'Linking' if args.apply else 'Dry run of linking'} by their own tags: "
        f"{len(found)} files the fingerprint found no match for"
    )
    for c in found:
        song = f" ({c.song})" if c.song else ""
        print(f"{c.row.path}{song}  [{c.result}]")
    counts: dict[str, int] = {}
    for c in found:
        key = c.result.split(":", 1)[0].split(";", 1)[0]
        counts[key] = counts.get(key, 0) + 1
    for key, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {n:4d}  {key}")
    if record_name:
        print(f"Recorded as {record_name}; music_rollback can undo the file writes.")
    if not args.apply:
        print("Nothing was changed. Run again with --apply to carry it out.")
    return 1 if any(c.result.startswith("failed") for c in found) else 0


if __name__ == "__main__":
    sys.exit(main())
