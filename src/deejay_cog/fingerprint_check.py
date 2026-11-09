"""Find, and undo, fingerprint links that retitled a library file to
another song. Run locally, against production:

    doppler run --config prd -- uv run python -m deejay_cog.fingerprint_check
    doppler run --config prd -- uv run python -m deejay_cog.fingerprint_check --apply

Until the fingerprint function checked a match against the file's own tags,
a confident AcoustID match was linked and written even when the file's own
title and artist named another song (AcoustID's links to recordings are
crowd-sourced, and some are wrong): "Beggin" by Madcon was retitled "Don't
Worry". This reads every library-identify run record in
``MUSIC_TAGGING_STATE_FOLDER_ID`` and takes each in-place write made by a
fingerprint (``identified_by`` "fingerprint ...") whose old title and artist
do not agree with the new ones (:func:`deejay_cog.music_tags.tags_agree`).

Without ``--apply`` it is a dry run that lists them. With it, each one's run
is rolled back (:func:`deejay_cog.music_rollback.rollback`: the Drive
revision the run kept is written back, only while the file still holds what
the run wrote), and its registry row is made unidentified, its link
cleared, with a note saying why, for the owner's unidentified-files page.
A library entry the run created stays in the catalog.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Protocol

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import (
    LibraryFile,
    LibraryFileRegister,
)
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI

from deejay_cog.api_client import api_client
from deejay_cog.library_identify import FLOW_NAME as IDENTIFY_FLOW_NAME
from deejay_cog.music_rollback import rollback
from deejay_cog.music_safety import STATE_FOLDER_VAR
from deejay_cog.music_tags import tags_agree

log = logger_mod.get_logger()

#: Registry rows are read a page at a time.
PAGE = 500


class Api(Protocol):
    """The part of the API client this uses."""

    def register_library_file(self, payload: LibraryFileRegister) -> LibraryFile: ...

    def list_library_files(
        self,
        *,
        present: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[LibraryFile]: ...


@dataclass
class Mismatch:
    """One fingerprint write that retitled a file to another song."""

    run_id: str
    file_id: str
    name: str
    was: str
    became: str
    result: str = ""


def mismatches(records: list[dict[str, Any]]) -> list[Mismatch]:
    """Each done in-place fingerprint write whose old title and artist do
    not agree with the new ones, newest record last."""
    found: list[Mismatch] = []
    for record in sorted(records, key=lambda r: str(r.get("started_at", ""))):
        for entry in record.get("entries") or []:
            if entry.get("action") != "in_place" or entry.get("state") != "done":
                continue
            if not str(entry.get("identified_by", "")).startswith("fingerprint"):
                continue
            old = entry.get("old_tags") or {}
            new = entry.get("new_tags") or {}
            old_title, old_artist = (
                str(old.get("title", "")),
                str(old.get("artist", "")),
            )
            new_title, new_artist = (
                str(new.get("title", "")),
                str(new.get("artist", "")),
            )
            if not old_title.strip() or not old_artist.strip():
                continue  # no usable tags of its own: nothing to disagree with
            if tags_agree(old_title, old_artist, new_title, new_artist):
                continue
            found.append(
                Mismatch(
                    run_id=str(record.get("run_id", "")),
                    file_id=str(entry.get("file_id", "")),
                    name=str(entry.get("original_name", entry.get("file_id", ""))),
                    was=f"{old_artist} - {old_title}",
                    became=f"{new_artist} - {new_title}",
                )
            )
    return found


def read_run_records(drive: Any, state_folder: str) -> list[dict[str, Any]]:
    """Every library-identify write run record."""
    out: list[dict[str, Any]] = []
    files = drive.list_files(
        parent_id=state_folder,
        name_contains=f"{IDENTIFY_FLOW_NAME}-",
        include_folders=False,
    )
    for f in files:
        name = str(f.name)
        if not name.startswith(f"{IDENTIFY_FLOW_NAME}-") or not name.endswith(".json"):
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
            and data.get("flow") == IDENTIFY_FLOW_NAME
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


def check(drive: Any, api: Api, state_folder: str, *, apply: bool) -> list[Mismatch]:
    """List, and with ``apply`` undo, each mismatched fingerprint write."""
    found = mismatches(read_run_records(drive, state_folder))
    if not apply:
        for m in found:
            m.result = "would roll back and unlink"
        return found
    rows = registry_rows(api) if found else {}
    for m in found:
        try:
            _, steps = rollback(drive, state_folder, m.run_id, apply=True)
            results = [s.result or s.skipped or "" for s in steps]
            if not any(r == "done" for r in results):
                m.result = f"left alone: {'; '.join(results) or 'nothing to undo'}"
                continue
            row = rows.get(m.file_id)
            if row is not None:
                api.register_library_file(
                    LibraryFileRegister(
                        drive_file_id=row.drive_file_id,
                        library_root_id=row.library_root_id,
                        path=row.path,
                        identification_status="unidentified",
                        catalog_id=None,
                        identify_note=(
                            f"fingerprint said {m.became}, the file says {m.was}; "
                            "rolled back, left for review"
                        ),
                    )
                )
            m.result = "rolled back and unlinked"
        except Exception as exc:  # noqa: BLE001 — one file must not stop the rest
            log.exception("fingerprint-check: failed on %s", m.name)
            m.result = f"failed: {type(exc).__name__}: {exc}"
    return found


def main(argv: list[str] | None = None) -> int:
    """Print, and with ``--apply`` undo, the mismatches. Returns the exit
    status."""
    parser = argparse.ArgumentParser(
        prog="python -m deejay_cog.fingerprint_check",
        description=(
            "Find fingerprint links that retitled a library file to another "
            "song, and undo them."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="roll them back and unlink them (without this, only list them)",
    )
    args = parser.parse_args(argv)
    state_folder = os.getenv(STATE_FOLDER_VAR, "").strip()
    if not state_folder:
        print(f"{STATE_FOLDER_VAR} is not set; nothing done.", file=sys.stderr)
        return 2
    drive = GoogleAPI.from_env().drive
    api = api_client(base_url=api_base_url().strip() or None)
    found = check(drive, api, state_folder, apply=args.apply)
    print(
        f"{'Undoing' if args.apply else 'Dry run:'} {len(found)} fingerprint "
        "links that retitled a file to another song"
    )
    for m in found:
        print(f"{m.name}: {m.was!r} -> {m.became!r}  [{m.result}]  (run {m.run_id})")
    if not args.apply:
        print("Nothing was changed. Run again with --apply to carry it out.")
    return 1 if any(m.result.startswith("failed") for m in found) else 0


if __name__ == "__main__":
    sys.exit(main())
