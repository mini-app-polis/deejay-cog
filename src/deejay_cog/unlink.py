"""Clear one library file's catalog link, for a file linked to another
song. Run locally:

    doppler run -- uv run python -m deejay_cog.unlink "<file name>"          # what it would do
    doppler run -- uv run python -m deejay_cog.unlink "<file name>" --apply  # do it

The file is named by its Drive id, its path under the library root, or its
name alone when no other present file has that name. Without ``--apply``
it prints the file's registry row and changes nothing. With ``--apply`` the
row is made unidentified, its link cleared, with a note saying why
(``--note``, kept after ``UNLINKED_BY_OWNER``), so the file goes to the
owner's unidentified-files page to be linked again. library-identify never
links such a file itself: the identification it had was wrong. With no link, the library sync and reconcile leave its tags
alone: the catalog entry it was wrongly linked to is never written into it.
The file's tags are not changed here; ``music_rollback --file`` undoes a
run's write to one file.
"""

from __future__ import annotations

import argparse
import sys

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import LibraryFile, LibraryFileRegister
from mini_app_polis.environment import api_base_url

from deejay_cog.api_client import api_client
from deejay_cog.fingerprint_check import Api, registry_rows
from deejay_cog.library_identify import UNLINKED_BY_OWNER

log = logger_mod.get_logger()

#: The note the row is left with when ``--note`` gives none.
DEFAULT_NOTE = "it was linked to another song"


class UnlinkError(RuntimeError):
    """The file cannot be found, or more than one file has that name."""


def find_row(rows: dict[str, LibraryFile], name: str) -> LibraryFile:
    """The present row ``name`` names: a Drive id, a path, or a file name
    that only one present file has."""
    name = name.strip()
    if name in rows:
        return rows[name]
    by_path = [r for r in rows.values() if r.path.strip("/") == name.strip("/")]
    if by_path:
        return by_path[0]
    by_name = [r for r in rows.values() if r.path.rsplit("/", 1)[-1] == name]
    if not by_name:
        raise UnlinkError(f"no present library file {name!r}")
    if len(by_name) > 1:
        paths = ", ".join(sorted(r.path for r in by_name))
        raise UnlinkError(f"more than one file named {name!r}: {paths}")
    return by_name[0]


def unlink(api: Api, name: str, *, apply: bool, note: str = DEFAULT_NOTE) -> str:
    """Find the file, and with ``apply`` clear its link. Returns what was
    (or would be) done."""
    row = find_row(registry_rows(api), name)
    now = f"{row.path}: {row.identification_status}, catalog {row.catalog_id}"
    if row.catalog_id is None:
        return f"{now}; no link to clear"
    if not apply:
        return f"{now}; would unlink it"
    api.register_library_file(
        LibraryFileRegister(
            drive_file_id=row.drive_file_id,
            library_root_id=row.library_root_id,
            path=row.path,
            identification_status="unidentified",
            catalog_id=None,
            identify_note=UNLINKED_BY_OWNER + note,
        )
    )
    return f"{now}; unlinked"


def main(argv: list[str] | None = None) -> int:
    """Print, and with ``--apply`` clear, one file's catalog link. Returns
    the exit status."""
    parser = argparse.ArgumentParser(
        prog="python -m deejay_cog.unlink",
        description="Clear one library file's catalog link.",
    )
    parser.add_argument("file", help="its Drive id, path, or name")
    parser.add_argument(
        "--note", default=DEFAULT_NOTE, help="why, shown with the file for review"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="clear the link (without this, only print what would be done)",
    )
    args = parser.parse_args(argv)
    api = api_client(base_url=api_base_url().strip() or None)
    try:
        print(unlink(api, args.file, apply=args.apply, note=args.note))
    except UnlinkError as exc:
        print(f"{exc}; nothing done.", file=sys.stderr)
        return 2
    if not args.apply:
        print("Nothing was changed. Run again with --apply to carry it out.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
