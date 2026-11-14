"""List library files that look linked to another song. Read-only. Run locally:

    doppler run --config prd -- uv run python -m deejay_cog.link_audit

Every present, identified library file is compared with its catalog entry
by its file name, which intake writes as ``Title_Artist.mp3`` (older files
as ``Title_Artist_BPM.mp3``): nothing is downloaded, and nothing is
written, to the files, the registry or the catalog.

A name's title and artist each agree with the entry's when either, with
case, spacing and symbols left out, holds the other (a title also with what
each puts in brackets left out) (so "PharrellWilliams"
agrees with "Pharrell Williams", "Cheerleader" with "Cheerleader (Felix
Jaehn remix)"). The files are listed in two groups:

- **likely another song**: neither the title nor the artist agrees, as
  for a fingerprint that matched the wrong recording ("Despacito_Luis
  Fonsi.mp3" linked to Katy Perry's "California Gurls");
- **title differs**: the artist agrees and the title does not, which is
  another song by the same artist or a title the entry spells differently.

A name with no artist part is compared by its title alone. A file whose
name was itself written from a wrong match agrees with that match and is
not found here. Each line names the file, the entry it is linked to and the
entry's id, for :mod:`deejay_cog.unlink` and the unidentified-files page.
"""

from __future__ import annotations

import argparse
import re
import sys
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from mini_app_polis.api.contract import (
    CATALOG_DISPLAY_MAX_IDS,
    CatalogDisplay,
    CatalogDisplayRequest,
    LibraryFile,
)
from mini_app_polis.environment import api_base_url

from deejay_cog.api_client import api_client
from deejay_cog.fingerprint_check import registry_rows
from deejay_cog.library_reconcile import _fold

#: A name part that is only a tempo ("..._100.mp3").
_BPM_PART = re.compile(r"^\d{2,3}(?:\.\d+)?$")
#: A bracketed part of a title: its version, a feature or a note.
_BRACKETED = re.compile(r"[\[(][^\])]*[\])]")
#: Artist parts that name nobody.
_NO_ARTIST = {"", "unknown", "various", "variousartists"}


class Api(Protocol):
    """The part of the API client this uses."""

    def list_library_files(self, **kwargs: Any) -> list[LibraryFile]:
        """One page of registered files."""
        ...

    def read_catalog_display(
        self, payload: CatalogDisplayRequest
    ) -> list[CatalogDisplay | None]:
        """Each entry's owned fields as a file should carry them."""
        ...


@dataclass(frozen=True)
class Suspect:
    """One file whose name disagrees with its entry."""

    path: str
    drive_file_id: str
    catalog_id: uuid.UUID
    entry: str
    both: bool

    @property
    def line(self) -> str:
        """The file, the entry it is linked to, and the entry's id."""
        return f"{self.path}  ->  {self.entry}  [{self.catalog_id}]"


def name_parts(path: str) -> tuple[str, str]:
    """The title and artist a file name gives, with a trailing tempo left
    out: ``Title_Artist_100.mp3`` is ("Title", "Artist")."""
    stem = path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    parts = stem.split("_")
    while len(parts) > 1 and _BPM_PART.match(parts[-1].strip()):
        parts.pop()
    title = parts[0]
    artist = " ".join(parts[1:])
    return title.strip(), artist.strip()


def agrees(one: str, other: str) -> bool:
    """Whether either text, with case, spacing and symbols left out, holds
    the other."""
    a, b = _fold(one), _fold(other)
    return bool(a) and bool(b) and (a in b or b in a)


def same_title(name_title: str, entry_title: str) -> bool:
    """Whether a name's title is the entry's, whole or without what each
    puts in brackets ("Cheerleader (Remix)" and "Cheerleader (Felix Jaehn
    remix)")."""
    return agrees(name_title, entry_title) or agrees(
        _BRACKETED.sub(" ", name_title), _BRACKETED.sub(" ", entry_title)
    )


def judge(row: LibraryFile, display: CatalogDisplay) -> Suspect | None:
    """The file as a suspect when its name disagrees with its entry."""
    title, artist = name_parts(row.path)
    if not _fold(title):
        return None
    if same_title(title, display.title):
        return None
    no_artist = _fold(artist) in _NO_ARTIST
    both = not no_artist and not agrees(artist, display.artist)
    assert row.catalog_id is not None
    return Suspect(
        row.path,
        row.drive_file_id,
        row.catalog_id,
        f"{display.title} — {display.artist}",
        both or no_artist,
    )


def audit(
    rows: list[LibraryFile], displays: dict[uuid.UUID, CatalogDisplay]
) -> list[Suspect]:
    """Every identified row whose name disagrees with its entry, by path."""
    found: list[Suspect] = []
    for row in rows:
        if row.identification_status != "identified" or row.catalog_id is None:
            continue
        display = displays.get(row.catalog_id)
        if display is None:
            continue
        suspect = judge(row, display)
        if suspect is not None:
            found.append(suspect)
    return sorted(found, key=lambda s: s.path.lower())


def read_displays(api: Api, rows: list[LibraryFile]) -> dict[uuid.UUID, CatalogDisplay]:
    """Each linked entry's display, a batch at a time."""
    ids = list(dict.fromkeys(r.catalog_id for r in rows if r.catalog_id))
    found: dict[uuid.UUID, CatalogDisplay] = {}
    for start in range(0, len(ids), CATALOG_DISPLAY_MAX_IDS):
        batch = ids[start : start + CATALOG_DISPLAY_MAX_IDS]
        for display in api.read_catalog_display(CatalogDisplayRequest(ids=batch)):
            if display is not None:
                found[display.id] = display
    return found


def report(suspects: list[Suspect], compared: int) -> str:
    """The audit as text: the likely wrong links, then titles that differ."""
    likely = [s for s in suspects if s.both]
    title_only = [s for s in suspects if not s.both]
    lines = [f"LIKELY ANOTHER SONG ({len(likely)})"]
    lines += [f"  {s.line}" for s in likely]
    lines += ["", f"TITLE DIFFERS, SAME ARTIST ({len(title_only)})"]
    lines += [f"  {s.line}" for s in title_only]
    lines += ["", f"{compared} linked files compared; nothing was changed."]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    """Print the audit. Returns the exit status."""
    parser = argparse.ArgumentParser(
        prog="python -m deejay_cog.link_audit",
        description="List library files that look linked to another song.",
    )
    parser.parse_args(argv)
    api: Any = api_client(base_url=api_base_url().strip() or None)
    rows = [
        r
        for r in registry_rows(api).values()
        if r.identification_status == "identified" and r.catalog_id
    ]
    print(report(audit(rows, read_displays(api, rows)), len(rows)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
