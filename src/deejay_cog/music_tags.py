"""Tag a music file in Drive from the catalog, in place (ADR-0007, stage 1).

A file is identified the way a play of it is: its ID3 tags are read into
the fields a VirtualDJ set export carries, built into an ``IngestTrack``
with the same :func:`~deejay_cog.ingest_to_api.build_ingest_tracks` the set
import uses, and looked up in the catalog with ``POST /v1/catalog/lookup``.
The API resolves it with the ingest's own matching, so a file and its plays
name the same catalog entry.

On a match the catalog's fields are written over the file's: title (with
the remix in parentheses), artist, year, genre, BPM, label where the catalog
has them, and ``TXXX:CATALOG_ID``. Every other frame is left as it is. The
new bytes replace the same Drive file, so its name, folder and file id do not
change and VirtualDJ keeps finding it by its path.

A file whose tags already say what the catalog says is not written again, so
a run that writes a file does not make the next run write it once more.
"""

from __future__ import annotations

import enum
import io
import os
import tempfile
from dataclasses import dataclass
from typing import Any, Protocol

from mini_app_polis.api.contract import (
    CatalogLookupRequest,
    CatalogMatch,
    IngestTrack,
)
from mutagen.id3 import (
    ID3,
    TBPM,
    TCON,
    TDRC,
    TIT2,
    TPE1,
    TPUB,
    TXXX,
    ID3NoHeaderError,
)

from deejay_cog.ingest_to_api import build_ingest_tracks

#: The user-defined frame that joins a file to its catalog entry.
CATALOG_ID_DESC = "CATALOG_ID"

#: How a tagged file is sent back to Drive.
MP3_MIME_TYPE = "audio/mpeg"


class Outcome(enum.Enum):
    """What happened to one file."""

    TAGGED = "tagged"
    """Matched in the catalog and written with its tags."""

    ALREADY_TAGGED = "already_tagged"
    """Matched, and its tags already said what the catalog says."""

    NOT_IN_CATALOG = "not_in_catalog"
    """Has a title and artist, but the catalog has no song it is a play of."""

    NEEDS_FINGERPRINT = "needs_fingerprint"
    """Has no usable title or artist, so it cannot be looked up by its tags."""


@dataclass(frozen=True)
class FileResult:
    """One file's outcome, and the catalog entry it matched, if any."""

    file_id: str
    name: str
    outcome: Outcome
    match: CatalogMatch | None = None


class CatalogLookup(Protocol):
    """The part of the API client this module calls."""

    def lookup_catalog(
        self, payload: CatalogLookupRequest
    ) -> list[CatalogMatch | None]: ...


class DriveFiles(Protocol):
    """The part of the Drive facade this module calls."""

    def download_file_bytes(self, file_id: str) -> Any: ...

    def update_file(
        self, file_id: str, filepath: str, *, mime_type: str | None = None
    ) -> None: ...


def _load(data: bytes) -> ID3:
    """The file's ID3 tag, or an empty one when it has none."""
    try:
        return ID3(io.BytesIO(data))
    except ID3NoHeaderError:
        return ID3()


def _text(tags: ID3, frame_id: str) -> str:
    frame = tags.get(frame_id)
    if frame is None or not getattr(frame, "text", None):
        return ""
    return str(frame.text[0]).strip()


def read_tag_row(data: bytes) -> dict[str, str]:
    """A file's tags as the row fields a VirtualDJ set export carries.

    The remix is not read from a frame of its own: a file carries it in the
    title's parentheses, as VirtualDJ does, and the catalog lookup resolves
    the version from the title the same way it does for a play.
    """
    tags = _load(data)
    year = _text(tags, "TDRC")
    return {
        "title": _text(tags, "TIT2"),
        "artist": _text(tags, "TPE1"),
        "label": _text(tags, "TPUB"),
        "genre": _text(tags, "TCON"),
        "bpm": _text(tags, "TBPM"),
        "year": year[:4] if year[:4].isdigit() else "",
    }


def ingest_track(row: dict[str, str]) -> IngestTrack | None:
    """The row as the track a set import would send, or None when it has no
    usable title or artist."""
    tracks = build_ingest_tracks([row])
    return tracks[0] if tracks else None


def catalog_title(match: CatalogMatch) -> str:
    """The title frame's text: the catalog's title, with its remix in
    parentheses when it has one."""
    remix = (match.remix or "").strip()
    return f"{match.title} ({remix})" if remix else match.title


def _bpm_text(bpm: float) -> str:
    """BPM as ID3 stores it: a whole number."""
    return str(round(bpm))


def write_catalog_tags(data: bytes, match: CatalogMatch) -> bytes | None:
    """The file's bytes with the catalog's tags written, or None when its
    tags already say what the catalog says.

    Only the frames the catalog supplies are set; a field the catalog lacks
    keeps the file's own value, and every other frame is left as it is.
    """
    tags = _load(data)
    wanted: list[tuple[str, Any]] = [
        ("TIT2", TIT2(encoding=3, text=[catalog_title(match)])),
        ("TPE1", TPE1(encoding=3, text=[match.artist])),
    ]
    if match.release_year is not None:
        wanted.append(("TDRC", TDRC(encoding=3, text=[str(match.release_year)])))
    if match.genre:
        wanted.append(("TCON", TCON(encoding=3, text=[match.genre])))
    if match.bpm is not None:
        wanted.append(("TBPM", TBPM(encoding=3, text=[_bpm_text(match.bpm)])))
    if match.label:
        wanted.append(("TPUB", TPUB(encoding=3, text=[match.label])))
    catalog_key = f"TXXX:{CATALOG_ID_DESC}"
    wanted.append(
        (
            catalog_key,
            TXXX(encoding=3, desc=CATALOG_ID_DESC, text=[str(match.id)]),
        )
    )

    if all(_same(tags.get(key), frame) for key, frame in wanted):
        return None
    for key, frame in wanted:
        # delall then add, keyed by HashKey, replaces exactly this frame:
        # for TXXX that is the CATALOG_ID one, not every user-defined frame.
        tags.delall(key)
        tags.add(frame)

    buffer = io.BytesIO(data)
    tags.save(buffer, v2_version=4)
    return buffer.getvalue()


def _same(existing: Any, wanted: Any) -> bool:
    """Whether a frame already holds the text that would be written."""
    if existing is None:
        return False
    return [str(t) for t in existing.text] == [str(t) for t in wanted.text]


def tag_file(drive: DriveFiles, api: CatalogLookup, file_id: str) -> FileResult:
    """Look one Drive file up in the catalog and, on a match, write the
    catalog's tags back over the same file.

    Never renames, moves or deletes the file: only its contents change, and
    only when a match differs from what it already carries.
    """
    downloaded = drive.download_file_bytes(file_id)
    name = str(getattr(downloaded, "name", "") or file_id)
    data: bytes = downloaded.data

    track = ingest_track(read_tag_row(data))
    if track is None:
        return FileResult(file_id, name, Outcome.NEEDS_FINGERPRINT)

    match = api.lookup_catalog(CatalogLookupRequest(tracks=[track]))[0]
    if match is None:
        return FileResult(file_id, name, Outcome.NOT_IN_CATALOG)

    tagged = write_catalog_tags(data, match)
    if tagged is None:
        return FileResult(file_id, name, Outcome.ALREADY_TAGGED, match)

    _replace_contents(drive, file_id, name, tagged)
    return FileResult(file_id, name, Outcome.TAGGED, match)


def _replace_contents(drive: DriveFiles, file_id: str, name: str, data: bytes) -> None:
    """Write ``data`` over the Drive file's contents (``files.update`` with
    new media). The file's name, folder and id stay as they are."""
    suffix = os.path.splitext(name)[1] or ".mp3"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as handle:
        handle.write(data)
        path = handle.name
    try:
        drive.update_file(file_id, path, mime_type=MP3_MIME_TYPE)
    finally:
        os.unlink(path)
