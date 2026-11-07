"""Tag a music file in Drive from the catalog, in place (ADR-0007, stage 1).

A file is identified the way a play of it is: its ID3 tags are read into
the fields a VirtualDJ set export carries, built into an ``IngestTrack``
with the same :func:`~deejay_cog.ingest_to_api.build_ingest_tracks` the set
import uses, and looked up in the catalog with ``POST /v1/catalog/lookup``.
The API resolves it with the ingest's own matching, so a file and its plays
name the same catalog entry.

On a match the catalog's fields are written over the file's: title (with
the remix in parentheses), artist, year, genre, BPM, label where the catalog
has them, and ``TXXX:CATALOG_ID``. Every other frame is left as it is. With
``id_only`` (the library backfill) only ``TXXX:CATALOG_ID`` is written. The
new bytes replace the same Drive file, so its name, folder and file id do not
change and VirtualDJ keeps finding it by its path.

A file whose tags already say what the catalog says is not written again, so
a run that writes a file does not make the next run write it once more.
"""

from __future__ import annotations

import enum
import io
import os
import re
import tempfile
from dataclasses import dataclass
from typing import Any, Protocol

from mini_app_polis.api.contract import (
    CatalogDisplay,
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
from mutagen.mp3 import MP3

from deejay_cog.ingest_to_api import build_ingest_tracks
from deejay_cog.music_safety import check_audio_unchanged

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
    ) -> list[CatalogMatch | None]:
        """One catalog entry, or None, per track in the request, in order."""
        ...


class DriveFiles(Protocol):
    """The part of the Drive facade this module calls."""

    def download_file_bytes(self, file_id: str) -> Any:
        """The file's name and contents (``.name``, ``.data``)."""
        ...

    def update_file(
        self, file_id: str, filepath: str, *, mime_type: str | None = None
    ) -> None:
        """Replace the file's contents with the file at ``filepath``, in place."""
        ...


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
    text = str(frame.text[0])
    if getattr(frame, "encoding", None) == 0:
        text = _repair_latin1(text)
    return text.strip()


def _repair_latin1(text: str) -> str:
    """The text as UTF-8 when a Latin-1 frame holds valid UTF-8 bytes.

    Some taggers write UTF-8 but mark the frame Latin-1. Genuine Latin-1
    text is almost never valid UTF-8, so it is returned unchanged.
    """
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


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


#: Release labels a file's title may carry: copies with different labels are
#: one song in the catalog, and each file keeps its own label.
EDIT_LABELS = (
    "Super Clean",
    "Clean Version",
    "Clean",
    "Explicit Version",
    "Explicit",
    "Dirty",
    "Radio Edit",
    "No Intro Edit",
    "Intro Edit",
    "Edited",
)
_LABEL_PART = re.compile(r"[\[(]([^\])]*)[\])]|\s+-\s+(.*)$")


def edit_label(title: str) -> str | None:
    """The release label in a file's title ("Song (No Intro Edit)" -> "No
    Intro Edit"), or None when it carries none."""
    for match in _LABEL_PART.finditer(title):
        part = " ".join((match.group(1) or match.group(2) or "").split())
        for label in EDIT_LABELS:
            if part.lower() == label.lower():
                return label
    return None


def comment_text(data: bytes) -> str:
    """The file's comment frame, as text: empty when it has none."""
    tags = _load(data)
    frames = tags.getall("COMM")
    for frame in frames:
        text = " ".join(str(t) for t in frame.text).strip()
        if text:
            return text
    return ""


def linked_catalog_id(data: bytes) -> str:
    """The catalog entry the file is linked to (``TXXX:CATALOG_ID``), or ""."""
    return _text(_load(data), f"TXXX:{CATALOG_ID_DESC}")


def duration_secs(data: bytes) -> float | None:
    """The audio's length in seconds, or None when it cannot be read."""
    try:
        info = MP3(io.BytesIO(data)).info
    except Exception:  # noqa: BLE001 — a length is a nice-to-have
        return None
    length = getattr(info, "length", None)
    return float(length) if length else None


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


def write_catalog_tags(
    data: bytes, match: CatalogMatch, *, id_only: bool = False
) -> bytes | None:
    """The file's bytes with the catalog's tags written, or None when its
    tags already say what the catalog says.

    Only the frames the catalog supplies are set; a field the catalog lacks
    keeps the file's own value, and every other frame is left as it is.
    With ``id_only``, only ``TXXX:CATALOG_ID`` is written: no other frame
    changes, even an empty one.
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
    if id_only:
        wanted = []
    wanted.append(
        (
            f"TXXX:{CATALOG_ID_DESC}",
            TXXX(encoding=3, desc=CATALOG_ID_DESC, text=[str(match.id)]),
        )
    )
    return _write_frames(data, tags, wanted)


def display_title(display: CatalogDisplay, label: str | None) -> str:
    """The title a file carries: the entry's displayed title, followed by
    the file's own release label in parentheses when it has one
    (ADR-0008, decision 6)."""
    return f"{display.title} ({label})" if label else display.title


def write_display_tags(
    data: bytes, display: CatalogDisplay, *, label: str | None
) -> bytes | None:
    """The file's bytes with the catalog's owned fields written as the
    display rules render them, or None when its tags already say so.

    Title (with the file's own release label), artist, year, genre, BPM (as
    the rules round it) and label where the catalog has them, and
    ``TXXX:CATALOG_ID``. A field the catalog leaves empty keeps the file's
    value; the comment and every other frame are left as they are.
    """
    tags = _load(data)
    wanted: list[tuple[str, Any]] = [
        ("TIT2", TIT2(encoding=3, text=[display_title(display, label)])),
        ("TPE1", TPE1(encoding=3, text=[display.artist])),
    ]
    if display.release_year is not None:
        wanted.append(("TDRC", TDRC(encoding=3, text=[str(display.release_year)])))
    if display.genre:
        wanted.append(("TCON", TCON(encoding=3, text=[display.genre])))
    if display.bpm is not None:
        wanted.append(("TBPM", TBPM(encoding=3, text=[f"{display.bpm:g}"])))
    if display.label:
        wanted.append(("TPUB", TPUB(encoding=3, text=[display.label])))
    wanted.append(
        (
            f"TXXX:{CATALOG_ID_DESC}",
            TXXX(encoding=3, desc=CATALOG_ID_DESC, text=[str(display.id)]),
        )
    )
    return _write_frames(data, tags, wanted)


def write_recording_tags(
    data: bytes, *, title: str, artist: str, year: str | None = None
) -> bytes | None:
    """The file's bytes with a recording's title, artist and year written, or
    None when its tags already say so.

    For a file identified by its fingerprint whose song the catalog does not
    have: it gets usable tags, and no ``TXXX:CATALOG_ID``. Every other frame
    is left as it is.
    """
    tags = _load(data)
    wanted: list[tuple[str, Any]] = [
        ("TIT2", TIT2(encoding=3, text=[title])),
        ("TPE1", TPE1(encoding=3, text=[artist])),
    ]
    if year:
        wanted.append(("TDRC", TDRC(encoding=3, text=[year])))
    return _write_frames(data, tags, wanted)


def _write_frames(
    data: bytes, tags: ID3, wanted: list[tuple[str, Any]]
) -> bytes | None:
    """``data`` with ``wanted`` written over ``tags``, or None when every
    frame already holds what would be written."""
    if all(_same(tags.get(key), frame) for key, frame in wanted):
        return None
    for key, frame in wanted:
        # delall then add, keyed by HashKey, replaces exactly this frame:
        # for TXXX that is the CATALOG_ID one, not every user-defined frame.
        tags.delall(key)
        tags.add(frame)

    buffer = io.BytesIO(data)
    if tags.version[:2] == (2, 3):
        # mutagen reads a v2.3 tag as v2.4 (TYER becomes TDRC, and so on).
        # Saving it back as v2.3 keeps the file's own frames as they were,
        # so a write changes only the frames in ``wanted``.
        tags.update_to_v23()
        tags.save(buffer, v2_version=3)
    else:
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

    # The audio must come back byte for byte; AudioChanged stops the run.
    check_audio_unchanged(data, tagged)
    replace_contents(drive, file_id, name, tagged)
    return FileResult(file_id, name, Outcome.TAGGED, match)


def replace_contents(drive: DriveFiles, file_id: str, name: str, data: bytes) -> None:
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
