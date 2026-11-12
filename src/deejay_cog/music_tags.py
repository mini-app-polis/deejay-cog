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

FX Music's files in other formats (FLAC, Ogg Vorbis, Ogg Opus, M4A, WAV,
AIFF) are read through the shared tagger, and only their link is ever
written, in each format's own field (:mod:`deejay_cog.audio_formats`); any
other write to one raises :class:`~deejay_cog.audio_formats.NotTaggable`.
"""

from __future__ import annotations

import enum
import io
import os
import re
import tempfile
import unicodedata
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

from deejay_cog import audio_formats
from deejay_cog.audio_formats import NotTaggable, other_format
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
    return _repair_latin1(str(frame.text[0])).strip()


def _repair_latin1(text: str) -> str:
    """The text as UTF-8 when it is UTF-8 bytes read as Latin-1.

    Some taggers write UTF-8 but mark the frame Latin-1; others then copy
    that garbled text into a UTF-16 or UTF-8 frame, so the repair does not
    depend on the frame's encoding. Genuine Latin-1 text (``Beyoncé``) is
    almost never valid UTF-8, and text outside Latin-1 (``М'ята``) cannot be
    re-read, so both are returned unchanged.
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

    A file in another format is read through the shared tagger; one it
    cannot read has every field empty.
    """
    fmt = other_format(data)
    if fmt is not None:
        return _other_row(data, fmt)
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


def _other_read(
    data: bytes, fmt: audio_formats.AudioFormat | audio_formats.UnsupportedFile
) -> tuple[dict[str, str], str]:
    """A non-MP3 file's tags and link, both empty when it cannot be read."""
    if isinstance(fmt, audio_formats.UnsupportedFile):
        return {}, ""
    try:
        return audio_formats.read(data, fmt)
    except NotTaggable:
        return {}, ""


def _other_row(
    data: bytes, fmt: audio_formats.AudioFormat | audio_formats.UnsupportedFile
) -> dict[str, str]:
    """:func:`read_tag_row` for a file in a format other than MP3."""
    tags, _ = _other_read(data, fmt)
    year = tags.get("year", "").strip()
    return {
        "title": tags.get("tracktitle", "").strip(),
        "artist": tags.get("artist", "").strip(),
        "label": tags.get("label", "").strip(),
        "genre": tags.get("genre", "").strip(),
        "bpm": tags.get("bpm", "").strip(),
        "year": year[:4] if year[:4].isdigit() else "",
    }


_AGREE_PARTS = re.compile(r"[\[(][^\])]*[\])]|\s+-\s+.*$")
_AGREE_CREDIT = re.compile(r"\s+(?:feat\.?|ft\.?|featuring)\s+.*$", re.IGNORECASE)
_AGREE_SPLIT = re.compile(
    r"\s*(?:,|&|\bx\b|\band\b|\bfeat\.?|\bft\.?|\bfeaturing\b|\bwith\b)\s*",
    re.IGNORECASE,
)


def _agree_key(text: str) -> str:
    """``text`` folded for agreement: accents, case, "&" and punctuation
    ignored."""
    folded = unicodedata.normalize("NFKD", text.replace("&", " and "))
    folded = "".join(c for c in folded if not unicodedata.combining(c))
    key = re.sub(r"[^a-z0-9]+", "", folded.lower())
    return key.replace("rmix", "remix")


def _title_core(title: str) -> str:
    """A title without its bracketed or " - " parts and its feat. credit."""
    return _agree_key(_AGREE_CREDIT.sub("", _AGREE_PARTS.sub("", title)))


def _main_artist(artist: str) -> str:
    """The first credited artist."""
    return _agree_key(_AGREE_SPLIT.split(artist.strip(), maxsplit=1)[0])


#: Bracketed or " - " parts of a title that name the release, not another
#: recording: a credit, a release label, a remaster, a plain mix.
_NOT_A_VERSION = re.compile(
    r"^(?:feat\.?|ft\.?|featuring|with)\b"
    r"|^(?:super clean|clean|explicit|dirty|edited|radio edit|radio version"
    r"|no intro edit|intro edit|clean version|explicit version|album version"
    r"|single version|lp version|original mix|original version|main|mono|stereo)$"
    r"|remaster|^\d{4}$|official|lyric video|originally (?:by|performed)"
    r"|promo only|clean edit|^part\b",
    re.IGNORECASE,
)
_PARTS = re.compile(r"[\[(]([^\])]*)[\])]|\s+-\s+(.*)$")


def _versions(title: str) -> set[str]:
    """The versions a title names (a remix, an acoustic take, a cover),
    folded: its bracketed or " - " parts that are not credits or labels."""
    found = set()
    for match in _PARTS.finditer(title):
        if match.start() == 0:
            continue  # a title that opens with a bracket: the bracket is its name
        part = (match.group(1) or match.group(2) or "").strip()
        if part and not _NOT_A_VERSION.search(part):
            key = _agree_key(part)
            if key:
                found.add(key)
    return found


def tags_agree(
    file_title: str, file_artist: str, found_title: str, found_artist: str
) -> bool:
    """Whether a recording found for a file (by its fingerprint) is the song
    and version the file's own title and artist name: the titles, without
    versions or credits, contain one another, so do the main artists, and a
    version either title names (a remix, an acoustic take) is in the other
    too. A cover found under its original artist, another song by the same
    artist, or the original found for a remix, does not agree."""
    mine, found = _title_core(file_title), _title_core(found_title)
    if not mine or not found or (mine not in found and found not in mine):
        return False
    my_main, found_main = _main_artist(file_artist), _main_artist(found_artist)
    if not my_main or not found_main:
        return False
    if not (
        my_main in _agree_key(found_artist) or found_main in _agree_key(file_artist)
    ):
        return False
    mine_v, found_v = _versions(file_title), _versions(found_title)
    found_key, file_key = _agree_key(found_title), _agree_key(file_title)

    def covered(version: str, other_key: str, other_versions: set[str]) -> bool:
        return version in other_key or any(o in version for o in other_versions)

    return all(covered(v, found_key, found_v) for v in mine_v) and all(
        covered(v, file_key, mine_v) for v in found_v
    )


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

#: An owner edit's version, in the owner-edit pair ``< >``: a cut the owner
#: made by hand ("Song <Intro Cut>"), not the published recording and not a
#: release label (ADR-0008, decided 2026-11-11).
_OWNER_EDIT = re.compile(r"<([^<>]*)>")


def owner_edit_version(title: str) -> str | None:
    """The owner-edit version a file's title names ("Song <Intro Cut>" ->
    "Intro Cut"), or None when the title is not an owner edit."""
    for match in _OWNER_EDIT.finditer(title):
        version = " ".join(match.group(1).split())
        if version:
            return version
    return None


def is_owner_edit(title: str) -> bool:
    """Whether a file's title names an owner edit (a version in ``< >``)."""
    return owner_edit_version(title) is not None


def owner_edit_fits(title: str, match: CatalogMatch) -> bool:
    """Whether a catalog entry found for a file can be its entry: a title
    naming an owner edit fits only an owner-edit entry, never the published
    song's. Any other title fits as before."""
    return match.owner_edit or not is_owner_edit(title)


def edit_label(title: str) -> str | None:
    """The release label in a file's title ("Song (No Intro Edit)" -> "No
    Intro Edit"), or None when it carries none. A version in ``< >`` is an
    owner edit, never a release label, so it is not read."""
    title = _OWNER_EDIT.sub(" ", title)
    for match in _LABEL_PART.finditer(title):
        part = " ".join((match.group(1) or match.group(2) or "").split())
        for label in EDIT_LABELS:
            if part.lower() == label.lower():
                return label
    return None


def comment_text(data: bytes) -> str:
    """The file's comment frame, as text: empty when it has none."""
    fmt = other_format(data)
    if fmt is not None:
        return _other_read(data, fmt)[0].get("comment", "").strip()
    tags = _load(data)
    frames = tags.getall("COMM")
    for frame in frames:
        text = " ".join(str(t) for t in frame.text).strip()
        if text:
            return text
    return ""


def linked_catalog_id(data: bytes) -> str:
    """The catalog entry the file is linked to (``TXXX:CATALOG_ID``, or the
    format's own field: :mod:`deejay_cog.audio_formats`), or ""."""
    fmt = other_format(data)
    if fmt is not None:
        return _other_read(data, fmt)[1]
    return _text(_load(data), f"TXXX:{CATALOG_ID_DESC}")


def duration_secs(data: bytes) -> float | None:
    """The audio's length in seconds, or None when it cannot be read."""
    if other_format(data) is not None:
        return audio_formats.duration(data)
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
    parentheses when it has one (an owner edit's in ``< >``)."""
    remix = (match.remix or "").strip()
    if not remix:
        return match.title
    return (
        f"{match.title} <{remix}>" if match.owner_edit else f"{match.title} ({remix})"
    )


def _bpm_text(bpm: float) -> str:
    """BPM as ID3 stores it: a whole number."""
    return str(round(bpm))


def write_catalog_tags(
    data: bytes,
    match: CatalogMatch,
    *,
    id_only: bool = False,
    label: str | None = None,
) -> bytes | None:
    """The file's bytes with the catalog's tags written, or None when its
    tags already say what the catalog says.

    Only the frames the catalog supplies are set; a field the catalog lacks
    keeps the file's own value, and every other frame is left as it is.
    With ``id_only``, only ``TXXX:CATALOG_ID`` is written: no other frame
    changes, even an empty one. ``label`` is the file's own release label,
    kept in parentheses after the catalog's title (ADR-0008, decision 6).
    A file in another format takes the link only (``id_only``).
    """
    fmt = other_format(data)
    if fmt is not None:
        if not id_only:
            raise NotTaggable("only the catalog link is written to a non-MP3 file")
        return _other_link(data, fmt, str(match.id))
    tags = _load(data)
    title = catalog_title(match)
    wanted: list[tuple[str, Any]] = [
        ("TIT2", TIT2(encoding=3, text=[f"{title} ({label})" if label else title])),
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
    (ADR-0008, decision 6). A title that already carries that label (an fx
    entry made from a file's own title keeps it, "Panda [Clean]") is not
    given it twice."""
    if not label or edit_label(display.title) == label:
        return display.title
    return f"{display.title} ({label})"


def display_fields(
    display: CatalogDisplay, *, label: str | None
) -> dict[str, str | None]:
    """The owned fields as a file carries them, by name, as the display
    rules render the entry: the title with the file's own release label,
    artist, year, genre, BPM (as the rules round it) and label. A field the
    catalog leaves empty is None, and the file keeps its own value for it.
    """
    return {
        "title": display_title(display, label),
        "artist": display.artist or None,
        "year": None if display.release_year is None else str(display.release_year),
        "genre": display.genre or None,
        "bpm": None if display.bpm is None else f"{display.bpm:g}",
        "label": display.label or None,
    }


def needs_owner_edit_entry(title: str, display: CatalogDisplay) -> bool:
    """Whether a linked file's title names an owner edit its entry is not:
    the entry is the published song (or another owner edit), so the file
    belongs on an owner-edit entry of its own. The owner retitled it in its
    tags, and that is his choice (ADR-0008, decided 2026-11-11)."""
    if not is_owner_edit(title):
        return False
    if not display.owner_edit:
        return True
    shown = display_title(display, edit_label(title))
    return _agree_key(title) != _agree_key(shown)


#: The frame each owned field is written to.
OWNED_FRAMES = {
    "title": TIT2,
    "artist": TPE1,
    "year": TDRC,
    "genre": TCON,
    "bpm": TBPM,
    "label": TPUB,
}


def write_owned_fields(data: bytes, values: dict[str, str]) -> bytes | None:
    """The file's bytes with only the owned fields in ``values`` written
    (``catalog_id`` writes ``TXXX:CATALOG_ID``), or None when they already
    hold those values. Every other frame, the comment included, is left as
    it is. A file in another format takes ``catalog_id`` only."""
    fmt = other_format(data)
    if fmt is not None:
        if any(value for name, value in values.items() if name != "catalog_id"):
            raise NotTaggable("only the catalog link is written to a non-MP3 file")
        if "catalog_id" not in values:
            return None
        return _other_link(data, fmt, values["catalog_id"])
    tags = _load(data)
    wanted: list[tuple[str, Any]] = []
    for name, value in values.items():
        if name == "catalog_id":
            wanted.append(
                (
                    f"TXXX:{CATALOG_ID_DESC}",
                    TXXX(encoding=3, desc=CATALOG_ID_DESC, text=[value]),
                )
            )
        elif value:
            frame = OWNED_FRAMES[name]
            wanted.append((frame.__name__, frame(encoding=3, text=[value])))
    if not wanted:
        return None
    return _write_frames(data, tags, wanted)


def _other_link(
    data: bytes,
    fmt: audio_formats.AudioFormat | audio_formats.UnsupportedFile,
    catalog_id: str,
) -> bytes | None:
    """A non-MP3 file's bytes with its link set, or None when it holds it."""
    if isinstance(fmt, audio_formats.UnsupportedFile):
        raise NotTaggable(f"{fmt.reason.value} ({fmt.detail})")
    return audio_formats.write_link(data, fmt, catalog_id)


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
    frame already holds what would be written. Never writes an ID3 tag
    into a file in another format."""
    if other_format(data) is not None:
        raise NotTaggable("ID3 frames are written to MP3 files only")
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
