"""FX Music's audio formats other than MP3 (ADR-0010, decision 2).

The main library is MP3 only, and its files are read and written through
their ID3 tags as they always were (:mod:`deejay_cog.music_tags`,
:mod:`deejay_cog.music_safety`). FX Music also holds FLAC, Ogg Vorbis, Ogg
Opus, M4A, WAV and AIFF. Those are read through the shared
:class:`~mini_app_polis.mp3.tag.AudioTagger`, from a temporary local copy
(it works on paths), and only the link to the catalog is ever written to
them, in each format's own custom field:

- FLAC, Ogg Vorbis, Ogg Opus: the Vorbis comment ``CATALOG_ID``;
- M4A: the freeform atom ``----:com.apple.iTunes:CATALOG_ID``;
- WAV, AIFF: the ``TXXX:CATALOG_ID`` frame of their ID3 chunk, as on an MP3.

A file's format is told from its contents, never its name, and a file is
never converted: a write rewrites its tag block only, and the audio is
checked as for an MP3, per format (:func:`audio_payload`,
:func:`stream_info`). Bytes in none of these formats are left to the ID3
code, as before; bytes that look like one of them but cannot be tagged are
never written.
"""

from __future__ import annotations

import contextlib
import io
import os
import struct
import tempfile
from collections.abc import Iterator
from typing import Any

import mutagen
from mini_app_polis.mp3.tag import AudioFormat, AudioTagger, UnsupportedFile
from mini_app_polis.mp3.tag.models import UnsupportedReason
from mutagen.id3 import ID3, TXXX
from mutagen.mp4 import MP4FreeForm, MP4Tags
from mutagen.ogg import OggPage

#: The custom field holding the catalog link, as its Vorbis comment and ID3
#: user-defined frame are named.
CATALOG_ID_FIELD = "CATALOG_ID"
#: The M4A freeform atom holding the catalog link.
MP4_CATALOG_ID = f"----:com.apple.iTunes:{CATALOG_ID_FIELD}"

#: How a file of each format is sent back to Drive, and the name ending of
#: its temporary local copy.
MIME_TYPES: dict[AudioFormat, str] = {
    AudioFormat.MP3: "audio/mpeg",
    AudioFormat.FLAC: "audio/flac",
    AudioFormat.OGG_VORBIS: "audio/ogg",
    AudioFormat.OGG_OPUS: "audio/ogg",
    AudioFormat.M4A: "audio/mp4",
    AudioFormat.WAV: "audio/wav",
    AudioFormat.AIFF: "audio/aiff",
}
_SUFFIXES: dict[AudioFormat, str] = {
    AudioFormat.MP3: ".mp3",
    AudioFormat.FLAC: ".flac",
    AudioFormat.OGG_VORBIS: ".ogg",
    AudioFormat.OGG_OPUS: ".opus",
    AudioFormat.M4A: ".m4a",
    AudioFormat.WAV: ".wav",
    AudioFormat.AIFF: ".aiff",
}

#: The formats whose tags are an ID3 chunk inside the file.
_ID3_CHUNK = (AudioFormat.WAV, AudioFormat.AIFF)
#: The formats tagged with Vorbis comments.
_VORBIS = (AudioFormat.FLAC, AudioFormat.OGG_VORBIS, AudioFormat.OGG_OPUS)


class NotTaggable(ValueError):  # noqa: N818 — named for what it is
    """The bytes are audio the shared tagger cannot tag, or a write other
    than the link was asked of a format other than MP3: nothing is written."""


def _skip_id3(data: bytes) -> bytes:
    """``data`` after a leading ID3v2 tag (some FLAC files carry one)."""
    if data[:3] != b"ID3" or len(data) < 10:
        return data
    raw = data[6:10]
    if any(b & 0x80 for b in raw):
        return data
    size = (raw[0] << 21) | (raw[1] << 14) | (raw[2] << 7) | raw[3]
    return data[10 + size + (10 if data[5] & 0x10 else 0) :]


def other_format(data: bytes) -> AudioFormat | UnsupportedFile | None:
    """The format of ``data`` when it is not an MP3, from its contents: one
    of FLAC, Ogg Vorbis, Ogg Opus, M4A, WAV and AIFF; an
    :class:`UnsupportedFile` for an Ogg, RIFF or IFF container holding
    something else; None for anything else, left to the ID3 code."""
    body = _skip_id3(data)
    if body[:4] == b"fLaC":
        return AudioFormat.FLAC
    if data[:4] == b"OggS":
        head = data[:4096]
        if b"\x01vorbis" in head:
            return AudioFormat.OGG_VORBIS
        if b"OpusHead" in head:
            return AudioFormat.OGG_OPUS
        return UnsupportedFile("", UnsupportedReason.UNSUPPORTED_FORMAT, "Ogg")
    if data[4:8] == b"ftyp" and data[:3] != b"ID3":
        return AudioFormat.M4A
    if data[:4] == b"RIFF":
        if data[8:12] == b"WAVE":
            return AudioFormat.WAV
        return UnsupportedFile("", UnsupportedReason.UNSUPPORTED_FORMAT, "RIFF")
    if data[:4] == b"FORM":
        if data[8:12] in (b"AIFF", b"AIFC"):
            return AudioFormat.AIFF
        return UnsupportedFile("", UnsupportedReason.UNSUPPORTED_FORMAT, "IFF")
    return None


@contextlib.contextmanager
def local_copy(data: bytes, fmt: AudioFormat) -> Iterator[str]:
    """``data`` in a temporary local file (under the system's temporary
    folder), removed afterwards."""
    with tempfile.NamedTemporaryFile(suffix=_SUFFIXES[fmt], delete=False) as handle:
        handle.write(data)
        path = handle.name
    try:
        yield path
    finally:
        os.unlink(path)


def _link(mfile: Any, fmt: AudioFormat) -> str:
    tags = getattr(mfile, "tags", None)
    if tags is None:
        return ""
    if fmt in _ID3_CHUNK:
        frame = tags.get(f"TXXX:{CATALOG_ID_FIELD}")
        return str(frame.text[0]).strip() if frame is not None and frame.text else ""
    if fmt is AudioFormat.M4A:
        values = tags.get(MP4_CATALOG_ID)
        return bytes(values[0]).decode("utf-8").strip() if values else ""
    values = tags.get(CATALOG_ID_FIELD)
    return str(values[0]).strip() if values else ""


def read(data: bytes, fmt: AudioFormat) -> tuple[dict[str, str], str]:
    """The file's tags as the shared tagger reads them (its TagSnapshot's
    keys: ``tracktitle``, ``artist``, ``year``, ``comment``, ``bpm``,
    ``label`` and the rest), and its catalog link ("" when it has none).
    Raises :class:`NotTaggable` when the tagger cannot read it."""
    with local_copy(data, fmt) as path:
        snapshot = AudioTagger().read(path)
        if isinstance(snapshot, UnsupportedFile):
            raise NotTaggable(
                f"{snapshot.reason.value}: {snapshot.detail}".rstrip(": ")
            )
        return dict(snapshot.tags), _link(mutagen.File(path), fmt)


def untaggable(data: bytes) -> str | None:
    """Why the shared tagger cannot tag ``data``, or None when it can (or
    when the bytes are left to the ID3 code)."""
    fmt = other_format(data)
    if isinstance(fmt, UnsupportedFile):
        return f"{fmt.reason.value} ({fmt.detail})"
    if fmt is None:
        return None
    try:
        read(data, fmt)
    except NotTaggable as exc:
        return str(exc)
    except Exception as exc:  # noqa: BLE001 — any parse failure is untaggable
        return f"unreadable ({type(exc).__name__}: {exc})"
    return None


def write_link(data: bytes, fmt: AudioFormat, catalog_id: str) -> bytes | None:
    """``data`` with only its catalog link set to ``catalog_id``, in the
    format's own custom field, or None when it already holds it. Every
    other tag, and the audio, is left as it is."""
    with local_copy(data, fmt) as path:
        mfile = mutagen.File(path)
        if mfile is None:
            raise NotTaggable("not readable as audio")
        if _link(mfile, fmt) == catalog_id:
            return None
        if mfile.tags is None:
            mfile.add_tags()
        tags = mfile.tags
        if fmt in _ID3_CHUNK:
            assert isinstance(tags, ID3)
            tags.delall(f"TXXX:{CATALOG_ID_FIELD}")
            tags.add(TXXX(encoding=3, desc=CATALOG_ID_FIELD, text=[catalog_id]))
        elif fmt is AudioFormat.M4A:
            assert isinstance(tags, MP4Tags)
            tags[MP4_CATALOG_ID] = [MP4FreeForm(catalog_id.encode("utf-8"))]
        else:
            tags[CATALOG_ID_FIELD] = [catalog_id]
        mfile.save()
        with open(path, "rb") as handle:
            return handle.read()


def duration(data: bytes) -> float | None:
    """The audio's length in seconds, or None when it cannot be read."""
    try:
        info = mutagen.File(io.BytesIO(data)).info
    except Exception:  # noqa: BLE001 — a length is a nice-to-have
        return None
    length = getattr(info, "length", None)
    return float(length) if length else None


# --- the audio, per format --------------------------------------------------


def _flac_payload(data: bytes) -> bytes:
    """The STREAMINFO block (with the audio's MD5) and every audio frame."""
    data = _skip_id3(data)
    i, streaminfo = 4, b""
    while True:
        if i + 4 > len(data):
            raise ValueError("a FLAC metadata block runs past the end of the file")
        header = data[i]
        size = int.from_bytes(data[i + 1 : i + 4], "big")
        if header & 0x7F == 0:
            streaminfo = data[i + 4 : i + 4 + size]
        i += 4 + size
        if header & 0x80:
            break
    return streaminfo + data[i:]


def _ogg_payload(data: bytes) -> bytes:
    """Every packet but the comment header, and the last granule position."""
    pages = []
    handle = io.BytesIO(data)
    while True:
        try:
            pages.append(OggPage(handle))
        except EOFError:
            break
    packets = OggPage.to_packets(pages, strict=False)
    if len(packets) < 2:
        raise ValueError("an Ogg stream without its header packets")
    del packets[1]
    return b"".join(packets) + struct.pack(">q", pages[-1].position)


def _iff_chunks(data: bytes, endian: str, keep: set[bytes]) -> bytes:
    """The named chunks of a RIFF (WAV) or FORM (AIFF) file, in order."""
    out, i = b"", 12
    while i + 8 <= len(data):
        cid = data[i : i + 4]
        (size,) = struct.unpack(f"{endian}I", data[i + 4 : i + 8])
        if cid in keep:
            out += cid + data[i + 8 : i + 8 + size]
        i += 8 + size + (size & 1)
    return out


def _m4a_payload(data: bytes) -> bytes:
    """The contents of every top-level ``mdat`` atom (the encoded audio)."""
    out, i = b"", 0
    while i + 8 <= len(data):
        size, kind = struct.unpack(">I4s", data[i : i + 8])
        header = 8
        if size == 1:
            (size,) = struct.unpack(">Q", data[i + 8 : i + 16])
            header = 16
        elif size == 0:
            size = len(data) - i
        if size < header:
            raise ValueError("an MP4 atom with a malformed size")
        if kind == b"mdat":
            out += data[i + header : i + size]
        i += size
    return out


def audio_payload(fmt: AudioFormat, data: bytes) -> bytes:
    """``data``'s audio, outside its tag block, for a format other than MP3:
    what a tag write must leave byte for byte as it was. Raises ValueError
    when the file cannot be taken apart."""
    if fmt is AudioFormat.FLAC:
        return _flac_payload(data)
    if fmt in (AudioFormat.OGG_VORBIS, AudioFormat.OGG_OPUS):
        return _ogg_payload(data)
    if fmt is AudioFormat.M4A:
        return _m4a_payload(data)
    if fmt is AudioFormat.WAV:
        return _iff_chunks(data, "<", {b"fmt ", b"data"})
    if fmt is AudioFormat.AIFF:
        return _iff_chunks(data, ">", {b"COMM", b"SSND"})
    raise ValueError(f"no audio check for {fmt.value}")


def stream_info(data: bytes) -> tuple[object, ...]:
    """The stream as mutagen reads it: its kind, length, sample rate,
    channels and bitrate."""
    mfile = mutagen.File(io.BytesIO(data))
    if mfile is None:
        raise ValueError("not readable as audio")
    info = mfile.info
    return (
        type(info).__name__,
        info.length,
        getattr(info, "sample_rate", None),
        getattr(info, "channels", None),
        getattr(info, "bitrate", None),
    )
