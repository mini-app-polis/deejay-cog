"""Tiny audio files in the formats FX Music holds besides MP3.

FLAC, Ogg Vorbis, Ogg Opus and M4A come from ``fixtures/audio`` (0.05 s of
a 440 Hz tone at 8 kHz mono, written with no tags, as common-python-utils
tests the shared tagger with); WAV and AIFF are built here.
"""

from __future__ import annotations

import io
import math
import struct
import wave
from pathlib import Path

from mini_app_polis.mp3.tag import AudioFormat, AudioTagger

from deejay_cog import audio_formats

FIXTURES = Path(__file__).parent / "fixtures" / "audio"

#: Every format FX Music tags besides MP3.
OTHER_FORMATS = (
    AudioFormat.FLAC,
    AudioFormat.OGG_VORBIS,
    AudioFormat.OGG_OPUS,
    AudioFormat.M4A,
    AudioFormat.WAV,
    AudioFormat.AIFF,
)

#: A file name for each format, as Drive would list it.
NAMES = {
    AudioFormat.FLAC: "bow.flac",
    AudioFormat.OGG_VORBIS: "bow.ogg",
    AudioFormat.OGG_OPUS: "bow.opus",
    AudioFormat.M4A: "bow.m4a",
    AudioFormat.WAV: "bow.wav",
    AudioFormat.AIFF: "bow.aiff",
}

_FILES = {
    AudioFormat.FLAC: "tone.flac",
    AudioFormat.OGG_VORBIS: "tone.ogg",
    AudioFormat.OGG_OPUS: "tone.opus",
    AudioFormat.M4A: "tone.m4a",
}
_RATE = 8000
_FRAMES = 400


def _samples() -> list[int]:
    return [int(8000 * math.sin(2 * math.pi * 440 * i / _RATE)) for i in range(_FRAMES)]


def _wav() -> bytes:
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(_RATE)
        w.writeframes(struct.pack(f"<{_FRAMES}h", *_samples()))
    return buffer.getvalue()


def _aiff() -> bytes:
    exponent = _RATE.bit_length() - 1
    rate = struct.pack(">HQ", 16383 + exponent, _RATE << (63 - exponent))
    comm = struct.pack(">hIh", 1, _FRAMES, 16) + rate
    ssnd = struct.pack(">II", 0, 0) + struct.pack(f">{_FRAMES}h", *_samples())
    body = b"AIFF"
    for cid, data in ((b"COMM", comm), (b"SSND", ssnd)):
        body += cid + struct.pack(">I", len(data)) + data
    return b"FORM" + struct.pack(">I", len(body)) + body


def sample(fmt: AudioFormat) -> bytes:
    """An untagged file in ``fmt``."""
    if fmt is AudioFormat.WAV:
        return _wav()
    if fmt is AudioFormat.AIFF:
        return _aiff()
    return (FIXTURES / _FILES[fmt]).read_bytes()


def tagged(fmt: AudioFormat, *, catalog_id: str | None = None, **tags: str) -> bytes:
    """A file in ``fmt`` with ``tags`` (the shared tagger's metadata keys:
    title, artist, year, genre, bpm, label, comment) and, when given, its
    catalog link, written as the pipeline writes it."""
    data = sample(fmt)
    if tags:
        with audio_formats.local_copy(data, fmt) as path:
            AudioTagger().write(path, tags)
            data = Path(path).read_bytes()
    if catalog_id is not None:
        linked = audio_formats.write_link(data, fmt, catalog_id)
        assert linked is not None
        data = linked
    return data
