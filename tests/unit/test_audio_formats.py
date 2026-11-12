"""FX Music's formats other than MP3 (ADR-0010): read through the shared
tagger, only the catalog link written, in each format's own field, and the
audio checked per format."""

from __future__ import annotations

import io
from unittest.mock import MagicMock

import mutagen
import pytest
from audio_fixtures import NAMES, OTHER_FORMATS, sample, tagged
from mini_app_polis.api.contract import CatalogMatch
from mini_app_polis.mp3.tag import AudioFormat, UnsupportedFile
from music_fakes import FakeDrive, mp3
from mutagen.id3 import TIT2, TPE1

from deejay_cog import audio_formats, music_safety, music_tags
from deejay_cog.audio_formats import NotTaggable, other_format
from deejay_cog.music_safety import (
    AudioChanged,
    AudioCheckError,
    FlowMode,
    RunRecord,
    check_audio_unchanged,
    tag_snapshot,
    write_in_place,
)
from deejay_cog.music_tags import (
    comment_text,
    duration_secs,
    linked_catalog_id,
    read_tag_row,
    write_catalog_tags,
    write_owned_fields,
    write_recording_tags,
)

LINK = "6f1c2f8e-0b7c-4c1e-9a52-1f3f0f3e2d10"
TAGS = {
    "title": "Bow <Intro Cut>",
    "artist": "Etta James",
    "year": "2016",
    "genre": "Neo Soul",
    "bpm": "92",
    "label": "Atlantic",
    "comment": "cue: drop at 0:12",
}


@pytest.fixture(params=OTHER_FORMATS, ids=lambda f: f.value)
def fmt(request: pytest.FixtureRequest) -> AudioFormat:
    return request.param


def _raw_link(data: bytes, fmt: AudioFormat) -> str:
    """The link as the format's own custom field holds it."""
    tags = mutagen.File(io.BytesIO(data)).tags
    if fmt in (AudioFormat.WAV, AudioFormat.AIFF):
        return str(tags["TXXX:CATALOG_ID"].text[0])
    if fmt is AudioFormat.M4A:
        return bytes(tags["----:com.apple.iTunes:CATALOG_ID"][0]).decode()
    return str(tags["CATALOG_ID"][0])


# --- telling the format --------------------------------------------------------


def test_each_format_is_told_from_its_contents(fmt: AudioFormat) -> None:
    assert other_format(sample(fmt)) is fmt


def test_an_mp3_is_left_to_the_id3_code() -> None:
    assert other_format(mp3()) is None
    assert other_format(mp3(TIT2(encoding=3, text=["x"]))) is None


@pytest.mark.parametrize(
    "data",
    [
        b"OggS" + b"\x00" * 60 + b"Speex   ",
        b"RIFF\x00\x00\x00\x00AVI ",
        b"FORM\x00\x00\x00\x008SVX",
    ],
    ids=["ogg-speex", "riff-avi", "iff-8svx"],
)
def test_another_kind_of_container_is_untaggable(data: bytes) -> None:
    assert isinstance(other_format(data), UnsupportedFile)
    assert audio_formats.untaggable(data)
    assert read_tag_row(data)["title"] == ""
    with pytest.raises(NotTaggable):
        write_owned_fields(data, {"catalog_id": LINK})


def test_a_broken_file_in_a_supported_format_is_untaggable() -> None:
    broken = b"fLaC" + b"\x00" * 40
    assert audio_formats.untaggable(broken)
    assert read_tag_row(broken)["title"] == ""
    assert audio_formats.untaggable(sample(AudioFormat.FLAC)) is None
    assert audio_formats.untaggable(mp3()) is None


# --- reading ----------------------------------------------------------------------


def test_each_format_is_read_through_the_shared_tagger(fmt: AudioFormat) -> None:
    data = tagged(fmt, catalog_id=LINK, **TAGS)

    assert read_tag_row(data) == {
        "title": "Bow <Intro Cut>",
        "artist": "Etta James",
        "label": "Atlantic",
        "genre": "Neo Soul",
        "bpm": "92",
        "year": "2016",
    }
    assert linked_catalog_id(data) == LINK
    assert comment_text(data) == "cue: drop at 0:12"
    assert duration_secs(data)
    snapshot = tag_snapshot(data)
    assert snapshot["title"] == "Bow <Intro Cut>" and snapshot["catalog_id"] == LINK


def test_an_untagged_file_reads_empty(fmt: AudioFormat) -> None:
    data = sample(fmt)
    assert set(read_tag_row(data).values()) == {""}
    assert linked_catalog_id(data) == "" and comment_text(data) == ""


# --- writing the link -----------------------------------------------------------


def test_the_link_goes_in_the_formats_own_field_with_the_audio_unchanged(
    fmt: AudioFormat,
) -> None:
    data = tagged(fmt, **TAGS)

    linked = write_owned_fields(data, {"catalog_id": LINK})

    assert linked is not None
    assert other_format(linked) is fmt  # never converted
    assert _raw_link(linked, fmt) == LINK
    check_audio_unchanged(data, linked)
    # Every other tag as it was.
    assert read_tag_row(linked) == read_tag_row(data)
    assert comment_text(linked) == comment_text(data)
    # Already linked: nothing to write.
    assert write_owned_fields(linked, {"catalog_id": LINK}) is None
    # Linked again elsewhere: replaced, not added.
    other = write_owned_fields(linked, {"catalog_id": "other"})
    assert other is not None and linked_catalog_id(other) == "other"
    check_audio_unchanged(linked, other)


def test_the_backfill_writes_the_link_only(fmt: AudioFormat) -> None:
    data = tagged(fmt, **TAGS)
    match = CatalogMatch(id=LINK, title="Other", artist="Someone", library="fx")

    linked = write_catalog_tags(data, match, id_only=True)

    assert linked is not None and linked_catalog_id(linked) == LINK
    assert read_tag_row(linked)["title"] == "Bow <Intro Cut>"
    with pytest.raises(NotTaggable):
        write_catalog_tags(data, match)


def test_nothing_but_the_link_is_ever_written_to_another_format(
    fmt: AudioFormat,
) -> None:
    data = tagged(fmt, **TAGS)
    with pytest.raises(NotTaggable):
        write_owned_fields(data, {"title": "Catalog Title"})
    with pytest.raises(NotTaggable):
        write_owned_fields(data, {"genre": "Pop", "catalog_id": LINK})
    with pytest.raises(NotTaggable):
        write_recording_tags(data, title="T", artist="A")
    # An empty value writes nothing, as for an MP3.
    assert write_owned_fields(data, {"title": ""}) is None


def test_an_mp3_is_written_through_id3_as_before() -> None:
    data = mp3(TIT2(encoding=3, text=["T"]), TPE1(encoding=3, text=["A"]))
    linked = write_owned_fields(data, {"catalog_id": LINK})
    assert linked is not None and linked[:3] == b"ID3"
    assert linked_catalog_id(linked) == LINK


# --- the audio check ------------------------------------------------------------


def _flip_last_byte(data: bytes) -> bytes:
    return data[:-1] + bytes([data[-1] ^ 0x01])


@pytest.mark.parametrize(
    "fmt",
    [AudioFormat.FLAC, AudioFormat.M4A, AudioFormat.WAV, AudioFormat.AIFF],
    ids=lambda f: f.value,
)
def test_changed_audio_is_caught(fmt: AudioFormat) -> None:
    data = sample(fmt)
    if fmt is AudioFormat.M4A:
        at = data.index(b"mdat") + 8
        changed = data[:at] + bytes([data[at] ^ 0x01]) + data[at + 1 :]
    else:
        changed = _flip_last_byte(data)
    with pytest.raises(AudioChanged):
        check_audio_unchanged(data, changed)


def test_changed_ogg_audio_is_caught() -> None:
    data = sample(AudioFormat.OGG_VORBIS)
    linked = write_owned_fields(data, {"catalog_id": LINK})
    assert linked is not None
    # The last page's audio, its checksum left stale.
    changed = linked[:-3] + bytes([linked[-3] ^ 0x01]) + linked[-2:]
    with pytest.raises(AudioChanged):
        check_audio_unchanged(data, changed)


def test_a_file_that_changed_format_is_caught(fmt: AudioFormat) -> None:
    with pytest.raises(AudioChanged, match="no longer"):
        check_audio_unchanged(sample(fmt), mp3())


def test_an_untaggable_container_is_never_passed_as_unchanged() -> None:
    data = b"RIFF\x00\x00\x00\x00AVI "
    with pytest.raises(AudioCheckError):
        check_audio_unchanged(data, data)


# --- Drive ---------------------------------------------------------------------


def test_a_file_is_sent_back_as_its_own_format(fmt: AudioFormat) -> None:
    drive = MagicMock()
    music_safety.update_contents(drive, "f1", NAMES[fmt], sample(fmt))
    assert (
        drive.update_file.call_args.kwargs["mime_type"]
        == (audio_formats.MIME_TYPES[fmt])
    )
    music_safety.update_contents(drive, "f2", "song.mp3", mp3())
    assert drive.update_file.call_args.kwargs["mime_type"] == "audio/mpeg"


def test_a_tag_read_takes_another_format_whole(
    fmt: AudioFormat, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(music_safety, "TAG_HEAD_BYTES", 16)
    drive = FakeDrive()
    data = tagged(fmt, catalog_id=LINK, **TAGS)
    file_id = drive.add(NAMES[fmt], "fx", data)

    read = music_safety.read_tag_bytes(drive, file_id)

    assert read.whole and read.data == data
    assert linked_catalog_id(read.data) == LINK


def test_a_link_is_written_in_place_safely(fmt: AudioFormat) -> None:
    drive = FakeDrive()
    data = tagged(fmt, **TAGS)
    file_id = drive.add(NAMES[fmt], "fx", data)
    record = RunRecord("library-identify", "i-1", FlowMode.WRITE).attach(drive, "state")
    linked = write_owned_fields(data, {"catalog_id": LINK})
    assert linked is not None

    entry = write_in_place(
        drive,
        record,
        file_id=file_id,
        name=NAMES[fmt],
        original=data,
        tagged=linked,
        identified_by="own tags (FX Music)",
    )

    assert entry.state == "done"
    assert drive.items[file_id].data == linked
    assert entry.old_tags["catalog_id"] == "" and entry.new_tags["catalog_id"] == LINK
    assert entry.new_tags["title"] == "Bow <Intro Cut>"


def test_music_tags_reads_only_through_id3_for_an_mp3(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def refuse(*_: object) -> None:
        raise AssertionError("an MP3 is not read through the shared tagger")

    monkeypatch.setattr(audio_formats, "read", refuse)
    data = mp3(TIT2(encoding=3, text=["T"]), TPE1(encoding=3, text=["A"]))
    assert music_tags.read_tag_row(data)["title"] == "T"
    assert tag_snapshot(data)["title"] == "T"
