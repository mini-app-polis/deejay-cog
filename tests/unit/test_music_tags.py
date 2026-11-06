"""Tests for music_tags: read a file's tags, look it up, write the catalog's
tags back over the same Drive file."""

from __future__ import annotations

import io
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from mini_app_polis.api.contract import CatalogLookupRequest, CatalogMatch
from mutagen.id3 import APIC, COMM, ID3, TALB, TBPM, TCON, TDRC, TIT2, TPE1, TPUB, TXXX

from deejay_cog import music_tags
from deejay_cog.music_safety import AudioChanged
from deejay_cog.music_tags import Outcome

#: One MPEG-1 Layer III frame header (128 kbps, 44.1 kHz) and its body,
#: repeated: enough audio for mutagen to treat the bytes as an MP3.
_FRAME = b"\xff\xfb\x90\x64" + b"\x00" * 413
_AUDIO = _FRAME * 8

_CATALOG_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")


def _mp3(*frames) -> bytes:
    """An MP3 whose ID3 tag holds ``frames`` (no tag at all when empty)."""
    if not frames:
        return _AUDIO
    tags = ID3()
    for frame in frames:
        tags.add(frame)
    buffer = io.BytesIO(_AUDIO)
    tags.save(buffer, v2_version=4)
    return buffer.getvalue()


def _tags(data: bytes) -> ID3:
    return ID3(io.BytesIO(data))


def _match(**overrides) -> CatalogMatch:
    fields = {
        "id": _CATALOG_ID,
        "title": "Temple",
        "remix": "Niko The Kid Remix",
        "artist": "Thomas Gold",
        "label": "Armada",
        "genre": "House",
        "bpm": 124.0,
        "release_year": 2019,
    }
    fields.update(overrides)
    return CatalogMatch(**fields)


@dataclass
class _Downloaded:
    file_id: str
    name: str
    data: bytes
    mime_type: str = "audio/mpeg"


@dataclass
class FakeDrive:
    files: dict[str, tuple[str, bytes]]
    updates: list[tuple[str, bytes, str | None]] = field(default_factory=list)
    calls: list[str] = field(default_factory=list)

    def download_file_bytes(self, file_id: str) -> _Downloaded:
        self.calls.append("download")
        name, data = self.files[file_id]
        return _Downloaded(file_id=file_id, name=name, data=data)

    def update_file(
        self, file_id: str, filepath: str, *, mime_type: str | None = None
    ) -> None:
        self.calls.append("update")
        self.updates.append((file_id, Path(filepath).read_bytes(), mime_type))

    # Anything that renames, moves or deletes would fail the test loudly.
    def __getattr__(self, name: str):
        raise AssertionError(f"unexpected Drive call: {name}")


@dataclass
class FakeApi:
    answer: CatalogMatch | None
    requests: list[CatalogLookupRequest] = field(default_factory=list)

    def lookup_catalog(
        self, payload: CatalogLookupRequest
    ) -> list[CatalogMatch | None]:
        self.requests.append(payload)
        return [self.answer for _ in payload.tracks]


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def test_read_tag_row_reads_the_fields_a_set_export_carries() -> None:
    data = _mp3(
        TIT2(encoding=3, text=["Temple (Niko The Kid Remix)"]),
        TPE1(encoding=3, text=["Thomas Gold"]),
        TPUB(encoding=3, text=["Armada"]),
        TCON(encoding=3, text=["House"]),
        TBPM(encoding=3, text=["124"]),
        TDRC(encoding=3, text=["2019-05-01"]),
    )

    assert music_tags.read_tag_row(data) == {
        "title": "Temple (Niko The Kid Remix)",
        "artist": "Thomas Gold",
        "label": "Armada",
        "genre": "House",
        "bpm": "124",
        "year": "2019",
    }


def test_a_latin1_frame_holding_utf8_bytes_is_read_as_utf8() -> None:
    data = _mp3(
        TIT2(encoding=0, text=["Ð\x9c'Ñ\x8fÑ\x82Ð°"]),
        TPE1(encoding=0, text=["KAZKA"]),
    )

    row = music_tags.read_tag_row(data)

    assert row["title"] == "М'ята"
    assert row["artist"] == "KAZKA"


def test_genuine_latin1_text_is_left_as_it_is() -> None:
    data = _mp3(TPE1(encoding=0, text=["Beyoncé"]))

    assert music_tags.read_tag_row(data)["artist"] == "Beyoncé"


def test_a_utf8_frame_is_read_as_it_is() -> None:
    data = _mp3(TIT2(encoding=3, text=["М'ята"]))

    assert music_tags.read_tag_row(data)["title"] == "М'ята"


def test_the_ingest_track_is_built_the_way_a_set_import_builds_it() -> None:
    row = music_tags.read_tag_row(
        _mp3(
            TIT2(encoding=3, text=["Temple"]),
            TPE1(encoding=3, text=["Thomas Gold"]),
            TBPM(encoding=3, text=["124"]),
            TDRC(encoding=3, text=["2019"]),
        )
    )
    track = music_tags.ingest_track(row)

    assert track is not None
    assert (track.title, track.artist, track.bpm, track.release_year) == (
        "Temple",
        "Thomas Gold",
        124.0,
        2019,
    )


def test_a_file_with_no_tag_has_no_track() -> None:
    assert music_tags.ingest_track(music_tags.read_tag_row(_mp3())) is None


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def test_a_match_is_written_with_the_remix_in_the_titles_parentheses() -> None:
    original = _mp3(
        TIT2(encoding=3, text=["temple"]),
        TPE1(encoding=3, text=["thomas gold"]),
        TALB(encoding=3, text=["Some Album"]),
    )

    written = music_tags.write_catalog_tags(original, _match())

    assert written is not None
    tags = _tags(written)
    assert tags["TIT2"].text == ["Temple (Niko The Kid Remix)"]
    assert tags["TPE1"].text == ["Thomas Gold"]
    assert str(tags["TDRC"].text[0]) == "2019"
    assert tags["TCON"].text == ["House"]
    assert tags["TBPM"].text == ["124"]
    assert tags["TPUB"].text == ["Armada"]
    assert tags[f"TXXX:{music_tags.CATALOG_ID_DESC}"].text == [str(_CATALOG_ID)]
    # No separate remix frame.
    assert "TIT3" not in tags and "TPE4" not in tags
    # Every other frame is left as it was.
    assert tags["TALB"].text == ["Some Album"]
    # The audio after the tag is untouched.
    assert written.endswith(_AUDIO)


def test_a_match_without_a_remix_writes_the_title_alone() -> None:
    written = music_tags.write_catalog_tags(
        _mp3(TIT2(encoding=3, text=["x"]), TPE1(encoding=3, text=["y"])),
        _match(remix=None),
    )

    assert written is not None
    assert _tags(written)["TIT2"].text == ["Temple"]


def test_a_field_the_catalog_lacks_keeps_the_files_own_value() -> None:
    written = music_tags.write_catalog_tags(
        _mp3(
            TIT2(encoding=3, text=["x"]),
            TPE1(encoding=3, text=["y"]),
            TCON(encoding=3, text=["Disco"]),
            TPUB(encoding=3, text=["Own Label"]),
        ),
        _match(genre=None, label=None, bpm=None, release_year=None),
    )

    assert written is not None
    tags = _tags(written)
    assert tags["TCON"].text == ["Disco"]
    assert tags["TPUB"].text == ["Own Label"]
    assert "TBPM" not in tags and "TDRC" not in tags


def test_other_user_defined_frames_survive() -> None:
    written = music_tags.write_catalog_tags(
        _mp3(
            TIT2(encoding=3, text=["x"]),
            TPE1(encoding=3, text=["y"]),
            TXXX(encoding=3, desc="VDJ_CUES", text=["1.5;30.0"]),
        ),
        _match(),
    )

    assert written is not None
    assert _tags(written)["TXXX:VDJ_CUES"].text == ["1.5;30.0"]


def test_tags_that_already_match_are_not_written_again() -> None:
    once = music_tags.write_catalog_tags(
        _mp3(TIT2(encoding=3, text=["x"]), TPE1(encoding=3, text=["y"])), _match()
    )
    assert once is not None

    assert music_tags.write_catalog_tags(once, _match()) is None


def test_id_only_writes_just_the_catalog_id_to_a_file_with_empty_fields() -> None:
    written = music_tags.write_catalog_tags(
        _mp3(TIT2(encoding=3, text=["x"]), TPE1(encoding=3, text=["y"])),
        _match(),
        id_only=True,
    )

    assert written is not None
    tags = _tags(written)
    assert sorted(tags.keys()) == sorted(
        ["TIT2", "TPE1", f"TXXX:{music_tags.CATALOG_ID_DESC}"]
    )
    assert tags["TIT2"].text == ["x"]
    assert tags["TPE1"].text == ["y"]
    assert tags[f"TXXX:{music_tags.CATALOG_ID_DESC}"].text == [str(_CATALOG_ID)]


def test_id_only_with_the_catalog_id_present_writes_nothing() -> None:
    once = music_tags.write_catalog_tags(
        _mp3(TIT2(encoding=3, text=["x"]), TPE1(encoding=3, text=["y"])),
        _match(),
        id_only=True,
    )
    assert once is not None

    assert music_tags.write_catalog_tags(once, _match(), id_only=True) is None


def test_id_only_leaves_every_other_frame_identical() -> None:
    original = _mp3(
        TIT2(encoding=3, text=["x"]),
        TPE1(encoding=3, text=["y"]),
        TCON(encoding=3, text=[" "]),
        COMM(encoding=3, lang="eng", desc="", text=["a comment"]),
        APIC(encoding=3, mime="image/png", type=3, desc="", data=b"\x89PNG-bytes"),
        TXXX(encoding=3, desc="MOOD", text=["dark"]),
        TXXX(encoding=3, desc=music_tags.CATALOG_ID_DESC, text=["old-id"]),
    )
    before = _tags(original)

    written = music_tags.write_catalog_tags(original, _match(), id_only=True)

    assert written is not None
    after = _tags(written)
    catalog_key = f"TXXX:{music_tags.CATALOG_ID_DESC}"
    assert sorted(after.keys()) == sorted(before.keys())
    for key in sorted(before.keys()):
        if key == catalog_key:
            assert after[key].text == [str(_CATALOG_ID)]
        else:
            assert after[key] == before[key]


# ---------------------------------------------------------------------------
# One Drive file, end to end
# ---------------------------------------------------------------------------


def _tagged_file() -> bytes:
    return _mp3(
        TIT2(encoding=3, text=["temple (niko the kid remix)"]),
        TPE1(encoding=3, text=["thomas gold"]),
        TALB(encoding=3, text=["Keep Me"]),
    )


def test_a_matched_file_is_written_back_over_the_same_drive_file() -> None:
    drive = FakeDrive(files={"f1": ("Temple.mp3", _tagged_file())})
    api = FakeApi(answer=_match())

    result = music_tags.tag_file(drive, api, "f1")

    assert result.outcome is Outcome.TAGGED
    assert result.match == _match()
    assert api.requests[0].tracks[0].title == "temple (niko the kid remix)"
    assert len(drive.updates) == 1
    file_id, data, mime_type = drive.updates[0]
    assert (file_id, mime_type) == ("f1", "audio/mpeg")
    tags = _tags(data)
    assert tags["TIT2"].text == ["Temple (Niko The Kid Remix)"]
    assert tags["TALB"].text == ["Keep Me"]
    # Only download and update: no rename, move or delete.
    assert drive.calls == ["download", "update"]


def test_a_file_the_catalog_does_not_know_is_unchanged() -> None:
    drive = FakeDrive(files={"f1": ("Unplayed.mp3", _tagged_file())})

    result = music_tags.tag_file(drive, FakeApi(answer=None), "f1")

    assert result.outcome is Outcome.NOT_IN_CATALOG
    assert drive.updates == []


def test_a_file_already_carrying_the_catalogs_tags_is_unchanged() -> None:
    already = music_tags.write_catalog_tags(_tagged_file(), _match())
    assert already is not None
    drive = FakeDrive(files={"f1": ("Temple.mp3", already)})

    result = music_tags.tag_file(drive, FakeApi(answer=_match()), "f1")

    assert result.outcome is Outcome.ALREADY_TAGGED
    assert drive.updates == []


def test_an_untagged_file_needs_fingerprinting_and_is_not_looked_up() -> None:
    drive = FakeDrive(files={"f1": ("track01.mp3", _mp3())})
    api = FakeApi(answer=_match())

    result = music_tags.tag_file(drive, api, "f1")

    assert result.outcome is Outcome.NEEDS_FINGERPRINT
    assert api.requests == []
    assert drive.updates == []


def test_a_file_with_a_title_but_no_artist_needs_fingerprinting() -> None:
    drive = FakeDrive(
        files={"f1": ("x.mp3", _mp3(TIT2(encoding=3, text=["Only a title"])))}
    )

    result = music_tags.tag_file(drive, FakeApi(answer=_match()), "f1")

    assert result.outcome is Outcome.NEEDS_FINGERPRINT


def test_a_write_that_would_change_the_audio_is_refused(monkeypatch) -> None:
    data = _mp3(TIT2(encoding=3, text=["temple"]), TPE1(encoding=3, text=["tg"]))
    drive = FakeDrive({"f1": ("song.mp3", data)})
    monkeypatch.setattr(
        music_tags, "write_catalog_tags", lambda *_: data[:-1] + b"\x01"
    )

    with pytest.raises(AudioChanged):
        music_tags.tag_file(drive, FakeApi(_match()), "f1")

    assert drive.updates == []
