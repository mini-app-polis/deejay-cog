"""The safety layer: modes, the audio check, no emptied tags, the run record
and the in-place write the library backfill will use."""

from __future__ import annotations

import io
import json

import pytest
from mini_app_polis.api import ApiUnavailable, KaianoApiError
from mini_app_polis.api.contract import FeatureFlagItem
from music_fakes import AUDIO, FakeDrive, mp3
from mutagen.id3 import ID3, TCON, TIT2, TPE1

from deejay_cog import music_safety as safety
from deejay_cog.music_safety import FlowMode

#: The real reader: tests/conftest.py swaps in the fake flags for every test.
REAL_READ_FLAGS = safety._read_flags

FLAG = "flags.deejay_cog.library_scan"


@pytest.mark.parametrize(
    ("raw", "mode"),
    [
        (None, FlowMode.OFF),
        ("", FlowMode.OFF),
        ("off", FlowMode.OFF),
        ("dry-run", FlowMode.DRY_RUN),
        (" Dry-Run ", FlowMode.DRY_RUN),
        ("write", FlowMode.WRITE),
        ("WRITE", FlowMode.WRITE),
        ("dryrun", FlowMode.OFF),
        ("true", FlowMode.OFF),
        ("1", FlowMode.OFF),
    ],
)
def test_modes_default_to_off(raw, mode) -> None:
    assert safety.parse_mode(raw) is mode


def test_an_unknown_mode_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    safety.parse_mode("yes", setting="flags.deejay_cog.new_music")
    assert "flags.deejay_cog.new_music" in caplog.text


class _Flags:
    """A client whose ``list_flags`` answers ``flags``, or raises them."""

    def __init__(self, flags):
        self.flags = flags
        self.reads = 0

    def list_flags(self):
        self.reads += 1
        if isinstance(self.flags, Exception):
            raise self.flags
        return self.flags


@pytest.fixture
def api(monkeypatch: pytest.MonkeyPatch):
    """Read the flags through the real reader, from a fake client."""
    clients: list[_Flags] = []

    def answer(flags):
        client = _Flags(flags)
        clients.append(client)
        monkeypatch.setattr(safety, "_read_flags", REAL_READ_FLAGS)
        monkeypatch.setattr(safety, "api_client", lambda **_: client)
        return client

    return answer


def _flag(value, *, name=FLAG, enabled=True) -> FeatureFlagItem:
    return FeatureFlagItem(name=name, enabled=enabled, value=value)


@pytest.mark.parametrize(
    ("value", "mode"),
    [("off", FlowMode.OFF), ("dry-run", FlowMode.DRY_RUN), ("write", FlowMode.WRITE)],
)
def test_a_step_reads_its_mode_from_its_flag(api, value, mode) -> None:
    api([_flag(value)])

    assert safety.flow_mode(FLAG) is mode


@pytest.mark.parametrize(
    "error",
    [
        ApiUnavailable(status_code=0, message="connection refused", path="/v1/flags"),
        KaianoApiError(status_code=500, message="boom", path="/v1/flags"),
    ],
    ids=["unreachable", "refused"],
)
def test_flags_that_cannot_be_read_are_off(
    api, error, caplog: pytest.LogCaptureFixture
) -> None:
    api(error)

    assert safety.flow_mode(FLAG) is FlowMode.OFF
    assert "could not read the feature flags" in caplog.text


def test_a_missing_flag_is_off(api, caplog: pytest.LogCaptureFixture) -> None:
    api([_flag("write", name="flags.deejay_cog.library_sync")])

    assert safety.flow_mode(FLAG) is FlowMode.OFF
    assert FLAG in caplog.text


@pytest.mark.parametrize("value", [None, "", "on", "yes"])
def test_an_empty_or_unknown_value_is_off(api, value) -> None:
    api([_flag(value)])

    assert safety.flow_mode(FLAG) is FlowMode.OFF


def test_an_unknown_value_is_logged(api, caplog: pytest.LogCaptureFixture) -> None:
    api([_flag("yes")])

    safety.flow_mode(FLAG)

    assert FLAG in caplog.text


def test_a_switched_off_flag_is_off_whatever_its_value(api) -> None:
    api([_flag("write", enabled=False)])

    assert safety.flow_mode(FLAG) is FlowMode.OFF


def test_several_modes_are_read_in_one_call(api) -> None:
    other = "flags.deejay_cog.new_music"
    client = api([_flag("write"), _flag("dry-run", name=other)])

    assert safety.flow_modes(FLAG, other) == {
        FLAG: FlowMode.WRITE,
        other: FlowMode.DRY_RUN,
    }
    assert client.reads == 1


def test_the_stricter_mode_wins() -> None:
    assert safety.stricter(FlowMode.WRITE, FlowMode.DRY_RUN) is FlowMode.DRY_RUN
    assert safety.stricter(FlowMode.DRY_RUN, FlowMode.OFF) is FlowMode.OFF
    assert safety.stricter(FlowMode.WRITE, FlowMode.WRITE) is FlowMode.WRITE


# ---------------------------------------------------------------------------
# The audio check
# ---------------------------------------------------------------------------


def _syncsafe(n: int) -> bytes:
    return bytes([(n >> 21) & 0x7F, (n >> 14) & 0x7F, (n >> 7) & 0x7F, n & 0x7F])


def test_the_payload_is_the_audio_without_any_tag() -> None:
    v2 = (
        b"ID3\x04\x00\x10" + _syncsafe(5) + b"abcde" + b"3DI\x04\x00\x10" + _syncsafe(5)
    )
    ape_body = b"x" * 8
    ape_footer = (
        b"APETAGEX"
        + (2000).to_bytes(4, "little")
        + (len(ape_body) + 32).to_bytes(4, "little")
        + (0).to_bytes(4, "little")
        + (0x80000000).to_bytes(4, "little")
        + b"\x00" * 8
    )
    ape = b"APETAGEX" + b"\x00" * 24 + ape_body + ape_footer
    v1 = b"TAG" + b"\x00" * 125
    assert safety.audio_payload(v2 + AUDIO + ape + v1) == AUDIO
    assert safety.audio_payload(AUDIO) == AUDIO


def test_a_tag_written_by_mutagen_leaves_the_audio_alone() -> None:
    original = mp3(TIT2(encoding=3, text=["a"]))
    tags = ID3(io.BytesIO(original))
    tags.add(TPE1(encoding=3, text=["someone with a long name" * 20]))
    buffer = io.BytesIO(original)
    tags.save(buffer, v2_version=4)

    safety.check_audio_unchanged(original, buffer.getvalue())


def test_a_changed_byte_of_audio_is_caught() -> None:
    original = mp3(TIT2(encoding=3, text=["a"]))
    changed = mp3(TIT2(encoding=3, text=["b"]), audio=AUDIO[:-1] + b"\x07")
    with pytest.raises(safety.AudioChanged):
        safety.check_audio_unchanged(original, changed)


def test_a_malformed_tag_cannot_be_shown_unchanged() -> None:
    broken = b"ID3\x04\x00\x00\x80\x80\x80\x80" + AUDIO
    with pytest.raises(safety.AudioCheckError):
        safety.check_audio_unchanged(broken, broken)


def test_no_audio_cannot_be_shown_unchanged() -> None:
    with pytest.raises(safety.AudioCheckError):
        safety.check_audio_unchanged(b"", b"")


def test_a_field_with_a_value_is_never_emptied() -> None:
    before = mp3(TIT2(encoding=3, text=["a"]), TCON(encoding=3, text=["Pop"]))
    after = mp3(TIT2(encoding=3, text=["b"]))
    with pytest.raises(safety.TagEmptied, match="genre"):
        safety.check_no_emptied_tags(before, after)
    safety.check_no_emptied_tags(after, before)


# ---------------------------------------------------------------------------
# The run record
# ---------------------------------------------------------------------------


def test_a_record_is_created_once_then_updated(drive: FakeDrive) -> None:
    record = safety.RunRecord("flow", "r-1", FlowMode.WRITE).attach(drive, "state")
    record.add(safety.RecordEntry("f", "a.mp3", "deliver"))
    record.checkpoint()
    record.entries[0].state = "done"
    record.finish("complete")

    (item,) = drive.in_folder("state")
    assert item.name == "flow-r-1.json"
    saved = json.loads(item.data)
    assert saved["status"] == "complete"
    assert saved["entries"][0]["state"] == "done"
    assert [m for m, _ in drive.calls if m in ("upload_bytes", "update_file")] == [
        "upload_bytes",
        "update_file",
    ]


def test_a_redelivered_run_keeps_one_record_with_every_write(
    drive: FakeDrive,
) -> None:
    first = safety.RunRecord("flow", "r-1", FlowMode.WRITE).attach(drive, "state")
    first.add(safety.RecordEntry("f1", "a.mp3", "in_place", state="done"))
    first.checkpoint()

    again = safety.RunRecord("flow", "r-1", FlowMode.WRITE).attach(drive, "state")
    again.add(safety.RecordEntry("f2", "b.mp3", "in_place"))
    again.checkpoint()
    again.finish("complete")

    (item,) = drive.in_folder("state")
    saved = json.loads(item.data)
    assert [e["file_id"] for e in saved["entries"]] == ["f1", "f2"]
    assert saved["entries"][0]["state"] == "done"
    assert saved["status"] == "complete"


def test_records_already_saved_twice_are_merged_into_one(drive: FakeDrive) -> None:
    for file_id in ("f1", "f2"):
        old = safety.RunRecord("flow", "r-1", FlowMode.WRITE)
        old.add(safety.RecordEntry(file_id, f"{file_id}.mp3", "in_place"))
        drive.add("flow-r-1.json", "state", json.dumps(old.to_json()).encode())

    again = safety.RunRecord("flow", "r-1", FlowMode.WRITE).attach(drive, "state")
    again.add(safety.RecordEntry("f3", "c.mp3", "in_place"))
    again.checkpoint()

    first = drive.in_folder("state")[0]
    assert [e["file_id"] for e in json.loads(first.data)["entries"]] == [
        "f1",
        "f2",
        "f3",
    ]
    assert "upload_bytes" not in [m for m, _ in drive.calls]


def test_a_repeated_dry_run_writes_over_its_record(drive: FakeDrive) -> None:
    for name in ("a.mp3", "b.mp3"):
        record = safety.RunRecord("flow", "r-1", FlowMode.DRY_RUN).attach(
            drive, "state"
        )
        record.add(safety.RecordEntry("f", name, "in_place", state="planned"))
        record.finish("complete")

    (item,) = drive.in_folder("state")
    assert [e["original_name"] for e in json.loads(item.data)["entries"]] == ["b.mp3"]


def test_a_save_with_nothing_new_is_skipped(drive: FakeDrive) -> None:
    record = safety.RunRecord("flow", "r-1", FlowMode.WRITE).attach(drive, "state")
    record.add(safety.RecordEntry("f", "a.mp3", "deliver"))
    record.checkpoint()
    record.checkpoint()
    record.entries[0].state = "done"
    record.checkpoint()
    record.checkpoint()

    saves = [m for m, _ in drive.calls if m in ("upload_bytes", "update_file")]
    assert saves == ["upload_bytes", "update_file"]


def test_a_dry_run_record_is_saved_only_at_the_end_and_marked(
    drive: FakeDrive,
) -> None:
    record = safety.RunRecord("flow", "r-1", FlowMode.DRY_RUN).attach(drive, "state")
    record.checkpoint()
    assert drive.calls == []
    record.finish("complete")
    (item,) = drive.in_folder("state")
    assert item.name == "flow-r-1.dry-run.json"
    assert json.loads(item.data)["dry_run"] is True


def test_a_record_that_cannot_be_saved_raises_mid_run(drive: FakeDrive) -> None:
    record = safety.RunRecord("flow", "r-1", FlowMode.WRITE).attach(drive, "state")
    drive.upload_bytes = None  # type: ignore[assignment,method-assign]
    with pytest.raises(TypeError):
        record.checkpoint()
    record.finish("failed")  # the final save never raises


# ---------------------------------------------------------------------------
# In place (the library backfill's write)
# ---------------------------------------------------------------------------


def _in_place(drive: FakeDrive, mode: FlowMode, tagged: bytes, original: bytes):
    file_id = drive.add("Song_Artist.mp3", "library", original)
    record = safety.RunRecord("music-backfill", "r-1", mode).attach(drive, "state")
    entry = safety.write_in_place(
        drive,
        record,
        file_id=file_id,
        name="Song_Artist.mp3",
        original=original,
        tagged=tagged,
        identified_by="catalog",
    )
    return file_id, entry


def test_in_place_keeps_a_revision_and_never_renames_or_moves(
    drive: FakeDrive,
) -> None:
    original = mp3(TIT2(encoding=3, text=["song"]))
    tagged = mp3(TIT2(encoding=3, text=["Song"]))
    file_id, entry = _in_place(drive, FlowMode.WRITE, tagged, original)

    assert entry.state == "done"
    assert entry.revision_kept == "r1"
    assert drive.items[file_id].kept == {"r1"}
    assert drive.items[file_id].data == tagged
    assert drive.items[file_id].name == "Song_Artist.mp3"
    assert drive.items[file_id].parents == ["library"]
    assert {m for m, _ in drive.writes(except_folder="state")} == {
        "keep_revision",
        "update_file",
    }


def test_in_place_saves_the_record_three_times_a_file_after_the_first(
    drive: FakeDrive,
) -> None:
    record = safety.RunRecord("music-backfill", "r-1", FlowMode.WRITE).attach(
        drive, "state"
    )
    for n in range(2):
        original = mp3(TIT2(encoding=3, text=[f"song {n}"]))
        file_id = drive.add(f"{n}.mp3", "library", original)
        before = len(drive.calls)
        safety.write_in_place(
            drive,
            record,
            file_id=file_id,
            name=f"{n}.mp3",
            original=original,
            tagged=mp3(TIT2(encoding=3, text=[f"Song {n}"])),
            identified_by="catalog",
            head_revision="r1",
        )
        saves = [
            m
            for m, args in drive.calls[before:]
            if m in ("upload_bytes", "update_file") and args[0] != file_id
        ]
        # The first file also saves the record before its first change.
        assert len(saves) == (4 if n == 0 else 3)
    assert drive.items[file_id].kept == {"r1"}


def test_in_place_dry_run_writes_nothing(drive: FakeDrive) -> None:
    original = mp3(TIT2(encoding=3, text=["song"]))
    file_id, entry = _in_place(
        drive, FlowMode.DRY_RUN, mp3(TIT2(encoding=3, text=["Song"])), original
    )
    assert entry.state == "planned"
    assert drive.writes() == []
    assert drive.items[file_id].data == original


def test_in_place_restores_the_original_when_the_write_changed_the_audio(
    drive: FakeDrive, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = mp3(TIT2(encoding=3, text=["song"]))
    tagged = mp3(TIT2(encoding=3, text=["Song"]))
    real_update = drive.update_file
    first = [True]

    def corrupting_update(file_id, path, *, mime_type=None):
        real_update(file_id, path, mime_type=mime_type)
        if (
            first[0]
            and file_id in drive.items
            and "library" in drive.items[file_id].parents
        ):
            first[0] = False
            drive.items[file_id].data = drive.items[file_id].data[:-1] + b"\x09"

    monkeypatch.setattr(drive, "update_file", corrupting_update)

    with pytest.raises(safety.AudioChanged):
        _in_place(drive, FlowMode.WRITE, tagged, original)

    (library,) = drive.in_folder("library")
    assert library.data == original
    saved = json.loads(drive.in_folder("state")[0].data)
    assert saved["entries"][0]["state"] == "restored"


# -- reading tags from the start and end of a file ----------------------------


def _v1(title: str, artist: str) -> bytes:
    """An ID3v1 block, as some taggers append one."""
    return (
        b"TAG"
        + title.encode().ljust(30, b"\0")
        + artist.encode().ljust(30, b"\0")
        + b"\0" * 30  # album
        + b"2019"
        + b"\0" * 30  # comment
        + b"\xff"  # no genre
    )


def _tagged() -> bytes:
    """An ID3v2-tagged file that also ends in an ID3v1 block."""
    return mp3(TIT2(encoding=3, text=["Honesty"])) + _v1("Old Title", "Pink Sweat$")


def _same_tags(read: bytes, whole: bytes) -> None:
    from deejay_cog.music_tags import linked_catalog_id, read_tag_row

    assert read_tag_row(read) == read_tag_row(whole)
    assert linked_catalog_id(read) == linked_catalog_id(whole)
    assert safety.tag_snapshot(read) == safety.tag_snapshot(whole)


def test_a_tag_inside_the_start_is_read_without_the_whole_file(
    drive: FakeDrive, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = _tagged()
    tag_length = safety.id3v2_length(data)
    assert tag_length is not None
    monkeypatch.setattr(safety, "TAG_HEAD_BYTES", tag_length + 16)
    file_id = drive.add("a.mp3", "library", data)

    read = safety.read_tag_bytes(drive, file_id)

    assert not read.whole and len(read.data) < len(data)
    assert drive.downloads == []
    assert drive.ranges == [
        (file_id, f"bytes=0-{tag_length + 15}"),
        (file_id, f"bytes=-{safety.TAG_TAIL_BYTES}"),
    ]
    _same_tags(read.data, data)
    # The ID3v1 block at the end is read too: its artist fills the frame
    # the ID3v2 tag lacks.
    from deejay_cog.music_tags import read_tag_row

    assert read_tag_row(read.data)["artist"] == "Pink Sweat$"
    assert safety.whole_bytes(drive, file_id, read) == data
    assert drive.downloads == [file_id]


def test_a_tag_that_runs_past_the_start_reads_the_whole_file(
    drive: FakeDrive, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = _tagged()
    tag_length = safety.id3v2_length(data)
    assert tag_length is not None
    monkeypatch.setattr(safety, "TAG_HEAD_BYTES", tag_length - 1)
    file_id = drive.add("a.mp3", "library", data)

    read = safety.read_tag_bytes(drive, file_id)

    assert read.whole and read.data == data
    assert drive.downloads == [file_id]
    _same_tags(read.data, data)
    assert safety.whole_bytes(drive, file_id, read) == data
    assert drive.downloads == [file_id]  # not downloaded again


@pytest.mark.parametrize(
    "data", [AUDIO, AUDIO + _v1("Honesty", "Pink Sweat$")], ids=["bare", "v1-only"]
)
def test_a_file_with_no_id3v2_tag_reads_as_before(
    drive: FakeDrive, monkeypatch: pytest.MonkeyPatch, data: bytes
) -> None:
    monkeypatch.setattr(safety, "TAG_HEAD_BYTES", 64)
    file_id = drive.add("a.mp3", "library", data)

    read = safety.read_tag_bytes(drive, file_id)

    assert not read.whole and drive.downloads == []
    _same_tags(read.data, data)


def test_a_small_file_is_read_whole_in_one_range(drive: FakeDrive) -> None:
    data = _tagged()
    file_id = drive.add("a.mp3", "library", data)

    read = safety.read_tag_bytes(drive, file_id)

    assert read.whole and read.data == data
    assert drive.downloads == [] and len(drive.ranges) == 1


def test_an_empty_file_falls_back_to_the_whole_file(drive: FakeDrive) -> None:
    file_id = drive.add("a.mp3", "library", b"")

    read = safety.read_tag_bytes(drive, file_id)

    assert read.whole and read.data == b""
    assert drive.downloads == [file_id]


@pytest.mark.parametrize(
    ("head", "length"),
    [
        (b"\xff\xfb\x90\x64" + b"\0" * 6, 0),
        (b"ID3\x04\x00\x00" + _syncsafe(1000), 1010),
        (b"ID3\x04\x00\x10" + _syncsafe(1000), 1020),  # with a footer
        (b"ID3\x04\x00\x00\x00\x00\x80\x00", None),  # size not synchsafe
        (b"ID3\x04", None),  # cut short
    ],
)
def test_the_id3v2_length_is_read_from_its_header(head, length) -> None:
    assert safety.id3v2_length(head) == length
