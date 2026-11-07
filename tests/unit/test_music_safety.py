"""The safety layer: modes, the audio check, no emptied tags, the run record
and the in-place write the library backfill will use."""

from __future__ import annotations

import io
import json

import pytest
from music_fakes import AUDIO, FakeDrive, mp3
from mutagen.id3 import ID3, TCON, TIT2, TPE1

from deejay_cog import music_safety as safety
from deejay_cog.music_safety import FlowMode


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
    safety.parse_mode("yes", setting="NEW_MUSIC_MODE")
    assert "NEW_MUSIC_MODE" in caplog.text


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
