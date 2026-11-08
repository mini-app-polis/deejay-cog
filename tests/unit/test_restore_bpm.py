"""Restoring BPM: only a write that moved the tempo is undone, only the BPM
field, and only while the file still holds what the write left."""

from __future__ import annotations

import json

from music_fakes import FakeDrive, mp3
from mutagen.id3 import TBPM, TIT2

from deejay_cog import restore_bpm
from deejay_cog.music_safety import FlowMode, RunRecord, tag_snapshot, write_in_place
from deejay_cog.music_tags import write_owned_fields

LIB, STATE = "library", "state"


def _file(drive: FakeDrive, name: str, bpm: str) -> str:
    return drive.add(
        name, LIB, mp3(TIT2(encoding=3, text=[name]), TBPM(encoding=3, text=[bpm]))
    )


def _reconcile_write(
    drive: FakeDrive, record: RunRecord, file_id: str, bpm: str
) -> None:
    data = drive.download_file_bytes(file_id).data
    tagged = write_owned_fields(data, {"bpm": bpm})
    assert tagged is not None
    write_in_place(
        drive,
        record,
        file_id=file_id,
        name=drive.items[file_id].name,
        original=data,
        tagged=tagged,
        identified_by="reconcile (catalog wins)",
    )


def _bpm(drive: FakeDrive, file_id: str) -> str:
    return tag_snapshot(drive.download_file_bytes(file_id).data)["bpm"]


def _library(drive: FakeDrive) -> dict[str, str]:
    files = {
        "tempo": _file(drive, "Hymn_90.mp3", "90.00"),
        "rounding": _file(drive, "Heartlines_90.mp3", "89.52"),
        "changed": _file(drive, "HighHeels_120.mp3", "120.00"),
    }
    record = RunRecord("library-reconcile", "r-1", FlowMode.WRITE).attach(drive, STATE)
    _reconcile_write(drive, record, files["tempo"], "102")
    _reconcile_write(drive, record, files["rounding"], "89.5")
    _reconcile_write(drive, record, files["changed"], "110")
    record.finish("complete")
    # Edited since the write: the restore must leave it alone.
    data = drive.download_file_bytes(files["changed"]).data
    drive.items[files["changed"]].data = write_owned_fields(data, {"bpm": "115"})
    # A record of differences shares the prefix and is not a run record.
    drive.add(
        "library-reconcile-differences-x.json",
        STATE,
        json.dumps({"differences": {}, "saved_at": "x"}).encode(),
    )
    return files


def test_dry_run_plans_only_tempo_changes_and_writes_nothing() -> None:
    drive = FakeDrive()
    files = _library(drive)
    before = len(drive.writes(except_folder=STATE))

    _, restores = restore_bpm.restore(drive, STATE, apply=False, run_id="d-1")

    by_id = {r.file_id: r for r in restores}
    assert set(by_id) == {files["tempo"], files["changed"]}
    assert by_id[files["tempo"]].before == "90.00"
    assert by_id[files["tempo"]].result == "would restore"
    assert by_id[files["changed"]].result.startswith("left alone")
    assert len(drive.writes(except_folder=STATE)) == before
    assert _bpm(drive, files["tempo"]) == "102"


def test_apply_puts_back_the_files_own_bpm_and_nothing_else() -> None:
    drive = FakeDrive()
    files = _library(drive)

    name, restores = restore_bpm.restore(drive, STATE, apply=True, run_id="a-1")

    assert _bpm(drive, files["tempo"]) == "90.00"
    assert (
        tag_snapshot(drive.download_file_bytes(files["tempo"]).data)["title"]
        == "Hymn_90.mp3"
    )
    assert _bpm(drive, files["rounding"]) == "89.5"
    assert _bpm(drive, files["changed"]) == "115"
    assert name == "library-bpm-restore-a-1.json"
    saved = [i for i in drive.in_folder(STATE) if i.name == name]
    entries = json.loads(saved[0].data.decode())["entries"]
    assert [e["file_id"] for e in entries] == [files["tempo"]]
    assert entries[0]["state"] == "done"
