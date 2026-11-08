"""Restoring styling: only a write that flattened a chosen styling is
undone, only that field, and only while the file is as the write left it."""

from __future__ import annotations

import json

import pytest
from music_fakes import FakeDrive, mp3
from mutagen.id3 import TIT2, TPE1

from deejay_cog import restore_styling
from deejay_cog.music_safety import FlowMode, RunRecord, tag_snapshot, write_in_place
from deejay_cog.music_tags import write_owned_fields

LIB, STATE = "library", "state"


@pytest.mark.parametrize(
    ("before", "after", "lost"),
    [
        ("WSTRN", "Wstrn", True),
        ("i'm so tired...", "I'm So Tired", True),
        ("Boyz II Men", "Boyz Ii Men", True),
        ("GoldFish", "Goldfish", False),
        ("If We Never Met", "if we never met", False),
        ("Gerald Levert & Jaheim", "Gerald Levert, Jaheim", False),
        ("Hands Up", "Hands Up (feat. DNCE)", False),
        ("", "Wstrn", False),
    ],
)
def test_flattened(before: str, after: str, lost: bool) -> None:
    assert restore_styling.flattened(before, after) is lost


def _file(drive: FakeDrive, name: str, title: str, artist: str) -> str:
    return drive.add(
        name, LIB, mp3(TIT2(encoding=3, text=[title]), TPE1(encoding=3, text=[artist]))
    )


def _write(drive: FakeDrive, record: RunRecord, file_id: str, **values: str) -> None:
    data = drive.download_file_bytes(file_id).data
    tagged = write_owned_fields(data, values)
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


def _tags(drive: FakeDrive, file_id: str) -> dict[str, str]:
    return tag_snapshot(drive.download_file_bytes(file_id).data)


def _library(drive: FakeDrive) -> dict[str, str]:
    files = {
        "caps": _file(drive, "In2_WSTRN_98.mp3", "In2", "WSTRN"),
        "lower": _file(
            drive, "imsotired_109.mp3", "i'm so tired...", "Lauv & Troye Sivan"
        ),
        "sloppy": _file(drive, "HoldYourKite_105.mp3", "Hold Your Kite", "GoldFish"),
        "changed": _file(drive, "ILYSB_84.mp3", "ILYSB", "LANY"),
    }
    record = RunRecord("library-reconcile", "r-1", FlowMode.WRITE).attach(drive, STATE)
    _write(drive, record, files["caps"], artist="Wstrn")
    _write(
        drive, record, files["lower"], title="I'm So Tired", artist="Lauv, Troye Sivan"
    )
    _write(drive, record, files["sloppy"], artist="Goldfish")
    _write(drive, record, files["changed"], artist="Lany")
    record.finish("complete")
    data = drive.download_file_bytes(files["changed"]).data
    drive.items[files["changed"]].data = write_owned_fields(data, {"artist": "L A N Y"})
    return files


def test_dry_run_plans_only_lost_styling_and_writes_nothing() -> None:
    drive = FakeDrive()
    files = _library(drive)
    before = len(drive.writes(except_folder=STATE))

    _, restores = restore_styling.restore(drive, STATE, apply=False, run_id="d-1")

    planned = {(r.file_id, r.field): r.result for r in restores}
    assert planned == {
        (files["caps"], "artist"): "would restore",
        (files["lower"], "title"): "would restore",
        (files["changed"], "artist"): "left alone: its artist is 'L A N Y' now",
    }
    assert len(drive.writes(except_folder=STATE)) == before


def test_apply_restores_only_the_flattened_fields() -> None:
    drive = FakeDrive()
    files = _library(drive)

    name, _ = restore_styling.restore(drive, STATE, apply=True, run_id="a-1")

    assert _tags(drive, files["caps"])["artist"] == "WSTRN"
    lower = _tags(drive, files["lower"])
    assert lower["title"] == "i'm so tired..."
    assert lower["artist"] == "Lauv, Troye Sivan"  # the joiner write stands
    assert _tags(drive, files["sloppy"])["artist"] == "Goldfish"
    assert _tags(drive, files["changed"])["artist"] == "L A N Y"
    saved = [i for i in drive.in_folder(STATE) if i.name == name]
    entries = json.loads(saved[0].data.decode())["entries"]
    assert sorted(e["file_id"] for e in entries) == sorted(
        [files["caps"], files["lower"]]
    )
