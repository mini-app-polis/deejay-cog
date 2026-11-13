"""Sending the New Music copies intake named from an old file name back
through intake: listed from the run records, carried out only with --write,
nothing touched that has changed since."""

from __future__ import annotations

import json
from typing import Any

import pytest
from music_fakes import FakeDrive, mp3
from mutagen.id3 import TIT2, TPE1
from pydantic import ValidationError

from deejay_cog import new_music
from deejay_cog import new_music_redo as redo
from deejay_cog.music_safety import FlowMode, RunRecord

IN, DEST, REVIEW, ORIG, STATE = "in", "dest", "review", "orig", "state"
FOLDERS = new_music.IntakeFolders(IN, DEST, REVIEW, ORIG, STATE)
JUNK_TITLE = "CandyCleanKahlo113 Kahlo 113"


class FakeApi:
    def __init__(self) -> None:
        self.absent: list[list[str]] = []

    def mark_library_files_absent(self, payload: Any) -> None:
        self.absent.append(list(payload.drive_file_ids))


def _song(title: str, artist: str) -> bytes:
    return mp3(TIT2(encoding=3, text=[title]), TPE1(encoding=3, text=[artist]))


def _intake_run(drive: FakeDrive, run_id: str = "r-1") -> dict[str, str]:
    """A new-music run delivering one copy named from a junk title and one
    from an ordinary title."""
    record = RunRecord(
        "new-music", run_id, FlowMode.WRITE, folders=FOLDERS.as_dict()
    ).attach(drive, STATE)
    intake = new_music.Intake(
        drive, FOLDERS, record, new_music.IntakeSummary(FlowMode.WRITE)
    )
    junk = _song(JUNK_TITLE, "Kah-lo")
    fine = _song("Temple", "TG")
    junk_id = drive.add("Candy.mp3", IN, junk)
    fine_id = drive.add("Temple.mp3", IN, fine)
    intake.deliver(junk_id, "Candy.mp3", junk, junk, identified_by="tags")
    intake.deliver(fine_id, "Temple.mp3", fine, fine, identified_by="tags")
    record.finish("complete")
    copies = {i.name: i.id for i in drive.in_folder(DEST)}
    return {
        "junk": junk_id,
        "fine": fine_id,
        "junk_copy": next(v for k, v in copies.items() if k.startswith("CandyClean")),
    }


def test_propose_lists_only_the_copies_named_from_a_file_name(drive: FakeDrive) -> None:
    ids = _intake_run(drive)

    [row] = redo.propose(drive, STATE)

    assert row.action == "redo"
    assert (row.file_id, row.copy_id) == (ids["junk"], ids["junk_copy"])
    assert row.check == "a trailing BPM number"
    assert (row.title, row.artist, row.run_id) == (JUNK_TITLE, "Kah-lo", "r-1")


def test_propose_ignores_dry_runs(drive: FakeDrive) -> None:
    junk = _song(JUNK_TITLE, "Kah-lo")
    record = RunRecord(
        "new-music", "dry", FlowMode.DRY_RUN, folders=FOLDERS.as_dict()
    ).attach(drive, STATE)
    intake = new_music.Intake(
        drive, FOLDERS, record, new_music.IntakeSummary(FlowMode.DRY_RUN)
    )
    file_id = drive.add("Candy.mp3", IN, junk)
    intake.deliver(file_id, "Candy.mp3", junk, junk, identified_by="tags")
    record.finish("complete")

    assert redo.propose(drive, STATE) == []


def test_apply_without_write_changes_nothing(drive: FakeDrive) -> None:
    _intake_run(drive)
    rows = redo.propose(drive, STATE)
    api = FakeApi()
    before = len(drive.calls)

    lines = redo.apply(drive, api, STATE, rows, write=False)

    assert drive.calls[before:] == []
    assert api.absent == []
    assert [line.split()[0] for line in lines] == ["do", "do"]


def test_apply_trashes_the_copy_marks_it_absent_and_returns_the_original(
    drive: FakeDrive,
) -> None:
    ids = _intake_run(drive)
    api = FakeApi()

    lines = redo.apply(drive, api, STATE, redo.propose(drive, STATE), write=True)

    assert all(line.endswith("[done]") for line in lines)
    assert drive.items[ids["junk_copy"]].trashed is True
    assert drive.items[ids["junk"]].parents == [IN]
    assert api.absent == [[ids["junk_copy"]]]
    assert drive.items[ids["fine"]].parents == [ORIG]  # not listed, not touched
    assert not any(m.startswith("delete") for m, _ in drive.calls)
    [saved] = [
        i for i in drive.in_folder(STATE) if i.name.startswith("new-music-redo-")
    ]
    assert len(json.loads(saved.data)["steps"]) == 2


def test_with_the_original_gone_the_copy_itself_goes_back(drive: FakeDrive) -> None:
    ids = _intake_run(drive)
    drive.items[ids["junk"]].trashed = True
    api = FakeApi()

    lines = redo.apply(drive, api, STATE, redo.propose(drive, STATE), write=True)

    assert lines == [
        f"do    {drive.items[ids['junk_copy']].name}: move the copy to the input "
        "folder (its original is gone)  [done]"
    ]
    assert drive.items[ids["junk_copy"]].parents == [IN]
    assert drive.items[ids["junk_copy"]].trashed is False
    assert api.absent == [[ids["junk_copy"]]]


def test_a_copy_filed_since_is_left_and_its_original_stays(drive: FakeDrive) -> None:
    ids = _intake_run(drive)
    drive.items[ids["junk_copy"]].parents = ["library"]  # the owner filed it
    api = FakeApi()

    lines = redo.apply(drive, api, STATE, redo.propose(drive, STATE), write=True)

    assert lines[0].startswith("skip") and "moved since the run" in lines[0]
    assert drive.items[ids["junk_copy"]].trashed is False
    assert drive.items[ids["junk"]].parents == [ORIG]
    assert api.absent == []


def test_a_skipped_row_and_an_unknown_copy_change_nothing(drive: FakeDrive) -> None:
    ids = _intake_run(drive)
    [row] = redo.propose(drive, STATE)
    stranger = redo.Row(**{**row.__dict__, "copy_id": "other"})
    skipped = redo.Row(**{**row.__dict__, "action": "skip"})
    before = len(drive.calls)

    lines = redo.apply(drive, FakeApi(), STATE, [skipped, stranger], write=True)

    assert lines[0].endswith(": skipped")
    assert "no delivery of this copy" in lines[1]
    assert [m for m, _ in drive.calls[before:] if m != "download_file_bytes"] == []
    assert drive.items[ids["junk_copy"]].trashed is False


def test_the_csv_round_trips_and_checks_its_action(tmp_path) -> None:
    rows = [
        redo.Row("redo", "A_B.mp3", "c1", "f1", "underscores", "A_B", "B", "r", "t")
    ]
    path = tmp_path / "p.csv"
    redo.write_csv(str(path), rows)
    assert redo.read_csv(str(path)) == rows

    path.write_text("action,copy_id\nrename,c1\n", encoding="utf-8")
    with pytest.raises(ValidationError):
        redo.read_csv(str(path))


def test_the_csv_model_has_every_column() -> None:
    assert list(redo.CsvRow.model_fields) == redo.COLUMNS
