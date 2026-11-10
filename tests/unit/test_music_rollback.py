"""Rollback from a run record: dry by default, trash never delete, and
nothing touched that has changed since the run."""

from __future__ import annotations

import io
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from music_fakes import FakeDrive, mp3
from mutagen.id3 import ID3, TBPM, TCON, TIT2, TPE1

from deejay_cog import music_rollback as rb
from deejay_cog import new_music
from deejay_cog.music_safety import FlowMode, RunRecord, write_in_place

IN, DEST, REVIEW, ORIG, STATE = "in", "dest", "review", "orig", "state"
FOLDERS = new_music.IntakeFolders(IN, DEST, REVIEW, ORIG, STATE)


def _intake_run(drive: FakeDrive, mode: FlowMode = FlowMode.WRITE) -> dict[str, str]:
    """A new-music run of two files: one delivered, one sent to review."""
    original = mp3(TIT2(encoding=3, text=["temple"]), TPE1(encoding=3, text=["tg"]))
    delivered = drive.add("track01.mp3", IN, original)
    reviewed = drive.add("odd.mp3", IN, original)
    record = RunRecord("new-music", "r-1", mode, folders=FOLDERS.as_dict()).attach(
        drive, STATE
    )
    intake = new_music.Intake(drive, FOLDERS, record, new_music.IntakeSummary(mode))
    tagged = mp3(TIT2(encoding=3, text=["Temple"]), TPE1(encoding=3, text=["TG"]))
    intake.deliver(delivered, "track01.mp3", original, tagged, identified_by="catalog")
    intake.review(reviewed, "odd.mp3", original, "below confidence")
    record.finish("complete")
    copies = drive.in_folder(DEST)
    return {
        "delivered": delivered,
        "reviewed": reviewed,
        "copy": copies[0].id if copies else "",
    }


def test_a_dry_rollback_changes_nothing(drive: FakeDrive) -> None:
    ids = _intake_run(drive)
    before = len(drive.calls)

    name, steps = rb.rollback(drive, STATE, "r-1", apply=False)

    assert name == "new-music-r-1.json"
    assert drive.calls[before:] == []
    assert len([s for s in steps if s.action]) == 3
    assert drive.items[ids["copy"]].trashed is False
    assert drive.items[ids["delivered"]].parents == [ORIG]


def test_rollback_trashes_the_copy_and_moves_the_originals_back(
    drive: FakeDrive,
) -> None:
    ids = _intake_run(drive)

    _, steps = rb.rollback(drive, STATE, "r-1", apply=True)

    assert all(s.result == "done" for s in steps)
    assert drive.items[ids["copy"]].trashed is True
    assert ids["copy"] in drive.items  # trashed, not deleted
    assert drive.items[ids["delivered"]].parents == [IN]
    assert drive.items[ids["reviewed"]].parents == [IN]
    assert not any(m.startswith("delete") for m, _ in drive.calls)
    saved = [i for i in drive.in_folder(STATE) if i.name.startswith("music-rollback-")]
    assert len(saved) == 1
    assert len(json.loads(saved[0].data)["steps"]) == 3


def test_rollback_leaves_alone_what_moved_or_changed_since(drive: FakeDrive) -> None:
    ids = _intake_run(drive)
    drive.items[ids["copy"]].parents = ["library"]  # the owner filed it
    drive.items[ids["reviewed"]].parents = ["elsewhere"]

    _, steps = rb.rollback(drive, STATE, "r-1", apply=True)

    assert drive.items[ids["copy"]].trashed is False
    assert drive.items[ids["reviewed"]].parents == ["elsewhere"]
    assert drive.items[ids["delivered"]].parents == [IN]
    assert sum(1 for s in steps if s.skipped) == 2


def test_a_copy_edited_since_is_not_trashed(drive: FakeDrive) -> None:
    ids = _intake_run(drive)
    drive.items[ids["copy"]].data += b"edited"

    rb.rollback(drive, STATE, "r-1", apply=True)

    assert drive.items[ids["copy"]].trashed is False


def test_a_second_rollback_finds_nothing_left_to_do(drive: FakeDrive) -> None:
    _intake_run(drive)
    rb.rollback(drive, STATE, "r-1", apply=True)

    _, steps = rb.rollback(drive, STATE, "r-1", apply=False)

    assert all(s.action is None for s in steps)


def test_a_dry_run_record_is_refused(drive: FakeDrive) -> None:
    _intake_run(drive, FlowMode.DRY_RUN)

    with pytest.raises(rb.RollbackError, match="dry run"):
        rb.rollback(drive, STATE, "r-1", apply=True)
    assert not any(m in ("move_file", "trash_file") for m, _ in drive.calls)


def test_an_unknown_run_is_refused(drive: FakeDrive) -> None:
    with pytest.raises(rb.RollbackError, match="no run record"):
        rb.rollback(drive, STATE, "nope", apply=True)


def test_a_copy_the_record_missed_is_found_by_its_source(drive: FakeDrive) -> None:
    ids = _intake_run(drive)
    (item,) = [i for i in drive.in_folder(STATE) if i.name == "new-music-r-1.json"]
    record = json.loads(item.data)
    # As a run stopped after the upload and before recording it.
    record["entries"][0].update({"copy": None, "move": None, "state": "pending"})
    item.data = json.dumps(record).encode()

    rb.rollback(drive, STATE, "r-1", apply=True)

    assert drive.items[ids["copy"]].trashed is True
    assert drive.items[ids["delivered"]].parents == [IN]


def test_tags_written_in_place_are_undone_from_the_kept_revision(
    drive: FakeDrive,
) -> None:
    original = mp3(TIT2(encoding=3, text=["song"]))
    file_id = drive.add("Song_Artist.mp3", "library", original)
    record = RunRecord("music-backfill", "r-2", FlowMode.WRITE).attach(drive, STATE)
    write_in_place(
        drive,
        record,
        file_id=file_id,
        name="Song_Artist.mp3",
        original=original,
        tagged=mp3(TIT2(encoding=3, text=["Song"])),
        identified_by="catalog",
    )
    record.finish("complete")

    forgotten: list[list[str]] = []

    _, steps = rb.rollback(drive, STATE, "r-2", apply=True, forget=forgotten.append)

    assert drive.items[file_id].data == original
    assert drive.items[file_id].parents == ["library"]
    assert drive.items[file_id].name == "Song_Artist.mp3"
    # What the rollback put back is not read as an edit made in the file.
    assert forgotten == [[file_id]]
    assert steps[-1].result == "done"


def test_a_rollback_with_nothing_put_back_forgets_nothing(drive: FakeDrive) -> None:
    _intake_run(drive)
    forgotten: list[list[str]] = []

    rb.rollback(drive, STATE, "r-1", apply=True, forget=forgotten.append)

    assert forgotten == []


def test_the_command_needs_the_state_folder(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    monkeypatch.delenv("MUSIC_TAGGING_STATE_FOLDER_ID", raising=False)
    assert rb.main(["r-1"]) == 2
    assert "not set" in capsys.readouterr().err


def test_the_command_is_dry_unless_told_to_apply(
    drive: FakeDrive, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    ids = _intake_run(drive)
    monkeypatch.setenv("MUSIC_TAGGING_STATE_FOLDER_ID", STATE)
    google = MagicMock()
    google.from_env.return_value = SimpleNamespace(drive=drive)
    monkeypatch.setattr(rb, "GoogleAPI", google)

    assert rb.main(["r-1"]) == 0
    assert "Nothing was changed" in capsys.readouterr().out
    assert drive.items[ids["delivered"]].parents == [ORIG]

    assert rb.main(["r-1", "--apply"]) == 0
    assert drive.items[ids["delivered"]].parents == [IN]


def _sync_run(drive: FakeDrive, run_id: str, identified_by: str) -> str:
    """A library-sync run that wrote one file in place."""
    original = mp3(TIT2(encoding=3, text=["Show Me"]))
    file_id = drive.add(f"ShowMe_{run_id}.mp3", "library", original)
    record = RunRecord("library-sync", run_id, FlowMode.WRITE).attach(drive, STATE)
    write_in_place(
        drive,
        record,
        file_id=file_id,
        name=f"ShowMe_{run_id}.mp3",
        original=original,
        tagged=mp3(TIT2(encoding=3, text=["Show Me (Remix)"])),
        identified_by=identified_by,
    )
    record.finish("complete")
    return file_id


def test_the_catalog_put_back_runs_are_found_and_no_others(drive: FakeDrive) -> None:
    _sync_run(drive, "pb-1", rb.CATALOG_PUT_BACK)
    _sync_run(drive, "pb-2", rb.CATALOG_PUT_BACK)
    _sync_run(drive, "chunk-1", "sync (catalog wins on the fields that moved)")
    _sync_run(drive, "undo-1", "sync (bulk change outside the pipeline undone)")
    dry = RunRecord("library-sync", "pb-3", FlowMode.DRY_RUN).attach(drive, STATE)
    dry.finish("complete")

    assert rb.catalog_put_back_runs(drive, STATE) == ["pb-2", "pb-1"]


def test_the_command_rolls_back_every_catalog_put_back(
    drive: FakeDrive, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    _sync_run(drive, "pb-1", rb.CATALOG_PUT_BACK)
    _sync_run(drive, "pb-2", rb.CATALOG_PUT_BACK)
    monkeypatch.setenv("MUSIC_TAGGING_STATE_FOLDER_ID", STATE)
    google = MagicMock()
    google.from_env.return_value = SimpleNamespace(drive=drive)
    monkeypatch.setattr(rb, "GoogleAPI", google)

    assert rb.main(["--catalog-put-backs"]) == 0
    out = capsys.readouterr().out
    assert "library-sync-pb-1.json" in out and "library-sync-pb-2.json" in out
    assert rb.main(["pb-1", "--catalog-put-backs"]) == 2


def test_a_file_changed_since_gets_back_only_what_the_run_wrote_still_there(
    drive: FakeDrive,
) -> None:
    """The reconcile rounding a BPM after the run does not keep the run's
    title and genre on the file."""

    def song(title: str, genre: str, bpm: str) -> bytes:
        return mp3(
            TIT2(encoding=3, text=[title]),
            TCON(encoding=3, text=[genre]),
            TBPM(encoding=3, text=[bpm]),
        )

    original = song("Show Me", "R&B", "115.42")
    file_id = drive.add("ShowMe_Usher_115.mp3", "library", original)
    record = RunRecord("library-sync", "pb-1", FlowMode.WRITE).attach(drive, STATE)
    write_in_place(
        drive,
        record,
        file_id=file_id,
        name="ShowMe_Usher_115.mp3",
        original=original,
        tagged=song("Show Me (Remix)", "R&B/Soul", "115.42"),
        identified_by=rb.CATALOG_PUT_BACK,
    )
    record.finish("complete")
    drive.items[file_id].data = song("Show Me (Remix)", "R&B/Soul", "115.4")

    _, steps = rb.rollback(drive, STATE, "pb-1", apply=True, forget=lambda _: None)

    frames = {
        k: str(f.text[0]) for k, f in ID3(io.BytesIO(drive.items[file_id].data)).items()
    }
    assert frames == {"TIT2": "Show Me", "TCON": "R&B", "TBPM": "115.4"}
    assert steps[0].result == "done"


def test_a_catalog_put_back_forgets_every_file_it_wrote(drive: FakeDrive) -> None:
    """A file already put back, or changed since, still sheds the values
    the put-back recorded for it."""
    file_id = _sync_run(drive, "pb-1", rb.CATALOG_PUT_BACK)
    drive.items[file_id].data = mp3(TIT2(encoding=3, text=["Show Me"]))
    forgotten: list[list[str]] = []

    rb.rollback(
        drive,
        STATE,
        "pb-1",
        apply=True,
        forget=forgotten.append,
        forget_every_file=True,
    )

    assert forgotten == [[file_id]]
