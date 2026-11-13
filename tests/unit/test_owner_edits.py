"""owner_edits: propose a ``< >`` retitle for each main-library file whose name
or title suggests an owner edit, and write only what the owner approved."""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

import pytest
from audio_fixtures import tagged
from mini_app_polis.api.contract import LibraryFile
from mini_app_polis.mp3.tag import AudioFormat
from music_fakes import FakeDrive, mp3
from mutagen.id3 import TIT2, TPE1
from pydantic import ValidationError

from deejay_cog import owner_edits
from deejay_cog.music_tags import read_tag_row
from deejay_cog.owner_edits import Row, owner_edit_title, propose_row

MAIN, FX, STATE = "main-root", "fx-root", "state"
_NOW = dt.datetime(2026, 11, 12, tzinfo=dt.UTC)


def _row(
    path: str, drive_id: str = "id", *, root: str = MAIN, **kw: Any
) -> LibraryFile:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "drive_file_id": drive_id,
        "library_root_id": root,
        "path": path,
        "identification_status": "identified",
        "present": True,
        "trash_requested": False,
        "first_seen": _NOW,
        "last_seen": _NOW,
    }
    values.update(kw)
    return LibraryFile(**values)


class FakeApi:
    def __init__(self, rows: list[LibraryFile]) -> None:
        self.rows = rows

    def list_library_files(
        self,
        *,
        present: bool | None = None,  # noqa: ARG002 — the real client's filter
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[LibraryFile]:
        start = offset or 0
        return self.rows[start : start + (limit or len(self.rows))]


def _mp3(title: str, artist: str = "Etta James") -> bytes:
    return mp3(TIT2(encoding=3, text=[title]), TPE1(encoding=3, text=[artist]))


# --- the proposed title -----------------------------------------------------------


@pytest.mark.parametrize(
    ("title", "version", "expected"),
    [
        (
            "Baby What You Want Me To Do (No Intro Edit)",
            "Intro Cut",
            "Baby What You Want Me To Do <Intro Cut>",
        ),
        (
            "Jamming [JT Season 2 JV Team Routine]",
            "Routine Cut",
            "Jamming <Routine Cut>",
        ),
        ("Song - Self Censored", "Self Censored", "Song <Self Censored>"),
        ("Song (Radio Edit) (No Intro)", "Intro Cut", "Song (Radio Edit) <Intro Cut>"),
    ],
)
def test_the_edit_part_is_dropped_and_the_version_goes_on(
    title: str, version: str, expected: str
) -> None:
    assert owner_edit_title(title, version) == expected


def test_a_plain_owner_edit_is_proposed_as_a_retitle() -> None:
    row = _row("BabyWhatYouWantMeToDo_NoIntroEdit_EttaJames_100.mp3")

    got = propose_row(row, "Baby What You Want Me To Do (No Intro Edit)")

    assert got is not None
    assert got.action == "retitle"
    assert got.title == "Baby What You Want Me To Do <Intro Cut>"


def test_a_bare_edited_is_left_for_the_owner() -> None:
    got = propose_row(_row("SongName_Edited_Artist_100.mp3"), "Song Name (Edited)")

    assert got is not None
    assert got.action == "skip"
    assert got.title == "Song Name <Edit>"
    assert "official" in got.note


@pytest.mark.parametrize(
    ("path", "title"),
    [
        ("capsize_radioedit_frenship.mp3", "Capsize (Radio Edit)"),
        ("LetsGo_RadioEdit_CalvinHarrisNeYo_128.mp3", "Lets Go (Radio Edit)"),
    ],
)
def test_a_radio_edit_is_not_proposed(path: str, title: str) -> None:
    assert propose_row(_row(path), title) is None


def test_a_file_already_marked_or_with_no_sign_is_not_proposed() -> None:
    assert propose_row(_row("a.mp3"), "Song <Intro Cut>") is None
    assert propose_row(_row("Credit_Artist_100.mp3"), "Credit") is None


# --- proposing --------------------------------------------------------------------


def test_propose_reads_only_main_library_files_whose_name_or_label_hints() -> None:
    drive = FakeDrive()
    cut = drive.add(
        "BabyNoIntroEdit.mp3", MAIN, _mp3("Baby What You Want Me To Do (No Intro Edit)")
    )
    labeled = drive.add("Song.mp3", MAIN, _mp3("Song (Self Censored)"))
    plain = drive.add("Other.mp3", MAIN, _mp3("Other"))
    fx = drive.add("IntroCut.mp3", FX, _mp3("Walk On (Intro Cut)"))
    api = FakeApi(
        [
            _row("Baby_NoIntroEdit_EttaJames_100.mp3", cut),
            _row("Song_Artist_100.mp3", labeled, edit_label="Censored"),
            _row("Other_Artist_100.mp3", plain),
            _row("_WalkOn/IntroCut.mp3", fx, root=FX),
        ]
    )

    rows = owner_edits.propose(drive, api, FX)

    assert [(r.path, r.action, r.title) for r in rows] == [
        (
            "Baby_NoIntroEdit_EttaJames_100.mp3",
            "retitle",
            "Baby What You Want Me To Do <Intro Cut>",
        ),
        ("Song_Artist_100.mp3", "retitle", "Song <Self Censored>"),
    ]
    downloaded = {
        args[0] for name, args in drive.calls if name == "download_file_bytes"
    }
    assert plain not in downloaded and fx not in downloaded


def test_the_csv_round_trips(tmp_path) -> None:
    rows = [Row("retitle", "a.mp3", "id1", 'A "B" <Intro Cut>', "A, B", "n")]
    path = tmp_path / "p.csv"

    owner_edits.write_csv(str(path), rows)

    assert owner_edits.read_csv(str(path)) == rows


def test_a_reviewed_csv_is_read_leniently_but_its_action_is_checked(tmp_path) -> None:
    path = tmp_path / "p.csv"
    path.write_text("action,path,extra\n retitle ,a.mp3,ignored\n", encoding="utf-8")
    [row] = owner_edits.read_csv(str(path))
    assert row.action == "retitle" and row.path == "a.mp3" and row.file_id == ""

    path.write_text("action,path\ntag,a.mp3\n", encoding="utf-8")
    with pytest.raises(ValidationError):
        owner_edits.read_csv(str(path))


def test_the_csv_model_has_every_column() -> None:
    assert list(owner_edits.CsvRow.model_fields) == owner_edits.COLUMNS


# --- writing ----------------------------------------------------------------------


@pytest.mark.parametrize("fmt", [AudioFormat.MP3, AudioFormat.M4A])
def test_only_the_title_is_written_in_the_files_own_format(fmt: AudioFormat) -> None:
    data = (
        _mp3("Song (No Intro)")
        if fmt is AudioFormat.MP3
        else tagged(fmt, title="Song (No Intro)", artist="Etta James")
    )

    out = owner_edits.retitled(data, "Song <Intro Cut>")

    row = read_tag_row(out)
    assert (row["title"], row["artist"]) == ("Song <Intro Cut>", "Etta James")


def test_apply_without_write_changes_nothing() -> None:
    drive = FakeDrive()
    cut = drive.add("Baby.mp3", MAIN, _mp3("Baby (No Intro Edit)"))
    before = len(drive.writes(except_folder=STATE))

    _, results = owner_edits.apply(
        drive,
        STATE,
        [Row("retitle", "Baby.mp3", cut, "Baby <Intro Cut>", "", "")],
        write=False,
        run_id="d-1",
    )

    assert results == ["Baby.mp3: would retitle to Baby <Intro Cut>"]
    assert len(drive.writes(except_folder=STATE)) == before


def test_apply_with_write_retitles_approved_rows_only() -> None:
    drive = FakeDrive()
    cut = drive.add("Baby.mp3", MAIN, _mp3("Baby (No Intro Edit)"))
    other = drive.add("Song.mp3", MAIN, _mp3("Song (Edited)"))
    rows = [
        Row("retitle", "Baby.mp3", cut, "Baby <Intro Cut>", "", ""),
        Row("skip", "Song.mp3", other, "Song <Edit>", "", ""),
        Row("retitle", "Song.mp3", other, "Song (Edited)", "", ""),
    ]

    record, results = owner_edits.apply(drive, STATE, rows, write=True, run_id="w-1")

    assert results[0] == "Baby.mp3: retitled to Baby <Intro Cut>"
    assert results[1] == "Song.mp3: skipped"
    assert results[2] == "Song.mp3: skipped: the title has no version in < >"
    assert (
        read_tag_row(drive.download_file_bytes(cut).data)["title"] == "Baby <Intro Cut>"
    )
    assert (
        read_tag_row(drive.download_file_bytes(other).data)["title"] == "Song (Edited)"
    )
    assert record.startswith("owner-edits-")
