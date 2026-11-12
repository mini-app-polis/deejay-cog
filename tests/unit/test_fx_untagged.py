"""fx_untagged: propose a title and artist (or a trash) for each FX Music file
whose tags name no song, and write only what the owner approved."""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

import pytest
from audio_fixtures import sample, tagged
from mini_app_polis.api.contract import LibraryFile, LibraryFilesAbsentRequest
from mini_app_polis.mp3.tag import AudioFormat
from music_fakes import FakeDrive, mp3
from mutagen.id3 import TIT2, TPE1

from deejay_cog import fx_untagged
from deejay_cog.fx_untagged import Row, copy_of, name_guess
from deejay_cog.music_tags import read_tag_row

FX, OTHER, STATE = "fx-root", "main-root", "state"
_NOW = dt.datetime(2026, 11, 12, tzinfo=dt.UTC)


def _row(path: str, drive_id: str, *, root: str = FX, **kw: Any) -> LibraryFile:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "drive_file_id": drive_id,
        "library_root_id": root,
        "path": path,
        "identification_status": "unidentified",
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
        self.absent: list[list[str]] = []

    def list_library_files(
        self,
        *,
        present: bool | None = None,  # noqa: ARG002 — the real client's filter
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[LibraryFile]:
        start = offset or 0
        return self.rows[start : start + (limit or len(self.rows))]

    def mark_library_files_absent(self, payload: LibraryFilesAbsentRequest) -> Any:
        self.absent.append(list(payload.drive_file_ids))
        return None


# --- reading a name ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "title", "artist"),
    [
        ("_WalkOn/Lady GaGa - Applause.mp3", "Applause", "Lady GaGa"),
        (
            "_WalkOn/02 - Sandstorm (Original Version).mp3",
            "Sandstorm (Original Version)",
            "",
        ),
        ("_AwardMusic/LadyBriGoodMood_Edited.mp3", "Lady Bri Good Mood", "Edited"),
        ("_WalkOn/VA - Footloose.mp3", "Footloose", ""),
        ("_WalkOn/Be Our Guest.m4a", "Be Our Guest", ""),
    ],
)
def test_a_title_and_artist_are_read_from_the_name(
    path: str, title: str, artist: str
) -> None:
    got_title, got_artist, note = name_guess(path)
    assert (got_title, got_artist) == (title, artist)
    assert note


def test_va_is_not_taken_as_an_artist() -> None:
    _, artist, note = name_guess("_WalkOn/VA - Come And Get Your Love.mp3")
    assert artist == ""
    assert "VA" in note


def test_a_copy_names_the_file_it_copies() -> None:
    siblings = {"_MCIntro/Intro.mp3", "_MCIntro/Intro copy.mp3", "_WalkOn/Intro.mp3"}
    assert copy_of("_MCIntro/Intro copy.mp3", siblings) == "_MCIntro/Intro.mp3"
    assert copy_of("_MCIntro/Intro.mp3", siblings) == ""


def test_a_copy_in_another_folder_is_not_a_copy() -> None:
    assert copy_of("_MCIntro/Intro copy.mp3", {"_WalkOn/Intro.mp3"}) == ""


# --- proposing --------------------------------------------------------------------


def _fx_library() -> tuple[FakeDrive, FakeApi]:
    drive = FakeDrive()
    blank = drive.add("Be Our Guest.mp3", FX, mp3())
    named = drive.add(
        "Applause.mp3",
        FX,
        mp3(TIT2(encoding=3, text=["Applause"]), TPE1(encoding=3, text=["Lady GaGa"])),
    )
    original = drive.add("Intro.mp3", FX, mp3())
    copy = drive.add("Intro copy.mp3", FX, mp3())
    main = drive.add("Song.mp3", OTHER, mp3())
    api = FakeApi(
        [
            _row("_WalkOn/Be Our Guest.mp3", blank),
            _row("_WalkOn/Lady GaGa - Applause.mp3", named),
            _row("_MCIntro/Intro.mp3", original),
            _row("_MCIntro/Intro copy.mp3", copy),
            _row("Song.mp3", main, root=OTHER),
        ]
    )
    return drive, api


def test_propose_takes_only_fx_files_whose_tags_name_no_song() -> None:
    drive, api = _fx_library()
    before = len(drive.writes())

    rows = fx_untagged.propose(drive, api, FX)

    by_path = {r.path: r for r in rows}
    assert set(by_path) == {
        "_MCIntro/Intro copy.mp3",
        "_MCIntro/Intro.mp3",
        "_WalkOn/Be Our Guest.mp3",
    }
    assert by_path["_MCIntro/Intro copy.mp3"].action == "trash"
    assert by_path["_WalkOn/Be Our Guest.mp3"].action == "skip"  # no artist
    assert by_path["_WalkOn/Be Our Guest.mp3"].title == "Be Our Guest"
    assert len(drive.writes()) == before


def test_the_csv_round_trips(tmp_path) -> None:
    rows = [Row("tag", "_WalkOn/a.mp3", "id1", "A, B", 'C "D"', "", "", "note")]
    path = tmp_path / "p.csv"

    fx_untagged.write_csv(str(path), rows)

    assert fx_untagged.read_csv(str(path)) == rows


# --- writing ----------------------------------------------------------------------


@pytest.mark.parametrize("fmt", [AudioFormat.MP3, AudioFormat.M4A, AudioFormat.FLAC])
def test_the_title_and_artist_are_written_in_the_files_own_format(
    fmt: AudioFormat,
) -> None:
    data = mp3() if fmt is AudioFormat.MP3 else tagged(fmt, comment="keep me")

    out = fx_untagged.retitled(data, "Be Our Guest", "Angela Lansbury")

    row = read_tag_row(out)
    assert (row["title"], row["artist"]) == ("Be Our Guest", "Angela Lansbury")


def test_apply_without_write_changes_nothing() -> None:
    drive, api = _fx_library()
    blank = next(i for i, f in drive.items.items() if f.name == "Be Our Guest.mp3")
    copy = next(i for i, f in drive.items.items() if f.name == "Intro copy.mp3")
    before = len(drive.writes(except_folder=STATE))
    rows = [
        Row(
            "tag",
            "_WalkOn/Be Our Guest.mp3",
            blank,
            "Be Our Guest",
            "Angela Lansbury",
            "",
            "",
            "",
        ),
        Row("trash", "_MCIntro/Intro copy.mp3", copy, "", "", "", "", ""),
    ]

    _, results = fx_untagged.apply(drive, api, STATE, rows, write=False, run_id="d-1")

    assert results == [
        "_WalkOn/Be Our Guest.mp3: would set Angela Lansbury - Be Our Guest",
        "_MCIntro/Intro copy.mp3: would move to Drive's trash",
    ]
    assert len(drive.writes(except_folder=STATE)) == before
    assert api.absent == []


def test_apply_with_write_tags_trashes_and_skips() -> None:
    drive, api = _fx_library()
    blank = next(i for i, f in drive.items.items() if f.name == "Be Our Guest.mp3")
    copy = next(i for i, f in drive.items.items() if f.name == "Intro copy.mp3")
    original = next(i for i, f in drive.items.items() if f.name == "Intro.mp3")
    rows = [
        Row(
            "tag",
            "_WalkOn/Be Our Guest.mp3",
            blank,
            "Be Our Guest",
            "Angela Lansbury",
            "",
            "",
            "",
        ),
        Row("trash", "_MCIntro/Intro copy.mp3", copy, "", "", "", "", ""),
        Row("tag", "_MCIntro/Intro.mp3", original, "Intro", "", "", "", ""),
        Row("skip", "_MCIntro/Intro.mp3", original, "", "", "", "", ""),
    ]

    record, results = fx_untagged.apply(
        drive, api, STATE, rows, write=True, run_id="w-1"
    )

    assert results[0] == "_WalkOn/Be Our Guest.mp3: set Angela Lansbury - Be Our Guest"
    assert results[1] == "_MCIntro/Intro copy.mp3: moved to Drive's trash"
    assert results[2].endswith("skipped: the title or artist is empty")
    assert results[3].endswith(": skipped")
    written = read_tag_row(drive.download_file_bytes(blank).data)
    assert (written["title"], written["artist"]) == ("Be Our Guest", "Angela Lansbury")
    assert ("trash_file", (copy,)) in drive.calls
    assert api.absent == [[copy]]
    assert record.startswith("fx-untagged-")


def test_an_untagged_m4a_sample_reads_as_naming_no_song() -> None:
    # The proposal relies on this: a file with no tags is one identify skips.
    assert read_tag_row(sample(AudioFormat.M4A))["title"] == ""
