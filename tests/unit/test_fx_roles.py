"""fx_roles: each FX Music file's role, from its folder, at the start of its
comment; a dry run lists every change, --write writes only the comment."""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

import pytest
from audio_fixtures import tagged
from mini_app_polis.api.contract import LibraryFile, LibraryFilesAbsentRequest
from mini_app_polis.mp3.tag import FX_ROLES, AudioFormat
from music_fakes import FakeDrive, mp3
from mutagen.id3 import COMM, TIT2, TPE1

from deejay_cog import audio_formats, fx_roles
from deejay_cog import music_rollback as rb
from deejay_cog.fx_roles import FOLDER_ROLES, folder_role, read_comment
from deejay_cog.music_safety import check_audio_unchanged
from deejay_cog.music_tags import read_tag_row

FX, OTHER, STATE = "fx-root", "main-root", "state"
_NOW = dt.datetime(2026, 11, 14, tzinfo=dt.UTC)


def _row(path: str, drive_id: str, *, root: str = FX, **kw: Any) -> LibraryFile:
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

    def mark_library_files_absent(self, payload: LibraryFilesAbsentRequest) -> Any:
        raise AssertionError(f"fx_roles never marks files absent: {payload}")


def _song(comment: str | None = None) -> bytes:
    frames: list[Any] = [
        TIT2(encoding=3, text=["Applause"]),
        TPE1(encoding=3, text=["Lady GaGa"]),
    ]
    if comment is not None:
        frames.append(COMM(encoding=3, lang="eng", desc="", text=[comment]))
    return mp3(*frames)


def _other_tags(data: bytes, fmt: AudioFormat) -> dict[str, str]:
    """Every tag the shared tagger reads but the comment."""
    tags, link = audio_formats.read(data, fmt)
    return {**{k: v for k, v in tags.items() if k != "comment"}, "link": link}


# --- the folder's role ------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "role"),
    [
        ("_WalkOn/Applause.mp3", "Walk On"),
        ("_BowMusic/Bow.mp3", "Bow"),
        ("_MCIntro/Intro.m4a", "MC Intro"),
        ("_RotationMusic/Spin.flac", "Rotation"),
        ("EventSpecific/Swingtime 2026/_WalkOn/Applause.mp3", "Walk On"),
        ("_WalkOn/_BowMusic/Bow.mp3", "Bow"),
        ("_BowMusic/Extras/Bow.mp3", "Bow"),
        ("_AwardMusic/Award.mp3", None),
        ("_WalkOn.mp3", None),
        ("Applause.mp3", None),
    ],
)
def test_a_files_role_is_its_nearest_role_folders(path: str, role: str | None) -> None:
    assert folder_role(path) == role


def test_every_folder_names_a_shared_role() -> None:
    assert set(FOLDER_ROLES.values()) == set(FX_ROLES)


# --- reading and writing the comment ----------------------------------------------


@pytest.mark.parametrize("fmt", [AudioFormat.MP3, AudioFormat.M4A, AudioFormat.FLAC])
def test_only_the_comment_is_written_in_the_files_own_format(fmt: AudioFormat) -> None:
    if fmt is AudioFormat.MP3:
        data = _song("<Competitor Routine>")
    else:
        data = tagged(
            fmt, title="Applause", artist="Lady GaGa", comment="<Competitor Routine>"
        )
    out = fx_roles.with_comment(data, "<FX><Walk On><Competitor Routine>")

    assert read_comment(out) == "<FX><Walk On><Competitor Routine>"
    assert _other_tags(out, fmt) == _other_tags(data, fmt)
    assert read_tag_row(out)["title"] == "Applause"
    check_audio_unchanged(data, out)


def test_a_file_the_tagger_cannot_read_raises_not_taggable() -> None:
    with pytest.raises(audio_formats.NotTaggable):
        read_comment(b"OggS" + b"\x00" * 64)


# --- the run ----------------------------------------------------------------------


def _fx_library() -> tuple[FakeDrive, FakeApi, dict[str, str]]:
    drive = FakeDrive()
    ids = {
        "blank": drive.add("Applause.mp3", FX, _song()),
        "hand": drive.add("Routine.mp3", FX, _song("<Competitor Routine>")),
        "fx": drive.add("Bow.mp3", FX, _song("<FX><Judges>")),
        "done": drive.add("Spin.mp3", FX, _song("<FX><Rotation> keep")),
        "conflict": drive.add("Intro.mp3", FX, _song("<FX><Walk On>")),
        "event": drive.add("Event.flac", FX, tagged(AudioFormat.FLAC, comment="note")),
        "none": drive.add("Award.mp3", FX, _song()),
        "bad": drive.add("Broken.ogg", FX, b"OggS" + b"\x00" * 64),
        "trashing": drive.add("Gone.mp3", FX, _song()),
        "main": drive.add("Song.mp3", OTHER, _song()),
    }
    api = FakeApi(
        [
            _row("_WalkOn/Applause.mp3", ids["blank"]),
            _row("_WalkOn/Routine.mp3", ids["hand"]),
            _row("_BowMusic/Bow.mp3", ids["fx"]),
            _row("_RotationMusic/Spin.mp3", ids["done"]),
            _row("_MCIntro/Intro.mp3", ids["conflict"]),
            _row("EventSpecific/Swingtime/_BowMusic/Event.flac", ids["event"]),
            _row("_AwardMusic/Award.mp3", ids["none"]),
            _row("_WalkOn/Broken.ogg", ids["bad"]),
            _row("_WalkOn/Gone.mp3", ids["trashing"], trash_requested=True),
            _row("_WalkOn/Song.mp3", ids["main"], root=OTHER),
        ]
    )
    return drive, api, ids


_EXPECTED = {
    "_WalkOn/Applause.mp3": ("", "<FX><Walk On>"),
    "_WalkOn/Routine.mp3": (
        "<Competitor Routine>",
        "<FX><Walk On><Competitor Routine>",
    ),
    "_BowMusic/Bow.mp3": ("<FX><Judges>", "<FX><Bow><Judges>"),
    "EventSpecific/Swingtime/_BowMusic/Event.flac": ("note", "<FX><Bow> note"),
}


def test_the_dry_run_lists_every_change_and_changes_nothing() -> None:
    drive, api, _ = _fx_library()

    report = fx_roles.run(drive, api, FX, STATE, write=False, run_id="d-1")

    assert {c.path: (c.before, c.after) for c in report.written} == _EXPECTED
    assert report.tagged == ["_RotationMusic/Spin.mp3"]
    assert [(c.path, c.comment, c.role) for c in report.conflicts] == [
        ("_MCIntro/Intro.mp3", "<FX><Walk On>", "MC Intro")
    ]
    assert [p for p, _ in report.unreadable] == ["_WalkOn/Broken.ogg"]
    assert report.failed == []
    assert report.record == ""
    assert drive.writes() == []


def test_the_dry_run_report_names_each_change_and_conflict() -> None:
    drive, api, _ = _fx_library()

    lines = fx_roles.render(
        fx_roles.run(drive, api, FX, STATE, write=False), write=False
    )

    text = "\n".join(lines)
    assert "Would write 4 comments:" in text
    assert (
        "_WalkOn/Routine.mp3: '<Competitor Routine>' -> '<FX><Walk On><Competitor Routine>'"
        in text
    )
    assert (
        "_MCIntro/Intro.mp3: comment '<FX><Walk On>', folder's role 'MC Intro'" in text
    )
    assert "Not readable by the tagger: 1" in text


def test_write_writes_only_the_comments_and_records_a_run_rollback_undoes() -> None:
    drive, api, ids = _fx_library()
    originals = {key: drive.items[i].data for key, i in ids.items()}

    report = fx_roles.run(drive, api, FX, STATE, write=True, run_id="w-1")

    assert {c.path: (c.before, c.after) for c in report.written} == _EXPECTED
    assert report.failed == []
    assert report.record == "fx-roles-w-1.json"
    for key, path in [
        ("blank", "_WalkOn/Applause.mp3"),
        ("hand", "_WalkOn/Routine.mp3"),
        ("fx", "_BowMusic/Bow.mp3"),
        ("event", "EventSpecific/Swingtime/_BowMusic/Event.flac"),
    ]:
        data = drive.items[ids[key]].data
        assert read_comment(data) == _EXPECTED[path][1]
        assert read_tag_row(data) == read_tag_row(originals[key])
    for key in ("done", "conflict", "none", "bad", "trashing", "main"):
        assert drive.items[ids[key]].data == originals[key]
    for item in drive.items.values():
        assert item.trashed is False
    assert {i.name for i in drive.items.values()} >= {
        "Applause.mp3",
        "Routine.mp3",
        "Bow.mp3",
        "Event.flac",
    }
    assert not any(
        method in ("move_file", "rename_file", "trash_file", "delete_file")
        for method, _ in drive.calls
    )

    rb.rollback(drive, STATE, "w-1", apply=True, forget=lambda _ids: None)

    for key in ("blank", "hand", "fx", "event"):
        assert drive.items[ids[key]].data == originals[key]


def test_a_second_run_changes_nothing_new() -> None:
    drive, api, _ = _fx_library()
    fx_roles.run(drive, api, FX, STATE, write=True, run_id="w-1")

    report = fx_roles.run(drive, api, FX, STATE, write=False, run_id="d-2")

    assert report.written == []
    assert len(report.tagged) == 5


def test_the_command_needs_the_fx_root(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    monkeypatch.delenv("FX_LIBRARY_ROOT_FOLDER_ID", raising=False)
    assert fx_roles.main([]) == 2
    assert "FX_LIBRARY_ROOT_FOLDER_ID" in capsys.readouterr().err
