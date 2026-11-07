"""reconcile_admin: the working-session commands over the library reconcile's
records, approvals and cursor."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from music_fakes import DriveItem, FakeDrive

from deejay_cog import library_reconcile as rec
from deejay_cog import reconcile_admin as admin
from deejay_cog.music_safety import STATE_FOLDER_VAR

STATE = "state"


@pytest.fixture
def drive(monkeypatch: pytest.MonkeyPatch) -> FakeDrive:
    drive = FakeDrive()
    drive.items[STATE] = DriveItem(STATE, "state", ["elsewhere"], mime_type="folder")
    google = MagicMock()
    google.from_env.return_value = SimpleNamespace(drive=drive)
    monkeypatch.setattr(admin, "GoogleAPI", google)
    monkeypatch.setenv(STATE_FOLDER_VAR, STATE)
    return drive


def _put(drive: FakeDrive, name: str, data: dict) -> None:
    drive.add(name, STATE, json.dumps(data).encode(), mime_type="application/json")


def _get(drive: FakeDrive, name: str) -> dict | None:
    found = [i for i in drive.in_folder(STATE) if i.name == name]
    return json.loads(found[0].data) if found else None


def _diff(path: str, field: str, file: str, catalog: str, file_id: str = "d1") -> dict:
    return {
        "path": path,
        "drive_file_id": file_id,
        "field": field,
        "file": file,
        "catalog": catalog,
    }


def _record(saved_at: str, **differences: list[dict]) -> dict:
    return {"saved_at": saved_at, "differences": differences}


# -- review -----------------------------------------------------------------


def test_only_reconcile_records_of_differences_are_read() -> None:
    assert admin.is_record("library-reconcile-r1.dry-run.json")
    assert admin.is_record("library-reconcile-r1.differences.json")
    assert not admin.is_record("library-reconcile-r1.json")  # a write's record
    assert not admin.is_record("library-reconcile-cursor.json")
    assert not admin.is_record("library-backfill-r1.dry-run.json")


def test_the_latest_record_wins_for_each_file_and_field() -> None:
    old = _record(
        "2026-11-07T10:00", genre_remapped=[_diff("a.mp3", "genre", "R&B", "Soul")]
    )
    new = _record(
        "2026-11-07T12:00", genre_remapped=[_diff("a.mp3", "genre", "R&B", "R&B/Soul")]
    )
    [item] = admin.latest_items([new, old])
    assert item.catalog_value == "R&B/Soul"


def test_records_before_since_are_left_out() -> None:
    old = _record("2026-11-07T10:00", fill_up=[_diff("a.mp3", "bpm", "100", "")])
    assert admin.latest_items([old], since="2026-11-07T11:00") == []


def test_the_review_marks_each_kind_and_other_file_approved_or_held() -> None:
    items = admin.latest_items(
        [
            _record(
                "2026-11-07T12:00",
                bpm_rounding=[_diff("x/a.mp3", "bpm", "99.99", "100")],
                case_punctuation=[_diff("x/b.mp3", "artist", "RAC", "Rac", "d2")],
                other=[
                    _diff("x/c.mp3", "artist", "A", "A feat. B", "d3"),
                    _diff("x/d.mp3", "title", "Bodies", "Bodie", "d4"),
                ],
            )
        ]
    )
    approvals, _ = rec.parse_approvals(
        {"kinds": ["bpm_rounding"], "files": {"d3": ["artist"]}}
    )

    text = admin.review_text(items, approvals)

    assert "## bpm_rounding (1, approved)" in text
    assert "## case_punctuation (1, held)" in text
    assert "c.mp3 [d3] artist: 'A' -> 'A feat. B' [approved]" in text
    assert "d.mp3 [d4] title: 'Bodies' -> 'Bodie' [held]" in text


def test_review_reads_the_state_folders_records(drive, capsys, tmp_path) -> None:
    _put(
        drive,
        "library-reconcile-r1.dry-run.json",
        _record(
            "2026-11-07T12:00", year_later=[_diff("a.mp3", "year", "2003", "2007")]
        ),
    )
    _put(drive, "library-reconcile-cursor.json", {"position": 3})
    out = tmp_path / "review.md"

    assert admin.main(["review", "--out", str(out)]) == 0

    assert "## year_later (1, held)" in capsys.readouterr().out
    assert "a.mp3 [d1] year: '2003' -> '2007'" in out.read_text()


# -- approvals --------------------------------------------------------------


def test_approve_adds_kinds_and_files_and_takes_kinds_back() -> None:
    out = admin.changed_approvals(
        {"kinds": ["genre_filled", "fill_up"], "files": {"d1": ["artist"]}},
        kinds=["bpm_rounding", "genre_filled"],
        revoke=["fill_up"],
        files=["d1:title,artist", "d2:catalog_id"],
    )
    assert out == {
        "kinds": ["genre_filled", "bpm_rounding"],
        "files": {"d1": ["artist", "title"], "d2": ["catalog_id"]},
    }


@pytest.mark.parametrize("bad", ["d1", "d1:", ":artist", "d1:colour"])
def test_a_badly_written_file_approval_is_refused(bad: str) -> None:
    with pytest.raises(admin.AdminError):
        admin.changed_approvals({}, files=[bad])


def test_other_cannot_be_approved_as_a_whole_kind() -> None:
    with pytest.raises(admin.AdminError, match="file by file"):
        admin.changed_approvals({}, kinds=["other"])


def test_approve_saves_only_with_apply(drive, capsys) -> None:
    assert admin.main(["approve", "--kind", "bpm_rounding"]) == 0
    assert _get(drive, rec.APPROVALS_NAME) is None
    assert "Nothing was saved" in capsys.readouterr().out

    assert (
        admin.main(
            ["approve", "--kind", "bpm_rounding", "--file", "d1:artist", "--apply"]
        )
        == 0
    )
    assert _get(drive, rec.APPROVALS_NAME) == {
        "kinds": ["bpm_rounding"],
        "files": {"d1": ["artist"]},
    }


def test_a_refused_approval_changes_nothing(drive, capsys) -> None:
    _put(drive, rec.APPROVALS_NAME, {"kinds": ["fill_up"], "files": {}})
    assert admin.main(["approve", "--file", "d1:colour", "--apply"]) == 2
    assert "nothing changed" in capsys.readouterr().err
    assert _get(drive, rec.APPROVALS_NAME) == {"kinds": ["fill_up"], "files": {}}


# -- cursor -----------------------------------------------------------------


def test_the_cursor_moves_only_with_apply_and_keeps_its_other_fields(drive) -> None:
    _put(drive, rec.CURSOR_NAME, {"flow": rec.FLOW_NAME, "position": 272, "passes": 0})

    assert admin.main(["cursor", "--position", "0"]) == 0
    assert _get(drive, rec.CURSOR_NAME)["position"] == 272

    assert admin.main(["cursor", "--position", "0", "--apply"]) == 0
    cursor = _get(drive, rec.CURSOR_NAME)
    assert (cursor["position"], cursor["passes"], cursor["flow"]) == (
        0,
        0,
        rec.FLOW_NAME,
    )


def test_there_is_no_cursor_to_move_before_a_first_run(drive, capsys) -> None:
    assert admin.main(["cursor", "--position", "0", "--apply"]) == 2
    assert "nothing to move" in capsys.readouterr().err


def test_without_a_state_folder_nothing_is_done(monkeypatch, capsys) -> None:
    monkeypatch.delenv(STATE_FOLDER_VAR, raising=False)
    assert admin.main(["approvals"]) == 2
    assert STATE_FOLDER_VAR in capsys.readouterr().err


# -- run --------------------------------------------------------------------


def test_run_sets_the_mode_and_chunk_it_is_given(monkeypatch, capsys) -> None:
    seen = {}

    def fake_reconcile():
        seen["mode"] = rec.os.environ.get(rec.MODE_VAR)
        seen["chunk"] = rec.os.environ.get(rec.CHUNK_SIZE_VAR)
        summary = rec.ReconcileSummary(rec.FlowMode.DRY_RUN)
        summary.record_name = "library-reconcile-local.dry-run.json"
        summary.position = summary.compared = 5
        return summary

    monkeypatch.setattr(admin, "library_reconcile", fake_reconcile)
    monkeypatch.delenv(rec.MODE_VAR, raising=False)
    monkeypatch.delenv(rec.CHUNK_SIZE_VAR, raising=False)

    assert admin.main(["run", "--mode", "dry-run", "--chunk", "5"]) == 0

    assert seen == {"mode": "dry-run", "chunk": "5"}
    assert "files 1-5, 5 compared" in capsys.readouterr().out
