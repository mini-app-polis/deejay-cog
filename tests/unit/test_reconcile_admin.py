"""reconcile_admin: the working-session commands over the library reconcile's
records, approvals and cursor."""

from __future__ import annotations

import datetime as dt
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from mini_app_polis.api.contract import ReconcileApprovals, ReconcileApprovalsSet
from music_fakes import DriveItem, FakeDrive

from deejay_cog import library_reconcile as rec
from deejay_cog import reconcile_admin as admin
from deejay_cog.music_safety import STATE_FOLDER_VAR
from flag_fakes import FLAGS

STATE = "state"


class FakeApi:
    """The API's approvals, as the admin commands read and seed them."""

    def __init__(self) -> None:
        self.approvals = ReconcileApprovals()
        self.seeds: list[ReconcileApprovalsSet] = []

    def read_reconcile_approvals(self) -> ReconcileApprovals:
        return self.approvals

    def seed_reconcile_approvals(self, payload: ReconcileApprovalsSet):
        if self.approvals.updated_at is not None:
            raise RuntimeError("409 Approvals are already set")
        self.seeds.append(payload)
        self.approvals = ReconcileApprovals(
            kinds=payload.kinds,
            files=payload.files,
            updated_at=dt.datetime(2026, 11, 7, tzinfo=dt.UTC),
            updated_by="seed:deejay-cog",
        )
        return self.approvals


@pytest.fixture
def api(monkeypatch: pytest.MonkeyPatch) -> FakeApi:
    fake = FakeApi()
    monkeypatch.setattr(admin, "api_client", lambda **_: fake)
    return fake


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


def test_the_review_marks_what_is_approved_for_the_file() -> None:
    items = admin.latest_items(
        [
            _record(
                "2026-11-10T12:00",
                year_later=[_diff("x/a.mp3", "year", "2003", "2007")],
                other=[_diff("x/c.mp3", "artist", "A & B", "A", "d3")],
            )
        ]
    )
    approvals = rec.Approvals(
        file_kinds=frozenset({"year_later"}), file_files={"d3": frozenset({"artist"})}
    )

    text = admin.review_text(items, approvals)

    assert "## year_later (1, approved for the file)" in text
    assert "c.mp3 [d3] artist: 'A & B' -> 'A' [approved for the file]" in text


def test_review_reads_the_state_folders_records(drive, api, capsys, tmp_path) -> None:
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


def test_approvals_prints_what_the_api_holds(api, capsys) -> None:
    assert admin.main(["approvals"]) == 0
    assert "No approvals are set yet" in capsys.readouterr().out

    api.approvals = ReconcileApprovals(
        kinds=["fill_up"], updated_at=dt.datetime(2026, 11, 7, tzinfo=dt.UTC)
    )
    assert admin.main(["approvals"]) == 0
    assert '"fill_up"' in capsys.readouterr().out


def test_seed_sends_the_old_file_only_with_apply(drive, api, capsys) -> None:
    _put(
        drive,
        rec.APPROVALS_NAME,
        {"kinds": ["fill_up", "bpm_rounding"], "files": {"d1": ["title"]}},
    )

    assert admin.main(["seed"]) == 0
    assert api.seeds == []
    assert "Nothing was sent" in capsys.readouterr().out

    assert admin.main(["seed", "--apply"]) == 0
    [seed] = api.seeds
    assert seed.kinds == ["fill_up", "bpm_rounding"] and seed.files == {"d1": ["title"]}
    assert "2 kinds, 1 files" in capsys.readouterr().out


def test_seed_sends_nothing_once_the_api_holds_approvals(drive, api, capsys) -> None:
    _put(drive, rec.APPROVALS_NAME, {"kinds": ["fill_up"], "files": {}})
    api.approvals = ReconcileApprovals(
        kinds=["bpm_rounding"], updated_at=dt.datetime(2026, 11, 7, tzinfo=dt.UTC)
    )

    assert admin.main(["seed", "--apply"]) == 2

    assert "already holds approvals" in capsys.readouterr().err
    assert api.seeds == []  # nothing was sent: no refused request
    assert api.approvals.kinds == ["bpm_rounding"]


@pytest.mark.parametrize(
    "data",
    [{"kinds": ["other"]}, {"kinds": ["everything"]}, {"files": {"d1": ["colour"]}}],
)
def test_an_old_file_that_does_not_parse_is_not_seeded(
    drive, api, capsys, data
) -> None:
    _put(drive, rec.APPROVALS_NAME, data)
    assert admin.main(["seed", "--apply"]) == 2
    assert api.seeds == []


def test_with_no_old_file_there_is_nothing_to_seed(drive, api, capsys) -> None:
    assert admin.main(["seed", "--apply"]) == 2
    assert "nothing to seed" in capsys.readouterr().err


# -- cursor -----------------------------------------------------------------


def test_the_cursor_moves_only_with_apply_and_keeps_its_other_fields(drive) -> None:
    _put(
        drive,
        rec.CURSOR_NAME,
        {"flow": rec.FLOW_NAME, "position": 272, "after": "row-id", "passes": 0},
    )

    assert admin.main(["cursor", "--position", "0"]) == 0
    assert _get(drive, rec.CURSOR_NAME)["position"] == 272

    assert admin.main(["cursor", "--position", "0", "--apply"]) == 0
    cursor = _get(drive, rec.CURSOR_NAME)
    assert (cursor["position"], cursor["passes"], cursor["flow"]) == (
        0,
        0,
        rec.FLOW_NAME,
    )
    # The file it last compared goes, so the next run starts at the position.
    assert "after" not in cursor


def test_there_is_no_cursor_to_move_before_a_first_run(drive, capsys) -> None:
    assert admin.main(["cursor", "--position", "0", "--apply"]) == 2
    assert "nothing to move" in capsys.readouterr().err


def test_without_a_state_folder_nothing_is_done(monkeypatch, capsys) -> None:
    monkeypatch.delenv(STATE_FOLDER_VAR, raising=False)
    assert admin.main(["cursor"]) == 2
    assert STATE_FOLDER_VAR in capsys.readouterr().err


# -- run --------------------------------------------------------------------


def test_run_with_the_mode_off_runs_nothing_and_says_so(monkeypatch, capsys) -> None:
    called = MagicMock()
    monkeypatch.setattr(admin, "library_reconcile", called)
    monkeypatch.setitem(FLAGS, rec.MODE_FLAG, "off")

    assert admin.main(["run", "--chunk", "5"]) == 2

    called.assert_not_called()
    assert "--mode" in capsys.readouterr().err


def test_run_sets_the_mode_and_chunk_it_is_given(monkeypatch, capsys) -> None:
    seen = {}

    def fake_reconcile(*, mode=None):
        seen["mode"] = mode.value
        seen["chunk"] = rec.os.environ.get(rec.CHUNK_SIZE_VAR)
        summary = rec.ReconcileSummary(rec.FlowMode.DRY_RUN)
        summary.record_name = "library-reconcile-local.dry-run.json"
        summary.position = summary.compared = 5
        return summary

    monkeypatch.setattr(admin, "library_reconcile", fake_reconcile)
    monkeypatch.delitem(FLAGS, rec.MODE_FLAG, raising=False)
    monkeypatch.delenv(rec.CHUNK_SIZE_VAR, raising=False)

    assert admin.main(["run", "--mode", "dry-run", "--chunk", "5"]) == 0

    assert seen == {"mode": "dry-run", "chunk": "5"}
    assert "files 1-5, 5 compared" in capsys.readouterr().out


def test_run_without_a_mode_uses_the_flags(monkeypatch, capsys) -> None:
    seen = {}

    def fake_reconcile(*, mode=None):
        seen["mode"] = mode
        summary = rec.ReconcileSummary(mode)
        summary.record_name = "library-reconcile-local.json"
        return summary

    monkeypatch.setattr(admin, "library_reconcile", fake_reconcile)
    monkeypatch.setitem(FLAGS, rec.MODE_FLAG, "write")

    assert admin.main(["run"]) == 0

    assert seen == {"mode": rec.FlowMode.WRITE}


def test_a_mode_given_does_not_change_the_flag(monkeypatch, capsys) -> None:
    def fake_reconcile(*, mode=None):
        return rec.ReconcileSummary(mode)

    monkeypatch.setattr(admin, "library_reconcile", fake_reconcile)
    monkeypatch.setitem(FLAGS, rec.MODE_FLAG, "off")

    admin.main(["run", "--mode", "dry-run"])

    assert FLAGS[rec.MODE_FLAG] == "off"
