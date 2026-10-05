"""The repair pass's pure parts: names, dates, and which sheets count."""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from deejay_cog import config, repair


@pytest.mark.parametrize(
    ("sheet", "expected"),
    [
        ("2026-10-02 TC Rebels.csv", "2026-10-02 TC Rebels"),
        ("2026-10-02 TC Rebels.CSV", "2026-10-02 TC Rebels"),
        ("2026-10-02 TC Rebels", "2026-10-02 TC Rebels"),
        ("2026-03-01 St. Paul Swing", "2026-03-01 St. Paul Swing"),
    ],
)
def test_set_name_drops_only_a_trailing_csv(sheet, expected) -> None:
    assert repair.set_name(sheet) == expected


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("2026-10-02 TC Rebels", (dt.date(2026, 10, 2), "TC Rebels")),
        ("2026-10-02_untitled", None),
        ("2026-13-40 Nowhere", None),
        ("2026-10-02 ", None),
        ("notes", None),
    ],
)
def test_parse_set_name(name, expected) -> None:
    assert repair._parse_set_name(name) == expected


def _file(id_, name, mime):
    return SimpleNamespace(id=id_, name=name, mime_type=mime)


def test_recent_set_sheets_reads_only_recent_dated_sheets_newest_first(
    monkeypatch,
) -> None:
    monkeypatch.setattr(config, "DJ_SETS_FOLDER_ID", "root")
    folders = {
        "root": [
            _file("y25", "2025", repair.FOLDER_MIME),
            _file("y26", "2026", repair.FOLDER_MIME),
            _file("sum", "Summary", repair.FOLDER_MIME),
        ],
        "y25": [_file("old", "2025-12-31 NYE.csv", repair.SHEET_MIME)],
        "y26": [
            _file("a", "2026-05-01 Early.csv", repair.SHEET_MIME),
            _file("b", "2026-09-01 Late.csv", repair.SHEET_MIME),
            _file("c", "2026-08-01 Raw.csv", "text/csv"),  # not a sheet
            _file("d", "2026-08-01_untitled", repair.SHEET_MIME),  # no venue
            _file("e", "2026-01-01 Too Old.csv", repair.SHEET_MIME),
        ],
    }
    drive = SimpleNamespace(list_files=lambda fid, **_k: folders[fid])

    sheets = repair.recent_set_sheets(SimpleNamespace(drive=drive), dt.date(2026, 4, 1))

    assert [s.name for s in sheets] == ["2026-09-01 Late", "2026-05-01 Early"]
    assert sheets[0].venue == "Late"
    assert sheets[0].sheet_id == "b"


def test_nothing_recent_means_no_api_or_spotify_calls(monkeypatch) -> None:
    monkeypatch.setattr(repair, "recent_set_sheets", lambda *_a: [])
    client = MagicMock()
    monkeypatch.setattr(repair, "api_client", lambda: client)
    sp = MagicMock()

    result = repair.repair_recent_sets(SimpleNamespace(), sp=sp)

    assert result == repair.RepairResult()
    client.list_sets.assert_not_called()
    sp.assert_not_called()


def test_skip_leaves_this_runs_sets_alone(monkeypatch) -> None:
    sheet = repair.SetSheet("s1", "2026-10-04 Today", dt.date(2026, 10, 4), "Today")
    monkeypatch.setattr(repair, "recent_set_sheets", lambda *_a: [sheet])
    client = MagicMock()
    monkeypatch.setattr(repair, "api_client", lambda: client)

    result = repair.repair_recent_sets(
        SimpleNamespace(), sp=None, skip={"2026-10-04 Today"}
    )

    assert result == repair.RepairResult()
    client.list_sets.assert_not_called()


def test_today_is_the_sets_timezone(monkeypatch) -> None:
    monkeypatch.setattr(config, "TIMEZONE", "Pacific/Kiritimati")  # UTC+14
    kiritimati = repair.today()
    monkeypatch.setattr(config, "TIMEZONE", "Pacific/Pago_Pago")  # UTC-11
    assert (kiritimati - repair.today()).days == 1
