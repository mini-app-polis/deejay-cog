from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import deejay_cog.ingest_to_api as ingest


def test_read_tracks_from_sheet_handles_missing_columns_gracefully():
    g = SimpleNamespace()
    g.sheets = SimpleNamespace(
        get_metadata=MagicMock(
            return_value={"sheets": [{"properties": {"title": "Sheet1"}}]}
        ),
        read_values=MagicMock(
            return_value=[
                ["Title", "Artist"],
                ["Song", "Artist"],
            ]
        ),
    )

    tracks = ingest.read_tracks_from_sheet(g, "ssid")
    assert tracks and tracks[0]["title"] == "Song"
    assert tracks[0]["genre"] == ""
    assert tracks[0]["length"] == ""


def test_build_ingest_payload_converts_mmss_length_and_skips_empty_title_or_artist():
    raw_tracks = [
        {"play_order": 1, "title": "Song", "artist": "Artist", "length": "02:30"},
        {"play_order": 2, "title": "", "artist": "Artist", "length": "01:00"},
        {"play_order": 3, "title": "Song2", "artist": "", "length": "01:00"},
    ]
    payload = ingest.build_ingest_payload(
        set_date="2024-01-01",
        venue="Venue",
        source_file="label",
        tracks=raw_tracks,
    ).model_dump(mode="json")
    assert payload["set_date"] == "2024-01-01"
    assert payload["venue"] == "Venue"
    assert payload["source_file"] == "label"
    assert len(payload["tracks"]) == 1
    assert payload["tracks"][0]["length_secs"] == 150


def _sheet(meta=None, values=None, *, error=None):
    get_metadata = MagicMock(
        return_value=meta
        if meta is not None
        else {"sheets": [{"properties": {"title": "Sheet1"}}]}
    )
    if error is not None:
        get_metadata.side_effect = error
    return SimpleNamespace(
        sheets=SimpleNamespace(
            get_metadata=get_metadata,
            read_values=MagicMock(return_value=values),
        )
    )


@pytest.mark.parametrize(
    "g",
    [
        _sheet(error=RuntimeError("403")),
        _sheet(meta={"sheets": []}),
        _sheet(meta={"sheets": [{"properties": {}}]}),
        _sheet(values=[]),
        _sheet(values=[["Title", "Artist"]]),
    ],
    ids=["unreadable", "no tabs", "no tab title", "empty", "header only"],
)
def test_read_tracks_from_sheet_returns_nothing_rather_than_raising(g):
    assert ingest.read_tracks_from_sheet(g, "ssid") == []


def test_read_tracks_from_sheet_numbers_rows_and_pads_short_ones():
    g = _sheet(
        values=[
            [" Title ", "ARTIST", None, "Play_Time"],
            ["Song", "Artist", "x", " 21:04 "],
            ["Short"],
        ]
    )

    first, second = ingest.read_tracks_from_sheet(g, "ssid")

    assert first["play_order"] == 1
    assert first["play_time"] == "21:04"
    assert second == {**second, "play_order": 2, "title": "Short", "artist": ""}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("4:02", 242),
        ("120:00", 7200),
        ("4:60", None),
        ("4m02", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_length_secs(value, expected):
    assert ingest._parse_length_secs(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("21:04", "21:04"),
        ("9:04:30", "9:04:30"),
        (" ", None),
        ("late", None),
        (None, None),
    ],
)
def test_parse_play_time(value, expected):
    assert ingest._parse_play_time(value) == expected


def test_build_ingest_tracks_drops_unparseable_numbers_and_keeps_the_row():
    [track] = ingest.build_ingest_tracks(
        [
            {
                "play_order": "n/a",
                "title": "Song",
                "artist": "Artist",
                "bpm": "fast",
                "year": "'90s",
                "play_time": "21:04",
                "remix": "  ",
            }
        ]
    )

    assert track.bpm is None
    assert track.release_year is None
    assert track.play_order == 1
    assert track.remix is None
    assert str(track.play_time) == "21:04:00"
