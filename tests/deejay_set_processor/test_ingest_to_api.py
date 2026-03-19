from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import deejay_set_processor.ingest_to_api as ingest


def test_parse_track_row_converts_mmss_length_to_seconds():
    col_index = {"title": 0, "artist": 1, "length": 2}
    row = ["Song", "Artist", "02:30"]
    parsed = ingest.parse_track_row(row, col_index, play_order=1)
    assert parsed is not None
    assert parsed["length_secs"] == 150


def test_parse_track_row_handles_missing_columns_as_null():
    col_index = {"title": 0, "artist": 1}
    row = ["Song", "Artist"]
    parsed = ingest.parse_track_row(row, col_index, play_order=1)
    assert parsed is not None
    assert parsed["genre"] is None
    assert parsed["bpm"] is None
    assert parsed["release_year"] is None


def test_parse_track_row_skips_rows_with_empty_title_or_artist():
    col_index = {"title": 0, "artist": 1}
    assert ingest.parse_track_row(["", "Artist"], col_index, play_order=1) is None
    assert ingest.parse_track_row(["Song", ""], col_index, play_order=1) is None


def test_ingest_new_sets_to_api_posts_each_set_with_correct_payload_shape(monkeypatch):
    g = SimpleNamespace()
    g.sheets = SimpleNamespace(
        get_metadata=MagicMock(
            return_value={"sheets": [{"properties": {"title": "Sheet1"}}]}
        ),
        read_values=MagicMock(
            return_value=[
                ["Title", "Artist", "Length"],
                ["Song A", "Artist A", "01:00"],
                ["Song B", "Artist B", "02:00"],
            ]
        ),
    )

    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://example.test")
    monkeypatch.setenv("KAIANO_API_OWNER_ID", "owner-123")

    client = SimpleNamespace(post=MagicMock(return_value={"ok": True}))

    with patch.object(ingest, "KaianoApiClient", return_value=client) as mock_client:
        summary = ingest.ingest_new_sets_to_api(
            g,
            new_spreadsheet_ids=["ssid-1", "ssid-2"],
            set_metadata=[
                {
                    "spreadsheet_id": "ssid-1",
                    "date": "2024-01-01",
                    "venue": "Venue",
                    "label": "2024-01-01_Set",
                },
                {
                    "spreadsheet_id": "ssid-2",
                    "date": "2024-01-02",
                    "venue": "Venue2",
                    "label": "2024-01-02_Set",
                },
            ],
        )

    mock_client.assert_called_once()
    assert client.post.call_count == 2

    path, payload = client.post.call_args_list[0].args
    assert path == "/v1/ingest"
    assert payload["set_date"] == "2024-01-01"
    assert payload["venue"] == "Venue"
    assert payload["source_file"] == "2024-01-01_Set"
    assert payload["owner_id"] == "owner-123"
    assert isinstance(payload["tracks"], list)
    assert payload["tracks"][0]["play_order"] == 1

    assert summary.sets_sent == 2
    assert summary.sets_failed == 0
    assert summary.total_tracks == 4
    assert summary.failures == []


def test_ingest_new_sets_to_api_failure_on_one_set_does_not_abort(monkeypatch):
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

    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://example.test")

    class FakeError(ingest.KaianoApiError):
        pass

    def post_side_effect(path, payload):
        if payload["source_file"] == "bad":
            raise FakeError("nope")
        return {"ok": True}

    client = SimpleNamespace(post=MagicMock(side_effect=post_side_effect))

    with patch.object(ingest, "KaianoApiClient", return_value=client):
        summary = ingest.ingest_new_sets_to_api(
            g,
            new_spreadsheet_ids=["ssid-bad", "ssid-ok"],
            set_metadata=[
                {
                    "spreadsheet_id": "ssid-bad",
                    "date": "2024-01-01",
                    "venue": "V",
                    "label": "bad",
                },
                {
                    "spreadsheet_id": "ssid-ok",
                    "date": "2024-01-02",
                    "venue": "V2",
                    "label": "ok",
                },
            ],
        )

    assert client.post.call_count == 2
    assert summary.sets_sent == 1
    assert summary.sets_failed == 1
    assert summary.failures and summary.failures[0]["label"] == "bad"
    assert summary.sets_sent + summary.sets_failed == 2


def test_ingest_new_sets_to_api_skips_empty_sheet(monkeypatch):
    g = SimpleNamespace()
    g.sheets = SimpleNamespace(
        get_metadata=MagicMock(
            return_value={"sheets": [{"properties": {"title": "Sheet1"}}]}
        ),
        read_values=MagicMock(return_value=[["Title", "Artist"]]),
    )

    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://example.test")
    client = SimpleNamespace(post=MagicMock())

    with patch.object(ingest, "KaianoApiClient", return_value=client):
        summary = ingest.ingest_new_sets_to_api(
            g,
            new_spreadsheet_ids=["ssid-1"],
            set_metadata=[
                {
                    "spreadsheet_id": "ssid-1",
                    "date": "2024-01-01",
                    "venue": "V",
                    "label": "label",
                }
            ],
        )

    client.post.assert_not_called()
    assert summary.sets_sent == 0
    assert summary.sets_failed == 0
    assert summary.failures == []
