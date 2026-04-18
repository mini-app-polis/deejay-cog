from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from prefect.testing.utilities import prefect_test_harness

import deejay_cog.ingest_live_history as live


def test_build_live_plays_payload_parses_entries_correctly(monkeypatch) -> None:
    monkeypatch.setattr(live.config, "TIMEZONE", "America/Chicago")
    entries = [
        SimpleNamespace(dt="2024-06-01 14:30", title="Song A", artist="Artist A"),
        SimpleNamespace(dt="2024-06-01 15:00", title="Song B", artist="Artist B"),
    ]
    payload = live.build_live_plays_payload(entries)
    assert "plays" in payload
    assert len(payload["plays"]) == 2
    assert payload["plays"][0]["title"] == "Song A"
    assert payload["plays"][0]["artist"] == "Artist A"
    assert payload["plays"][1]["title"] == "Song B"
    pa0 = payload["plays"][0]["played_at"]
    assert pa0.startswith("2024-06-01T")
    assert "14:30" in pa0


def test_build_live_plays_payload_skips_unparseable_dt(monkeypatch) -> None:
    monkeypatch.setattr(live.config, "TIMEZONE", "America/Chicago")
    entries = [
        SimpleNamespace(dt="not-a-datetime", title="Song", artist="Artist"),
        SimpleNamespace(dt="2024-06-01 12:00", title="Good", artist="Artist"),
    ]
    payload = live.build_live_plays_payload(entries)
    assert len(payload["plays"]) == 1
    assert payload["plays"][0]["title"] == "Good"


def test_build_live_plays_payload_skips_missing_title_or_artist(monkeypatch) -> None:
    monkeypatch.setattr(live.config, "TIMEZONE", "America/Chicago")
    entries = [
        SimpleNamespace(dt="2024-06-01 12:00", title="", artist="Artist"),
        SimpleNamespace(dt="2024-06-01 12:01", title="Song", artist=""),
        SimpleNamespace(dt="2024-06-01 12:02", title="Ok", artist="OkArtist"),
    ]
    payload = live.build_live_plays_payload(entries)
    assert len(payload["plays"]) == 1
    assert payload["plays"][0]["title"] == "Ok"


def test_ingest_live_history_skips_when_no_api_url(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "")
    mock_drive = SimpleNamespace(
        get_all_m3u_files=MagicMock(),
        download_m3u_file_data=MagicMock(),
    )
    fake_g = SimpleNamespace(drive=mock_drive)
    client = SimpleNamespace(post=MagicMock())

    with (
        patch.object(live.GoogleAPI, "from_env", return_value=fake_g),
        patch.object(live, "KaianoApiClient", return_value=client) as mock_client,
        patch.object(live, "post_run_finding") as mock_post,
    ):
        summary = live.ingest_live_history.fn()

    mock_post.assert_called_once()
    mock_client.assert_not_called()
    mock_drive.get_all_m3u_files.assert_not_called()
    client.post.assert_not_called()
    assert summary.plays_sent == 0
    assert summary.plays_failed == 0
    assert summary.files_processed == 0
    assert summary.files_failed == 0


def test_ingest_live_history_sends_plays_and_returns_summary(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://example.test")

    fake_entries = [
        SimpleNamespace(dt="2024-01-15 22:30", title="Track One", artist="Artist One"),
    ]
    parse_mock = MagicMock(return_value=fake_entries)
    m3u_instance = MagicMock()
    m3u_instance.parse = SimpleNamespace(parse_m3u_lines=parse_mock)

    fake_g = SimpleNamespace(
        drive=SimpleNamespace(
            get_all_m3u_files=MagicMock(
                return_value=[{"id": "m3u-1", "name": "2024-01-15.m3u"}]
            ),
            download_m3u_file_data=MagicMock(return_value=["#EXTM3U", "line"]),
        )
    )

    client = SimpleNamespace(post=MagicMock(return_value={"ok": True}))

    with (
        patch.object(live.GoogleAPI, "from_env", return_value=fake_g),
        patch.object(live, "KaianoApiClient", return_value=client) as mock_client_cls,
        patch.object(live, "M3UToolbox", return_value=m3u_instance),
        patch.object(live, "post_run_finding") as mock_post,
        prefect_test_harness(),
    ):
        summary = live.ingest_live_history.fn()

    mock_post.assert_called_once()
    mock_client_cls.assert_called_once_with(base_url="https://example.test")
    parse_mock.assert_called_once()
    client.post.assert_called_once()
    path, payload = client.post.call_args.args
    assert path == "/v1/live-plays"
    assert "owner_id" not in payload
    assert "plays" in payload
    assert len(payload["plays"]) == 1
    assert payload["plays"][0]["title"] == "Track One"
    assert payload["plays"][0]["artist"] == "Artist One"
    assert "played_at" in payload["plays"][0]

    assert summary.plays_sent == 1
    assert summary.plays_failed == 0
    assert summary.files_processed == 1
    assert summary.files_failed == 0


def test_ingest_live_history_sends_all_parsed_entries(monkeypatch) -> None:
    """Parser returns oldest-first; every parsed entry is posted."""
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://example.test")

    fake_entries = [
        SimpleNamespace(
            dt=f"2024-01-15 {10 + i:02d}:00", title=f"Track{i}", artist=f"Artist{i}"
        )
        for i in range(6)
    ]
    parse_mock = MagicMock(return_value=fake_entries)
    m3u_instance = MagicMock()
    m3u_instance.parse = SimpleNamespace(parse_m3u_lines=parse_mock)

    fake_g = SimpleNamespace(
        drive=SimpleNamespace(
            get_all_m3u_files=MagicMock(
                return_value=[{"id": "m3u-1", "name": "2024-01-15.m3u"}]
            ),
            download_m3u_file_data=MagicMock(return_value=["#EXTM3U", "line"]),
        )
    )

    client = SimpleNamespace(post=MagicMock(return_value={"ok": True}))

    with (
        patch.object(live.GoogleAPI, "from_env", return_value=fake_g),
        patch.object(live, "KaianoApiClient", return_value=client),
        patch.object(live, "M3UToolbox", return_value=m3u_instance),
        patch.object(live, "post_run_finding") as mock_post,
        prefect_test_harness(),
    ):
        summary = live.ingest_live_history.fn()

    mock_post.assert_called_once()
    _, payload = client.post.call_args.args
    assert "owner_id" not in payload
    assert len(payload["plays"]) == 6
    assert [p["title"] for p in payload["plays"]] == [f"Track{i}" for i in range(6)]
    assert summary.plays_sent == 6
