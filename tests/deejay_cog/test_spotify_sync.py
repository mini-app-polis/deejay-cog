from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from deejay_cog import spotify_sync as ss


def test_normalize_playlist_item_full() -> None:
    item = {
        "id": "pid",
        "name": "Mix",
        "uri": "spotify:playlist:pid",
        "type": "playlist",
        "public": True,
        "collaborative": False,
        "snapshot_id": "snap",
        "external_urls": {"spotify": "https://open.spotify.com/playlist/pid"},
        "tracks": {"total": 10},
        "owner": {"id": "oid", "display_name": "Owner"},
    }
    out = ss._normalize_playlist_item(item)
    assert out["id"] == "pid"
    assert out["name"] == "Mix"
    assert out["url"] == "https://open.spotify.com/playlist/pid"
    assert out["tracks_total"] == 10
    assert out["owner"]["id"] == "oid"


def test_normalize_playlist_item_minimal() -> None:
    out = ss._normalize_playlist_item({})
    assert out["id"] == ""
    assert out["name"] == ""
    assert out["url"] == ""
    assert out["owner"]["id"] == ""


def test_fetch_all_playlists_direct_helper() -> None:
    sp = SimpleNamespace(
        get_all_playlists=MagicMock(return_value=[{"id": "1", "name": "A"}]),
    )
    out = ss.fetch_all_playlists(sp)
    assert len(out) == 1
    sp.get_all_playlists.assert_called_once()


def test_fetch_all_playlists_spotipy_pagination() -> None:
    page1 = {
        "items": [{"id": "1"}],
        "next": "x",
    }
    page2 = {
        "items": [{"id": "2"}],
        "next": None,
    }
    client = SimpleNamespace(
        current_user_playlists=MagicMock(side_effect=[page1, page2]),
    )
    sp = SimpleNamespace(client=client)
    out = ss.fetch_all_playlists(sp)
    assert [p["id"] for p in out] == ["1", "2"]
    assert client.current_user_playlists.call_count == 2


def test_fetch_all_playlists_no_client() -> None:
    sp = SimpleNamespace()
    assert ss.fetch_all_playlists(sp) == []


def test_probe_still_tolerates_a_missing_method() -> None:
    """Narrowing to AttributeError must not break method probing."""
    client = SimpleNamespace(
        current_user_playlists=MagicMock(
            return_value={"items": [{"id": "1"}], "next": None}
        ),
    )
    sp = SimpleNamespace(client=client)
    assert [p["id"] for p in ss.fetch_all_playlists(sp)] == ["1"]
    client.current_user_playlists.assert_called_once()


def test_fetch_all_playlists_propagates_errors_from_existing_methods() -> None:
    sp = SimpleNamespace(
        get_all_playlists=MagicMock(side_effect=RuntimeError("429 rate limited")),
    )
    with pytest.raises(RuntimeError, match="429"):
        ss.fetch_all_playlists(sp)


def test_push_playlists_to_api_skips_when_kaiano_base_url_missing(
    monkeypatch, caplog
) -> None:
    import logging

    monkeypatch.delenv("KAIANO_API_BASE_URL", raising=False)
    caplog.set_level(logging.WARNING)
    assert ss.push_playlists_to_api(object()) is None
    assert "KAIANO_API_BASE_URL" in caplog.text


def test_push_playlists_to_api_posts_expected_payload(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    raw = [
        {
            "id": "pid",
            "name": "Mix",
            "uri": "spotify:playlist:pid",
            "type": "playlist",
            "public": True,
            "collaborative": False,
            "snapshot_id": "snap",
            "external_urls": {"spotify": "https://open.spotify.com/playlist/pid"},
            "tracks": {"total": 10},
            "owner": {"id": "oid", "display_name": "Owner"},
        }
    ]
    mock_client = MagicMock()
    mock_client.post.return_value = {"data": {"upserted": 1, "unchanged": 2}}
    mock_cls = MagicMock(return_value=mock_client)
    mock_cls.from_env = MagicMock(return_value=mock_client)

    with (
        patch.object(ss, "fetch_all_playlists", return_value=raw),
        patch("deejay_cog.spotify_sync.api_client", mock_cls),
    ):
        out = ss.push_playlists_to_api(object())

    assert out == 1
    mock_client.post.assert_called_once()
    path, payload = mock_client.post.call_args[0]
    assert path == "/v1/spotify/playlists"
    assert len(payload["playlists"]) == 1
    pl = payload["playlists"][0]
    assert pl == {
        "id": "pid",
        "name": "Mix",
        "url": "https://open.spotify.com/playlist/pid",
        "uri": "spotify:playlist:pid",
        "type": "playlist",
        "public": True,
        "collaborative": False,
        "snapshot_id": "snap",
        "tracks_total": 10,
        "owner_id": "oid",
        "owner_name": "Owner",
    }


def test_push_playlists_to_api_defaults_public_collaborative_tracks_total(
    monkeypatch,
) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    raw = [
        {
            "id": "p1",
            "name": "A",
            "uri": "",
            "type": "playlist",
            "public": None,
            "collaborative": None,
            "snapshot_id": "",
            "tracks": {},
            "owner": {"id": "o1"},
        }
    ]
    mock_client = MagicMock()
    mock_client.post.return_value = {"data": {"upserted": 0, "unchanged": 1}}
    mock_cls = MagicMock(return_value=mock_client)
    mock_cls.from_env = MagicMock(return_value=mock_client)

    with (
        patch.object(ss, "fetch_all_playlists", return_value=raw),
        patch("deejay_cog.spotify_sync.api_client", mock_cls),
    ):
        out = ss.push_playlists_to_api(object())

    assert out == 0
    pl = mock_client.post.call_args[0][1]["playlists"][0]
    assert pl["public"] is True
    assert pl["collaborative"] is False
    assert pl["tracks_total"] == 0


def test_push_playlists_to_api_raises_on_kaiano_api_error(monkeypatch, caplog) -> None:
    import logging

    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    mock_client = MagicMock()
    mock_client.post.side_effect = ss.KaianoApiError(
        502, "upstream failed", "/v1/spotify/playlists"
    )
    mock_cls = MagicMock(return_value=mock_client)
    mock_cls.from_env = MagicMock(return_value=mock_client)

    caplog.set_level(logging.ERROR)
    with (
        patch.object(ss, "fetch_all_playlists", return_value=[]),
        patch("deejay_cog.spotify_sync.api_client", mock_cls),
        pytest.raises(ss.KaianoApiError),
    ):
        ss.push_playlists_to_api(object())

    assert "Spotify playlist push to API failed" in caplog.text


def test_push_playlists_to_api_raises_when_upserted_count_missing(
    monkeypatch, caplog
) -> None:
    import logging

    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    mock_client = MagicMock()
    mock_client.post.return_value = {"data": {"unchanged": 1}}
    mock_cls = MagicMock(return_value=mock_client)
    mock_cls.from_env = MagicMock(return_value=mock_client)

    caplog.set_level(logging.ERROR)
    with (
        patch.object(ss, "fetch_all_playlists", return_value=[]),
        patch("deejay_cog.spotify_sync.api_client", mock_cls),
        pytest.raises(ValueError, match="missing upserted count"),
    ):
        ss.push_playlists_to_api(object())

    assert "missing upserted count" in caplog.text


def test_update_spotify_radio_playlist_adds_and_trims() -> None:
    sp = MagicMock()
    outcome = ss.update_spotify_radio_playlist(sp, "pl1", ["u1", "u2"])
    assert outcome.ok is True
    sp.add_tracks_to_specific_playlist.assert_called_once_with("pl1", ["u1", "u2"])
    sp.trim_playlist_to_limit.assert_called_once()


def test_update_spotify_radio_playlist_skips_empty_uris() -> None:
    sp = MagicMock()
    outcome = ss.update_spotify_radio_playlist(sp, "pl1", [])
    assert outcome.ok is True
    sp.add_tracks_to_specific_playlist.assert_not_called()


def test_update_spotify_radio_playlist_skips_empty_playlist_id() -> None:
    sp = MagicMock()
    outcome = ss.update_spotify_radio_playlist(sp, None, ["u1"])
    assert outcome.ok is True
    sp.add_tracks_to_specific_playlist.assert_not_called()


def test_update_spotify_radio_playlist_returns_failure_on_exception() -> None:
    sp = MagicMock()
    sp.add_tracks_to_specific_playlist.side_effect = RuntimeError("api down")
    outcome = ss.update_spotify_radio_playlist(sp, "pl", ["u1"])
    assert outcome.ok is False
    assert outcome.detail == "RuntimeError: api down"


def test_create_spotify_playlist_for_file_updates_existing() -> None:
    sp = MagicMock()
    sp.find_playlist_by_name.return_value = {"id": "existing"}
    pid = ss.create_spotify_playlist_for_file(sp, "2024-01-01", ["u1"])
    assert pid == "existing"
    sp.find_playlist_by_name.assert_called_once_with("2024-01-01")
    sp.clear_playlist.assert_called_once_with("existing")
    sp.add_tracks_to_specific_playlist.assert_called_once_with("existing", ["u1"])
    sp.create_playlist.assert_not_called()


def test_create_spotify_playlist_for_file_clears_existing_before_add() -> None:
    sp = MagicMock()
    sp.find_playlist_by_name.return_value = {"id": "existing"}
    ss.create_spotify_playlist_for_file(sp, "2024-03-15 MADjam", ["u1", "u2"])
    assert sp.method_calls == [
        call.find_playlist_by_name("2024-03-15 MADjam"),
        call.clear_playlist("existing"),
        call.add_tracks_to_specific_playlist("existing", ["u1", "u2"]),
    ]


def test_create_spotify_playlist_for_file_skips_clear_when_creating_new() -> None:
    sp = MagicMock()
    sp.find_playlist_by_name.return_value = None
    sp.create_playlist.return_value = "newpl"
    ss.create_spotify_playlist_for_file(sp, "My Set", ["u1"])
    sp.clear_playlist.assert_not_called()


def test_create_spotify_playlist_for_file_creates_new_dedupes() -> None:
    sp = MagicMock()
    sp.find_playlist_by_name.return_value = None
    sp.create_playlist.return_value = "newpl"
    pid = ss.create_spotify_playlist_for_file(sp, "2024-01-01", ["a", "a", "b"])
    assert pid == "newpl"
    sp.create_playlist.assert_called_once()
    assert sp.create_playlist.call_args[0][0] == "2024-01-01"
    sp.add_tracks_to_specific_playlist.assert_called_once_with("newpl", ["a", "b"])


def test_create_spotify_playlist_for_file_returns_none_empty_uris() -> None:
    sp = MagicMock()
    assert ss.create_spotify_playlist_for_file(sp, "2024-01-01", []) is None
    sp.find_playlist_by_name.assert_not_called()


def test_create_spotify_playlist_for_file_returns_none_when_create_returns_none() -> (
    None
):
    sp = MagicMock()
    sp.find_playlist_by_name.return_value = None
    sp.create_playlist.return_value = None
    assert ss.create_spotify_playlist_for_file(sp, "2024-01-01", ["u1"]) is None


def test_create_spotify_playlist_for_file_raises_on_failure() -> None:
    sp = MagicMock()
    sp.find_playlist_by_name.return_value = {"id": "existing"}
    sp.clear_playlist.return_value = None
    sp.add_tracks_to_specific_playlist.side_effect = RuntimeError("add failed")
    with pytest.raises(RuntimeError, match="add failed"):
        ss.create_spotify_playlist_for_file(sp, "2024-01-01", ["u1"])


def test_get_spotify_client_returns_instance_when_credentials_set(monkeypatch) -> None:
    monkeypatch.setenv("SPOTIPY_CLIENT_ID", "cid")
    monkeypatch.setenv("SPOTIPY_REFRESH_TOKEN", "rtok")
    fake = MagicMock()
    with patch.object(ss, "SpotifyAPI") as m_api:
        m_api.from_env.return_value = fake
        out = ss.get_spotify_client()
    assert out is fake
    m_api.from_env.assert_called_once()


def test_get_spotify_client_returns_none_when_client_id_missing(monkeypatch) -> None:
    monkeypatch.delenv("SPOTIPY_CLIENT_ID", raising=False)
    monkeypatch.setenv("SPOTIPY_REFRESH_TOKEN", "rtok")
    with patch.object(ss, "SpotifyAPI") as m_api:
        assert ss.get_spotify_client() is None
    m_api.from_env.assert_not_called()


def test_get_spotify_client_returns_none_when_refresh_token_missing(
    monkeypatch,
) -> None:
    monkeypatch.setenv("SPOTIPY_CLIENT_ID", "cid")
    monkeypatch.delenv("SPOTIPY_REFRESH_TOKEN", raising=False)
    with patch.object(ss, "SpotifyAPI") as m_api:
        assert ss.get_spotify_client() is None
    m_api.from_env.assert_not_called()


def test_get_spotify_client_returns_none_when_from_env_raises(monkeypatch) -> None:
    monkeypatch.setenv("SPOTIPY_CLIENT_ID", "cid")
    monkeypatch.setenv("SPOTIPY_REFRESH_TOKEN", "rtok")
    with patch.object(ss, "SpotifyAPI") as m_api:
        m_api.from_env.side_effect = RuntimeError("oauth broken")
        assert ss.get_spotify_client() is None


def test_sync_set_to_spotify_searches_and_updates_playlists(monkeypatch) -> None:
    monkeypatch.setattr(ss, "SPOTIFY_RADIO_PLAYLIST_ID", "radio")
    sp = MagicMock()
    sp.search_track.side_effect = ["uri1", None, "uri2"]
    tracks = [
        {"artist": "A", "title": "T1"},
        {"artist": "B", "title": "T2"},
        {"artist": "C", "title": "T3"},
    ]
    with (
        patch.object(
            ss, "update_spotify_radio_playlist", return_value=ss.SyncOutcome.success()
        ) as m_radio,
        patch.object(
            ss, "create_spotify_playlist_for_file", return_value="pl-day"
        ) as m_create,
    ):
        outcome = ss.sync_set_to_spotify(sp, "2024-01-01", tracks)
    assert outcome.ok is True
    assert sp.search_track.call_count == 3
    m_radio.assert_called_once_with(sp, "radio", ["uri1", "uri2"])
    m_create.assert_called_once_with(sp, "2024-01-01", ["uri1", "uri2"])


def test_sync_set_to_spotify_skips_tracks_missing_artist_or_title(monkeypatch) -> None:
    monkeypatch.setattr(ss, "SPOTIFY_RADIO_PLAYLIST_ID", "radio")
    sp = MagicMock()
    sp.search_track.return_value = "u1"
    tracks = [
        {"artist": "", "title": "T"},
        {"artist": "A", "title": ""},
        {"artist": "A", "title": "T"},
    ]
    with (
        patch.object(
            ss, "update_spotify_radio_playlist", return_value=ss.SyncOutcome.success()
        ) as m_radio,
        patch.object(
            ss, "create_spotify_playlist_for_file", return_value="x"
        ) as m_create,
    ):
        outcome = ss.sync_set_to_spotify(sp, "2024-01-01", tracks)
    assert outcome.ok is True
    sp.search_track.assert_called_once_with("A", "T")
    m_radio.assert_called_once_with(sp, "radio", ["u1"])
    m_create.assert_called_once_with(sp, "2024-01-01", ["u1"])


def test_sync_set_to_spotify_succeeds_when_no_spotify_matches(monkeypatch) -> None:
    monkeypatch.setattr(ss, "SPOTIFY_RADIO_PLAYLIST_ID", "radio")
    sp = MagicMock()
    sp.search_track.return_value = None
    tracks = [{"artist": "A", "title": "T"}]
    with (
        patch.object(
            ss, "update_spotify_radio_playlist", return_value=ss.SyncOutcome.success()
        ) as m_radio,
        patch.object(
            ss, "create_spotify_playlist_for_file", return_value=None
        ) as m_create,
    ):
        outcome = ss.sync_set_to_spotify(sp, "2024-01-01", tracks)
    assert outcome.ok is True
    m_radio.assert_called_once_with(sp, "radio", [])
    m_create.assert_called_once_with(sp, "2024-01-01", [])


def test_sync_set_to_spotify_returns_failure_when_internal_error(monkeypatch) -> None:
    monkeypatch.setattr(ss, "SPOTIFY_RADIO_PLAYLIST_ID", "radio")
    sp = MagicMock()
    sp.search_track.side_effect = RuntimeError("api down")
    tracks = [{"artist": "A", "title": "T"}]
    outcome = ss.sync_set_to_spotify(sp, "2024-01-01", tracks)
    assert outcome.ok is False
    assert outcome.detail == "RuntimeError: api down"


def test_sync_returns_failure_when_the_radio_update_breaks(monkeypatch) -> None:
    """A partial sync is a failure the flow can count."""
    monkeypatch.setattr(ss, "SPOTIFY_RADIO_PLAYLIST_ID", "radio")
    sp = MagicMock()
    sp.search_track.return_value = "u1"
    sp.add_tracks_to_specific_playlist.side_effect = RuntimeError("token expired")
    tracks = [{"artist": "A", "title": "T"}]
    with patch.object(ss, "create_spotify_playlist_for_file", return_value="pl"):
        outcome = ss.sync_set_to_spotify(sp, "2024-01-01", tracks)
    assert outcome.ok is False
    assert outcome.detail == "RuntimeError: token expired"


def test_sync_set_to_spotify_passes_full_set_name_to_playlist_create(
    monkeypatch,
) -> None:
    monkeypatch.setattr(ss, "SPOTIFY_RADIO_PLAYLIST_ID", "radio")
    sp = MagicMock()
    sp.search_track.return_value = "u1"
    full_name = "2024-03-15 MADjam"
    tracks = [{"artist": "A", "title": "T"}]
    with (
        patch.object(
            ss, "update_spotify_radio_playlist", return_value=ss.SyncOutcome.success()
        ),
        patch.object(
            ss, "create_spotify_playlist_for_file", return_value="pl"
        ) as m_create,
    ):
        ss.sync_set_to_spotify(sp, full_name, tracks)
    m_create.assert_called_once_with(sp, full_name, ["u1"])
