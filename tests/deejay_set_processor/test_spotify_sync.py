import json
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

from deejay_set_processor import spotify_sync as ss


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


def test_write_playlist_snapshot_json_writes_valid_json(tmp_path) -> None:
    out_path = tmp_path / "snap.json"
    playlists = [{"id": "x", "name": "Zed"}]

    fake_sp = object()
    with (
        patch.object(ss, "fetch_all_playlists", return_value=playlists),
        patch.object(ss, "_normalize_playlist_item", side_effect=lambda p: p),
    ):
        ss.SPOTIFY_PLAYLIST_SNAPSHOT_JSON_PATH = str(out_path)
        path = ss.write_playlist_snapshot_json(fake_sp)

    assert path == str(out_path)
    data = json.loads(out_path.read_text(encoding="utf-8"))
    assert data["playlist_count"] == 1
    assert len(data["playlists"]) == 1
    assert "generated_at" in data


def test_write_playlist_snapshot_json_returns_none_on_write_error(tmp_path) -> None:
    bad_path = tmp_path / "nope" / "x.json"
    fake_sp = object()
    with (
        patch.object(ss, "fetch_all_playlists", return_value=[]),
        patch.object(ss, "_normalize_playlist_item", side_effect=lambda p: p),
    ):
        ss.SPOTIFY_PLAYLIST_SNAPSHOT_JSON_PATH = str(bad_path)
        with patch("builtins.open", side_effect=OSError("fail")):
            assert ss.write_playlist_snapshot_json(fake_sp) is None


def test_update_spotify_radio_playlist_adds_and_trims() -> None:
    sp = MagicMock()
    ss.update_spotify_radio_playlist(sp, "pl1", ["u1", "u2"])
    sp.add_tracks_to_specific_playlist.assert_called_once_with("pl1", ["u1", "u2"])
    sp.trim_playlist_to_limit.assert_called_once()


def test_update_spotify_radio_playlist_skips_empty_uris() -> None:
    sp = MagicMock()
    ss.update_spotify_radio_playlist(sp, "pl1", [])
    sp.add_tracks_to_specific_playlist.assert_not_called()


def test_update_spotify_radio_playlist_skips_empty_playlist_id() -> None:
    sp = MagicMock()
    ss.update_spotify_radio_playlist(sp, None, ["u1"])
    sp.add_tracks_to_specific_playlist.assert_not_called()


def test_update_spotify_radio_playlist_swallows_exceptions() -> None:
    sp = MagicMock()
    sp.add_tracks_to_specific_playlist.side_effect = RuntimeError("api down")
    ss.update_spotify_radio_playlist(sp, "pl", ["u1"])


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


def test_get_spotify_client_returns_none_when_refresh_token_missing(monkeypatch) -> None:
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
        patch.object(ss, "update_spotify_radio_playlist") as m_radio,
        patch.object(
            ss, "create_spotify_playlist_for_file", return_value="pl-day"
        ) as m_create,
    ):
        pid = ss.sync_set_to_spotify(sp, "2024-01-01", tracks)
    assert pid == "pl-day"
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
        patch.object(ss, "update_spotify_radio_playlist") as m_radio,
        patch.object(ss, "create_spotify_playlist_for_file", return_value="x") as m_create,
    ):
        ss.sync_set_to_spotify(sp, "2024-01-01", tracks)
    sp.search_track.assert_called_once_with("A", "T")
    m_radio.assert_called_once_with(sp, "radio", ["u1"])
    m_create.assert_called_once_with(sp, "2024-01-01", ["u1"])


def test_sync_set_to_spotify_returns_none_when_no_spotify_matches(monkeypatch) -> None:
    monkeypatch.setattr(ss, "SPOTIFY_RADIO_PLAYLIST_ID", "radio")
    sp = MagicMock()
    sp.search_track.return_value = None
    tracks = [{"artist": "A", "title": "T"}]
    with (
        patch.object(ss, "update_spotify_radio_playlist") as m_radio,
        patch.object(
            ss, "create_spotify_playlist_for_file", return_value=None
        ) as m_create,
    ):
        assert ss.sync_set_to_spotify(sp, "2024-01-01", tracks) is None
    m_radio.assert_called_once_with(sp, "radio", [])
    m_create.assert_called_once_with(sp, "2024-01-01", [])


def test_sync_set_to_spotify_returns_none_when_internal_error(monkeypatch) -> None:
    monkeypatch.setattr(ss, "SPOTIFY_RADIO_PLAYLIST_ID", "radio")
    sp = MagicMock()
    sp.search_track.side_effect = RuntimeError("api down")
    tracks = [{"artist": "A", "title": "T"}]
    assert ss.sync_set_to_spotify(sp, "2024-01-01", tracks) is None


def test_sync_set_to_spotify_passes_full_set_name_to_playlist_create(
    monkeypatch,
) -> None:
    monkeypatch.setattr(ss, "SPOTIFY_RADIO_PLAYLIST_ID", "radio")
    sp = MagicMock()
    sp.search_track.return_value = "u1"
    full_name = "2024-03-15 MADjam"
    tracks = [{"artist": "A", "title": "T"}]
    with (
        patch.object(ss, "update_spotify_radio_playlist"),
        patch.object(
            ss, "create_spotify_playlist_for_file", return_value="pl"
        ) as m_create,
    ):
        ss.sync_set_to_spotify(sp, full_name, tracks)
    m_create.assert_called_once_with(sp, full_name, ["u1"])
