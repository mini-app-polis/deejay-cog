"""Spotify sync for DJ sets sourced from Google Sheets (CSV pipeline).

Updates the radio playlist and per-set playlists from sheet track rows, and
pushes a full Spotify playlist snapshot to the Kaiano API when configured.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from mini_app_polis import logger as logger_mod
from mini_app_polis.api import KaianoApiError  # type: ignore
from mini_app_polis.spotify import SpotifyAPI

from .api_client import api_client

log = logger_mod.get_logger()


@dataclass(frozen=True)
class SyncOutcome:
    """What a Spotify operation actually did.

    ``None`` was doing three jobs — "nothing to do", "worked, nothing to
    return" and "broke" — and the caller could not tell them apart, so
    the flow's spotify_failed counter never moved and a lost sync
    reported SUCCESS.
    """

    ok: bool
    detail: str = ""

    @classmethod
    def success(cls) -> SyncOutcome:
        return cls(True)

    @classmethod
    def failure(cls, exc: BaseException) -> SyncOutcome:
        return cls(False, f"{type(exc).__name__}: {exc}")


# Credentials are read lazily, on every access, the way mini_app_polis.config
# reads its own. A module-level constant frozen at import diverged from it in
# any process that outlives its import.
#
# The radio playlist ID is not one of these: credentials decide whether a
# Spotify client can exist at all, while a missing playlist ID breaks only the
# radio playlist and is reported by update_spotify_radio_playlist.
SPOTIFY_CREDENTIAL_ENV = (
    "SPOTIPY_CLIENT_ID",
    "SPOTIPY_CLIENT_SECRET",
    "SPOTIPY_REFRESH_TOKEN",
)

DEFAULT_PLAYLIST_DESCRIPTION = (
    "Generated automatically by Deejay Marvel Automation Tools. "
    "Spreadsheets of history and song-not-found logs can be found at "
    "www.kaianolevine.com/dj-marvel"
)


def radio_playlist_id() -> str | None:
    """The long-running radio playlist ID, or None when unset."""
    return os.getenv("SPOTIFY_RADIO_PLAYLIST_ID")


def missing_spotify_credentials() -> list[str]:
    """Credential env vars this path needs and does not have.

    The one "is Spotify usable" check. There were three: two copies in the
    flow over these three names, and one in ``get_spotify_client`` over two
    of them, so a run missing only ``SPOTIPY_CLIENT_SECRET`` passed.
    """
    return [name for name in SPOTIFY_CREDENTIAL_ENV if not os.getenv(name)]


def _first_attr(obj: Any, names: list[str]) -> Any:
    """Return the first existing attribute value on obj from a list of names."""
    for n in names:
        if hasattr(obj, n):
            return getattr(obj, n)
    return None


def _extract_external_url(playlist: dict) -> str:
    # Spotify returns external_urls: {spotify: 'https://open.spotify.com/playlist/...'}
    external = playlist.get("external_urls") or {}
    if isinstance(external, dict):
        return external.get("spotify", "") or ""
    return ""


def _normalize_playlist_item(p: dict) -> dict:
    """Normalize a Spotify playlist object into a stable JSON-friendly dict."""
    owner = p.get("owner") or {}
    tracks = p.get("tracks") or {}

    return {
        "id": p.get("id", ""),
        "name": p.get("name", ""),
        "url": _extract_external_url(p),
        "uri": p.get("uri", ""),
        "type": p.get("type", "playlist"),
        "public": p.get("public"),
        "collaborative": p.get("collaborative"),
        "snapshot_id": p.get("snapshot_id", ""),
        "tracks_total": tracks.get("total"),
        "owner": {
            "id": owner.get("id", ""),
            "display_name": owner.get("display_name", ""),
        },
    }


def fetch_all_playlists(sp: Any) -> list[dict]:
    """Fetch all playlists visible to the account.

    Pages ``current_user_playlists`` on the underlying spotipy client. A
    probe over five candidate wrapper method names used to run first; none
    exists on ``SpotifyAPI``, so every call already landed here.

    Expected return shape is a list of raw Spotify playlist dicts.
    """
    client = _first_attr(sp, ["client", "spotify", "sp", "_client", "_sp"])
    if client is None:
        return []

    fn = getattr(client, "current_user_playlists", None)
    if not callable(fn):
        return []

    items: list[dict] = []
    limit = 50
    offset = 0

    while True:
        page = fn(limit=limit, offset=offset)
        if not isinstance(page, dict):
            break

        page_items = page.get("items") or []
        if isinstance(page_items, list):
            items.extend(page_items)

        if page.get("next"):
            offset += limit
            continue

        break

    return items


def push_playlists_to_api(sp: Any) -> tuple[int, int] | None:
    """Fetch all playlists and push a full snapshot to the Kaiano API.

    Returns ``(upserted, unchanged)`` on success, or None if the push was
    skipped because ``KAIANO_API_BASE_URL`` is not set. Both counts, because
    "0 pushed" could not distinguish every playlist already being current
    from an empty snapshot. A skip is not a push, and callers log it as one.

    A ``KaianoApiError`` from the POST is re-raised after logging (callers
    used to read a ``None`` return as success and log "None playlists
    pushed"). A response with no ``upserted`` count is also a failed
    push: this raises ``ValueError`` rather than returning ``None`` or a
    sentinel, so the caller cannot claim the sync completed.
    """
    if not os.getenv("KAIANO_API_BASE_URL"):
        log.warning(
            "KAIANO_API_BASE_URL not set — skipping Spotify playlist push to API",
        )
        return None

    raw_playlists = fetch_all_playlists(sp)
    normalized = [
        _normalize_playlist_item(p) for p in raw_playlists if isinstance(p, dict)
    ]

    payload = {
        "playlists": [
            {
                "id": p["id"],
                "name": p["name"],
                "url": p["url"],
                "uri": p["uri"],
                "type": p["type"],
                "public": p["public"] if p["public"] is not None else True,
                "collaborative": p["collaborative"]
                if p["collaborative"] is not None
                else False,
                "snapshot_id": p["snapshot_id"],
                "tracks_total": p["tracks_total"]
                if p["tracks_total"] is not None
                else 0,
                "owner_id": p["owner"]["id"],
                "owner_name": p["owner"].get("display_name"),
            }
            for p in normalized
            if p.get("id") and p.get("name")
        ]
    }

    try:
        client = api_client()
        response = client.post("/v1/spotify/playlists", payload)
    except KaianoApiError as e:
        log.error("Spotify playlist push to API failed: %s", e)
        # Raised, not returned. Both call sites read None as success and
        # the flow logged "complete: None playlists pushed" on a total
        # failure. The callers below now count it.
        raise

    data = response.get("data") if isinstance(response, dict) else None
    if not isinstance(data, dict):
        log.error("Spotify playlist API response missing data: %s", response)
        raise ValueError("Spotify playlist API response missing data")

    upserted = data.get("upserted")
    unchanged = data.get("unchanged", 0)
    if upserted is None:
        log.error("Spotify playlist API response missing upserted count: %s", response)
        raise ValueError("Spotify playlist API response missing upserted count")

    log.info(
        "✅ Spotify playlists pushed to API: %s upserted, %s unchanged",
        upserted,
        unchanged,
    )
    return int(upserted), int(unchanged or 0)


def update_spotify_radio_playlist(
    sp: SpotifyAPI, playlist_id: str | None, found_uris: list[str]
) -> SyncOutcome:
    """Append tracks to the main radio playlist and trim it to the limit.

    A missing ``playlist_id`` is a misconfiguration, not a no-op. It used
    to share a ``SyncOutcome.success()`` with "there was nothing to add",
    which is the exact conflation SyncOutcome exists to remove.
    """
    if not playlist_id:
        return SyncOutcome(
            False,
            "SPOTIFY_RADIO_PLAYLIST_ID is not set — the radio playlist was not updated",
        )

    if not found_uris:
        return SyncOutcome.success()

    try:
        sp.add_tracks_to_specific_playlist(playlist_id, found_uris)
        sp.trim_playlist_to_limit(playlist_id=playlist_id)
    except Exception as e:
        log.error("Error updating Spotify radio playlist: %s", e, exc_info=True)
        return SyncOutcome.failure(e)
    return SyncOutcome.success()


def create_spotify_playlist_for_file(
    sp: SpotifyAPI, set_name: str, found_uris: list[str]
) -> SyncOutcome:
    """Create or replace a per-set Spotify playlist.

    If a playlist with the given name already exists, it is cleared and
    repopulated with ``found_uris``. Otherwise a new playlist is created.

    Returns a ``SyncOutcome`` rather than a playlist ID, which no caller
    used. It used to raise while its sibling returned an outcome, and the
    exception was caught a level up and flattened into a generic failure.
    """
    if not found_uris:
        return SyncOutcome.success()

    try:
        existing = sp.find_playlist_by_name(set_name)
        if existing:
            playlist_id = existing["id"]
            sp.clear_playlist(playlist_id)
            sp.add_tracks_to_specific_playlist(playlist_id, found_uris)
            return SyncOutcome.success()

        playlist_id = sp.create_playlist(set_name, DEFAULT_PLAYLIST_DESCRIPTION)
        if not playlist_id:
            # Previously an unlogged None, indistinguishable from a set
            # with no tracks to add.
            return SyncOutcome(
                False, f"Spotify returned no playlist id for '{set_name}'"
            )

        unique_uris = list(dict.fromkeys(found_uris))
        sp.add_tracks_to_specific_playlist(playlist_id, unique_uris)
        return SyncOutcome.success()

    except Exception as e:
        log.error(
            "Failed creating/updating playlist '%s': %s",
            set_name,
            e,
            exc_info=True,
        )
        return SyncOutcome.failure(e)


def get_spotify_client() -> SpotifyAPI | None:
    """Return SpotifyAPI.from_env() or None if it cannot be built."""
    missing = missing_spotify_credentials()
    if missing:
        log.warning(
            "Spotify credentials incomplete (%s not set) — "
            "skipping Spotify client initialization.",
            ", ".join(missing),
        )
        return None
    try:
        return SpotifyAPI.from_env()
    except Exception as e:
        log.error("Failed to initialize Spotify client: %s", e)
        return None


def sync_set_to_spotify(
    sp: SpotifyAPI,
    set_name: str,
    tracks: list[dict],
) -> SyncOutcome:
    """Search Spotify for each track and update playlists.

    Returns a SyncOutcome covering both the radio playlist and the per-set
    playlist. The CSV is already archived by the time this runs, so this is
    the only moment a partial sync can be recorded.
    """
    try:
        found_uris: list[str] = []
        matched: list[tuple[str, str]] = []
        not_found = 0

        for t in tracks:
            artist = str(t.get("artist") or "").strip()
            title = str(t.get("title") or "").strip()
            if not artist or not title:
                continue
            uri = sp.search_track(artist, title)
            if uri:
                found_uris.append(uri)
                matched.append((artist, title))
            else:
                not_found += 1

        log.info(
            "%s: %d found on Spotify, %d not found",
            set_name,
            len(matched),
            not_found,
        )

        radio = update_spotify_radio_playlist(sp, radio_playlist_id(), found_uris)
        per_set = create_spotify_playlist_for_file(sp, set_name, found_uris)
        if radio.ok and per_set.ok:
            return SyncOutcome.success()
        return SyncOutcome(
            False,
            "; ".join(o.detail for o in (radio, per_set) if not o.ok and o.detail),
        )
    except Exception as e:
        log.error("sync_set_to_spotify failed: %s", e, exc_info=True)
        return SyncOutcome.failure(e)
