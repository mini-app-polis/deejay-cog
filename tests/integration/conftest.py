"""Integration harness: the Lambda handler, end to end, with the world stubbed.

What runs for real: ``worker.lambda_handler``, both flows, the cog's own
helpers, the shared ``KaianoApiClient`` with its request and response
contract models, and ``RunReport`` delivery to ``/v1/notify``.

What is stubbed, and where:

* **api-kaianolevine-com** at the HTTP boundary, with respx (TEST-007).
  Every request the client sends is matched; an unmatched one fails the
  test, so nothing here can reach a real API.
* **Google Drive and Sheets** at the ``GoogleAPI`` facade, by a stateful
  in-memory fake. The facade belongs to common-python-utils and is tested
  there; googleapiclient speaks httplib2, which respx cannot intercept, and
  a fake at the REST level would have to reimplement Drive's query
  language to answer a folder listing. What matters to this cog is the
  state Drive is left in — where a file sits and what it is called after a
  run — and a stateful fake is what lets a test send the same message twice
  and look at the result.
* **Spotify** at ``SpotifyAPI.from_env``, for the same reason (spotipy
  uses requests). Off unless a test asks for it.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import pytest
import respx
from harness import (
    API_BASE,
    API_KEY,
    DJ_SETS_FOLDER,
    SOURCE_FOLDER,
    VDJ_HISTORY_FOLDER,
    FakeApi,
    FakeDrive,
    FakeGoogle,
    FakeSheets,
    FakeSpotify,
    LambdaContext,
    install_api,
)

# ── fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture
def google(monkeypatch: pytest.MonkeyPatch) -> FakeGoogle:
    from mini_app_polis.google import GoogleAPI

    import deejay_cog.config as config

    sheets = FakeSheets()
    fake = FakeGoogle(drive=FakeDrive(sheets), sheets=sheets)
    monkeypatch.setattr(GoogleAPI, "from_env", classmethod(lambda _cls, **_: fake))
    monkeypatch.setattr(config, "CSV_SOURCE_FOLDER_ID", SOURCE_FOLDER)
    monkeypatch.setattr(config, "DJ_SETS_FOLDER_ID", DJ_SETS_FOLDER)
    monkeypatch.setattr(config, "VDJ_HISTORY_FOLDER_ID", VDJ_HISTORY_FOLDER)
    monkeypatch.setattr(config, "TIMEZONE", "America/Chicago")
    # The .m3u parser reads the shared library's config, not this cog's.
    import mini_app_polis.config as shared_config

    monkeypatch.setattr(shared_config, "TIMEZONE", "America/Chicago", raising=False)
    return fake


@pytest.fixture
def api(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeApi]:
    """The API, stubbed. Production environment, this cog's own key."""
    monkeypatch.setenv("KAIANO_API_BASE_URL", API_BASE)
    monkeypatch.setenv("DEEJAY_COG_API_KEY", API_KEY)
    monkeypatch.delenv("KAIANO_API_KEY", raising=False)
    monkeypatch.delenv("KAIANO_API_BASE_URL_DEV", raising=False)
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        fake = FakeApi(router=router)
        install_api(router, fake, API_BASE)
        yield fake


@pytest.fixture(autouse=True)
def _no_spotify(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("SPOTIPY_CLIENT_ID", "SPOTIPY_CLIENT_SECRET", "SPOTIPY_REFRESH_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("SPOTIFY_RADIO_PLAYLIST_ID", raising=False)


@pytest.fixture
def spotify(monkeypatch: pytest.MonkeyPatch) -> FakeSpotify:
    """Spotify credentials present, and a fake account behind them."""
    from mini_app_polis.spotify import SpotifyAPI

    for name in ("SPOTIPY_CLIENT_ID", "SPOTIPY_CLIENT_SECRET", "SPOTIPY_REFRESH_TOKEN"):
        monkeypatch.setenv(name, f"test-{name.lower()}")
    monkeypatch.setenv("SPOTIFY_RADIO_PLAYLIST_ID", "pl-radio")
    fake = FakeSpotify()
    fake.playlists["pl-radio"] = {"id": "pl-radio", "name": "WCS Radio", "uris": []}
    monkeypatch.setattr(SpotifyAPI, "from_env", classmethod(lambda _cls: fake))
    return fake


@pytest.fixture
def handler() -> Callable[..., dict]:
    """``lambda_handler``, imported as Lambda would import it."""
    from deejay_cog.worker import lambda_handler

    def invoke(event: dict, context: Any | None = None) -> dict:
        return lambda_handler(
            event, context if context is not None else LambdaContext()
        )

    return invoke
