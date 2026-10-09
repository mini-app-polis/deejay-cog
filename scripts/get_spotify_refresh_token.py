"""
Local utility to obtain a Spotify OAuth refresh token.

Run it locally and sign in to Spotify in the browser it opens. It writes
the new token to Doppler's prd config itself, as SPOTIPY_REFRESH_TOKEN with
the date in SPOTIPY_REFRESH_TOKEN_ISSUED_AT, through your `doppler login`;
the prd sync carries it to the Lambda's SSM parameters. Nothing to copy.
If that write fails it prints the token instead, to store by hand.

Spotify refresh tokens expire six months after sign-in (enforced from
2026-07-20), and refreshing does not extend them, so this has to be run again
before then. It always signs in afresh: it never reads or writes spotipy's
token cache, which would otherwise hand back the old, possibly revoked token.

Usage:
    doppler run -- uv run python scripts/get_spotify_refresh_token.py

Prerequisites:
    - SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET in Doppler's dev config
      (the same Spotify app as production); SPOTIPY_REDIRECT_URI optional.
    - The redirect URI must be registered in your Spotify Developer Dashboard
      app settings (https://developer.spotify.com/dashboard).
"""

import datetime as dt
import os
from pathlib import Path

from mini_app_polis.doppler import DopplerError, doppler_project, set_secrets_with_cli
from spotipy.cache_handler import MemoryCacheHandler
from spotipy.oauth2 import SpotifyOAuth

#: The config production reads. Written on purpose: renewing the token is the
#: one local action that exists to change production.
PRD_CONFIG = "prd"

client_id = os.getenv("SPOTIPY_CLIENT_ID")
client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")
redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI", "http://127.0.0.1:8888/callback")

if not all([client_id, client_secret]):
    print(
        "❌ SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET are not set. Run this under "
        "`doppler run -- uv run python scripts/get_spotify_refresh_token.py`."
    )
    raise SystemExit(1)

sp_oauth = SpotifyOAuth(
    client_id=client_id,
    client_secret=client_secret,
    redirect_uri=redirect_uri,
    scope="playlist-modify-public playlist-modify-private",
    open_browser=True,
    # No token cache: a cached refresh token is exactly what has expired.
    cache_handler=MemoryCacheHandler(),
)

print(f"Opening browser for Spotify authorization (redirect URI: {redirect_uri}) ...")
code = sp_oauth.get_auth_response()
token_info = sp_oauth.get_access_token(code, as_dict=True, check_cache=False)

if not (token_info and token_info.get("refresh_token")):
    print("❌ Failed to retrieve token. Check your credentials and redirect URI.")
    raise SystemExit(1)

refresh_token = token_info["refresh_token"]
issued_at = dt.date.today().isoformat()
try:
    project = doppler_project(
        (Path(__file__).resolve().parent.parent / "doppler.yaml").read_text()
    )
    set_secrets_with_cli(
        {
            "SPOTIPY_REFRESH_TOKEN": refresh_token,
            "SPOTIPY_REFRESH_TOKEN_ISSUED_AT": issued_at,
        },
        project=project,
        config=PRD_CONFIG,
    )
except DopplerError as exc:
    print(f"\n⚠️  Could not write to Doppler ({exc}).")
    print("✅ REFRESH TOKEN:", refresh_token)
    print(
        f"\nStore it as SPOTIPY_REFRESH_TOKEN in Doppler {PRD_CONFIG}, and "
        f"SPOTIPY_REFRESH_TOKEN_ISSUED_AT={issued_at}."
    )
else:
    print(
        f"\n✅ Wrote SPOTIPY_REFRESH_TOKEN and SPOTIPY_REFRESH_TOKEN_ISSUED_AT="
        f"{issued_at} to Doppler {project}/{PRD_CONFIG}."
    )
print("It expires six months from now — renew before then.")
