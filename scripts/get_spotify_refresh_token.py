"""
Local utility to obtain a Spotify OAuth refresh token.

Run it locally, sign in to Spotify in the browser it opens, and store the
printed refresh token as SPOTIPY_REFRESH_TOKEN in Doppler (which syncs it to
the Lambda's SSM parameters).

Spotify refresh tokens expire six months after sign-in (enforced from
2026-07-20), and refreshing does not extend them, so this has to be run again
before then. It always signs in afresh: it never reads or writes spotipy's
token cache, which would otherwise hand back the old, possibly revoked token.

Usage:
    uv run python scripts/get_spotify_refresh_token.py

Prerequisites:
    - SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET, and SPOTIPY_REDIRECT_URI
      must be set in your local .env file.
    - The redirect URI must be registered in your Spotify Developer Dashboard
      app settings (https://developer.spotify.com/dashboard).
"""

import os

from dotenv import load_dotenv
from spotipy.cache_handler import MemoryCacheHandler
from spotipy.oauth2 import SpotifyOAuth

load_dotenv()

client_id = os.getenv("SPOTIPY_CLIENT_ID")
client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")
redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI", "http://127.0.0.1:8888/callback")

if not all([client_id, client_secret]):
    print(
        "❌ SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET must be set in your .env file."
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

if token_info and token_info.get("refresh_token"):
    print("\n✅ REFRESH TOKEN:", token_info["refresh_token"])
    print(
        "\nStore this as SPOTIPY_REFRESH_TOKEN in Doppler. It expires six months "
        "from now — renew before then."
    )
else:
    print("❌ Failed to retrieve token. Check your credentials and redirect URI.")
    raise SystemExit(1)
