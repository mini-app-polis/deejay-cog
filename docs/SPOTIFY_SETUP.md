# Spotify setup

This document covers the one-time setup needed to enable Spotify sync in
this pipeline.

---

## 1. Create a Spotify Developer app

1. Go to the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
   and log in with the Spotify account you want to use.
2. Click **Create app**.
3. Fill in a name and description (anything is fine).
4. Set the **Redirect URI** to `http://127.0.0.1:8888/callback` and save.
5. From the app settings, copy your **Client ID** and **Client Secret**.

---

## 2. Put the app credentials in Doppler

Set these in Doppler, in both the `dev` and `prd` configs of
`mini-app-polis-ecosystem`:

```
SPOTIPY_CLIENT_ID=your-client-id
SPOTIPY_CLIENT_SECRET=your-client-secret
SPOTIPY_REDIRECT_URI=http://127.0.0.1:8888/callback
```

---

## 3. Get a refresh token

Run the helper script:

```bash
doppler run -- uv run python scripts/get_spotify_refresh_token.py
```

This opens a browser window asking you to log in and authorize the app.
After authorizing, you will be redirected to `http://127.0.0.1:8888/callback`
— the page will likely show an error or be blank, which is expected. Copy
the full URL from the browser address bar and paste it into the terminal
when prompted.

The script will print:

```
✅ REFRESH TOKEN: AQD...
```

Copy that token — you only need to do this once.

---

## 4. Store the credentials

**Production.** Add these to Doppler; they sync to SSM Parameter Store, and
the Lambda loads them at cold start (all optional — declared in
`mini-app-polis/infra` `cogs.tf`):

| Name | Value |
|------|-------|
| `SPOTIPY_CLIENT_ID` | Your Spotify app client ID |
| `SPOTIPY_CLIENT_SECRET` | Your Spotify app client secret |
| `SPOTIPY_REFRESH_TOKEN` | The refresh token from step 3 |
| `SPOTIFY_RADIO_PLAYLIST_ID` | Spotify playlist ID for the radio playlist |

`SPOTIPY_REDIRECT_URI` is set on the function's environment in the same
file (`http://127.0.0.1:8888/callback`).

**Locally.** The same names in Doppler's `dev` config, used under `doppler run`.

The full playlist catalog is pushed to api-kaianolevine-com
(`POST /v1/spotify/playlists`) whenever an API base URL resolves; see
`docs/CONFIGURATION.md`.

To find a playlist ID: open the playlist in Spotify, click the three-dot
menu → Share → Copy link. The ID is the string after `/playlist/` and
before any `?`.

---

## 5. Verify

Run the CSV flow locally:

```bash
uv run python -u src/deejay_cog/process_new_files.py
```

The log should show the Spotify playlist sync completing, with no
"Spotify credentials incomplete" warning. If you see credential errors,
check that the names match those listed above exactly.
