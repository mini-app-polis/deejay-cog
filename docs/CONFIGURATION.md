# Configuration reference

This document lists every environment variable and config value used by **deejay-cog**, where they come from, and which scripts use them.

---

## System dependencies

The `retag-music` flow (`retag_music.py`) requires two runtime binaries that are not Python packages:

- `ffmpeg` (used by `pyacoustid` for audio decoding)
- `fpcalc` (used by `pyacoustid` for audio fingerprinting; provided by `chromaprint` / `libchromaprint-tools`)

Install on Ubuntu/Debian:

```bash
sudo apt-get install -y ffmpeg libchromaprint-tools
```

These dependencies are specific to `retag-music` and must be explicitly provisioned in the runtime environment when registering that deployment (for example via a custom Dockerfile or `nixpacks` config on Railway). No other deejay-cog flow requires these binaries.

---

## Environment variables

### GOOGLE_CREDENTIALS_JSON

| | |
|--|--|
| **Required** | Yes (for any script that talks to Drive/Sheets) |
| **Description** | Full JSON body of the Google credentials (service account or OAuth) used for Drive and Sheets API. |
| **Example** | `{"type": "service_account", "project_id": "...", ...}` |
| **Source** | GitHub Actions: **secret** `GOOGLE_CREDENTIALS_JSON`. Locally: set in env or `.env` (not committed). |
| **Used by** | `process_new_files.py`, `update_deejay_set_collection.py`, `generate_summaries.py`, `deduplicate_summary.py` (via kaiano `GoogleAPI.from_env()`). |

---

### LOGGING_LEVEL

| | |
|--|--|
| **Required** | No (defaults from common-python-utils) |
| **Description** | Log level for the application (e.g. `DEBUG`, `INFO`, `WARNING`). |
| **Example** | `INFO` |
| **Source** | GitHub Actions: **variable** `LOGGING_LEVEL`. Locally: env or config. |
| **Used by** | All scripts (via kaiano logger). |

---

### ANTHROPIC_API_KEY

| | |
|--|--|
| **Required** | No |
| **Description** | Anthropic API key (via **evaluator-cog**). Together with **`KAIANO_API_BASE_URL`**, it gates **production** `deejay_cog._pipeline_eval.post_run_finding` posts for **`process_new_files.py`** and **`ingest_live_history.py`**. Local-only flows (`generate_summaries.py`, `update_deejay_set_collection.py`, `retag_music.py`) never post findings regardless of this key. |
| **Example** | `sk-antropic-...` |
| **Source** | GitHub Actions: **secret** `ANTHROPIC_API_KEY` |
| **Used by** | `deejay_cog._pipeline_eval` (production flows only, for gated evaluation posts). |

---

### STANDARDS_VERSION

| | |
|--|--|
| **Required** | No (defaults to `6.0`) |
| **Description** | Version of the standards document being evaluated against. |
| **Example** | `6.0` |
| **Source** | GitHub Actions: environment variable `STANDARDS_VERSION` |
| **Used by** | `pipeline_evaluator.py` |

---

### CSV_SOURCE_FOLDER_ID

| | |
|--|--|
| **Required** | Yes for ingestion |
| **Description** | Google Drive folder ID of the “drop zone” where CSV and other files are placed for processing. |
| **Example** | `1abc...xyz` |
| **Source** | Set in **common-python-utils** config (e.g. env or repo variables) and read as `config.CSV_SOURCE_FOLDER_ID`. |
| **Used by** | `process_new_files.py` |

---

### DJ_SETS_FOLDER_ID

| | |
|--|--|
| **Required** | Yes |
| **Description** | Google Drive folder ID of the main DJ Sets folder that contains year subfolders and the Summary subfolder. |
| **Example** | `1def...uvw` |
| **Source** | common-python-utils config (`config.DJ_SETS_FOLDER_ID`). |
| **Used by** | `process_new_files.py`, `update_deejay_set_collection.py`, `generate_summaries.py` |

---

### OUTPUT_NAME

| | |
|--|--|
| **Required** | Yes for collection build |
| **Description** | Name of the master “DJ Set Collection” Google Sheet. |
| **Example** | `DJ Set Collection` |
| **Source** | common-python-utils config (`config.OUTPUT_NAME`). |
| **Used by** | `update_deejay_set_collection.py` |

---

### TEMP_TAB_NAME

| | |
|--|--|
| **Required** | Yes for collection build |
| **Description** | Name of the temporary tab used while building the collection spreadsheet. |
| **Example** | `Temp` |
| **Source** | common-python-utils config (`config.TEMP_TAB_NAME`). |
| **Used by** | `update_deejay_set_collection.py` |

---

### SUMMARY_TAB_NAME

| | |
|--|--|
| **Required** | Yes for collection build |
| **Description** | Name of the Summary tab in the collection spreadsheet. |
| **Example** | `Summary` |
| **Source** | common-python-utils config (`config.SUMMARY_TAB_NAME`). |
| **Used by** | `update_deejay_set_collection.py` |

---

### SUMMARY_FOLDER_NAME

| | |
|--|--|
| **Required** | Yes for summaries |
| **Description** | Name of the Summary subfolder under the DJ Sets folder (e.g. where “{Year} Summary” sheets live). |
| **Example** | `Summary` |
| **Source** | common-python-utils config (`config.SUMMARY_FOLDER_NAME`). |
| **Used by** | `generate_summaries.py` |

---

### ALLOWED_HEADERS

| | |
|--|--|
| **Required** | Yes for summaries |
| **Description** | List of column names (from set sheets) to include in summary sheets. |
| **Example** | `["Title", "Artist", "Length", "BPM", "Genre", "Year", "Comment"]` |
| **Source** | common-python-utils config (`config.ALLOWED_HEADERS`). |
| **Used by** | `generate_summaries.py` |

---

### desiredOrder

| | |
|--|--|
| **Required** | No (order can default) |
| **Description** | Order of columns in summary sheets (subset of allowed headers). |
| **Example** | `["Title", "Artist", "Length", "BPM", "Genre", "Year", "Comment"]` |
| **Source** | common-python-utils config (`config.desiredOrder`). |
| **Used by** | `generate_summaries.py` |

---

### DEEJAY_SET_COLLECTION_JSON_PATH

| | |
|--|--|
| **Required** | No |
| **Description** | File path where the DJ set collection JSON snapshot is written. |
| **Example** | `v1/deejay-sets/deejay_set_collection.json` |
| **Default** | `v1/deejay-sets/deejay_set_collection.json` (if not set in config). |
| **Source** | common-python-utils config (`config.DEEJAY_SET_COLLECTION_JSON_PATH`), or hardcoded default in `update_deejay_set_collection.py`. |
| **Used by** | `update_deejay_set_collection.py` |

---

### KAIANO_API_BASE_URL

| | |
|--|--|
| **Required** | No — API ingest and pipeline evaluation are both skipped if unset. |
| **Description** | Base URL for the deejay-marvel-api instance. Used to POST new sets, live history plays, and the full Spotify playlist catalog (`POST /v1/spotify/playlists` from `spotify_sync.push_playlists_to_api` after CSV Spotify sync). Also gates **production** pipeline evaluation posts in `deejay_cog._pipeline_eval` (requires **`ANTHROPIC_API_KEY`** as well). |
| **Example** | `https://your-api.railway.app` |
| **Source** | GitHub Actions: **variable** `KAIANO_API_BASE_URL`. Locally: `.env`. |
| **Used by** | `process_new_files.py`, `ingest_live_history.py`, `ingest_to_api.py`, `spotify_sync.py`, and `deejay_cog._pipeline_eval` (production evaluation gating). |

---

### KAIANO_API_OWNER_ID

| | |
|--|--|
| **Required** | No (falls back to `OWNER_ID` if not set) |
| **Description** | Owner ID sent with API requests to deejay-marvel-api. |
| **Example** | `your-owner-id` |
| **Source** | GitHub Actions: **variable** `KAIANO_API_OWNER_ID`. Locally: `.env`. |
| **Used by** | `process_new_files.py`, `ingest_live_history.py` |

---

### OWNER_ID

| | |
|--|--|
| **Required** | No (fallback for `KAIANO_API_OWNER_ID`) |
| **Description** | Fallback owner ID if `KAIANO_API_OWNER_ID` is not set. |
| **Source** | GitHub Actions: **variable** `OWNER_ID`. Locally: `.env`. |
| **Used by** | `process_new_files.py`, `ingest_live_history.py` |

---

### SPOTIPY_CLIENT_ID

| | |
|--|--|
| **Required** | Yes (all Spotify features) |
| **Description** | Spotify application client ID from the Spotify Developer Dashboard. |
| **Source** | GitHub Actions: **secret** `SPOTIPY_CLIENT_ID`. Locally: `.env`. |
| **Used by** | `spotify_sync.py`, `process_new_files.py` |

---

### SPOTIPY_CLIENT_SECRET

| | |
|--|--|
| **Required** | Yes (all Spotify features) |
| **Description** | Spotify application client secret from the Spotify Developer Dashboard. |
| **Source** | GitHub Actions: **secret** `SPOTIPY_CLIENT_SECRET`. Locally: `.env`. |
| **Used by** | `spotify_sync.py`, `process_new_files.py` |

---

### SPOTIPY_REFRESH_TOKEN

| | |
|--|--|
| **Required** | Yes (all Spotify features) |
| **Description** | OAuth refresh token for the Spotify account. Generated once locally using `scripts/get_spotify_refresh_token.py` — see `docs/SPOTIFY_SETUP.md` for full instructions. |
| **Source** | GitHub Actions: **secret** `SPOTIPY_REFRESH_TOKEN`. Locally: `.env`. |
| **Used by** | `spotify_sync.py`, `process_new_files.py` |

---

### SPOTIPY_REDIRECT_URI

| | |
|--|--|
| **Required** | No (defaults to `http://127.0.0.1:8888/callback`) |
| **Description** | OAuth redirect URI — must match what is registered in the Spotify Developer Dashboard. |
| **Source** | GitHub Actions: **variable** `SPOTIPY_REDIRECT_URI`. Locally: `.env`. |
| **Used by** | `spotify_sync.py` |

---

### SPOTIFY_RADIO_PLAYLIST_ID

| | |
|--|--|
| **Required** | No (radio playlist updates are skipped if unset) |
| **Description** | Spotify playlist ID for the standing radio playlist that matched tracks are appended to on every sync. |
| **Source** | GitHub Actions: **variable** `SPOTIFY_RADIO_PLAYLIST_ID`. Locally: `.env`. |
| **Used by** | `spotify_sync.py`, `process_new_files.py` |

---

### ACOUSTID_API_KEY

| | |
|--|--|
| **Required** | Yes (retag-music flow) |
| **Description** | AcoustID application API key used to fingerprint and identify audio files via AcoustID/MusicBrainz. |
| **Source** | GitHub Actions / Railway env / `.env`. |
| **Used by** | `retag_music.py` |

---

### RETAG_MIN_CONFIDENCE

| | |
|--|--|
| **Required** | No (defaults to `0.90`) |
| **Description** | Minimum identification confidence required before treating a match as high-confidence and moving to destination. |
| **Source** | GitHub Actions / Railway env / `.env`. |
| **Used by** | `retag_music.py` |

---

### RETAG_MAX_CANDIDATES

| | |
|--|--|
| **Required** | No (defaults to `5`) |
| **Description** | Maximum AcoustID candidates to inspect when selecting a match. |
| **Source** | GitHub Actions / Railway env / `.env`. |
| **Used by** | `retag_music.py` |

---

### MAX_UPLOADS_PER_RUN

| | |
|--|--|
| **Required** | No (defaults to `200`) |
| **Description** | Per-run ceiling for uploads performed by the retag-music flow. |
| **Source** | GitHub Actions / Railway env / `.env`. |
| **Used by** | `retag_music.py` |

---

## Prefect

### PREFECT_API_KEY

| | |
|--|--|
| **Required** | Yes (Prefect flow execution) |
| **Description** | API key for authenticating with Prefect Cloud. |
| **Source** | GitHub Actions: **secret** `PREFECT_API_KEY`. Locally: `.env`. |
| **Used by** | All flow scripts via `prefect cloud login`. |

---

### PREFECT_API_URL

| | |
|--|--|
| **Required** | Yes (Prefect flow execution) |
| **Description** | Prefect Cloud workspace API URL. |
| **Example** | `https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>` |
| **Source** | GitHub Actions: **variable** `PREFECT_API_URL`. Locally: `.env`. |
| **Used by** | All flow scripts via `prefect cloud login`. |

---

### PREFECT_ACCOUNT_SLUG

| | |
|--|--|
| **Required** | Yes (Prefect login step in workflows) |
| **Description** | Prefect Cloud account slug used in the workspace login command. |
| **Source** | GitHub Actions: **variable** `PREFECT_ACCOUNT_SLUG`. |
| **Used by** | All workflows (login step only — not read by Python scripts directly). |

---

### PREFECT_WORKSPACE_SLUG

| | |
|--|--|
| **Required** | Yes (Prefect login step in workflows) |
| **Description** | Prefect Cloud workspace slug used in the workspace login command. |
| **Source** | GitHub Actions: **variable** `PREFECT_WORKSPACE_SLUG`. |
| **Used by** | All workflows (login step only — not read by Python scripts directly). |

---

## GitHub-only (workflows)

### KAIANO_API_REPO_TOKEN

| | |
|--|--|
| **Required** | Yes (workflows that push to kaiano-api) |
| **Description** | GitHub personal access token used to clone and push the kaiano-api repo when copying JSON snapshots. Not read by Python scripts directly. |
| **Source** | GitHub Actions: **secret** `KAIANO_API_REPO_TOKEN`. |
| **Used by** | `update_dj_set_collection` workflow, `process_new_csv_files` workflow. |

---

## Summary table

| Variable | Required | Source (typical) | Scripts |
|----------|----------|------------------|--------|
| GOOGLE_CREDENTIALS_JSON | Yes | GitHub secret / env | All |
| LOGGING_LEVEL | No | GitHub variable / env | All |
| ANTHROPIC_API_KEY | No | GitHub secret / env | process_new_files, update_deejay_set_collection, generate_summaries, ingest_live_history (evaluation) |
| CSV_SOURCE_FOLDER_ID | Yes (ingestion) | kaiano config | process_new_files |
| DJ_SETS_FOLDER_ID | Yes | kaiano config | process_new_files, update_deejay_set_collection, generate_summaries |
| OUTPUT_NAME | Yes (collection) | kaiano config | update_deejay_set_collection |
| TEMP_TAB_NAME | Yes (collection) | kaiano config | update_deejay_set_collection |
| SUMMARY_TAB_NAME | Yes (collection) | kaiano config | update_deejay_set_collection |
| SUMMARY_FOLDER_NAME | Yes (summaries) | kaiano config | generate_summaries |
| ALLOWED_HEADERS | Yes (summaries) | kaiano config | generate_summaries |
| desiredOrder | No | kaiano config | generate_summaries |
| DEEJAY_SET_COLLECTION_JSON_PATH | No | kaiano config / default | update_deejay_set_collection |
| KAIANO_API_BASE_URL | No | GitHub variable / `.env` | process_new_files, update_deejay_set_collection, generate_summaries, ingest_live_history (API ingest, Spotify playlist catalog push, evaluation gating) |
| KAIANO_API_OWNER_ID | No | GitHub variable / `.env` | process_new_files, ingest_live_history |
| OWNER_ID | No | GitHub variable / `.env` | process_new_files, ingest_live_history |
| SPOTIPY_CLIENT_ID | Yes (Spotify) | GitHub secret / `.env` | spotify_sync, process_new_files |
| SPOTIPY_CLIENT_SECRET | Yes (Spotify) | GitHub secret / `.env` | spotify_sync, process_new_files |
| SPOTIPY_REFRESH_TOKEN | Yes (Spotify) | GitHub secret / `.env` | spotify_sync, process_new_files |
| SPOTIPY_REDIRECT_URI | No | GitHub variable / `.env` | spotify_sync |
| SPOTIFY_RADIO_PLAYLIST_ID | No | GitHub variable / `.env` | spotify_sync, process_new_files |
| ACOUSTID_API_KEY | Yes (retag-music) | GitHub variable / Railway env / `.env` | retag_music |
| RETAG_MIN_CONFIDENCE | No | GitHub variable / Railway env / `.env` | retag_music |
| RETAG_MAX_CANDIDATES | No | GitHub variable / Railway env / `.env` | retag_music |
| MAX_UPLOADS_PER_RUN | No | GitHub variable / Railway env / `.env` | retag_music |
| PREFECT_API_KEY | Yes (flows) | GitHub secret / `.env` | flow scripts (Prefect login) |
| PREFECT_API_URL | Yes (flows) | GitHub variable / `.env` | flow scripts (Prefect login) |
| PREFECT_ACCOUNT_SLUG | Yes (workflows) | GitHub variable | workflows (Prefect login) |
| PREFECT_WORKSPACE_SLUG | Yes (workflows) | GitHub variable | workflows (Prefect login) |
| KAIANO_API_REPO_TOKEN | Yes (kaiano-api push) | GitHub secret | update_dj_set_collection, process_new_csv_files |
