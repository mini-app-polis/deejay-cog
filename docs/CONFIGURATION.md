# Configuration reference

This document lists every environment variable and config value used by **deejay-set-processor**, where they come from, and which scripts use them.

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
| **Required** | No (defaults from kaiano-common-utils) |
| **Description** | Log level for the application (e.g. `DEBUG`, `INFO`, `WARNING`). |
| **Example** | `INFO` |
| **Source** | GitHub Actions: **variable** `LOGGING_LEVEL`. Locally: env or config. |
| **Used by** | All scripts (via kaiano logger). |

---

### CSV_SOURCE_FOLDER_ID

| | |
|--|--|
| **Required** | Yes for ingestion |
| **Description** | Google Drive folder ID of the “drop zone” where CSV and other files are placed for processing. |
| **Example** | `1abc...xyz` |
| **Source** | Set in **kaiano-common-utils** config (e.g. env or repo variables) and read as `config.CSV_SOURCE_FOLDER_ID`. |
| **Used by** | `process_new_files.py` |

---

### DJ_SETS_FOLDER_ID

| | |
|--|--|
| **Required** | Yes |
| **Description** | Google Drive folder ID of the main DJ Sets folder that contains year subfolders and the Summary subfolder. |
| **Example** | `1def...uvw` |
| **Source** | kaiano-common-utils config (`config.DJ_SETS_FOLDER_ID`). |
| **Used by** | `process_new_files.py`, `update_deejay_set_collection.py`, `generate_summaries.py` |

---

### OUTPUT_NAME

| | |
|--|--|
| **Required** | Yes for collection build |
| **Description** | Name of the master “DJ Set Collection” Google Sheet. |
| **Example** | `DJ Set Collection` |
| **Source** | kaiano-common-utils config (`config.OUTPUT_NAME`). |
| **Used by** | `update_deejay_set_collection.py` |

---

### TEMP_TAB_NAME

| | |
|--|--|
| **Required** | Yes for collection build |
| **Description** | Name of the temporary tab used while building the collection spreadsheet. |
| **Example** | `Temp` |
| **Source** | kaiano-common-utils config (`config.TEMP_TAB_NAME`). |
| **Used by** | `update_deejay_set_collection.py` |

---

### SUMMARY_TAB_NAME

| | |
|--|--|
| **Required** | Yes for collection build |
| **Description** | Name of the Summary tab in the collection spreadsheet. |
| **Example** | `Summary` |
| **Source** | kaiano-common-utils config (`config.SUMMARY_TAB_NAME`). |
| **Used by** | `update_deejay_set_collection.py` |

---

### SUMMARY_FOLDER_NAME

| | |
|--|--|
| **Required** | Yes for summaries |
| **Description** | Name of the Summary subfolder under the DJ Sets folder (e.g. where “{Year} Summary” sheets live). |
| **Example** | `Summary` |
| **Source** | kaiano-common-utils config (`config.SUMMARY_FOLDER_NAME`). |
| **Used by** | `generate_summaries.py` |

---

### ALLOWED_HEADERS

| | |
|--|--|
| **Required** | Yes for summaries |
| **Description** | List of column names (from set sheets) to include in summary sheets. |
| **Example** | `["Title", "Artist", "Length", "BPM", "Genre", "Year", "Comment"]` |
| **Source** | kaiano-common-utils config (`config.ALLOWED_HEADERS`). |
| **Used by** | `generate_summaries.py` |

---

### desiredOrder

| | |
|--|--|
| **Required** | No (order can default) |
| **Description** | Order of columns in summary sheets (subset of allowed headers). |
| **Example** | `["Title", "Artist", "Length", "BPM", "Genre", "Year", "Comment"]` |
| **Source** | kaiano-common-utils config (`config.desiredOrder`). |
| **Used by** | `generate_summaries.py` |

---

### DEEJAY_SET_COLLECTION_JSON_PATH

| | |
|--|--|
| **Required** | No |
| **Description** | File path where the DJ set collection JSON snapshot is written. |
| **Example** | `v1/deejay-sets/deejay_set_collection.json` |
| **Default** | `v1/deejay-sets/deejay_set_collection.json` (if not set in config). |
| **Source** | kaiano-common-utils config (`config.DEEJAY_SET_COLLECTION_JSON_PATH`), or hardcoded default in `update_deejay_set_collection.py`. |
| **Used by** | `update_deejay_set_collection.py` |

---

## GitHub-only (workflows)

- **KAIANO_API_REPO_TOKEN**: GitHub secret used by **update_dj_set_collection** to clone and push the **kaiano-api** repo when copying the JSON snapshot. Not used by the Python scripts themselves.

---

## Summary table

| Variable | Required | Source (typical) | Scripts |
|----------|----------|------------------|--------|
| GOOGLE_CREDENTIALS_JSON | Yes | GitHub secret / env | All |
| LOGGING_LEVEL | No | GitHub variable / env | All |
| CSV_SOURCE_FOLDER_ID | Yes (ingestion) | kaiano config | process_new_files |
| DJ_SETS_FOLDER_ID | Yes | kaiano config | process_new_files, update_deejay_set_collection, generate_summaries |
| OUTPUT_NAME | Yes (collection) | kaiano config | update_deejay_set_collection |
| TEMP_TAB_NAME | Yes (collection) | kaiano config | update_deejay_set_collection |
| SUMMARY_TAB_NAME | Yes (collection) | kaiano config | update_deejay_set_collection |
| SUMMARY_FOLDER_NAME | Yes (summaries) | kaiano config | generate_summaries |
| ALLOWED_HEADERS | Yes (summaries) | kaiano config | generate_summaries |
| desiredOrder | No | kaiano config | generate_summaries |
| DEEJAY_SET_COLLECTION_JSON_PATH | No | kaiano config / default | update_deejay_set_collection |
