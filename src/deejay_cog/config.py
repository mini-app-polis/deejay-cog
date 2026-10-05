import os

from dotenv import load_dotenv

load_dotenv()

# Google Drive folder IDs
CSV_SOURCE_FOLDER_ID = os.getenv(
    "CSV_SOURCE_FOLDER_ID", "1t4d_8lMC3ZJfSyainbpwInoDta7n69hC"
)
DJ_SETS_FOLDER_ID = os.getenv("DJ_SETS_FOLDER_ID", "1A0tKQ2DBXI1Bt9h--olFwnBNne3am-rL")
VDJ_HISTORY_FOLDER_ID = os.getenv(
    "VDJ_HISTORY_FOLDER_ID", "1HGxEr5ocY9JLtXcJqDRIOD95rXU6QLUW"
)
MUSIC_UPLOAD_SOURCE_FOLDER_ID = os.getenv(
    "MUSIC_UPLOAD_SOURCE_FOLDER_ID",
    "1Iu5TwzOXVqCDef2X8S5TZcFo1NdSHpRU",
)
MUSIC_TAGGING_OUTPUT_FOLDER_ID = os.getenv(
    "MUSIC_TAGGING_OUTPUT_FOLDER_ID",
    "17LjjgX4bFwxR4NOnnT38Aflp8DSPpjOu",
)

# Spreadsheet config
OUTPUT_NAME = os.getenv("OUTPUT_NAME", "DJ Set Collection")
TEMP_TAB_NAME = os.getenv("TEMP_TAB_NAME", "TempClear")
SUMMARY_TAB_NAME = os.getenv("SUMMARY_TAB_NAME", "Summary_Tab")
SUMMARY_FOLDER_NAME = os.getenv("SUMMARY_FOLDER_NAME", "Summary")

# Collection snapshot path (update_deejay_set_collection)
DEEJAY_SET_COLLECTION_JSON_PATH = os.getenv(
    "DEEJAY_SET_COLLECTION_JSON_PATH",
    "v1/deejay-sets/deejay_set_collection.json",
)

# Live history timezone (ingest_live_history)
TIMEZONE = os.getenv("TIMEZONE", "America/Chicago")

# SPOTIPY_REDIRECT_URI and LOGGING_LEVEL are read by common-python-utils
# (mini_app_polis.config, mini_app_polis.logger), not here.

# Summary sheet columns (generate_summaries)
ALLOWED_HEADERS = [
    "title",
    "artist",
    "remix",
    "comment",
    "genre",
    "length",
    "bpm",
    "year",
]
desiredOrder = [
    "Title",
    "Remix",
    "Artist",
    "Comment",
    "Genre",
    "Year",
    "BPM",
    "Length",
]
