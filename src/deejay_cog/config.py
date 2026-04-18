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
LIVE_HISTORY_SPREADSHEET_ID = os.getenv(
    "LIVE_HISTORY_SPREADSHEET_ID",
    "1DpUCQWK3vGGdzUC5JmXVeojqsM_hp7U2DcSEGq6cF-U",
)
PRIVATE_HISTORY_SPREADSHEET_ID = os.getenv(
    "PRIVATE_HISTORY_SPREADSHEET_ID",
    "1z9ZtI5mscyR0sP4KzD2FJjLkSUhR9XBtt8U9PLpBj3M",
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

# Spotify (OAuth redirect; SpotifyAPI.from_env reads env / mini_app_polis.config)
SPOTIPY_REDIRECT_URI = os.getenv(
    "SPOTIPY_REDIRECT_URI",
    "http://127.0.0.1:8888/callback",
)

# Other
SEP_CHARACTERS = "__"
LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "DEBUG").upper()
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
