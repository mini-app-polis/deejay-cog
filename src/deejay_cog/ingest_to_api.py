import contextlib
import re
from typing import Any

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import IngestSet, IngestTrack
from mini_app_polis.google import GoogleAPI

log = logger_mod.get_logger()


def _parse_length_secs(value: str | None) -> int | None:
    if not value:
        return None
    s = str(value).strip()
    m = re.match(r"^(?P<mm>\d{1,3}):(?P<ss>\d{2})$", s)
    if not m:
        return None
    mm = int(m.group("mm"))
    ss = int(m.group("ss"))
    if ss >= 60:
        return None
    return mm * 60 + ss


def _parse_play_time(value: str | None) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if re.match(r"^\d{1,2}:\d{2}(:\d{2})?$", s):
        return s
    return None


def read_tracks_from_sheet(g: GoogleAPI, spreadsheet_id: str) -> list[dict[str, Any]]:
    """Read raw track rows from a Google Sheet.

    Returns a list of raw row dicts (strings) keyed by known columns.
    Returns [] if sheet is empty or unreadable.
    """
    try:
        meta = g.sheets.get_metadata(spreadsheet_id, fields="sheets(properties(title))")
        sheets = meta.get("sheets") or []
        if not sheets:
            log.warning(f"⚠️ Sheet has no tabs: {spreadsheet_id}")
            return []
        title = sheets[0].get("properties", {}).get("title")
        if not title:
            log.warning(f"⚠️ Sheet tab title missing: {spreadsheet_id}")
            return []
        values = g.sheets.read_values(spreadsheet_id, f"{title}!A:Z")
    except Exception as e:
        log.warning(f"⚠️ Unreadable sheet {spreadsheet_id}: {e}")
        return []

    if not values or len(values) < 2:
        return []

    header = ["" if h is None else str(h).strip() for h in values[0]]
    header_canon = [h.strip().lower() for h in header]

    columns = [
        "label",
        "title",
        "remix",
        "artist",
        "comment",
        "genre",
        "length",
        "bpm",
        "year",
        "play_time",
    ]
    col_index = {c: header_canon.index(c) for c in columns if c in header_canon}

    raw_tracks: list[dict[str, Any]] = []
    for idx, row in enumerate(values[1:], start=1):
        row_out: dict[str, Any] = {"play_order": idx}
        for c in columns:
            i = col_index.get(c)
            if i is None or i >= len(row):
                row_out[c] = ""
                continue
            v = row[i]
            row_out[c] = "" if v is None else str(v).strip()
        raw_tracks.append(row_out)

    return raw_tracks


def build_ingest_tracks(tracks: list[dict]) -> list[IngestTrack]:
    """The tracks of a POST /v1/ingest payload, from raw track dicts.

    A row without a title or an artist is not a track and is skipped.
    """
    out_tracks: list[IngestTrack] = []
    for t in tracks:
        title = str(t.get("title") or "").strip()
        artist = str(t.get("artist") or "").strip()
        if not title or not artist:
            continue

        length_secs = _parse_length_secs(str(t.get("length") or "").strip())

        bpm_raw = str(t.get("bpm") or "").strip()
        try:
            bpm = float(bpm_raw) if bpm_raw else None
        except Exception:
            bpm = None

        year_raw = str(t.get("year") or "").strip()
        try:
            release_year = int(year_raw) if year_raw else None
        except Exception:
            release_year = None

        play_time = _parse_play_time(str(t.get("play_time") or "").strip())

        play_order = t.get("play_order")
        play_order_int = len(out_tracks) + 1
        if play_order is not None:
            with contextlib.suppress(Exception):
                play_order_int = int(play_order)

        out_tracks.append(
            IngestTrack.model_validate(
                {
                    "play_order": play_order_int,
                    "label": (str(t.get("label") or "").strip() or None),
                    "title": title,
                    "remix": (str(t.get("remix") or "").strip() or None),
                    "artist": artist,
                    "comment": (str(t.get("comment") or "").strip() or None),
                    "genre": (str(t.get("genre") or "").strip() or None),
                    "length_secs": length_secs,
                    "bpm": bpm,
                    "release_year": release_year,
                    "play_time": play_time,
                }
            )
        )

    return out_tracks


def build_ingest_payload(
    *,
    set_date: str | None,
    venue: str | None,
    source_file: str,
    tracks: list[dict],
) -> IngestSet:
    """Build the POST /v1/ingest payload from raw track dicts.

    Raises pydantic's ValidationError for a set the API would refuse — no
    date, say — before anything is sent.
    """
    return IngestSet.model_validate(
        {
            "set_date": set_date,
            "venue": venue,
            "source_file": source_file,
            "tracks": build_ingest_tracks(tracks),
        }
    )
