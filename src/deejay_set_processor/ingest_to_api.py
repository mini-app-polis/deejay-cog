import dataclasses
import json
import os
import re
import urllib.error
import urllib.request
from typing import Any

from kaiano import logger as logger_mod
from kaiano.google import GoogleAPI

log = logger_mod.get_logger()


try:
    # Preferred: provided by kaiano-common-utils (per ecosystem standards).
    from kaiano.api import KaianoApiClient, KaianoApiError  # type: ignore
except Exception:  # pragma: no cover

    class KaianoApiError(Exception):
        pass

    class KaianoApiClient:  # minimal fallback
        def __init__(self, base_url: str, owner_id: str | None = None):
            self.base_url = base_url.rstrip("/")
            self.owner_id = owner_id

        def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
            url = f"{self.base_url}{path}"
            body = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=body,
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = resp.read().decode("utf-8") if resp else ""
                    return json.loads(data) if data else {}
            except urllib.error.HTTPError as e:  # pragma: no cover
                raise KaianoApiError(
                    f"HTTP {e.code}: {e.read().decode('utf-8')}"
                ) from e
            except Exception as e:  # pragma: no cover
                raise KaianoApiError(str(e)) from e


@dataclasses.dataclass
class IngestSummary:
    sets_sent: int
    sets_failed: int
    total_tracks: int
    failures: list[dict]  # {"label": str, "error": str}


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


def parse_track_row(
    row: list[Any],
    col_index: dict[str, int],
    *,
    play_order: int,
) -> dict[str, Any] | None:
    """Parse a single track row using case-insensitive column mapping.

    Returns a normalized track dict, or None if required fields are missing.
    """

    def _cell(name: str) -> str:
        i = col_index.get(name, -1)
        if i < 0 or i >= len(row):
            return ""
        v = row[i]
        return "" if v is None else str(v).strip()

    title = _cell("title")
    artist = _cell("artist")
    if not title or not artist:
        return None

    length_secs = _parse_length_secs(_cell("length"))

    bpm_raw = _cell("bpm")
    try:
        bpm = float(bpm_raw) if bpm_raw else None
    except Exception:
        bpm = None

    year_raw = _cell("year")
    try:
        release_year = int(year_raw) if year_raw else None
    except Exception:
        release_year = None

    play_time = _parse_play_time(_cell("play_time"))

    out: dict[str, Any] = {
        "play_order": play_order,
        "label": _cell("label") or None,
        "title": title,
        "remix": _cell("remix") or None,
        "artist": artist,
        "comment": _cell("comment") or None,
        "genre": _cell("genre") or None,
        "length_secs": length_secs,
        "bpm": bpm,
        "release_year": release_year,
        "play_time": play_time,
    }
    return out


def _read_tracks_from_sheet(g: GoogleAPI, spreadsheet_id: str) -> list[dict[str, Any]]:
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
    col_index: dict[str, int] = {}
    for c in columns:
        if c in header_canon:
            col_index[c] = header_canon.index(c)

    tracks: list[dict[str, Any]] = []
    for idx, row in enumerate(values[1:], start=1):
        parsed = parse_track_row(row, col_index, play_order=idx)
        if parsed is None:
            continue
        tracks.append(parsed)
    return tracks


def ingest_new_sets_to_api(
    g: GoogleAPI,
    new_spreadsheet_ids: list[str],
    set_metadata: list[dict],
) -> IngestSummary:
    """
    For each newly processed set, read track data from Google Sheets
    and send to deejay-marvel-api via POST /v1/ingest.

    new_spreadsheet_ids: list of Google Sheets IDs for sets processed
                         in this pipeline run
    set_metadata: list of dicts with keys:
                  spreadsheet_id, date (YYYY-MM-DD), venue, label
    """
    base_url = os.getenv("KAIANO_API_BASE_URL", "").strip()
    owner_id = (os.getenv("KAIANO_API_OWNER_ID") or os.getenv("OWNER_ID") or "").strip()
    client = KaianoApiClient(base_url=base_url, owner_id=owner_id or None)

    meta_by_id = {m.get("spreadsheet_id"): m for m in set_metadata}

    sets_sent = 0
    sets_failed = 0
    total_tracks = 0
    failures: list[dict] = []

    for ssid in new_spreadsheet_ids:
        meta = meta_by_id.get(ssid) or {}
        label = meta.get("label") or ssid
        tracks = _read_tracks_from_sheet(g, ssid)
        if not tracks:
            log.warning(f"⚠️ Empty or unreadable sheet; skipping: {label}")
            continue

        total_tracks += len(tracks)
        log.info(f"🚀 Sending to API: {label} ({len(tracks)} tracks)")

        payload = {
            "set_date": meta.get("date") or None,
            "venue": meta.get("venue") or None,
            "source_file": label,
            "tracks": tracks,
        }
        if owner_id:
            payload["owner_id"] = owner_id

        try:
            client.post("/v1/ingest", payload)
            sets_sent += 1
            log.info(f"✅ Ingested: {label}")
        except KaianoApiError as e:
            sets_failed += 1
            err = str(e)
            log.info(f"❌ Failed to ingest: {label} — {err}")
            failures.append({"label": label, "error": err})

    return IngestSummary(
        sets_sent=sets_sent,
        sets_failed=sets_failed,
        total_tracks=total_tracks,
        failures=failures,
    )
