import dataclasses
import json
import os
import re
import urllib.error
import urllib.request
from typing import Any

from mini_app_polis import logger as logger_mod
from mini_app_polis.google import GoogleAPI

log = logger_mod.get_logger()


try:
    # Provided by common-python-utils (per ecosystem standards).
    from mini_app_polis.api import KaianoApiClient, KaianoApiError  # type: ignore
except Exception:  # pragma: no cover

    class KaianoApiError(Exception):
        """TODO: describe this class."""
        pass

    class KaianoApiClient:  # minimal fallback
        """TODO: describe this class."""
        def __init__(self, base_url: str):
            self.base_url = base_url

        def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
            """TODO: describe this function."""
            url = f"{self.base_url.rstrip('/')}{path}"
            body = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=body,
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            try:
                with urllib.request.urlopen(
                    req, timeout=float(os.getenv("DEEJAY_HTTP_TIMEOUT_SECS", "30"))
                ) as resp:
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
    """TODO: describe this class."""
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


def build_ingest_payload(
    *,
    set_date: str,
    venue: str,
    source_file: str,
    tracks: list[dict],
) -> dict[str, Any]:
    """Build POST /v1/ingest payload from raw track dicts."""
    out_tracks: list[dict[str, Any]] = []
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
        try:
            play_order_int = int(play_order)
        except Exception:
            play_order_int = len(out_tracks) + 1

        out_tracks.append(
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

    return {
        "set_date": set_date,
        "venue": venue,
        "source_file": source_file,
        "tracks": out_tracks,
    }


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
    client = KaianoApiClient(base_url=base_url)

    meta_by_id = {m.get("spreadsheet_id"): m for m in set_metadata}

    sets_sent = 0
    sets_failed = 0
    total_tracks = 0
    failures: list[dict] = []

    for ssid in new_spreadsheet_ids:
        meta = meta_by_id.get(ssid) or {}
        label = meta.get("label") or ssid
        raw_tracks = read_tracks_from_sheet(g, ssid)
        payload = build_ingest_payload(
            set_date=meta.get("date") or "",
            venue=meta.get("venue") or "",
            source_file=label,
            tracks=raw_tracks,
        )
        tracks = payload.get("tracks") or []
        if not tracks:
            log.warning(f"⚠️ Empty or unreadable sheet; skipping: {label}")
            continue

        total_tracks += len(tracks)
        log.info(f"🚀 Sending to API: {label} ({len(tracks)} tracks)")
        payload["set_date"] = meta.get("date") or None
        payload["venue"] = meta.get("venue") or None

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
