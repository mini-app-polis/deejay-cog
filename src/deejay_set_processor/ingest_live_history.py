from __future__ import annotations

import dataclasses
import datetime
import json
import os
import urllib.error
import urllib.request
from typing import Any

import kaiano.config as config
import pytz
from kaiano import logger as logger_mod
from kaiano.google import GoogleAPI
from kaiano.vdj.m3u import M3UToolbox
from prefect import flow, get_run_logger, task

log = logger_mod.get_logger()

try:
    from kaiano.api import KaianoApiClient, KaianoApiError  # type: ignore
except Exception:  # pragma: no cover

    class KaianoApiError(Exception):
        pass

    class KaianoApiClient:
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
            except urllib.error.HTTPError as e:
                raise KaianoApiError(
                    f"HTTP {e.code}: {e.read().decode('utf-8')}"
                ) from e
            except Exception as e:
                raise KaianoApiError(str(e)) from e


def _prefect_logger():
    try:
        return get_run_logger()
    except Exception:
        return log


@dataclasses.dataclass
class LiveIngestSummary:
    plays_sent: int
    plays_failed: int
    files_processed: int
    files_failed: int


def build_live_plays_payload(entries: list) -> dict[str, Any]:
    """
    Convert parsed M3U entries into a POST /v1/live-plays payload.
    Each entry has .dt (datetime string), .title, .artist.
    Entries where dt cannot be parsed to a full datetime are skipped.
    """
    plays = []
    for entry in entries:
        try:
            played_at = datetime.datetime.strptime(entry.dt, "%Y-%m-%d %H:%M")
            tz = pytz.timezone(config.TIMEZONE)
            played_at = tz.localize(played_at)
            played_at_iso = played_at.isoformat()
        except Exception:
            log.warning("Skipping entry with unparseable dt: %s", entry.dt)
            continue

        if not entry.title or not entry.artist:
            log.warning("Skipping entry with missing title or artist: %s", entry)
            continue

        plays.append(
            {
                "played_at": played_at_iso,
                "title": entry.title,
                "artist": entry.artist,
            }
        )

    return {"plays": plays}


@task(name="process-m3u-file")
def process_m3u_file(
    g: GoogleAPI,
    m3u_file: dict[str, Any],
    client: Any,
) -> tuple[int, int, bool]:
    """
    Process one .m3u file: parse, POST /v1/live-plays.

    Returns (plays_sent, plays_failed, file_ok).
    file_ok is False on API or unexpected errors; True on success or empty skip.
    """
    logger = _prefect_logger()
    m3u_tool = M3UToolbox()
    filename = m3u_file.get("name", "")
    logger.info("Processing: %s", filename)
    payload: dict[str, Any] | None = None
    try:
        lines = g.drive.download_m3u_file_data(m3u_file["id"])
        file_date_str = filename.replace(".m3u", "").strip()
        parsed_entries = m3u_tool.parse.parse_m3u_lines(lines, set(), file_date_str)
        parsed_entries = parsed_entries[-4:]

        payload = build_live_plays_payload(parsed_entries)
        if not payload["plays"]:
            logger.info("No valid plays in %s, skipping", filename)
            return (0, 0, True)

        logger.info(
            "Sending %d plays from %s to API...", len(payload["plays"]), filename
        )

        client.post("/v1/live-plays", payload)
        logger.info("✅ Sent %d plays from %s", len(payload["plays"]), filename)
        return (len(payload["plays"]), 0, True)

    except KaianoApiError as e:
        logger.error("❌ API error for %s: %s", filename, e)
        n_failed = len(payload.get("plays", [])) if payload is not None else 0
        return (0, n_failed, False)
    except Exception as e:
        logger.error("❌ Failed to process %s: %s", filename, e)
        return (0, 0, False)


@flow(
    name="ingest-live-history",
    description="Read VDJ .m3u history files from Drive and send plays to deejay-marvel-api.",
)
def ingest_live_history(g: GoogleAPI) -> LiveIngestSummary:
    """
    Read all .m3u files from Drive, parse them, and send plays to POST /v1/live-plays.
    """
    logger = _prefect_logger()
    base_url = os.getenv("KAIANO_API_BASE_URL", "").strip()
    owner_id = (os.getenv("KAIANO_API_OWNER_ID") or os.getenv("OWNER_ID") or "").strip()

    if not base_url:
        logger.warning("KAIANO_API_BASE_URL not set — skipping live history ingest")
        return LiveIngestSummary(
            plays_sent=0, plays_failed=0, files_processed=0, files_failed=0
        )

    client = KaianoApiClient(base_url=base_url, owner_id=owner_id or None)

    m3u_files = list(g.drive.get_all_m3u_files() or [])
    if not m3u_files:
        logger.info("No .m3u files found. Nothing to ingest.")
        return LiveIngestSummary(
            plays_sent=0, plays_failed=0, files_processed=0, files_failed=0
        )

    most_recent = m3u_files[0]
    logger.info("Processing most recent file: %s", most_recent.get("name", ""))
    ps, pf, file_ok = process_m3u_file(g, most_recent, client)

    plays_sent = ps
    plays_failed = pf
    files_processed = 1 if file_ok else 0
    files_failed = 0 if file_ok else 1

    logger.info(
        "Live history ingest complete. plays_sent=%d plays_failed=%d files_processed=%d files_failed=%d",
        plays_sent,
        plays_failed,
        files_processed,
        files_failed,
    )
    return LiveIngestSummary(
        plays_sent=plays_sent,
        plays_failed=plays_failed,
        files_processed=files_processed,
        files_failed=files_failed,
    )


if __name__ == "__main__":
    g = GoogleAPI.from_env()
    ingest_live_history(g)
