from __future__ import annotations

import dataclasses
import datetime
import os
from typing import Any

import pytz
from mini_app_polis import logger as logger_mod
from mini_app_polis.api import KaianoApiClient, KaianoApiError
from mini_app_polis.google import GoogleAPI
from mini_app_polis.vdj.m3u import M3UToolbox
from prefect import flow, task

import deejay_cog.config as config
from deejay_cog._pipeline_eval import (
    get_prefect_logger,
    make_failure_hook,
    post_run_finding,
)

log = logger_mod.get_logger()


@dataclasses.dataclass
class LiveIngestSummary:
    plays_sent: int
    plays_failed: int
    files_processed: int
    files_failed: int


def _success_text(
    summary: LiveIngestSummary,
    *,
    base_url_set: bool,
    had_files: bool,
) -> str:
    if not base_url_set:
        return "Run completed successfully. Skipped (KAIANO_API_BASE_URL not set)."
    if not had_files:
        return "Run completed successfully. No .m3u files found."
    return "Run completed successfully."


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


@task(name="process-m3u-file", retries=2, retry_delay_seconds=10)
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
    logger = get_prefect_logger()
    m3u_tool = M3UToolbox()
    filename = m3u_file.get("name", "")
    logger.info("Processing: %s", filename)
    payload: dict[str, Any] | None = None
    try:
        lines = g.drive.download_m3u_file_data(m3u_file["id"])
        file_date_str = filename.replace(".m3u", "").strip()
        parsed_entries = m3u_tool.parse.parse_m3u_lines(lines, set(), file_date_str)

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
    description="Read VDJ .m3u history files from Drive and send plays to api-kaianolevine-com.",
    on_failure=[make_failure_hook("ingest-live-history")],
    on_crashed=[make_failure_hook("ingest-live-history")],
)
def ingest_live_history() -> LiveIngestSummary:
    """
    Read .m3u files from Drive, parse them, and send plays to POST /v1/live-plays.

    Processes only the most-recent .m3u file, not all files.
    """
    logger = get_prefect_logger()
    g = GoogleAPI.from_env()
    base_url = os.getenv("KAIANO_API_BASE_URL", "").strip()
    m3u_files: list[dict[str, Any]] = []

    if not base_url:
        logger.warning("KAIANO_API_BASE_URL not set — skipping live history ingest")
        summary = LiveIngestSummary(
            plays_sent=0, plays_failed=0, files_processed=0, files_failed=0
        )
    else:
        client = KaianoApiClient(base_url=base_url)
        m3u_files = list(g.drive.get_all_m3u_files() or [])
        if not m3u_files:
            logger.info("No .m3u files found. Nothing to ingest.")
            summary = LiveIngestSummary(
                plays_sent=0, plays_failed=0, files_processed=0, files_failed=0
            )
        else:
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
            summary = LiveIngestSummary(
                plays_sent=plays_sent,
                plays_failed=plays_failed,
                files_processed=files_processed,
                files_failed=files_failed,
            )

    if summary.plays_failed == 0 and summary.files_failed == 0:
        post_run_finding(
            flow_name="ingest-live-history",
            severity="SUCCESS",
            text=_success_text(
                summary, base_url_set=bool(base_url), had_files=bool(m3u_files)
            ),
            plays_sent=summary.plays_sent,
            plays_failed=0,
            files_processed=summary.files_processed,
            files_failed=0,
        )
    else:
        post_run_finding(
            flow_name="ingest-live-history",
            severity="WARN",
            text=(
                "Completed with issues: "
                f"plays_failed={summary.plays_failed}, "
                f"files_failed={summary.files_failed}. "
                "Check the most recent .m3u file for parse or upload errors."
            ),
            plays_sent=summary.plays_sent,
            plays_failed=summary.plays_failed,
            files_processed=summary.files_processed,
            files_failed=summary.files_failed,
        )

    return summary


if __name__ == "__main__":
    ingest_live_history()
