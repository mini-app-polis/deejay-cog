from __future__ import annotations

import dataclasses
import datetime
import json
import os
import urllib.error
import urllib.request
from typing import Any

import mini_app_polis.config as config
import pytz
from mini_app_polis import logger as logger_mod
from mini_app_polis.google import GoogleAPI
from mini_app_polis.vdj.m3u import M3UToolbox
from pipeline_evaluator.evaluator import evaluate_pipeline_run
from prefect import flow, get_run_logger, task

log = logger_mod.get_logger()

try:
    from mini_app_polis.api import KaianoApiClient, KaianoApiError  # type: ignore
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


def _handle_flow_failure(flow, flow_run, state) -> None:
    """
    Prefect failure/crash hook: write a direct evaluation finding.
    Never raises.
    """
    logger = _prefect_logger()
    try:
        state_name = str(getattr(state, "name", "FAILED"))
        state_type = str(getattr(state, "type", "")).upper()
        severity = (
            "ERROR" if state_type == "CRASHED" or state_name == "Crashed" else "WARN"
        )
        run_id = str(getattr(flow_run, "id", "") or os.environ.get("GITHUB_RUN_ID", ""))
        if not run_id:
            run_id = "prefect-unknown-run"

        logger.error("Flow failure hook fired: run_id=%s state=%s", run_id, state_name)
        evaluate_pipeline_run(
            run_id=run_id,
            repo="deejay-cog",
            flow_name=flow.name,
            sets_imported=0,
            sets_failed=0,
            sets_skipped=0,
            total_tracks=0,
            failed_set_labels=[],
            api_ingest_success=True,
            sets_attempted=0,
            collection_update=False,
            direct_finding_text=f"Flow entered {state_name} unexpectedly",
            direct_severity=severity,
        )
    except Exception:
        logger.exception("Flow failure hook failed unexpectedly")


@dataclasses.dataclass
class LiveIngestSummary:
    plays_sent: int
    plays_failed: int
    files_processed: int
    files_failed: int


def _evaluate_live_ingest_run(summary: LiveIngestSummary) -> None:
    """Best-effort post-run evaluation for live history ingest."""
    logger = _prefect_logger()
    if not os.environ.get("ANTHROPIC_API_KEY") or not os.environ.get(
        "KAIANO_API_BASE_URL"
    ):
        return
    run_id = os.environ.get("GITHUB_RUN_ID", "local-run")
    try:
        sev = (
            "INFO"
            if summary.plays_failed == 0 and summary.files_failed == 0
            else "WARN"
        )
        evaluate_pipeline_run(
            run_id=run_id,
            repo="deejay-cog",
            flow_name="ingest-live-history",
            sets_imported=0,
            sets_failed=0,
            sets_skipped=0,
            total_tracks=0,
            failed_set_labels=[],
            api_ingest_success=True,
            sets_attempted=0,
            collection_update=False,
            direct_finding_text=(
                "Live history ingest: "
                f"plays_sent={summary.plays_sent}, plays_failed={summary.plays_failed}, "
                f"files_processed={summary.files_processed}, files_failed={summary.files_failed}"
            ),
            direct_severity=sev,
        )
    except Exception:
        logger.exception(
            "Live history pipeline evaluation raised unexpectedly (should be best-effort)"
        )


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
    description="Read VDJ .m3u history files from Drive and send plays to api-kaianolevine-com.",
    on_failure=[_handle_flow_failure],
    on_crashed=[_handle_flow_failure],
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
        summary = LiveIngestSummary(
            plays_sent=0, plays_failed=0, files_processed=0, files_failed=0
        )
        _evaluate_live_ingest_run(summary)
        return summary

    client = KaianoApiClient(base_url=base_url, owner_id=owner_id or None)

    m3u_files = list(g.drive.get_all_m3u_files() or [])
    if not m3u_files:
        logger.info("No .m3u files found. Nothing to ingest.")
        summary = LiveIngestSummary(
            plays_sent=0, plays_failed=0, files_processed=0, files_failed=0
        )
        _evaluate_live_ingest_run(summary)
        return summary

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
    _evaluate_live_ingest_run(summary)
    return summary


if __name__ == "__main__":
    g = GoogleAPI.from_env()
    ingest_live_history(g)
