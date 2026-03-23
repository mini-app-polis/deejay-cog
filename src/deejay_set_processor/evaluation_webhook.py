"""Prefect flow-run webhook bridge for pipeline evaluation findings."""

from __future__ import annotations

import json
import sys
from typing import Any

from kaiano import logger as logger_mod

from deejay_set_processor.pipeline_evaluator import evaluate_pipeline_run

log = logger_mod.get_logger()


def _read_event_payload_from_stdin() -> dict[str, Any]:
    raw = sys.stdin.read().strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        log.exception("evaluation_webhook: failed to parse input JSON")
        return {}


def _extract_flow_run_event_fields(payload: dict[str, Any]) -> dict[str, str]:
    """Handle flat webhook payloads and nested event payloads."""
    resource = payload.get("resource")
    if isinstance(resource, dict):
        payload = {**resource, **payload}

    return {
        "flow_run_id": str(
            payload.get("flow_run_id") or payload.get("id") or ""
        ).strip(),
        "flow_name": str(payload.get("flow_name") or "").strip(),
        "state_name": str(payload.get("state_name") or "").strip(),
        "state_type": str(payload.get("state_type") or "").strip().upper(),
        "start_time": str(payload.get("start_time") or "").strip(),
        "end_time": str(payload.get("end_time") or "").strip(),
    }


def _state_to_severity(state_type: str) -> str:
    normalized = (state_type or "").upper()
    if normalized == "CRASHED":
        return "ERROR"
    if normalized == "FAILED":
        return "WARN"
    return "INFO"


def handle_prefect_flow_run_event(payload: dict[str, Any]) -> None:
    """
    Best-effort handler for Prefect flow run state events.
    Never raises; logs and returns on failure.
    """
    try:
        fields = _extract_flow_run_event_fields(payload)
        flow_run_id = fields["flow_run_id"] or "prefect-unknown-run"
        flow_name = fields["flow_name"] or "unknown-flow"
        state_name = fields["state_name"] or "UNKNOWN"
        state_type = fields["state_type"] or "UNKNOWN"
        collection_update = flow_name == "update-dj-set-collection"

        if state_type in {"FAILED", "CRASHED"}:
            evaluate_pipeline_run(
                run_id=flow_run_id,
                repo="deejay-set-processor-dev",
                sets_imported=0,
                sets_failed=0,
                sets_skipped=0,
                total_tracks=0,
                failed_set_labels=[],
                api_ingest_success=True,
                sets_attempted=0,
                collection_update=collection_update,
                direct_finding_text=f"Flow {flow_name} entered {state_name} state",
                direct_severity=_state_to_severity(state_type),
            )
            return

        evaluate_pipeline_run(
            run_id=flow_run_id,
            repo="deejay-set-processor-dev",
            sets_imported=0,
            sets_failed=0,
            sets_skipped=0,
            total_tracks=0,
            failed_set_labels=[],
            api_ingest_success=True,
            sets_attempted=0,
            collection_update=collection_update,
        )
    except Exception:
        log.exception("evaluation_webhook: failed to handle flow run event")


def main() -> None:
    payload = _read_event_payload_from_stdin()
    handle_prefect_flow_run_event(payload)


if __name__ == "__main__":
    main()
