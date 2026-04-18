"""
Shared pipeline-evaluation plumbing for deejay-cog flows.

All five flows in this repo use this module for:
  - Getting a Prefect-aware logger with a stdlib fallback (PIPE-006).
  - Deriving a stable run ID from the Prefect runtime context.
  - Registering Prefect on_failure/on_crashed hooks that post a direct
    finding (severity=WARN for Failed, ERROR for Crashed).
  - Posting exactly one end-of-run finding per successful flow run.

Production flows (process_new_files, ingest_live_history) call these
helpers with the default production_only=True, which gates API posts
behind both the production_only flag AND the presence of
KAIANO_API_BASE_URL + ANTHROPIC_API_KEY.

Local-only and WIP flows (generate_summaries, update_deejay_set_collection,
retag_music) call these helpers with production_only=False, which makes
the hook fire locally (logs the failure) but unconditionally skips the
API post. This prevents local development runs from cluttering the
pipeline_evaluations table regardless of which env vars are set.

Public severities include SUCCESS; evaluator-cog's direct-finding path
only accepts INFO / WARN / ERROR, so SUCCESS is mapped to INFO when
calling evaluate_pipeline_run.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import Any, Literal

from mini_app_polis import logger as logger_mod
from prefect import get_run_logger

_log = logger_mod.get_logger()

Severity = Literal["SUCCESS", "WARN", "ERROR"]

# Keyword names accepted by evaluator_cog.flows.pipeline_eval.evaluate_pipeline_run
# (excluding run_id, repo, which we set here).
_EVALUATOR_KWARGS = frozenset(
    {
        "sets_imported",
        "sets_failed",
        "sets_skipped",
        "total_tracks",
        "failed_set_labels",
        "api_ingest_success",
        "sets_attempted",
        "collection_update",
        "unrecognized_filename_skips",
        "duplicate_csv_count",
        "folders_processed",
        "tabs_written",
        "total_sets",
        "json_snapshot_written",
        "folder_names",
        "source",
    }
)


def get_prefect_logger() -> logging.Logger:
    """
    Return the Prefect run logger if called inside a flow context,
    otherwise the module logger. Satisfies PIPE-006.
    """
    try:
        return get_run_logger()
    except Exception:
        return _log


def get_run_id() -> str:
    """
    Return a stable identifier for the current run.

    Resolution order:
      1. prefect.runtime.flow_run.id — set when running inside a Prefect flow
      2. PREFECT_FLOW_RUN_ID env var — set by the Prefect worker process
      3. "local-run" — fallback for direct invocations

    Deliberately does not consult GITHUB_RUN_ID. GitHub Actions is no
    longer a trigger mechanism for deejay-cog (ecosystem-standards
    PIPE-008, terminology.trigger).
    """
    try:
        from prefect.runtime import flow_run as _flow_run  # lazy import

        rid = getattr(_flow_run, "id", None)
        if rid:
            return str(rid)
    except Exception:
        pass

    env_rid = os.environ.get("PREFECT_FLOW_RUN_ID")
    if env_rid:
        return env_rid

    return "local-run"


def _should_post(production_only: bool) -> bool:
    """Gate: API posts happen only when both flags align."""
    if not production_only:
        return False
    if not os.environ.get("KAIANO_API_BASE_URL"):
        return False
    return bool(os.environ.get("ANTHROPIC_API_KEY"))


def _direct_severity_for_eval(severity: Severity) -> str:
    """Map public SUCCESS to evaluator INFO (direct findings)."""
    if severity == "SUCCESS":
        return "INFO"
    return severity


def _nonzero_extras(counters: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in counters.items():
        if v is None or v is False:
            continue
        if isinstance(v, int | float) and v == 0:
            continue
        if isinstance(v, list | tuple | dict | str) and len(v) == 0:
            continue
        out[k] = v
    return out


def _merge_extras_into_text(text: str, extras: dict[str, Any]) -> str:
    nz = _nonzero_extras(extras)
    if not nz:
        return text
    suffix = "; ".join(f"{k}={v}" for k, v in sorted(nz.items()))
    return f"{text} {suffix}" if text else suffix


def post_run_finding(
    flow_name: str,
    severity: Severity,
    text: str | None = None,
    *,
    production_only: bool = True,
    **raw_counters: Any,
) -> None:
    """
    Emit exactly one pipeline_eval finding for this run.

    Severity classification:
      SUCCESS — run completed end-to-end; no issues a human needs to review.
                Includes "nothing to do" (empty input) and intentional
                skip paths (API URL not set).
      WARN    — run completed but produced results worth a human look
                (e.g. some items failed, some inputs malformed).
      ERROR   — reserved for flow-level failure/crash; only emitted via
                the failure hook, not via this function.

    When production_only=False, this is a no-op regardless of env vars.
    When production_only=True, posts only if KAIANO_API_BASE_URL and
    ANTHROPIC_API_KEY are both set.

    Best-effort: exceptions in the API layer are logged, never raised.

    Counters whose names match evaluate_pipeline_run parameters are passed
    through; remaining keyword arguments are appended to the finding text
    as k=v pairs (non-zero values only) for observability.
    """
    logger = get_prefect_logger()

    if severity == "SUCCESS" and text is None:
        text = "Run completed successfully."

    counters = dict(raw_counters)
    eval_kw: dict[str, Any] = {}
    for key in list(counters.keys()):
        if key in _EVALUATOR_KWARGS:
            eval_kw[key] = counters.pop(key)
    text_final = _merge_extras_into_text(text or "", counters)

    if not _should_post(production_only):
        logger.debug(
            "pipeline_eval finding suppressed (flow=%s severity=%s production_only=%s)",
            flow_name,
            severity,
            production_only,
        )
        return

    try:
        from evaluator_cog.flows.pipeline_eval import evaluate_pipeline_run

        merged = {
            "sets_imported": 0,
            "sets_failed": 0,
            "sets_skipped": 0,
            "total_tracks": 0,
            "failed_set_labels": [],
            "api_ingest_success": True,
            "sets_attempted": 0,
            "collection_update": False,
            "unrecognized_filename_skips": 0,
            "duplicate_csv_count": 0,
            "folders_processed": 0,
            "tabs_written": 0,
            "total_sets": 0,
            "json_snapshot_written": False,
            "folder_names": None,
            "source": "flow_inline",
        }
        merged.update(eval_kw)

        evaluate_pipeline_run(
            run_id=get_run_id(),
            repo="deejay-cog",
            flow_name=flow_name,
            direct_finding_text=text_final,
            direct_severity=_direct_severity_for_eval(severity),
            **merged,
        )
    except Exception:
        logger.exception(
            "Pipeline evaluation post raised unexpectedly (should be best-effort)"
        )


def make_failure_hook(
    flow_name: str,
    *,
    production_only: bool = True,
) -> Callable[..., None]:
    """
    Return a Prefect on_failure/on_crashed hook that posts a direct finding
    with severity=WARN (Failed) or ERROR (Crashed).

    The hook always logs the failure locally. The API post is gated by
    production_only + env vars, same as post_run_finding.

    Returned callable never raises — hooks that raise can mask the
    underlying flow failure.
    """

    def _hook(flow, flow_run, state) -> None:  # noqa: ARG001
        logger = get_prefect_logger()
        try:
            state_name = str(getattr(state, "name", "FAILED"))
            state_type = str(getattr(state, "type", "")).upper()
            severity: Severity = (
                "ERROR"
                if state_type == "CRASHED" or state_name == "Crashed"
                else "WARN"
            )

            logger.error(
                "Flow failure hook fired: flow=%s run_id=%s state=%s production_only=%s",
                flow_name,
                get_run_id(),
                state_name,
                production_only,
            )

            post_run_finding(
                flow_name=flow_name,
                severity=severity,
                text=f"Flow entered {state_name} unexpectedly",
                production_only=production_only,
            )
        except Exception:
            logger.exception("Flow failure hook failed unexpectedly")

    return _hook
