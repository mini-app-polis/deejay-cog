"""Best-effort Claude evaluation of pipeline runs; posts findings to deejay-marvel-api."""

from __future__ import annotations

import json
import os
import re
from typing import Any

from kaiano import logger as logger_mod

log = logger_mod.get_logger()

_JSON_FENCE = re.compile(r"```(?:json)?\s*([\s\S]*?)\s*```", re.IGNORECASE)


def _normalize_finding(item: dict) -> dict:
    """
    Normalize a finding dict from Claude.
    Claude sometimes uses alternative key names instead of
    "finding". Always ensure the "finding" key is present.
    """
    if not item.get("finding"):
        for alt in ("message", "description", "detail", "text"):
            if item.get(alt):
                item["finding"] = item[alt]
                break
    if not item.get("finding"):
        item["finding"] = "No finding text returned by evaluator."
    return item


def _anthropic_messages_create(
    *,
    api_key: str,
    model: str,
    max_tokens: int,
    user_prompt: str,
) -> str:
    import httpx

    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    body: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": user_prompt}],
    }
    with httpx.Client(timeout=120.0) as client:
        r = client.post(url, headers=headers, json=body)
        r.raise_for_status()
        data = r.json()
    blocks = data.get("content") or []
    parts: list[str] = []
    for b in blocks:
        if isinstance(b, dict) and b.get("type") == "text":
            parts.append(str(b.get("text", "")))
    return "".join(parts).strip()


def _parse_findings_from_claude(text: str) -> tuple[list[dict[str, Any]], bool]:
    raw = text.strip()
    m = _JSON_FENCE.search(raw)
    if m:
        raw = m.group(1).strip()
    try:
        parsed_top = json.loads(raw)
    except json.JSONDecodeError:
        return [], False

    if isinstance(parsed_top, dict) and "findings" in parsed_top:
        inner = parsed_top["findings"]
        parsed = inner if isinstance(inner, list) else []
    elif isinstance(parsed_top, list):
        parsed = parsed_top
    else:
        return [], False

    validated: list[dict[str, Any]] = []
    for item in parsed:
        if isinstance(item, dict):
            validated.append(item)
    return [_normalize_finding(item) for item in validated], False


def _parse_findings_json(text: str) -> list[dict[str, Any]]:
    """Backward-compatible alias; returns findings list only."""
    findings, _ = _parse_findings_from_claude(text)
    return findings


def _build_prompt_csv(
    *,
    run_id: str,
    standards_version: str,
    sets_imported: int,
    sets_failed: int,
    sets_skipped: int,
    total_tracks: int,
    failed_set_labels: list[str],
    api_ingest_success: bool,
    sets_attempted: int,
    unrecognized_filename_skips: int,
    duplicate_csv_count: int,
) -> str:
    failed_labels = ", ".join(failed_set_labels) if failed_set_labels else "(none)"
    return f"""You are evaluating a DJ set CSV processing pipeline run against engineering standards v{standards_version}.

CSV PROCESSING evaluation context:
- GitHub Actions run_id: {run_id}
- sets_attempted: CSV files encountered for processing ({sets_attempted})
- sets_imported: successfully processed CSVs (uploaded as Google Sheet, moved to archive) ({sets_imported})
- sets_failed: CSVs renamed with FAILED_ prefix ({sets_failed})
- sets_skipped: non-CSV files moved out of the source folder ({sets_skipped})
- unrecognized_filename_skips: files skipped due to filename format ({unrecognized_filename_skips})
- possible_duplicate_csv: CSVs renamed as possible_duplicate_ and not uploaded ({duplicate_csv_count})
- total_tracks: total track rows across successfully processed sets ({total_tracks})
- failed_set_labels: {failed_labels}
- api_ingest_success: all API ingest attempts succeeded, or none were required ({api_ingest_success})

Respond with ONLY valid JSON (no markdown) in this exact shape:
{{"findings":[{{"dimension":"pipeline_consistency","severity":"INFO|WARN|ERROR","finding":"...","suggestion":""}}]}}

Rules:
- severity must be INFO, WARN, or ERROR (uppercase).
- dimension should be pipeline_consistency unless a different dimension is clearly justified.
- Cover gaps between counts (e.g. attempted vs imported vs failed vs duplicates).
- If api_ingest_success is false, include at least one WARN or ERROR about API ingest.
"""


def _build_prompt_collection(*, run_id: str, standards_version: str) -> str:
    return f"""You are evaluating a DJ set COLLECTION UPDATE pipeline run against engineering standards v{standards_version}.

COLLECTION_UPDATE evaluation context:
- This run rebuilt the master DJ set collection spreadsheet and JSON snapshot.
- No CSV processing happened in this run.
- The Python job reached the evaluation step without raising an uncaught exception (treat as successful completion of the collection job unless counts or context imply otherwise).
- GitHub Actions run_id: {run_id}

Evaluate: did the collection update complete successfully?
- Dimension: pipeline_consistency
- If collection_update=True and there are no failures implied: emit an INFO finding confirming the collection was updated successfully.

Respond with ONLY valid JSON (no markdown) in this exact shape:
{{"findings":[{{"dimension":"pipeline_consistency","severity":"INFO|WARN|ERROR","finding":"...","suggestion":""}}]}}

Rules:
- severity must be INFO, WARN, or ERROR (uppercase).
"""


def evaluate_pipeline_run(
    *,
    run_id: str,
    repo: str,
    sets_imported: int,
    sets_failed: int,
    sets_skipped: int,
    total_tracks: int,
    failed_set_labels: list[str],
    api_ingest_success: bool,
    sets_attempted: int = 0,
    collection_update: bool = False,
    unrecognized_filename_skips: int = 0,
    duplicate_csv_count: int = 0,
    direct_finding_text: str | None = None,
    direct_severity: str | None = None,
) -> None:
    """
    Call Claude, then POST each finding to KAIANO_API_BASE_URL /v1/evaluations.
    Never raises — logs and returns on any failure.
    """
    if not os.environ.get("KAIANO_API_BASE_URL"):
        return

    standards_version = os.environ.get("STANDARDS_VERSION", "6.0")
    model = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
    findings: list[dict[str, Any]] = []

    # Direct findings are used for failure/crash paths where a deterministic
    # signal is better than trying to infer context with Claude.
    if direct_finding_text:
        sev = str(direct_severity or "WARN").upper()
        if sev == "WARNING":
            sev = "WARN"
        if sev not in {"INFO", "WARN", "ERROR"}:
            sev = "WARN"
        findings = [
            {
                "dimension": "pipeline_consistency",
                "severity": sev,
                "finding": direct_finding_text.strip(),
                "suggestion": None,
            }
        ]
    else:
        if not os.environ.get("ANTHROPIC_API_KEY"):
            return
        try:
            if collection_update:
                user_prompt = _build_prompt_collection(
                    run_id=run_id, standards_version=standards_version
                )
            else:
                user_prompt = _build_prompt_csv(
                    run_id=run_id,
                    standards_version=standards_version,
                    sets_imported=sets_imported,
                    sets_failed=sets_failed,
                    sets_skipped=sets_skipped,
                    total_tracks=total_tracks,
                    failed_set_labels=failed_set_labels,
                    api_ingest_success=api_ingest_success,
                    sets_attempted=sets_attempted,
                    unrecognized_filename_skips=unrecognized_filename_skips,
                    duplicate_csv_count=duplicate_csv_count,
                )

            claude_text = _anthropic_messages_create(
                api_key=os.environ["ANTHROPIC_API_KEY"],
                model=model,
                max_tokens=4096,
                user_prompt=user_prompt,
            )
            log.debug("Claude raw response: %s", claude_text[:500])
            findings, _ = _parse_findings_from_claude(claude_text)
        except Exception:
            log.exception("pipeline evaluation: Claude request or parse failed")
            return

    err_ct = warn_ct = info_ct = 0

    try:
        from kaiano.api import KaianoApiClient  # type: ignore[attr-defined]
    except Exception:
        log.exception("pipeline evaluation: Kaiano API client not available")
        return

    api_client = KaianoApiClient.from_env()
    findings_posted = 0
    evaluator_failed = False

    for f in findings:
        if not isinstance(f, dict):
            continue
        sev = str(f.get("severity") or "INFO").upper()
        if sev == "WARNING":
            sev = "WARN"
        if sev == "ERROR":
            err_ct += 1
        elif sev == "WARN":
            warn_ct += 1
        else:
            sev = "INFO"
            info_ct += 1

        finding_text = (f.get("finding") or "").strip()
        if not finding_text:
            log.warning("Skipping finding with empty finding text")
            continue

        payload = {
            "run_id": run_id,
            "repo": repo,
            "dimension": f.get("dimension") or "pipeline_consistency",
            "severity": sev,
            "finding": finding_text,
            "suggestion": f.get("suggestion") or None,
            "standards_version": standards_version,
        }
        try:
            api_client.post("/v1/evaluations", payload)
            findings_posted += 1
        except Exception as e:
            log.warning("pipeline evaluation: failed to POST finding: %s", e)
            evaluator_failed = True

    log.info(
        "🤖 Evaluation complete: %d errors, %d warnings, %d info findings "
        "(%d posted, evaluator_failed=%s)",
        err_ct,
        warn_ct,
        info_ct,
        findings_posted,
        evaluator_failed,
    )


def build_csv_evaluation_prompt(
    *,
    run_id: str,
    standards_version: str,
    sets_imported: int,
    sets_failed: int,
    sets_skipped: int,
    total_tracks: int,
    failed_set_labels: list[str],
    api_ingest_success: bool,
    sets_attempted: int,
    unrecognized_filename_skips: int = 0,
    duplicate_csv_count: int = 0,
) -> str:
    """Exposed for tests (same body as internal CSV prompt)."""
    return _build_prompt_csv(
        run_id=run_id,
        standards_version=standards_version,
        sets_imported=sets_imported,
        sets_failed=sets_failed,
        sets_skipped=sets_skipped,
        total_tracks=total_tracks,
        failed_set_labels=failed_set_labels,
        api_ingest_success=api_ingest_success,
        sets_attempted=sets_attempted,
        unrecognized_filename_skips=unrecognized_filename_skips,
        duplicate_csv_count=duplicate_csv_count,
    )


def build_collection_evaluation_prompt(*, run_id: str, standards_version: str) -> str:
    """Exposed for tests (same body as internal collection prompt)."""
    return _build_prompt_collection(run_id=run_id, standards_version=standards_version)
