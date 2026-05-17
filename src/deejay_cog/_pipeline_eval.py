"""Deejay-cog wrappers around :mod:`mini_app_polis.pipeline_status`.

This module is a thin shim: every cog in the Kaiano ecosystem self-reports
the outcome of its runs the same way, and the actual machinery
(``post_run_finding``, ``make_failure_hook``, ``get_run_id``,
``get_prefect_logger``) lives in **common-python-utils** so it stays in
sync across cogs.

The shim provides two conveniences for deejay-cog callers:

1. Pre-binds ``repo="deejay-cog"`` on ``post_run_finding`` and
   ``make_failure_hook`` so call sites don't have to repeat it.
2. Filters out counters in ``_DEEJAY_ABSORBED_KWARGS`` before they reach
   the library, so the finding text only surfaces the counters
   deejay-cog actually wants in the human-readable suffix. Other cogs
   are free to pass any counters they like — flexibility belongs to the
   cog, not the library.

See ``docs/decisions/ADR-004-best-effort-pipeline-eval.md`` for the
decision record on best-effort posting.

Production flows (``process_new_files``, ``ingest_live_history``) call
these helpers with the default ``production_only=True``, which gates
API posts behind both the flag AND ``KAIANO_API_BASE_URL``. Local-only
and WIP flows (``generate_summaries``, ``update_deejay_set_collection``,
``retag_music``) call with ``production_only=False`` so they never POST
regardless of which env vars are set.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from mini_app_polis.pipeline_status import (
    Severity,
    get_prefect_logger,
    get_run_id,
)
from mini_app_polis.pipeline_status import (
    make_failure_hook as _make_failure_hook,
)
from mini_app_polis.pipeline_status import (
    post_run_finding as _post_run_finding,
)

REPO = "deejay-cog"
"""Repo identifier sent on every self-reported finding from this cog."""

# Counters that callers may pass as kwargs but which deejay-cog does not
# want surfaced in the human-readable finding text. They originated as
# parameters to the (now-deprecated) evaluator-cog LLM prompt path; they
# remain useful for callers to compute "real_issue" classification, but
# we drop them before delegating to the library.
_DEEJAY_ABSORBED_KWARGS = frozenset(
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
    }
)


def post_run_finding(
    flow_name: str,
    severity: Severity,
    text: str | None = None,
    *,
    production_only: bool = True,
    source: str = "flow_inline",
    **raw_counters: Any,
) -> None:
    """Emit exactly one self-reported finding for this deejay-cog run.

    Signature is identical to
    :func:`mini_app_polis.pipeline_status.post_run_finding` except that
    ``repo`` is pre-bound to ``"deejay-cog"`` and counters in
    :data:`_DEEJAY_ABSORBED_KWARGS` are dropped before being forwarded
    (so they don't clutter the human-readable text suffix).
    """
    extras = {k: v for k, v in raw_counters.items() if k not in _DEEJAY_ABSORBED_KWARGS}
    _post_run_finding(
        flow_name,
        severity,
        text,
        repo=REPO,
        production_only=production_only,
        source=source,
        **extras,
    )


def make_failure_hook(
    flow_name: str,
    *,
    production_only: bool = True,
) -> Callable[..., None]:
    """Return a Prefect ``on_failure`` / ``on_crashed`` hook for this cog.

    Pre-binds ``repo="deejay-cog"`` on the library helper.
    """
    return _make_failure_hook(flow_name, repo=REPO, production_only=production_only)


__all__ = [
    "REPO",
    "Severity",
    "get_prefect_logger",
    "get_run_id",
    "make_failure_hook",
    "post_run_finding",
]
