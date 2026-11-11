"""Deejay-cog wrappers around :mod:`mini_app_polis.pipeline_status`.

This module is a thin shim: every cog in the fleet reports the outcome of
its runs the same way, and the machinery (``post_run_finding``,
``RunReport`` and the shared logger) lives in **common-python-utils** so it
stays in sync across cogs. A report is a message to ``POST /v1/notify``;
nothing is persisted.

The shim provides three conveniences for deejay-cog callers:

1. Pre-binds ``repo="deejay-cog"`` on ``post_run_finding`` so call sites
   don't have to repeat it.
2. Filters out counters in ``_DEEJAY_ABSORBED_KWARGS`` before they reach
   the library, so the message text only surfaces the counters deejay-cog
   actually wants in the human-readable suffix.

   That filter applies to ``post_run_finding`` only. A flow that builds a
   :class:`RunReport` chooses what it carries by calling ``count()``, so
   there is nothing to absorb — which is the better end of the same
   trade, since the absorbed list is why a SUCCESS report could be sent
   carrying twelve counters and saying "Run completed successfully."
3. Passes this cog's version to the library, which stamps it on every
   message as ``(version=X.Y.Z)``. The version comes from ``_version.py``:
   it is source and ships in the Lambda zip (the deploy strips
   ``*.dist-info``), and the deploy builds from the release tag, so it is
   the version of what is running.

The logger re-exported here keeps its library name and returns the
library's shared module logger.

See ``docs/decisions/ADR-0004-best-effort-pipeline-eval.md`` for the
decision record on best-effort posting.

Production flows (``process_new_files``, ``ingest_live_history``,
``retag_music_from_catalog`` and ``fingerprint``) report through
:class:`RunReport` with the default ``production_only=True``, which sends
only when an API base URL resolves for the environment. The worker reports a
run that raised through ``post_run_finding``. Local-only flows
(``generate_summaries``, ``update_deejay_set_collection``) call with
``production_only=False`` so they never send.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from mini_app_polis.api.contract import QueueRequest
from mini_app_polis.pipeline_status import (
    DeliveryReport,
    Severity,
    get_prefect_logger,
)
from mini_app_polis.pipeline_status import (
    RunReport as _RunReport,
)
from mini_app_polis.pipeline_status import (
    post_run_finding as _post_run_finding,
)
from mini_app_polis.pipeline_status import (
    run_report as _run_report,
)

from deejay_cog._version import __version__

REPO = "deejay-cog"
"""Repo identifier sent on every self-reported finding from this cog."""


#: The request that asked for the run in progress (the queue message's
#: ``request``), set by the worker; None for a run no request asked for.
_REQUEST: ContextVar[QueueRequest | None] = ContextVar(
    "deejay_run_request", default=None
)

#: The section of a run report that says who asked for the run.
REQUEST_SECTION = "Requested"


@contextmanager
def requested(request: QueueRequest | None) -> Iterator[None]:
    """Every run report opened inside the block names ``request``: who
    asked for the run and the request's id. None names nothing."""
    token = _REQUEST.set(request)
    try:
        yield
    finally:
        _REQUEST.reset(token)


def name_request(report: _RunReport) -> None:
    """Put the run's request, when there is one, on ``report``: the caller
    (when the API knows it) and the request id. Counters, so they never
    make a quiet run notable or change its severity."""
    request = _REQUEST.get()
    if request is None:
        return
    if request.caller:
        report.count(
            "requested_by", request.caller, label="asked by {}", section=REQUEST_SECTION
        )
    report.count("request_id", request.id, label="request {}", section=REQUEST_SECTION)


@dataclass
class RunReport(_RunReport):
    """:class:`mini_app_polis.pipeline_status.RunReport`, carrying this cog's
    version and the run's request (:func:`name_request`)."""

    version: str | None = __version__

    def __post_init__(self) -> None:
        name_request(self)


@contextmanager
def run_report(flow_name: str, **kwargs: Any) -> Iterator[_RunReport]:
    """:func:`mini_app_polis.pipeline_status.run_report`, naming the run's
    request on the report (:func:`name_request`)."""
    with _run_report(flow_name, **kwargs) as report:
        name_request(report)
        yield report


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
    notable: bool = False,
    **raw_counters: Any,
) -> DeliveryReport:
    """Emit exactly one self-reported finding for this deejay-cog run.

    Signature is identical to
    :func:`mini_app_polis.pipeline_status.post_run_finding` except that
    ``repo`` is pre-bound to ``"deejay-cog"`` and counters in
    :data:`_DEEJAY_ABSORBED_KWARGS` are dropped before being forwarded
    (so they don't clutter the human-readable text suffix).

    Note that a SUCCESS report is logged rather than sent unless the
    caller passes ``notable=True``. Because the absorbed counters never
    reach the message, a notable SUCCESS call should also pass its own
    ``text`` — otherwise it announces "Run completed successfully." and
    says nothing about what the run actually did.
    """
    extras = {k: v for k, v in raw_counters.items() if k not in _DEEJAY_ABSORBED_KWARGS}
    return _post_run_finding(
        flow_name,
        severity,
        text,
        repo=REPO,
        production_only=production_only,
        source=source,
        notable=notable,
        version=__version__,
        **extras,
    )


__all__ = [
    "REPO",
    "REQUEST_SECTION",
    "RunReport",
    "Severity",
    "get_prefect_logger",
    "name_request",
    "post_run_finding",
    "requested",
    "run_report",
]
