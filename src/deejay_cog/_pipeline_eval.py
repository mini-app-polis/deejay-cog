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

   That filter applies to ``post_run_finding`` only. A flow that builds a
   :class:`RunReport` chooses what it carries by calling ``count()``, so
   there is nothing to absorb — which is the better end of the same
   trade, since the absorbed list is why a SUCCESS report could be sent
   carrying twelve counters and saying "Run completed successfully."
3. Stamps ``(processor=X.Y.Z)`` — the version of the code that is running —
   onto every message, from ``_version.py``. The library would stamp it
   itself, but it resolves the version from the installed distribution's
   metadata, and the Lambda deploy strips every ``*.dist-info`` from the
   zip; there the lookup fails and the library, by design, stamps nothing.
   ``_version.py`` is source and ships in the zip, and the deploy builds
   from the release tag, so it is the version of what is running. The
   library skips its own stamp when the text already carries one, so
   nothing is stamped twice where the distribution is installed.

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
from dataclasses import dataclass
from typing import Any

from mini_app_polis.pipeline_status import (
    DeliveryReport,
    Severity,
    get_prefect_logger,
    get_run_id,
)
from mini_app_polis.pipeline_status import (
    RunReport as _RunReport,
)
from mini_app_polis.pipeline_status import (
    make_failure_hook as _make_failure_hook,
)
from mini_app_polis.pipeline_status import (
    post_run_finding as _post_run_finding,
)

from deejay_cog._version import __version__

REPO = "deejay-cog"
"""Repo identifier sent on every self-reported finding from this cog."""

VERSION_STAMP = f"(processor={__version__})"
"""The running code's version, in the library's own stamp format."""


def stamp_version(text: str) -> str:
    """Append :data:`VERSION_STAMP` to ``text`` on a line of its own.

    Left alone when empty — an empty report is skipped by the library, and
    a version alone would turn it into a message that says nothing — and
    when already stamped. Any counters the library appends land after the
    stamp, so the last line of a message is its metadata.
    """
    if not text or "(processor=" in text:
        return text
    return f"{text}\n{VERSION_STAMP}"


@dataclass
class RunReport(_RunReport):
    """:class:`mini_app_polis.pipeline_status.RunReport`, version-stamped."""

    def text(self) -> str:
        return stamp_version(super().text())


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
    # The library's default, applied here so it can be stamped.
    if severity == "SUCCESS" and text is None:
        text = "Run completed successfully."
    return _post_run_finding(
        flow_name,
        severity,
        stamp_version(text) if text is not None else None,
        repo=REPO,
        production_only=production_only,
        source=source,
        notable=notable,
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
    "VERSION_STAMP",
    "RunReport",
    "Severity",
    "get_prefect_logger",
    "get_run_id",
    "make_failure_hook",
    "post_run_finding",
    "stamp_version",
]
