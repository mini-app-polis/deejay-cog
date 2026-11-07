"""The nightly ``library-backfill`` job: four steps over the music library,
each with its own mode, settings and report.

1. **library-scan** (:mod:`deejay_cog.library_scan`) — files added to and
   removed from the library reach the library registry.
2. **library-identify** (:mod:`deejay_cog.library_identify`) — the next
   unidentified files are identified, linked and written with the
   catalog's values.
3. **library-backfill** (:mod:`deejay_cog.library_backfill`) — the next
   chunk of the library is linked to the catalog.
4. **library-reconcile** (:mod:`deejay_cog.library_reconcile`) — the next
   chunk of linked files is compared with the catalog, by kind of
   difference.

A step other than the backfill that fails is reported as an ERROR finding and the next step runs
anyway; only the run's deadline stops them all.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from mini_app_polis import logger as logger_mod
from mini_app_polis.deadline import RunDeadlineReached
from mini_app_polis.lambda_worker import RunOutOfTime

from deejay_cog._pipeline_eval import post_run_finding
from deejay_cog.library_backfill import BackfillSummary, library_backfill
from deejay_cog.library_identify import FLOW_NAME as IDENTIFY_FLOW_NAME
from deejay_cog.library_identify import library_identify
from deejay_cog.library_reconcile import FLOW_NAME as RECONCILE_FLOW_NAME
from deejay_cog.library_reconcile import library_reconcile
from deejay_cog.library_scan import FLOW_NAME as SCAN_FLOW_NAME
from deejay_cog.library_scan import library_scan

log = logger_mod.get_logger()


def _step(flow_name: str, step: Callable[..., Any], run_id: str | None) -> None:
    """Run one step; a failure is reported and does not stop the job."""
    try:
        step(run_id=run_id)
    except (RunOutOfTime, RunDeadlineReached):
        raise
    except Exception as exc:  # noqa: BLE001 — the next step still runs
        log.exception("%s: failed; the nightly job carries on", flow_name)
        try:
            post_run_finding(
                flow_name,
                "ERROR",
                f"{flow_name} failed ({type(exc).__name__}: {exc}); "
                "the rest of the nightly library run carried on",
                run_id=run_id,
            )
        except Exception:  # noqa: BLE001 — the notification is not the job
            log.exception("%s: could not report the failure", flow_name)


def nightly_library_run(*, run_id: str | None = None) -> BackfillSummary:
    """The scan, the identify step, the backfill's chunk, then the
    reconcile's chunk.

    ``run_id`` is the queue message id when the Lambda worker runs this.
    The backfill raises as it always has (its own failures are its run's
    failures); the other steps are reported and passed.
    """
    _step(SCAN_FLOW_NAME, library_scan, run_id)
    _step(IDENTIFY_FLOW_NAME, library_identify, run_id)
    summary = library_backfill(run_id=run_id)
    _step(RECONCILE_FLOW_NAME, library_reconcile, run_id)
    return summary
