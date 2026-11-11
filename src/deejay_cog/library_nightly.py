"""The ``library-backfill`` library job: six steps over the music library,
each with its own mode, settings and report. The library reconcile is not
one of them: it is its own worker mode, ``library-reconcile``, on its own
schedule, so it has the run's whole time to write in.

1. **library-scan** (:mod:`deejay_cog.library_scan`) — files added to and
   removed from the library reach the library registry.
2. **library-trash** (:mod:`deejay_cog.library_trash`) — the copies the
   owner sent to the trash on the duplicates page go to Drive's trash.
3. **library-identify** (:mod:`deejay_cog.library_identify`) — the next
   unidentified files are identified, linked and written with the
   catalog's values.
4. **library-backfill** (:mod:`deejay_cog.library_backfill`) — the next
   chunk of the library is linked to the catalog.
5. **library-sync** (:mod:`deejay_cog.library_sync`) — catalog edits and
   display-rule changes are written to the files they affect.
6. **library-duplicates** (:mod:`deejay_cog.library_duplicates`) — files
   that look like copies of one song are grouped for the duplicates page.

A step other than the backfill that fails is reported as an ERROR finding and the next step runs
anyway; only the run's deadline stops them all. A step due once the shared
run deadline has passed is skipped, not started: it would only fail on its
first request, and the next run does it.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from mini_app_polis import logger as logger_mod
from mini_app_polis.deadline import RunDeadlineReached
from mini_app_polis.lambda_worker import RunOutOfTime

from deejay_cog._pipeline_eval import post_run_finding
from deejay_cog._shared_deadline import shared_deadline_passed
from deejay_cog.library_backfill import BackfillSummary, library_backfill
from deejay_cog.library_duplicates import FLOW_NAME as DUPLICATES_FLOW_NAME
from deejay_cog.library_duplicates import library_duplicates
from deejay_cog.library_identify import FLOW_NAME as IDENTIFY_FLOW_NAME
from deejay_cog.library_identify import library_identify
from deejay_cog.library_scan import FLOW_NAME as SCAN_FLOW_NAME
from deejay_cog.library_scan import library_scan
from deejay_cog.library_sync import FLOW_NAME as SYNC_FLOW_NAME
from deejay_cog.library_sync import library_sync
from deejay_cog.library_trash import FLOW_NAME as TRASH_FLOW_NAME
from deejay_cog.library_trash import library_trash

log = logger_mod.get_logger()


def _step(flow_name: str, step: Callable[..., Any], run_id: str | None) -> None:
    """Run one step; a failure is reported and does not stop the job. Past
    the shared run deadline the step is skipped: the next run does it."""
    if shared_deadline_passed():
        log.warning("%s: skipped, the run's deadline has passed", flow_name)
        return
    try:
        step(run_id=run_id)
    except (RunOutOfTime, RunDeadlineReached):
        raise
    except Exception as exc:  # noqa: BLE001 — the next step still runs
        log.exception("%s: failed; the library job carries on", flow_name)
        try:
            post_run_finding(
                flow_name,
                "ERROR",
                f"{flow_name} failed ({type(exc).__name__}: {exc}); "
                "the rest of the library job carried on",
                run_id=run_id,
            )
        except Exception:  # noqa: BLE001 — the notification is not the job
            log.exception("%s: could not report the failure", flow_name)


def nightly_library_run(*, run_id: str | None = None) -> BackfillSummary:
    """The scan, the trash, the identify step, the backfill's chunk, the
    sync, then the duplicate report.

    ``run_id`` is the queue message id when the Lambda worker runs this.
    The backfill raises as it always has (its own failures are its run's
    failures); the other steps are reported and passed.
    """
    _step(SCAN_FLOW_NAME, library_scan, run_id)
    _step(TRASH_FLOW_NAME, library_trash, run_id)
    _step(IDENTIFY_FLOW_NAME, library_identify, run_id)
    summary = library_backfill(run_id=run_id)
    _step(SYNC_FLOW_NAME, library_sync, run_id)
    _step(DUPLICATES_FLOW_NAME, library_duplicates, run_id)
    return summary
