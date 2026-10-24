"""The Lambda entrypoint: one queue message in, one flow run out.

deejay-cog used to register a Prefect router deployment and poll Prefect
Cloud for runs from a resident Railway container. It now runs on Lambda
behind its own FIFO queue, declared in mini-app-polis/infra: AWS polls the queue and invokes
:func:`lambda_handler`, so nothing of ours stays awake between DJ events.

The API is the only producer. watcher-cog asks it to run a mode, and it
enqueues one message:

    {"type": "deejay.run", "version": 1, "payload": {"mode": "<mode>"}}

**Nothing is deleted until the work is done.** This code deletes nothing
at all: the event source mapping deletes every record the handler does not
name in ``batchItemFailures``. So "do not delete" means "name it", and
:func:`mini_app_polis.lambda_worker.process_records` names every record
whose run raised. Get that backwards and a failed run is silently
discarded — the property the queue exists to remove.

**An unrecognised message is a producer bug.** The queue is deejay-cog's
alone, so a type, version or mode this consumer does not speak was
enqueued wrongly. It is reported back, exhausts its receives and lands in
the dead-letter queue where someone can see what produced it, rather than
being guessed at or dropped.

**One report per run, from the flow.** Each flow builds and sends its own
``RunReport``, and a message is exactly one flow run, so the flow's report
is the per-job report — the evaluator's rule that the report belongs in the
consumer rather than the handler exists because its handler runs N times
per job, which is not true here. What this module adds is the run id: the
SQS message id, which is also what the API answered the trigger with, so a
run can be traced from watcher's log to the report without a join table.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from googleapiclient.http import DEFAULT_HTTP_TIMEOUT_SEC
from mini_app_polis import load_secrets, timing
from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import DeejayRunMessage
from mini_app_polis.deadline import RunDeadlineReached
from mini_app_polis.lambda_worker import (
    RunOutOfTime,
    UnprocessableMessage,
    all_failed,
    deadline,
    parse_message,
    process_records,
)
from mini_app_polis.sentry import init_sentry

from deejay_cog._pipeline_eval import post_run_finding
from deejay_cog.ingest_live_history import ingest_live_history
from deejay_cog.process_new_files import process_new_csv_files_flow

log = logger_mod.get_logger()

#: How long before the function's timeout a run is stopped, so that it can
#: still send its report and the handler can still return. The report is
#: one POST with the API client's own timeout; this leaves room for it.
DEADLINE_MARGIN_SECONDS = 30

#: How much earlier still the shared library's Google and Spotify retries
#: stop. The library checks its deadline between attempts, never during one,
#: so a request started just before it can run for the client's whole
#: timeout.
REQUEST_HEADROOM_SECONDS = DEFAULT_HTTP_TIMEOUT_SEC

#: The router, formerly ``deejay_router`` in ``main.py``. A mode here is a
#: mode the API's ``DeejayRunRequest`` must also accept — the API rejects an
#: unknown one with a 422 before it is enqueued, and this refuses one that
#: got past it anyway.
MODES: dict[str, Callable[..., Any]] = {
    "process-new-files": process_new_csv_files_flow,
    "ingest-live-history": ingest_live_history,
}

#: The flow name a failure is reported under, per mode — the same names the
#: flows' own run reports use, so a crash and a clean run of one flow land
#: under one heading.
FLOW_NAMES = {
    "process-new-files": "process-new-csv-files",
    "ingest-live-history": "ingest-live-history",
}

# At import, not per invocation. A Lambda container is reused across
# invocations, so this runs once per cold start; initialising per call
# would pay the setup repeatedly and register duplicate integrations.
# An empty DSN in production logs an ERROR rather than passing silently
# (CD-038); the service tag tells this cog's events apart in the shared project.
init_sentry("deejay-cog")


def _mode_of(body: str) -> str:
    """The mode a message asks for. Raises UnprocessableMessage otherwise."""
    message = parse_message(body, DeejayRunMessage)
    mode = message.payload.mode
    if mode not in MODES:
        raise UnprocessableMessage(f"unknown mode {mode!r}; supported: {sorted(MODES)}")
    return mode


def process_message(body: str, *, run_id: str) -> None:
    """Run the flow one message asks for. Raises if it must be redelivered.

    A flow that raises is retried as a whole. That is safe for both flows
    because both are sweeps of what is currently in Drive: a CSV that was
    archived before the failure is not in the source folder the second
    time, and live history re-sends the most recent file's plays, which
    the API deduplicates.
    """
    mode = _mode_of(body)
    log.info("worker: run %s mode=%s", run_id, mode)
    MODES[mode](run_id=run_id)


def _report_failure(flow_name: str, what: str, exc: BaseException, run_id: str) -> None:
    """Say a run died, in the one place someone is watching.

    Replaces the Prefect ``on_failure`` / ``on_crashed`` hooks. A flow that
    raises never reaches its own report, so without this a failed run would
    be visible only in CloudWatch and, three receives later, the DLQ alarm.
    """
    try:
        post_run_finding(
            flow_name,
            "ERROR",
            f"{what} failed: {type(exc).__name__}: {exc}",
            source="queue_consumer",
            run_id=run_id,
        )
    except Exception:  # noqa: BLE001 — the notification is not the job
        log.exception("worker: could not report the failure")


def _flow_name_for(body: str) -> str:
    """Best-effort flow name for a failure report. Never raises."""
    try:
        return FLOW_NAMES[_mode_of(body)]
    except Exception:  # noqa: BLE001 — naming a report must not fail it
        return "deejay-cog"


def _mode_label(body: str) -> str:
    """The message's mode, for the timing line. Never raises."""
    try:
        return _mode_of(body)
    except Exception:  # noqa: BLE001 — a label must not fail the run
        return "unreadable"


def _attempt(record: dict[str, Any]) -> str:
    """Which receive of this message this is, or "?" when unknown."""
    return str((record.get("attributes") or {}).get("ApproximateReceiveCount", "?"))


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Run each record's flow, and name the records that must come back.

    Never raises. An exception escaping here fails the whole batch; at
    ``batch_size = 1`` that looks identical to reporting the one record,
    right up until the batch size changes. Reporting per record is correct
    at every size, and ``ReportBatchItemFailures`` on the mapping is what
    makes this return shape mean something.
    """
    # Settings as Doppler holds them now, not as they were at cold start: a
    # warm container otherwise ran on stale secrets and a stale
    # LOGGING_LEVEL until the next deploy. Cheap — one GetParameters call —
    # and a failed SSM read keeps the values already loaded. A required
    # parameter that has gone missing fails every record, so they come back
    # and the dead-letter queue sees them, rather than running half-configured.
    try:
        load_secrets(refresh=True)
    except Exception:  # noqa: BLE001 — nothing can run without its settings
        log.exception("worker: could not refresh settings from SSM")
        return all_failed(event)

    def handle(record: dict[str, Any]) -> None:
        message_id = str(record.get("messageId") or "")
        body = record.get("body") or ""
        attempt = _attempt(record)

        # One timing line per record: how much of the run was waiting,
        # and on what (mini_app_polis.timing).
        with timing.invocation(
            cog="deejay", mode=_mode_label(body), attempt=attempt
        ) as timed:
            try:
                # A run that outlives the function's timeout is killed outright —
                # no report, and the message comes back with nobody told why.
                # This stops it a margin early so the failure is the ordinary
                # kind (PIPE-020).
                with deadline(
                    context,
                    margin_seconds=DEADLINE_MARGIN_SECONDS,
                    request_headroom_seconds=REQUEST_HEADROOM_SECONDS,
                ):
                    process_message(body, run_id=message_id)
            except UnprocessableMessage:
                timed.label(outcome="unprocessable")
                raise
            except (RunOutOfTime, RunDeadlineReached) as exc:
                timed.label(outcome="deadline")
                # RunOutOfTime is a BaseException, so that no flow's
                # ``except Exception`` swallows it; caught here and nowhere
                # else. RunDeadlineReached is the shared library refusing a
                # call past the run deadline, raised where no flow catches it
                # (listing the drop zone, say). Either way the sweep stopped
                # mid-way, and the redelivery picks up whatever it left in
                # the drop zone.
                log.error("worker: run stopped at the deadline (attempt %s)", attempt)
                _report_failure(_flow_name_for(body), "a queued run", exc, message_id)
                raise
            except Exception as exc:
                timed.label(outcome="failed")
                log.exception("worker: run failed (attempt %s)", attempt)
                _report_failure(_flow_name_for(body), "a queued run", exc, message_id)
                raise
            timed.label(outcome="ok")

    def on_unprocessable(exc: UnprocessableMessage, record: dict[str, Any]) -> None:
        # Once, on the first receive. Every later receive fails the same way,
        # and where it ends up — the dead-letter queue — has an alarm of its
        # own; three reports of one bad message is noise.
        if _attempt(record) in ("1", "?"):
            _report_failure(
                "deejay-cog",
                "an unprocessable message",
                exc,
                str(record.get("messageId") or ""),
            )

    result = process_records(event, handle, on_unprocessable=on_unprocessable)
    failures = result["batchItemFailures"]
    if failures:
        records = event.get("Records", []) if isinstance(event, dict) else []
        log.warning(
            "worker: %d of %d record(s) returned to the queue",
            len(failures),
            len(records),
        )
    # Anything absent from this list is deleted by the mapping.
    return result
