"""The Lambda entrypoint: one queue message in, one flow run out.

deejay-cog used to register a Prefect router deployment and poll Prefect
Cloud for runs from a resident Railway container. It now runs on Lambda
behind its own queue, ``deejay-jobs``: AWS polls the queue and invokes
:func:`lambda_handler`, so nothing of ours stays awake between DJ events.

The API is the only producer. watcher-cog asks it to run a mode, and it
enqueues one message:

    {"type": "deejay.run", "version": 1, "payload": {"mode": "<mode>"}}

**Nothing is deleted until the work is done.** This code deletes nothing
at all: the event source mapping deletes every record the handler does not
name in ``batchItemFailures``. So "do not delete" means "name it", and every
failure below appends to that list. Get that backwards and a failed run is
silently discarded — the property the queue exists to remove.

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

import json
import os
from collections.abc import Callable
from typing import Any

import sentry_sdk
from mini_app_polis import logger as logger_mod
from mini_app_polis.environment import current_environment

from deejay_cog._pipeline_eval import post_run_finding
from deejay_cog.ingest_live_history import ingest_live_history
from deejay_cog.process_new_files import process_new_csv_files_flow

log = logger_mod.get_logger()

#: Must match api-kaianolevine-com's deejay_dispatch. A mismatch is a
#: message this consumer refuses rather than misreads.
MESSAGE_VERSION = 1
TYPE_RUN = "deejay.run"

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
sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    environment=current_environment().value,
)


class UnprocessableMessage(RuntimeError):
    """The message cannot be handled by this consumer, ever.

    Distinct from a run that failed: retrying will not help. It is still
    returned to the queue rather than dropped, so it reaches the dead-letter
    queue and someone sees what produced it.
    """


def _mode_of(body: str) -> str:
    """The mode a message asks for. Raises UnprocessableMessage otherwise."""
    try:
        message = json.loads(body)
    except ValueError as exc:
        raise UnprocessableMessage(f"body is not JSON: {exc}") from exc
    if not isinstance(message, dict):
        raise UnprocessableMessage("body is not an object")

    version = message.get("version")
    if version != MESSAGE_VERSION:
        raise UnprocessableMessage(
            f"message version {version!r}, this consumer speaks {MESSAGE_VERSION}"
        )

    kind = message.get("type")
    if kind != TYPE_RUN:
        raise UnprocessableMessage(f"unknown message type {kind!r}")

    payload = message.get("payload")
    if not isinstance(payload, dict):
        raise UnprocessableMessage("message carries no payload object")

    mode = payload.get("mode")
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


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ARG001
    """Run each record's flow, and name the records that must come back.

    Never raises. An exception escaping here fails the whole batch; at
    ``batch_size = 1`` that looks identical to reporting the one record,
    right up until the batch size changes. Reporting per record is correct
    at every size, and ``ReportBatchItemFailures`` on the mapping is what
    makes this return shape mean something.
    """
    records = event.get("Records", []) if isinstance(event, dict) else []
    failures: list[dict[str, str]] = []

    for record in records:
        message_id = str(record.get("messageId") or "")
        body = record.get("body") or ""
        attempt = (record.get("attributes") or {}).get("ApproximateReceiveCount", "?")

        try:
            process_message(body, run_id=message_id)
        except UnprocessableMessage as exc:
            log.error("worker: unprocessable message (attempt %s): %s", attempt, exc)
            # Once, on the first receive. Every later receive fails the same
            # way, and where it ends up — the dead-letter queue — has an
            # alarm of its own; five reports of one bad message is noise.
            if attempt in ("1", "?"):
                _report_failure(
                    "deejay-cog", "an unprocessable message", exc, message_id
                )
            failures.append({"itemIdentifier": message_id})
        except Exception as exc:  # noqa: BLE001 — every failure is a retry
            log.exception("worker: run failed (attempt %s)", attempt)
            _report_failure(_flow_name_for(body), "a queued run", exc, message_id)
            failures.append({"itemIdentifier": message_id})

    if failures:
        log.warning(
            "worker: %d of %d record(s) returned to the queue",
            len(failures),
            len(records),
        )

    # Anything absent from this list is deleted by the mapping. An empty
    # list means "all of it is done" — true only because every failure
    # above appended to it.
    return {"batchItemFailures": failures}
