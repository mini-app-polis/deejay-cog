"""Stop a run before the Lambda function's timeout kills it.

Lambda kills a timed-out invocation outright: no ``finally``, no report,
and the handler returns nothing, so the message comes back with nobody
told why. :func:`deadline` raises :class:`RunOutOfTime` a margin early,
inside the run, so it travels the ordinary failure path instead — this
cog's flows do not report themselves, so the handler reports it and names
the message for redelivery.
"""

from __future__ import annotations

import signal
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

#: How long before the function's timeout a run is stopped, so that it can
#: still send its report and the handler can still return. The report is
#: one POST with the API client's own timeout; this leaves room for it.
DEADLINE_MARGIN_SECONDS = 30


class RunOutOfTime(Exception):  # noqa: N818 — named for what happened
    """The run was about to be killed by the function's timeout.

    Lambda kills a timed-out invocation outright: no ``finally``, no
    report, and the handler returns nothing, so the message comes back
    with nobody told why. Raised a margin early instead, inside the run,
    it travels the ordinary failure path — the handler reports it and
    names the message for redelivery.
    """


@contextmanager
def deadline(context: Any) -> Iterator[None]:
    """Raise :class:`RunOutOfTime` a margin before the invocation's timeout.

    SIGALRM, because the run is blocking I/O on the main thread — which is
    where Lambda runs the handler and where the signal is delivered. A
    context without ``get_remaining_time_in_millis`` (a test, a local
    call) gets no deadline.
    """
    remaining = getattr(context, "get_remaining_time_in_millis", None)
    if remaining is None:
        yield
        return
    seconds = remaining() / 1000 - DEADLINE_MARGIN_SECONDS
    if seconds <= 0:
        raise RunOutOfTime("no time left in this invocation to start a run")

    def _expire(signum: int, frame: Any) -> None:  # noqa: ARG001
        raise RunOutOfTime(
            f"stopped {DEADLINE_MARGIN_SECONDS}s before the function timeout"
        )

    previous = signal.signal(signal.SIGALRM, _expire)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
