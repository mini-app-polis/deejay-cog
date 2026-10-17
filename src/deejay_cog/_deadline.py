"""Stop a run before the Lambda function's timeout kills it.

Lambda kills a timed-out invocation outright: no ``finally``, no report,
and the handler returns nothing, so the message comes back with nobody
told why. :func:`deadline` raises :class:`RunOutOfTime` a margin early,
inside the run, so it travels the ordinary failure path instead — this
cog's flows do not report themselves, so the handler reports it and names
the message for redelivery.

It also sets the shared library's run deadline
(:func:`mini_app_polis.deadline.run_deadline`) one request timeout earlier,
so a Google or Spotify retry loop stops short of the timeout instead of
spending the margin on backoff. Past that deadline every such call fails
fast with ``RunDeadlineReached`` — a ``TimeoutError``, which the flows'
``except Exception`` blocks would take for an ordinary failure. The flows
turn it into :class:`RunOutOfTime` where it matters (process_new_files),
and roll back under ``without_run_deadline()``.
"""

from __future__ import annotations

import signal
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from googleapiclient.http import DEFAULT_HTTP_TIMEOUT_SEC
from mini_app_polis.deadline import remaining_seconds, run_deadline

#: How long before the function's timeout a run is stopped, so that it can
#: still send its report and the handler can still return. The report is
#: one POST with the API client's own timeout; this leaves room for it.
DEADLINE_MARGIN_SECONDS = 30

#: How much earlier still the shared library's retries stop. The library
#: checks its deadline between attempts, never during one, so a request
#: started just before it can run for the client's whole timeout.
REQUEST_HEADROOM_SECONDS = DEFAULT_HTTP_TIMEOUT_SEC


class RunOutOfTime(BaseException):  # noqa: N818 — named for what happened
    """The run was about to be killed by the function's timeout.

    Lambda kills a timed-out invocation outright: no ``finally``, no
    report, and the handler returns nothing, so the message comes back
    with nobody told why. Raised a margin early instead, inside the run,
    it travels the ordinary failure path — the handler reports it and
    names the message for redelivery.

    A ``BaseException``, like ``KeyboardInterrupt``, because the flows
    contain their per-file failures with ``except Exception``. As an
    ``Exception`` it was caught by the first of those it met: the file in
    hand was renamed ``FAILED_``, the sweep carried on past the deadline,
    and the handler reported success until Lambda killed the run anyway.
    Only the handler catches it.
    """


def shared_deadline_passed() -> bool:
    """Whether the shared library's run deadline has passed.

    Past it, every Google and Spotify call fails fast with
    ``RunDeadlineReached`` — and the flows' ``except Exception`` blocks
    take that for an ordinary failure: an unreadable sheet reads as a set
    with no tracks, a refused listing as a possible duplicate. So an
    outcome reached past the deadline is not to be trusted, and the flows
    check this before acting on one. Always ``False`` with no deadline set.
    """
    remaining = remaining_seconds()
    return remaining is not None and remaining <= 0


@contextmanager
def deadline(context: Any) -> Iterator[None]:
    """Raise :class:`RunOutOfTime` a margin before the invocation's timeout.

    Inside, the shared library's run deadline is set a further
    :data:`REQUEST_HEADROOM_SECONDS` earlier, so its retries stop before
    the alarm does.

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
        with run_deadline(max(0, seconds - REQUEST_HEADROOM_SECONDS)):
            yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
