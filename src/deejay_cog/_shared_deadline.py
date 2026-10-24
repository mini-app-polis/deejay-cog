"""Whether the shared library's run deadline has passed.

The worker stops a run a margin before the Lambda timeout
(:func:`mini_app_polis.lambda_worker.deadline`) and sets the library's run
deadline (:func:`mini_app_polis.deadline.run_deadline`) one request timeout
earlier, so a Google or Spotify retry loop stops short of the timeout
instead of spending the margin on backoff. Past that deadline every such
call fails fast with ``RunDeadlineReached`` — a ``TimeoutError``, which the
flows' ``except Exception`` blocks would take for an ordinary failure. The
flows check :func:`shared_deadline_passed` and turn it into
:class:`~mini_app_polis.lambda_worker.RunOutOfTime` where it matters
(process_new_files), and roll back under ``without_run_deadline()``.
"""

from __future__ import annotations

from mini_app_polis.deadline import remaining_seconds


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
