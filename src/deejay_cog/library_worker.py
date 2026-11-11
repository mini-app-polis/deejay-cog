"""The library worker: deejay-cog's library-* modes, on a queue of their own.

A library run works until its deadline and is delivered again, so on the
shared deejay queue it held up every message behind it, live-play ingest
included. The library modes run here instead, from their own FIFO queue,
with a longer timeout; process-new-files and ingest-live-history stay on the
shared worker. This worker runs only library-* modes: any other mode it
receives is unprocessable, fails and dead-letters, so a routing mistake
shows up as the dead-letter alarm instead of running here.
"""

from __future__ import annotations

from typing import Any

from . import worker

#: Modes starting with this run on the library worker.
LIBRARY_MODE_PREFIX = "library-"


def is_library_mode(mode: str) -> bool:
    """Whether ``mode`` runs on the library worker."""
    return mode.startswith(LIBRARY_MODE_PREFIX)


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """The shared handler, refusing every mode but the library's."""
    return worker.lambda_handler(event, context, accepts=is_library_mode)
