"""The intake worker: deejay-cog's new-music mode, on a queue of its own.

A batch of several hundred new files drains NEW_MUSIC_MAX_FILES_PER_RUN at a
time, each run queueing the next, and on the shared deejay queue that held
process-new-files and ingest-live-history behind it for hours. New-music
runs come here instead, from their own FIFO queue; intake stays one run at a
time, since it moves files. This worker runs only new-music: any other mode
it receives is unprocessable, fails and dead-letters, so a routing mistake
shows up as the dead-letter alarm instead of running here.
"""

from __future__ import annotations

from typing import Any

from . import worker

#: The mode the intake worker runs.
INTAKE_MODE = "new-music"


def is_intake_mode(mode: str) -> bool:
    """Whether ``mode`` runs on the intake worker."""
    return mode == INTAKE_MODE


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """The shared handler, refusing every mode but new-music."""
    return worker.lambda_handler(event, context, accepts=is_intake_mode)
