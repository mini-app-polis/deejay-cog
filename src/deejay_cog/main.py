"""
Application entrypoint for deejay-cog.

Registers a single router-style Prefect deployment (`deejay-cog/deejay-cog`)
and starts a runner loop that polls for scheduled or manually triggered runs.

The router flow dispatches to one of the underlying production flows based
on the `mode` parameter. Callers (watcher-cog, Prefect UI, REST API, CLI)
must pass `mode` explicitly; an unknown or missing mode raises ValueError.

Supported modes:
    - "process-new-files"   → process_new_csv_files_flow
    - "ingest-live-history" → ingest_live_history

Railway start command: python -m deejay_cog.main

All flows run in-process on Railway with full access to environment
variables. No work pool required.

Startup registration is wrapped in ``serve_with_retry``
(``mini_app_polis.serve_resilience``) rather than calling ``prefect.serve()``
directly. Before its runner loop begins, ``serve()`` makes a blocking,
fail-fast HTTP call to Prefect Cloud to register the deployment; an
unguarded transient 503 on that call kills the process at boot. The
wrapper retries transient failures (429/5xx, network errors) with bounded
exponential backoff for 30 minutes, fails fast on configuration errors
(401/403/404), and on give-up posts one ``source="startup"``, CRITICAL
finding before re-raising so Railway's ON_FAILURE restart policy takes
over. See ADR-005.

Crash reporting has two distinct layers, and the distinction matters:

- **Flow-run crashes** — on Railway restart, any in-flight runs are
  interrupted and Prefect Cloud marks them as crashed. The ``on_crashed``
  hooks in each underlying flow report these to evaluator-cog.
- **Registration crashes** — a failure here happens *before any flow run
  exists*, so no hook can fire. ``serve_with_retry`` is what reports
  these. See ADR-001's amended Consequences section.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Literal

import sentry_sdk
from dotenv import load_dotenv
from mini_app_polis.environment import current_environment
from mini_app_polis.serve_resilience import serve_with_retry
from prefect import flow
from prefect.flows import flow as prefect_flow

from deejay_cog._pipeline_eval import REPO
from deejay_cog.ingest_live_history import ingest_live_history
from deejay_cog.process_new_files import process_new_csv_files_flow

#: Supported router modes. Declared as a Literal so Prefect Cloud's
#: "Custom Run" UI renders a dropdown (via the auto-generated JSON-schema
#: enum) instead of a free-form string field. Adding a new mode?
#: 1) add the string here, 2) add it to _MODE_DISPATCH, 3) document it
#: in the module docstring.
DeejayMode = Literal["process-new-files", "ingest-live-history"]

# Map of supported router modes to the underlying flow functions.
_MODE_DISPATCH: dict[str, Any] = {
    "process-new-files": process_new_csv_files_flow,
    "ingest-live-history": ingest_live_history,
}


@flow(name="deejay-cog")
def deejay_router(mode: DeejayMode) -> Any:
    """Single entrypoint flow that dispatches to a sub-flow by `mode`.

    Parameters
    ----------
    mode:
        Which underlying flow to run. Required. One of:
        "process-new-files", "ingest-live-history".

    Raises
    ------
    ValueError
        If `mode` somehow reaches the body without being a recognized
        dispatch key. The Literal annotation should already prevent this
        at Prefect's parameter-validation layer, but we keep the runtime
        guard so the flow never silently no-ops.
    """
    target = _MODE_DISPATCH.get(mode)
    if target is None:
        raise ValueError(
            f"Unknown deejay router mode: {mode!r}. "
            f"Supported modes: {sorted(_MODE_DISPATCH)}"
        )

    return target()


def main() -> None:
    """Register the deejay router flow and start the Prefect runner loop.

    ``serve_with_retry`` blocks for the life of the process on success,
    exactly as ``prefect.serve()`` does. It only returns by raising —
    when deployment registration fails and either the retry ceiling is
    exhausted or the error is a non-retryable configuration error.
    """
    load_dotenv()
    sentry_sdk.init(
        dsn=os.getenv("SENTRY_DSN"),
        environment=current_environment().value,
    )

    src_path = os.environ.get(
        "APP_SOURCE_PATH", str(Path(__file__).parent.parent.parent)
    )

    router = prefect_flow.from_source(
        source=src_path,
        entrypoint="src/deejay_cog/main.py:deejay_router",
    )

    serve_with_retry(router.to_deployment(name="deejay-cog"), repo=REPO)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
