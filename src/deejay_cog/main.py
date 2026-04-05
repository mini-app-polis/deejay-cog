"""
Application entrypoint for deejay-cog.

Registers all deejay-cog flows as Prefect Cloud deployments and starts
a runner loop that polls for scheduled or manually triggered runs.

Railway start command: python -m deejay_cog.main

All flows run in-process on Railway with full access to environment
variables. No work pool required.

On Railway restart, any in-flight runs are interrupted and Prefect Cloud
marks them as crashed. The on_crashed hooks in each flow handle crash
reporting to evaluator-cog automatically.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import sentry_sdk
from dotenv import load_dotenv
from prefect import serve
from prefect.flows import flow as prefect_flow


def main() -> None:
    """Register all flows and start the Prefect runner loop."""
    load_dotenv()
    sentry_sdk.init(dsn=os.getenv("SENTRY_DSN"), environment="production")

    src_path = os.environ.get(
        "APP_SOURCE_PATH", str(Path(__file__).parent.parent.parent)
    )

    process_new = prefect_flow.from_source(
        source=src_path,
        entrypoint="src/deejay_cog/process_new_files.py:process_new_csv_files_flow",
    )
    # generate_summaries and update_collection deferred — not served on Railway
    ingest_history = prefect_flow.from_source(
        source=src_path,
        entrypoint="src/deejay_cog/ingest_live_history.py:ingest_live_history",
    )

    serve(
        process_new.to_deployment(name="process-new-files"),
        ingest_history.to_deployment(name="ingest-live-history"),
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
