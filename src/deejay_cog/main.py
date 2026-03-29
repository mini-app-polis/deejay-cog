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

import sentry_sdk
from dotenv import load_dotenv
from prefect import serve

from deejay_cog.generate_summaries import generate_summaries_flow
from deejay_cog.ingest_live_history import ingest_live_history
from deejay_cog.process_new_files import process_new_csv_files_flow
from deejay_cog.update_deejay_set_collection import generate_dj_set_collection


def main() -> None:
    """Register all flows and start the Prefect runner loop."""
    load_dotenv()
    sentry_sdk.init(dsn=os.getenv("SENTRY_DSN"), environment="production")

    serve(
        process_new_csv_files_flow.to_deployment(name="process-new-files"),
        generate_summaries_flow.to_deployment(name="generate-summaries"),
        generate_dj_set_collection.to_deployment(name="update-deejay-set-collection"),
        ingest_live_history.to_deployment(name="ingest-live-history"),
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
