# ADR-001: Run Prefect flows in-process via prefect.serve()

Date: 2026-04-20

## Status

Superseded by [ADR-006](./ADR-006-lambda-behind-sqs.md) — deejay-cog runs on Lambda behind SQS, with no Prefect.

## Context

Prefect Cloud supports two primary deployment patterns:

1. **Work pools** - flows are registered with a work pool; separate
   worker processes pull work from the pool and execute flows.
2. **Serve** - a single process registers flows and stays alive,
   handling scheduled and manually triggered runs itself.

Work pools are more flexible for heterogeneous fleets (multi-cloud,
multi-runtime, dynamic scaling). Serve is simpler for single-service
deployments where one container runs one cog's flows.

Prefect Cloud's hobby tier caps deployments at 5 across the account.
That made multi-deployment work-pool fleets expensive per-slot, and
made serve the better match for this ecosystem's one-cog-per-service
architecture.

## Decision

`deejay-cog` registers its flows via `prefect.serve()` in `main.py` as
the Railway service's `python -m deejay_cog.main` entry point. Flows
run in-process with full access to environment variables and Doppler
secrets. No work pool is configured.

`main.py` serves two deployments:

- `process-new-files` - `process_new_csv_files_flow`
- `ingest-live-history` - `ingest_live_history`

Other flows (`generate_summaries`, `update_deejay_set_collection`,
`retag_music`) are deferred - not served on Railway because they are
run manually or on a different cadence.

## Consequences

**Easier:**

- Single process model matches Railway's service-container pattern.
- No work pool infrastructure to maintain.
- Environment variables and Doppler-injected secrets available
  directly to flow code without a separate worker bootstrap.
- On Railway restart, in-flight runs are interrupted and Prefect
  marks them as crashed. The `on_crashed` hooks in each flow handle
  crash reporting to evaluator-cog.

  **Amended 2026-08-19 (see [ADR-005](./ADR-005-serve-startup-resilience.md)).**
  This is true only for *flow-run* crashes. `on_crashed` and
  `on_failure` hooks attach to flow runs, so they cover a run that was
  interrupted mid-execution. They do **not** cover a crash during
  `serve()`'s own deployment registration, which happens before any
  flow run exists — no flow run, no hook, no finding. The 2026-07-22
  Prefect Cloud 503 killed all four `serve()`-based cogs in exactly
  that window and produced zero findings. Registration-time crashes
  are now covered by `mini_app_polis.serve_resilience.serve_with_retry`,
  which emits a `source="startup"` CRITICAL finding on give-up. Read
  this bullet as scoped to flow runs; ADR-005 covers the rest of the
  process lifecycle.

**Harder:**

- Horizontal scaling requires running multiple service instances,
  each serving the same flows. Prefect handles concurrency correctly
  via run leases, but resource planning is per-container.
- Adding a new served flow means editing `main.py` and counts
  against the 5-deployment hobby-tier cap.

## References

- ecosystem-standards CD-015 (prefect.serve is the canonical pattern)
- ecosystem-standards PIPE-008 (repository_dispatch retired in favor
  of prefect.serve)
- [ADR-005](./ADR-005-serve-startup-resilience.md) — amends the
  crash-reporting claim in Consequences above
