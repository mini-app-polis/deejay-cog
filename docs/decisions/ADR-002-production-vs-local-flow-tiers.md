# ADR-002: Production-vs-local flow tiers

Date: 2026-04-20

## Status

Accepted, amended.

> **Amended (Oct 2026).** Prefect deployments and `main.py` are gone
> ([ADR-006](./ADR-006-lambda-behind-sqs.md)), so the deployment-slot
> force no longer applies. The tier split stands: the production flows
> are the two modes in `deejay_cog.worker.MODES` (`process-new-files`,
> `ingest-live-history`), and the other three remain local-only.

## Context

deejay-cog defines five distinct flows, each handling a different
Drive-to-ecosystem ingestion path:

- `process_new_csv_files_flow` - process DJ set CSVs
- `ingest_live_history` - ingest VirtualDJ play history
- `generate_summaries` - generate human-readable set summaries
- `update_deejay_set_collection` - rebuild the master collection
  spreadsheet and JSON snapshot
- `retag_music` - audio-fingerprint-based retagging

Three forces act on which of these get registered as Prefect
deployments on Railway:

- **Deployment-slot cost.** Prefect Cloud's hobby tier caps the
  account at 5 deployments total across all cogs. Every slot this
  repo consumes is a slot another cog cannot use.
- **Trigger modality mismatch.** `process_new_csv_files_flow` and
  `ingest_live_history` are fired by watcher-cog when new files
  appear in watched Drive folders - a clean event-driven trigger.
  The other three flows are either cadenced independently
  (`generate_summaries` runs on demand after a DJ event) or
  experimental (`retag_music` has system-dependency requirements,
  see `docs/CONFIGURATION.md`, and is still stabilizing).
- **Runtime environment.** `retag_music` needs `ffmpeg` and `fpcalc`
  binaries provisioned in the container image. Adding these to the
  Railway deployment image for one flow would bloat every other
  flow's cold start.

## Decision

Serve exactly two flows to Prefect Cloud from `main.py`:

- `process-new-files` (from `process_new_csv_files_flow`)
- `ingest-live-history` (from `ingest_live_history`)

The other three flows remain in the repo and are callable locally or
on manual invocation. They are deliberately not registered with
`serve()`. A comment in `main.py` notes the deferral explicitly:
"generate_summaries and update_collection deferred - not served on
Railway."

This split is reflected in `docs/PIPELINE.md` under "Flow tiers"
(Production vs. Local-only).

## Consequences

- Two Prefect deployment slots consumed by deejay-cog, leaving three
  for other cogs on the hobby tier.
- The two production flows are visible in Prefect Cloud UI for
  run history, failure detection, and hook firing. The three
  local-only flows have no observability channel unless run
  interactively - a known gap.
- `retag_music`'s runtime dependencies (ffmpeg, fpcalc) do not need
  to be provisioned in the Railway image. When `retag_music` is
  eventually promoted to production, a separate deployment image
  or a custom Dockerfile will be required - documented in
  `docs/CONFIGURATION.md`.
- Promoting a deferred flow to production requires editing
  `main.py` to add its deployment registration, verifying the
  deployment-slot budget is not exceeded, and considering whether
  the flow needs watcher-cog integration or its own trigger.
- If the ecosystem migrates off the hobby tier, all five flows can
  be served without slot pressure - this decision's primary force
  disappears at that point.
