# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for this
repository. ADRs document significant architectural decisions, the
context around them, and their consequences.

## Format

Each ADR is a markdown file named `ADR-NNNN-short-slug.md` where `NNNN`
is a zero-padded four-digit sequence number starting at `0001`. This
matches the ecosystem-standards DOC-005 specification. An ADR in another
repository is cited with that repository's name first, as in
`ecosystem-standards ADR-0009`.

Each ADR uses three sections: **Context** (what forces motivated the
decision), **Decision** (what change is being made), and
**Consequences** (what becomes easier or harder).

## Index

- [ADR-0001: Run Prefect flows in-process via prefect.serve()](./ADR-0001-prefect-in-process-serve.md) — superseded by ADR-0006; amended by ADR-0007
- [ADR-0002: Production-vs-local flow tiers](./ADR-0002-production-vs-local-flow-tiers.md) — amended by ADR-0006 and ADR-0007
- [ADR-0003: On-demand cog with no expected cadence](./ADR-0003-on-demand-no-cadence.md) — amended by ADR-0006; amended 2026-11-03 (the nightly library backfill); amended 2026-11-10 (the library jobs run often and act only on change)
- [ADR-0004: Best-effort pipeline evaluation posting](./ADR-0004-best-effort-pipeline-eval.md) — amended: reports go to `/v1/notify`; amended by ADR-0007
- [ADR-0005: Resilient `serve()` startup — retry, restart policy, and the `startup` finding source](./ADR-0005-serve-startup-resilience.md) — amends ADR-0001; superseded by ADR-0006
- [ADR-0006: Run on Lambda behind SQS, not Prefect on Railway](./ADR-0006-lambda-behind-sqs.md) — supersedes ADR-0001 and ADR-0005; amended (FIFO queue, reserved concurrency); amended by ADR-0007
- [ADR-0007: Tag the Drive music library from the catalog, on Lambda](./ADR-0007-music-tagging-redesign.md) — amends ADR-0001, ADR-0002, ADR-0004 and ADR-0006; amended 2026-11-03 (two flows, safety rules)
- [ADR-0008: The catalog is the source of truth for the music library](./ADR-0008-catalog-source-of-truth.md) — builds on ADR-0007; amended by ADR-0009 (the library spans more than one folder)
- [ADR-0009: The library spans more than one Drive folder](./ADR-0009-library-folders.md) — builds on ADR-0008; how each library is tagged is decided in ADR-0010
- [ADR-0010: Each library is tagged by a named rule profile](./ADR-0010-tagging-rule-profiles.md) — builds on ADR-0009
