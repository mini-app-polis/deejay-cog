# ADR-0003: On-demand cog with no expected cadence

Date: 2026-04-23

## Status

Accepted, amended.

> **Amended (Oct 2026).** The two observability channels named in Decision
> are gone with Prefect ([ADR-0006](./ADR-0006-lambda-behind-sqs.md)). A run
> that raises is reported by the Lambda worker to `POST /v1/notify`, and a
> job that fails every receive lands in the dead-letter queue and fires
> `deejay-dlq-not-empty`; `deejay-queue-stalled` fires when nothing
> consumes the queue. `evaluator.yaml` no longer carries the CD-010
> exemption quoted below. The conclusion — no Healthchecks.io ping for an
> on-demand cog — stands.

## Context

ecosystem-standards CD-010 requires Healthchecks.io liveness pings for
every cog. The rule exists because most cogs run on a cadence -
polling loops (watcher-cog), scheduled flows (notes-ingest-cog,
evaluator-cog), or equivalent - and a silent cog is a signal that
something has gone wrong.

deejay-cog does not fit this model. Its production flows
(`process_new_files`, `ingest_live_history`) are triggered by
watcher-cog when files appear in the watched Drive folders, which
in turn happens only when Kaiano uploads DJ event data. That cadence
is roughly monthly, with long gaps where nothing fires - sometimes
weeks - and bursts of activity around DJ events.

A Healthchecks.io ping cadence for this service would have to be
either:

- Set to "monthly," which is too coarse to be useful as a liveness
  signal - a failure could go undetected for weeks.
- Set to the poll cadence of watcher-cog, with watcher-cog
  side-channel-pinging on deejay-cog's behalf - which defeats the
  purpose of CD-010 (observability per-cog) by conflating the
  trigger source's liveness with the target's.

The question was whether to paper over this with a Healthchecks ping
anyway or to acknowledge the mismatch.

## Decision

Exempt deejay-cog from CD-010 (Healthchecks.io liveness) rather than
add a ping that doesn't match the cog's operational model. The
exemption is documented in `evaluator.yaml` with the reason:
"on-demand cog with no expected cadence - flows run only when Kaiano
triggers DJ event processing (roughly monthly)."

Per-flow success and failure are observable through two other
channels instead:

- **Prefect Cloud run state.** Every triggered flow produces a run
  record with completion state (Completed, Failed, Crashed). These
  are visible in Prefect Cloud UI and scored by evaluator-cog.
- **Failure hooks posting findings.** Each served flow has an
  `on_failure` and `on_crashed` hook (see ADR-0004) that posts a
  direct finding to evaluator-cog when a run ends abnormally.

## Consequences

- deejay-cog does not ship Healthchecks.io pinging. No
  `HEALTHCHECKS_URL_*` env var, no heartbeat module. This is
  intentional and permanent for as long as the on-demand pattern
  holds.
- Silent failure modes are narrower: a flow that never gets
  triggered produces no signal of its own absence (correct behavior
  - there is no work to do), but a triggered flow that crashes
  produces a Prefect run with a non-Completed state plus a finding
  via the failure hook.
- If the cadence changes (e.g. deejay-cog starts handling a
  polling workload), this exemption should be revisited. The
  CD-010 exemption reason-string in `evaluator.yaml` serves as
  the trigger - if the "roughly monthly" claim becomes false, the
  exemption's justification is no longer valid.
- Other cogs cannot cite this ADR as precedent for skipping CD-010
  without demonstrating the same no-cadence property. CD-010
  remains the default; this ADR is the exception.
