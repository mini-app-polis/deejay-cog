# ADR-006: Run on Lambda behind SQS, not Prefect on Railway

Date: 2026-09-21

## Status

Accepted

Supersedes [ADR-001](./ADR-001-prefect-in-process-serve.md) and
[ADR-005](./ADR-005-serve-startup-resilience.md).

## Context

deejay-cog held a Railway container open to poll Prefect Cloud every ten
seconds for work that arrives roughly monthly (ADR-003). Memory was the
whole bill, and it bought nothing between DJ events. The fleet is moving
off Prefect onto one SQS queue and one Lambda function per cog; the
decision and its reasoning are evaluator-cog's
`docs/serverless-migration.md`, and evaluator-cog is the reference
implementation.

What Prefect was actually doing here, checked against the source:

- **The trigger path.** watcher-cog called `create_flow_run` on the
  `deejay-cog/deejay-cog` router. Replaced by watcher-cog POSTing
  `/v1/deejay/runs`; the API enqueues onto `deejay-jobs`.
- **Task retries.** Three `@task(retries=2)` — and all three were dead.
  `_ingest_set_to_api`, `process_m3u_file` and `retag_music_file` each catch
  every exception and return, and Prefect only retries a task that raises.
  Nothing to move to `tenacity`; the API client's own transport retries are
  what ever ran.
- **Failure hooks.** `on_failure` / `on_crashed`. Replaced by the worker
  reporting any run that raises, then naming its message in
  `batchItemFailures` so SQS redelivers it.
- **Run identity.** `get_run_id()` resolved Prefect's flow-run id. The
  worker now passes the SQS message id, which is also what the API answered
  the trigger with.
- **Concurrency.** None. The deployment had no limit.

## Decision

`deejay_cog.worker.lambda_handler` is the entrypoint. One message is one
flow run, `{"type": "deejay.run", "version": 1, "payload": {"mode": …}}`,
and the mode picks the flow exactly as `deejay_router(mode)` did.

Prefect is removed from the dependencies entirely, along with `main.py`,
`prefect.yaml` and `railway.json`. The two GitHub Actions workflows that
ran the routed flows on `repository_dispatch` are removed too: nothing in
the fleet sends those events, and a second trigger path for the same work
is what the cutover rule forbids.

The function is x86_64 and built on the runtime's Python, because deejay
has compiled dependencies and the deploy's import guard is only a test if
it loads the same binaries Lambda will.

## Consequences

- No resident process. Nothing runs between triggers.
- A run that raises is retried whole, three times, then dead-letters and
  fires `deejay-dlq-not-empty`. Safe because both flows are sweeps of what
  is in Drive: an archived CSV is not in the source folder the second time,
  and re-sent live plays are deduplicated by the API.
- Two sweeps can still overlap: the event source mapping's floor is two
  concurrent invocations. Setting `reserved_concurrency = 1` once the
  account's Lambda quota allows it closes that; until then it is no worse
  than the Prefect deployment, which had no limit.
- Rolling back is a rewrite, not a restart, deliberately — keeping the old
  runner runnable is how two consumers end up running.
- CD-015, CD-016, PIPE-004, PIPE-006 and PIPE-015 assume Prefect. They are
  retired or rescoped fleet-wide at the end of the migration, not
  exempted here one rule at a time.
