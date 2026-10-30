# ADR-0004: Best-effort pipeline-evaluation posting

Date: 2026-04-18

## Status

Accepted, amended.

> **Amended (Oct 2026).** The reporting path below is gone. Reports are
> built with `RunReport` / `post_run_finding` from common-python-utils
> (`mini_app_polis.pipeline_status`, wrapped by `_pipeline_eval.py`) and
> sent as messages to `POST /v1/notify`; nothing goes to `/v1/evaluations`
> and nothing passes through evaluator-cog. Prefect failure hooks
> (`make_failure_hook`) are replaced by the Lambda worker reporting any run
> that raises ([ADR-0006](./ADR-0006-lambda-behind-sqs.md)), and the send gate
> is now an API base URL resolving for the environment rather than the
> `KAIANO_API_BASE_URL` + `ANTHROPIC_API_KEY` pair. The best-effort and
> production-only guarantees stand.

> **Amended (Oct 2026), by [ADR-0007](./ADR-0007-music-tagging-redesign.md).** `retag_music` is gone. The
> local-only flows that pass `production_only=False` are
> `generate_summaries` and `update_deejay_set_collection`; music tagging
> (the `retag-music` mode and the fingerprint function) runs in production
> and reports as the other production flows do.

## Context

Every served flow needs to report its outcome to evaluator-cog so
pipeline-health views in the UI reflect reality. The reporting path
is a POST to the api-kaianolevine-com `/v1/evaluations` endpoint,
brokered through a helper in `evaluator_cog.flows.pipeline_eval`.

Three forces shape how this reporting should behave:

- **Cross-cog coupling risk.** If the pipeline-eval post is a
  required step in a flow's happy path, any failure in the
  reporting layer (network hiccup, api-kaianolevine-com down,
  auth expiry) becomes a flow failure - creating a finding that
  says "this flow failed" when in fact the flow's work succeeded
  and only the reporting bounced. That's a bad feedback loop:
  the reporting failure triggers a new reporting attempt for a
  flow that actually worked.
- **Local-development noise.** Running a flow locally for
  debugging or validation should not produce pipeline_evaluations
  rows. Each local run would otherwise clutter the production
  findings table and mislead the UI.
- **Observability where it matters.** Failures in reporting should
  still be visible - just not as flow-level failures. Logs are
  the right place: when the post raises, the exception should be
  logged with full traceback, but the flow continues.

## Decision

`_pipeline_eval.post_run_finding()` and `_pipeline_eval.make_failure_hook()`
implement three guarantees:

1. **Best-effort.** Any exception raised during the API post is
   caught and logged via `logger.exception(...)`. The function
   returns normally. Flow execution is never interrupted by a
   reporting error.

2. **Production-only gating.** The function accepts a
   `production_only: bool` parameter (default `True`). When
   `False`, the function logs that the finding was suppressed and
   returns without posting. Local-only flows
   (`generate_summaries`, `update_deejay_set_collection`,
   `retag_music`) call helpers with `production_only=False` to
   unconditionally suppress posts.

3. **Env-var dual-gate.** When `production_only=True`, posts
   additionally require both `KAIANO_API_BASE_URL` and
   `ANTHROPIC_API_KEY` to be set. This protects against accidental
   posts from environments where the production flags happen to
   be true but the API endpoint isn't configured (local dev with
   a stale `.env`, for example).

Failure hooks (`make_failure_hook`) inherit the same three
guarantees. They additionally set `severity="ERROR"` for Crashed
flow states and `severity="WARN"` for other failure states, and
set `source="flow_hook"` so the evaluator UI can render hook-originated
findings distinctly from end-of-flow inline findings.

## Consequences

- A flow that produces correct work but hits a reporting outage
  does not falsely report itself as failed. The outage is logged
  but doesn't poison the flow's outcome.
- Local development runs produce no pipeline_evaluations rows,
  regardless of whether the env vars happen to be set. Developers
  can iterate on a flow locally without worrying about polluting
  the findings table.
- A reporting outage is detectable only via logs - not via the
  pipeline-health UI itself. This is a deliberate tradeoff: the
  UI is for pipeline health, not for the health of the reporting
  channel that feeds it.
- Failure hooks never raise, which means Prefect's own
  hook-failure semantics (hook exceptions masking the underlying
  flow failure) do not apply here. The hook is a pure observer.
- The three-guarantee pattern is specific to this cog's posting
  layer. If another cog adopts a similar pattern, it should cite
  this ADR rather than copying the code - the decision-record is
  the durable artifact; the code may evolve.
