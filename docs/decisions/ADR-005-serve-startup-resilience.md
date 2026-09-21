# ADR-005: Resilient `serve()` startup — retry, restart policy, and the `startup` finding source

Date: 2026-08-19

## Status

Superseded by [ADR-006](./ADR-006-lambda-behind-sqs.md) — deejay-cog runs on Lambda behind SQS, with no Prefect.

Amends the Consequences section of
[ADR-001](./ADR-001-prefect-in-process-serve.md).

## Context

On 2026-07-22 a transient Prefect Cloud `503 Service Unavailable` on the
deployment-registration endpoint crashed all four `serve()`-based cogs
simultaneously. None auto-restarted. No pipeline finding was posted. The
fleet-down event was completely silent.

`prefect.serve()` is not resilient at its own front door. Before the
runner loop begins, it makes a blocking, fail-fast HTTP call to Prefect
Cloud (`read_deployment_by_name`) to register or resolve each deployment.
In `main.py` that call was unguarded — the only `except` was
`KeyboardInterrupt` — so a `503` propagated out of `main()` and the
process exited non-zero.

Every cog shares this same `serve()`-in-`main()` scaffolding per
ADR-001. It was one single point of failure hit four times, not four
coincidences.

Three independent defects converged:

1. **No retry on startup registration.** A seconds-long upstream blip
   killed a service that would otherwise have run for weeks.
2. **Railway restart policy was `NEVER`** (or an exhausted low cap).
   Nothing brought the services back after the crash.
3. **The crash was invisible.** Each flow's `on_crashed` / `on_failure`
   hooks are correctly wired (see `process_new_files.py` lines 533–534),
   but those hooks attach to *flow runs*. This crash happened during
   `serve()` registration, before any flow run existed, so no hook could
   fire. This is the open "infrastructure-level crash/failure capture"
   backlog item surfacing in production — except earlier than a crashed
   flow: a crashed process with no flow at all.

Defects 1 and 2 are independent. Retry rides out the blip in-process;
restart recovers when retries are exhausted, or when the failure is
something retries cannot fix (a wedged event loop, leaked in-process
state). Implementing only one leaves a real gap.

## Decision

**Layer 1 — retry in-process.** `main.py` calls
`mini_app_polis.serve_resilience.serve_with_retry(...)` instead of
`prefect.serve(...)`. The helper:

- Retries only transient errors: any HTTP `5xx`, plus `408` and `429`,
  plus network-level `httpx` transport errors (the whole
  `TimeoutException` and `NetworkError` branches, `ProxyError`, and
  `RemoteProtocolError`).
- Fails fast on the rest of `4xx` — `401`/`403`/`404`. A bad API key or
  a deleted deployment is a configuration error; retrying it burns the
  ceiling and delays the real signal.

The 5xx range is deliberately a range and not an allowlist of
`{500, 502, 503, 504}`. Prefect Cloud sits behind Cloudflare, whose edge
emits `520`–`529` (notably `522 Connection Timed Out` and `524 A Timeout
Occurred`) when an origin is degraded — the most common way a Prefect
Cloud incident actually presents at the client. An allowlist would fail
fast on exactly the symptom this ADR exists to survive, and would then
post a finding asserting the failure was a configuration error. The same
reasoning drives taking whole `httpx` exception branches rather than
naming leaf classes: `ConnectTimeout` is not a subclass of
`ConnectError`, so leaf-naming silently misses operationally identical
siblings.
- Backs off exponentially (`multiplier=2, min=2, max=30`) up to a
  **wall-clock ceiling of 30 minutes**, overridable per-process via the
  `SERVE_RETRY_MAX_SECONDS` env var.
- On give-up, POSTs exactly one `source="startup"`, `CRITICAL` finding
  via the library `post_run_finding`, best-effort, then **re-raises** so
  the process still exits non-zero.

The helper lives in `common-python-utils`, not in this repo. All four
`serve()`-based cogs pin `common-python-utils` at `rev = "main"` and
inherit it on next deploy. Copy-pasting it per cog would recreate the
four-way single point of failure this ADR exists to remove.

**Layer 2 — restart policy.** A version-controlled `railway.json`
declares `restartPolicyType: "ON_FAILURE"` with
`restartPolicyMaxRetries: 10`. Version-controlled rather than
dashboard-configured so the setting is reviewable, diffable, and
reproducible when a service is recreated.

**The `startup` finding source.** `source="startup"` joins the existing
`flow_inline` (end-of-run self-report) and `flow_hook` (Prefect
on_failure/on_crashed) values. A `startup` row means: no flow run exists,
the cog process itself failed. Its `run_id` resolves to `"local-run"`
because there is no Prefect flow run to attribute it to — which is
precisely the condition being reported.

`CRITICAL` was added to `mini_app_polis.pipeline_status.Severity` for
this path and this path only. Flow-run outcomes still cap at `ERROR`, no
matter how large the blast radius. Using `CRITICAL` as "ERROR but I
really mean it" would dilute the one signal that means "nothing is
running at all".

### Why 30 minutes

The two layers multiply: total outage coverage is roughly *layer-1
ceiling × layer-2 max retries*. The originally proposed ceiling — 8
attempts, about 2.5 minutes — combined with 10 restarts gives roughly 25
minutes before the service is down until someone notices. Prefect Cloud
incidents routinely run longer than that. At 30 minutes per process life,
the combined coverage is roughly five hours.

The counter-argument for a short ceiling is "hand off to Railway rather
than retry forever in isolation". That handoff buys nothing for *this*
failure — a restart re-runs the same code path against the same outage.
It is valuable only for failures a fresh process fixes but a retry does
not, which is why give-up still exists rather than retrying unbounded.

The ceiling is expressed in wall clock (`stop_after_delay`) rather than
attempt count (`stop_after_attempt`) because an attempt count silently
changes meaning whenever the backoff curve is tuned, and "ride out N
minutes" is the property being reasoned about. An attempt-count guard
remains as a runaway backstop only.

Note the real worst case is the ceiling plus one final backoff sleep
(≤30s) plus one attempt: tenacity evaluates its stop condition *after* an
attempt, not before.

### What was deliberately not changed

The `KeyboardInterrupt` guard, `deejay_router`, `_MODE_DISPATCH`, the
Sentry init, and the served deployment name (`deejay-cog`) are all
untouched. No watcher-cog UUID changes are needed — the deployment
identity is unchanged, so existing triggers keep working.

## Consequences

**Easier:**

- A transient Prefect Cloud blip no longer takes the cog down. A
  multi-hour incident no longer requires manual intervention.
- Registration failures are now visible in Pipeline Health as
  `source="startup"` CRITICAL rows. The July incident produced none.
- The fix is inherited fleet-wide from `common-python-utils` rather than
  maintained in four places.
- Railway's restart behaviour is in git, so recreating the service
  reproduces it.

**Harder:**

- A misconfigured cog now takes up to 30 minutes to surface a *transient*
  failure. Configuration errors still fail fast, so the common
  "deployed with a bad API key" case is unaffected.
- During a long outage the give-up finding fires once per process life —
  roughly two per hour per cog, times four cogs. That is the intended
  alerting cadence, but it is not silent.
- CD-015's source scanner cannot see through the wrapper. `main.py` no
  longer contains a bare `serve(` token at all, so the existing
  evaluator.yaml exemption stays until evaluator-cog's
  `check_prefect_serve_pattern` learns to accept `serve_with_retry(`.
  The exemption's reason was rewritten to describe the new state; the
  old one, citing `from prefect import serve` at `main.py:25`, is now
  false.
- `serve_with_retry` cannot depend on this repo's `_pipeline_eval` shim
  (it is shared across the fleet), so `repo=` must be passed explicitly.
  `main.py` passes `_pipeline_eval.REPO` rather than a second literal so
  the string still lives in one place.

## References

- ecosystem-standards CD-015 (prefect.serve is the canonical pattern)
- ecosystem-standards CD-016 (startup registration must be wrapped)
- ecosystem-standards CD-017 (version-controlled ON_FAILURE restart policy)
- ecosystem-standards ADR-006 (the ecosystem-wide record of this decision)
- `mini_app_polis.serve_resilience` in common-python-utils
- [ADR-001](./ADR-001-prefect-in-process-serve.md) — amended Consequences
- [ADR-004](./ADR-004-best-effort-pipeline-eval.md) — best-effort posting,
  which the startup finding follows
