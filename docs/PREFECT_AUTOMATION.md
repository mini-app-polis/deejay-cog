# Prefect Automation for Pipeline Evaluation

This document describes how to ensure pipeline crashes/failures are always
recorded as evaluation findings.

## Goal

Trigger evaluation when Prefect flow runs enter `Completed`, `Failed`, or
`Crashed` states so crash events are no longer invisible.

## Preferred Reliable Path (already implemented)

Both flows now include Prefect `on_failure` and `on_crashed` hooks that call
`evaluate_pipeline_run` directly with a pre-formed finding. This path does not
depend on Claude and does not require `ANTHROPIC_API_KEY`.

## Prefect Cloud Automation (Webhook) Setup

1. In Prefect Cloud, go to **Automations** -> **New automation**
2. Trigger: **Flow run state change**
3. Flows:
   - `update-dj-set-collection`
   - `process-new-csv-files`
4. States:
   - `Completed`
   - `Failed`
   - `Crashed`
5. Action: **Call webhook**
   - Option A (direct service endpoint):
     - `KAIANO_API_BASE_URL/v1/prefect-webhook`
   - Option B (dedicated endpoint in another service):
     - forward payload to a dedicated handler that executes
       `src/deejay_set_processor/evaluation_webhook.py`
6. Add required GitHub Actions variable:
   - `PREFECT_AUTOMATION_WEBHOOK_SECRET`

## Payload Expectations

`evaluation_webhook.py` expects a JSON payload containing:

- `flow_run_id`
- `flow_name`
- `state_name`
- `state_type`
- `start_time`
- `end_time`

For `FAILED`/`CRASHED`, it posts a direct finding like:

- `Flow {flow_name} entered {state_name} state`

For `COMPLETED`, it runs normal evaluator behavior.

## Alternative: send_event() Hooks Instead of Public Webhook

You can avoid exposing a webhook endpoint by using Prefect's `send_event()`
inside flow hooks (`on_completion`, `on_failure`, `on_crashed`) and consuming
those events internally.

Benefits:

- no public endpoint required
- lower operational risk
- events stay in Prefect's event system

Trade-off:

- requires event consumer wiring in your internal services.
