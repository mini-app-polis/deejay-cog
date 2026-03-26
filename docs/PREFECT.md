# Prefect Cloud Setup

This project uses Prefect Cloud for flow observability.

## First time setup

1. Create a free account at app.prefect.cloud
2. Create a workspace
3. Generate an API key:
   Settings → API Keys → Create API Key
4. Add to GitHub Actions secrets:
   `PREFECT_API_KEY` = your api key
5. Add to GitHub Actions **variables** (used by the `prefect cloud login` step in workflows):
   - `PREFECT_ACCOUNT_SLUG` — your Prefect Cloud account slug  
   - `PREFECT_WORKSPACE_SLUG` — your workspace slug  

   The login command uses them as `PREFECT_ACCOUNT_SLUG/PREFECT_WORKSPACE_SLUG` (two separate variables, not a single `PREFECT_WORKSPACE` value).

## Local development

Set environment variables:

```bash
PREFECT_API_KEY=your-api-key
PREFECT_API_URL=https://api.prefect.cloud/api/accounts/{account_id}/workspaces/{workspace_id}
```

Or use the CLI:

```bash
pip install prefect
prefect cloud login
```

## Flow runs

View all runs at: app.prefect.cloud

Each run shows: task-level logs, duration, success/failure

## Evaluation

Pipeline evaluation logic lives in the standalone **evaluator-cog** repo:  
https://github.com/kaianolevine/evaluator-cog  

For automation setup instructions see:  
https://github.com/kaianolevine/evaluator-cog/blob/main/docs/PREFECT_AUTOMATION.md  

**All four** Prefect flows (`process-new-csv-files`, `update-dj-set-collection`, `generate-summaries`, `ingest-live-history`) wire **`on_failure`** and **`on_crashed`** hooks that call `evaluate_pipeline_run` with a direct finding when a run fails or crashes, so crash detection does not depend on a Prefect automation being configured.

At **end of run**, each flow also calls `evaluate_pipeline_run` with **flow-specific counters** (CSV pipeline stats, collection/summary metrics, live ingest summary, etc.), gated on **`ANTHROPIC_API_KEY`** and **`KAIANO_API_BASE_URL`**.

Additionally, **all four** GitHub Actions pipeline workflows include a **Report failure to evaluator** step that runs on `if: failure()` and uses a **`curl` POST** to the evaluations API. That step runs even if the Python script never starts (for example when **`uv sync`** fails), so infra failures are still reported without a working virtualenv.
