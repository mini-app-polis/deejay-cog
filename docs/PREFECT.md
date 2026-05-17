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

## Railway deployment

deejay-cog runs on Railway as an always-on worker service.

**Railway start command:** `python -m deejay_cog.main`

This registers a single **router-style** deployment (`deejay-cog/deejay-cog`)
with Prefect Cloud and starts a runner loop that polls for scheduled or
manually triggered runs. The router dispatches to the underlying production
flow based on a required `mode` parameter:

- `mode="process-new-files"`  → `process_new_csv_files_flow`
- `mode="ingest-live-history"` → `ingest_live_history`

Calling the router with no `mode` (or an unknown one) raises `ValueError`.
Other (local-only / WIP) flows remain importable but are not served.
All environment variables from Railway are available to flows at runtime.

### First-time Railway setup

1. Create a new Railway service from the `deejay-cog` GitHub repo
2. Set all environment variables from `.env.example` in the Railway dashboard
3. Set the start command to `python -m deejay_cog.main`
4. Deploy — the service will start and register the served deployment with Prefect Cloud
5. Go to Prefect Cloud UI → Deployments and grab the new UUID for `deejay-cog/deejay-cog`
6. Update `watcher-cog`'s WATCHERS config with the new deployment UUID (both dj-sets and live-history watchers point at the same UUID; they pass different `mode` parameters)
7. Redeploy `watcher-cog`

### Triggering flows

The single `deejay` deployment is triggerable via:
- Prefect Cloud UI — manual "Custom Run", passing `mode` in parameters
- Prefect CLI — `prefect deployment run deejay-cog/deejay-cog -p mode=process-new-files`
- watcher-cog — calls `run_deployment()` with the deployment UUID and `parameters={"mode": "..."}`
- Prefect REST API — any HTTP client

## Evaluation

LLM-backed pipeline evaluation lives in **evaluator-cog**:
https://github.com/mini-app-polis/evaluator-cog

`deejay_cog._pipeline_eval` centralizes Prefect logging, run IDs, failure hooks,
and a single end-of-run `post_run_finding` call per flow. Findings on the
`pipeline_consistency` dimension are *self-reported*: the helper POSTs directly
to `/v1/evaluations` (with severity preserved, including SUCCESS) when
`production_only=True` and `KAIANO_API_BASE_URL` is set. Local-only and WIP
flows pass `production_only=False` so they never POST findings, regardless of
env. `ANTHROPIC_API_KEY` is not required for this self-reported path.
