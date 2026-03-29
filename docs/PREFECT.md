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

This registers all four flows as Prefect Cloud deployments and starts
a runner loop that polls for scheduled or manually triggered runs.
All environment variables from Railway are available to flows at runtime.

### First-time Railway setup

1. Create a new Railway service from the `deejay-cog` GitHub repo
2. Set all environment variables from `.env.example` in the Railway dashboard
3. Set the start command to `python -m deejay_cog.main`
4. Deploy — the service will start and register all four deployments with Prefect Cloud
5. Go to Prefect Cloud UI → Deployments and grab the new UUIDs
6. Update `watcher-cog`'s WATCHERS config with the new deployment UUIDs
7. Redeploy `watcher-cog`

### Triggering flows

Each flow is independently triggerable via:
- Prefect Cloud UI — manual "Quick Run" per deployment
- Prefect CLI — `prefect deployment run deejay-cog/process-new-files`
- watcher-cog — calls `run_deployment()` with the deployment UUID
- Prefect REST API — any HTTP client

## Evaluation

Pipeline evaluation logic lives in the standalone **evaluator-cog** repo:
https://github.com/mini-app-polis/evaluator-cog

All four flows wire `on_failure` and `on_crashed` hooks that call
`evaluate_pipeline_run` with a direct finding when a run fails or crashes.
Crash detection does not depend on a Prefect automation being configured.

At end of run, each flow also calls `evaluate_pipeline_run` with
flow-specific counters, gated on `ANTHROPIC_API_KEY` and `KAIANO_API_BASE_URL`.
