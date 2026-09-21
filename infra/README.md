# deejay-cog on AWS: one queue, one function

deejay-cog's queue, dead-letter queue, alarm, Lambda function and CI deploy
role. A copy of evaluator-cog's `infra/` with `name_prefix = "deejay"`.

**The runbook is evaluator-cog's** —
[evaluator-cog/infra/README.md](https://github.com/mini-app-polis/evaluator-cog/blob/main/infra/README.md):
order of operations, the cutover, rolling back, and what was verified once
for the whole account (Cloudflare does not challenge AWS egress, the DLQ
works, a zip is enough). Those are properties of the account, not of a cog,
and are not re-proven here. This file is what differs.

## What differs from evaluator-cog

**Nothing account-level.** `create_github_oidc_provider`,
`create_api_producer` and `create_account_budget` all default to **false**
in this directory. The OIDC provider, the API's producer user (whose
`*-jobs` policy already covers `deejay-jobs`) and the monthly budget live in
evaluator-cog's state. Defaults rather than tfvars, because a switch with
one correct value should not depend on remembering to pass it.

**x86_64, Python 3.11.** deejay has compiled dependencies (cryptography,
cffi, rpds-py) and the python3.11 runtime is Amazon Linux 2, glibc 2.26.
The shared `lambda-deploy.yml` builds wheels for exactly that and proves the
zip imports inside Lambda's own image; `ci.yml` passes it the architecture,
runtime and handler, which must match `worker.tf`.

The first deploy used a copied workflow that chose wheels for the runner
and checked imports on the runner. It passed, and the function failed at
import on Lambda (cryptography needing GLIBC_2.28). That is why the deploy is
shared now rather than copied.

**The Google stack stays in the zip.** Both routed flows use Drive and
Sheets. Measured on the zip the deploy workflow builds from the lock:
29.5 MB zipped, 153 MB unzipped, against 50 MB and 250 MB. boto3 is not
in deejay's dependency tree at all once Prefect is gone.

**The environment is near Lambda's 4 KB cap.** The limit covers every key
and value together, and the service-account JSON is most of it. Check the
JSON before the first apply — everything else is roughly 700 bytes, so the
JSON needs to be under about 3,300:

```bash
wc -c < path/to/service-account.json
```

If it is over, the environment is the wrong place for the credentials and
they belong in SSM Parameter Store — a code change, not a Terraform one.

**Timeout is 900 s and unmeasured.** See `worker_timeout_seconds`. Lower it
once the slowest run is known.

**Concurrency wants to be 1.** A process-new-files run is a sweep of one
Drive folder, and two at once race over the same files. The event source
mapping cannot go below 2, so `max_concurrency = 2` until the account's
Lambda quota allows `reserved_concurrency = 1` — `TODO(lambda-quota)` in
`variables.tf`. That is no worse than the Prefect deployment this replaced,
which had no limit.

## Order of operations for this cog

```bash
cp terraform.tfvars.example terraform.tfvars   # alert_email only; secrets come from Doppler
export AWS_PROFILE=miniapppolis
terraform init && terraform fmt -check && terraform validate
doppler run --project <deejay-cog project> --config prd --name-transformer tf-var -- \
  terraform plan -out tfplan    # expect: no budget, no OIDC provider, no producer user
terraform apply tfplan
```

Secrets are read from Doppler at plan time rather than copied into
`terraform.tfvars`: the variable names are Doppler's lowercased, and the
`tf-var` transformer turns them into `TF_VAR_*`. `terraform.tfvars` wins over
`TF_VAR_*`, so a secret left in it silently overrides Doppler. They still end
up in the local state file in plaintext, as any Terraform-managed secret
does, which is why state is gitignored.

1. **Apply.** The function is created holding a placeholder that cannot
   import, and the mapping is on. Until the first deploy, anything enqueued
   fails, retries and dead-letters — visible, not lost.
2. **Confirm the alert subscription.** AWS emails a link; an unconfirmed
   subscription delivers nothing.
3. **CI deploy path.** Three repository *variables* in GitHub (none are
   secret): `AWS_DEPLOY_ROLE_ARN`, `AWS_REGION`, `AWS_FUNCTION_NAME` from
   `terraform output`. The next release deploys; to redeploy without one,
   re-run the `deploy` job of the release's CI run. It fails unless the
   checksum AWS reports is the artifact it built.
4. **Probe**, as in evaluator-cog's runbook: a malformed record should come
   back in `batchItemFailures` with no `FunctionError`.
5. **Cut over.** Stop the Prefect deployment and scale the Railway service
   to zero, *then* deploy watcher-cog's API trigger. Stopping first leaves a
   gap that the next sweep closes; the other order runs both.
