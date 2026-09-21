# The worker, and the event source mapping that is the whole point: AWS
# polls the queue and invokes the function, so the polling still happens
# but it is not a container of yours doing it.

data "aws_iam_policy_document" "worker_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "worker" {
  name               = "${var.name_prefix}-worker"
  assume_role_policy = data.aws_iam_policy_document.worker_assume.json
}

data "aws_iam_policy_document" "worker" {
  # Read the queue. The event source mapping does the receiving, but it
  # does it with this role's permissions.
  statement {
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ChangeMessageVisibility",
    ]
    resources = [aws_sqs_queue.jobs.arn]
  }

  # Write logs. CloudWatch is inherited whether or not you want it, so it
  # is accepted as a second log destination rather than fought.
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.worker.arn}:*"]
  }
}

resource "aws_iam_role_policy" "worker" {
  name   = "${var.name_prefix}-worker"
  role   = aws_iam_role.worker.id
  policy = data.aws_iam_policy_document.worker.json
}

# Declared rather than left to Lambda's implicit creation, which retains
# forever and is owned by nothing.
resource "aws_cloudwatch_log_group" "worker" {
  name              = "/aws/lambda/${var.name_prefix}-worker"
  retention_in_days = var.log_retention_days
}

# Bootstrap code, and nothing more.
#
# `aws_lambda_function` cannot be created without a payload, and the real
# package comes from CI, which cannot run until the function exists. This
# is that chicken-and-egg and nothing else: it is read once, at create, and
# then `ignore_changes` below means Terraform never looks at it again.
#
# It is deliberately not runnable. The `handler` string points into
# `deejay_cog/`, which this archive does not contain, so an invocation
# that somehow arrives before the first deploy fails with an import error
# and the message is retried and then dead-lettered. That is the behaviour
# we want from an undeployed function.
#
# This used to be a stub worker under `infra/stub/` that logged its event,
# probed the API and returned success. Returning success is what made it
# dangerous — it consumed real jobs and discarded them, and an evaluation
# eaten that way is indistinguishable from one never enqueued. The things
# it was written to prove are proven (see "Verified" in README.md); they
# are properties of the account, not of a cog, so no later cog re-proves
# them. A placeholder that cannot succeed replaces it.
data "archive_file" "bootstrap" {
  type        = "zip"
  output_path = "${path.module}/bootstrap.zip"

  source {
    filename = "PLACEHOLDER"
    content  = "Replaced by the first CI deploy. See worker.tf.\n"
  }
}

resource "aws_lambda_function" "worker" {
  function_name = "${var.name_prefix}-worker"
  role          = aws_iam_role.worker.arn
  runtime       = "python3.11"

  # The real worker, not the bootstrap placeholder. Terraform owns this
  # because it is configuration rather than code, and the split matters:
  # CI can call UpdateFunctionCode and nothing else, so a compromised
  # workflow cannot repoint the function at a different entrypoint without
  # someone reviewing a .tf file.
  #
  # Deploying a zip whose layout does not match this string fails at the
  # first invocation with an import error, not at deploy time — the
  # package's top level must contain deejay_cog/. The deploy workflow
  # checks that before uploading.
  handler = "deejay_cog.worker.lambda_handler"

  # x86_64, not the evaluator's arm64, because deejay's dependencies are
  # not all pure Python — cryptography, cffi and rpds-py ship compiled
  # wheels. The deploy workflow builds on an x86_64 runner against the
  # runtime's Python, so the import guard it runs is a test of the exact
  # binaries Lambda will load. On arm64 the build would have to
  # cross-install, and the guard could no longer import what it checks.
  architectures = ["x86_64"]

  filename         = data.archive_file.bootstrap.output_path
  source_code_hash = data.archive_file.bootstrap.output_base64sha256

  timeout     = var.worker_timeout_seconds
  memory_size = var.worker_memory_mb

  # Set to 1 once the account quota allows it — see var.reserved_concurrency.
  reserved_concurrent_executions = var.reserved_concurrency

  # Lambda caps the whole map at 4 KB, keys included, and the service
  # account JSON is most of it. An apply that exceeds it fails with an
  # error naming the limit; README.md says how to check before applying.
  environment {
    variables = {
      KAIANO_API_BASE_URL       = var.kaiano_api_base_url
      DEEJAY_COG_API_KEY        = var.deejay_cog_api_key
      GOOGLE_CREDENTIALS_JSON   = var.google_credentials_json
      SPOTIPY_CLIENT_ID         = var.spotipy_client_id
      SPOTIPY_CLIENT_SECRET     = var.spotipy_client_secret
      SPOTIPY_REFRESH_TOKEN     = var.spotipy_refresh_token
      SPOTIPY_REDIRECT_URI      = "http://127.0.0.1:8888/callback"
      SPOTIFY_RADIO_PLAYLIST_ID = var.spotify_radio_playlist_id
      VDJ_HISTORY_FOLDER_ID     = var.vdj_history_folder_id
      SENTRY_DSN                = var.sentry_dsn
      ENVIRONMENT               = "production"
    }
  }

  depends_on = [aws_cloudwatch_log_group.worker]

  lifecycle {
    # Terraform owns the function's configuration; CI owns its code.
    #
    # Without this, every `terraform apply` after a deploy would quietly
    # roll the function back to the bootstrap placeholder — which is the
    # "which version is actually deployed" confusion that skipping a
    # container registry was supposed to avoid, reintroduced from the
    # other side.
    ignore_changes = [filename, source_code_hash]
  }
}

resource "aws_lambda_event_source_mapping" "jobs" {
  event_source_arn = aws_sqs_queue.jobs.arn
  function_name    = aws_lambda_function.worker.arn

  # On. There is no variable behind this any more, and that is the point.
  #
  # It used to be `var.worker_consumes_queue`, defaulting to false, so that
  # the stub could exist without racing the Railway container for messages
  # — SQS hands a message to exactly one consumer. The default is the
  # hazard: a bare `terraform apply` disabled the mapping, nothing
  # consumed, and nothing raised, because a queue with no consumer is not
  # an error. Releases looked fine for hours.
  #
  # deejay never had that overlap to guard. It moved from Prefect straight
  # to SQS, so its queue was created with exactly one reader and has never
  # had another. Nothing to toggle.
  enabled = true

  # One job per invocation. The handler's unit of work is one flow run,
  # and the visibility-timeout arithmetic above is per job — a batch would
  # make the deadline depend on how many arrived together. Batching is a
  # tuning knob for later, and it needs ReportBatchItemFailures before it
  # is safe, or one bad record redelivers its whole batch.
  batch_size = 1

  function_response_types = ["ReportBatchItemFailures"]

  # The ceiling that exists today. reserved_concurrent_executions on the
  # function is the one this account cannot set yet; this one is per
  # mapping and needs no quota, and cannot go below 2.
  scaling_config {
    maximum_concurrency = var.max_concurrency
  }
}
