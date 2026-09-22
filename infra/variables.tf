variable "region" {
  description = <<-DESC
    Match the Railway fleet, which runs in US East, and evaluator-cog's
    stack in the same account. The worker POSTs sets, tracks and plays to
    api-kaianolevine-com, so every round trip pays whatever distance
    separates the two.
  DESC
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = <<-DESC
    Prefix for every resource name. One prefix per cog.

    Load-bearing, not cosmetic: the queue is `<name_prefix>-jobs`, and the
    API derives the same name from the cog ("deejay") and its environment —
    `deejay-jobs` in production, `deejay-dev-jobs` elsewhere. A development
    stack is this directory applied with `deejay-dev`.
  DESC
  type        = string
  default     = "deejay"
}

variable "github_repo" {
  description = "owner/repo allowed to assume the deploy role via OIDC."
  type        = string
  default     = "mini-app-polis/deejay-cog"
}

# ── Budget ───────────────────────────────────────────────────────────────

variable "alert_email" {
  description = "Where this cog's DLQ alarm goes."
  type        = string

  # transcription-cog's first apply subscribed the example's placeholder,
  # and the alarm would have told nobody. SNS accepts any address; this
  # does not accept that one.
  validation {
    condition     = can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.alert_email)) && !endswith(lower(var.alert_email), "@example.com")
    error_message = "alert_email must be a real address, not the example.com placeholder from terraform.tfvars.example."
  }
}

variable "create_account_budget" {
  description = <<-DESC
    Whether this state owns the account's monthly budget.

    False here. The budget is account-level and evaluator-cog's state owns
    it; a second one would send every overspend alert twice and look like
    two problems. Same shape as create_github_oidc_provider.
  DESC
  type        = bool
  default     = false
}

variable "budget_limit_usd" {
  description = "Monthly budget, when create_account_budget is true. Set low on purpose — this should never fire."
  type        = number
  default     = 10
}

# ── Worker sizing ────────────────────────────────────────────────────────

variable "worker_timeout_seconds" {
  description = <<-DESC
    Above the slowest observed run with headroom. The queue's visibility
    timeout is derived from this rather than configured separately, so the
    two cannot drift apart.

    NOT YET MEASURED for deejay. 900 is Lambda's ceiling, chosen because a
    process-new-files sweep uploads and formats a Google Sheet per CSV and
    then syncs Spotify, and a timeout below the real duration kills a run
    mid-sweep and retries it. Lower it once the slowest run is known — the
    "Run complete in …" line on the flows' Discord reports is the number.

    The cost of leaving it high: a message that fails every time takes
    three visibility timeouts (~48 minutes) to reach the DLQ.
  DESC
  type        = number
  default     = 900
}

variable "worker_memory_mb" {
  description = "Lambda scales CPU with memory. The work is mostly waiting on Google and the API, so this is generous; tune against the billed-duration metric."
  type        = number
  default     = 1024
}

variable "reserved_concurrency" {
  description = <<-DESC
    For deejay this is the serialisation knob, not a throttle. process-new-files
    is a sweep of one Drive folder, and two sweeps running at once list the
    same files and race to upload and archive them. The GitHub Actions
    workflows this replaced serialised with a concurrency group; the Prefect
    deployment did not.

    1 became settable when the account's concurrent-executions quota was
    raised to 1,000 (2026-09-21). Before that AWS refused any reservation that
    left fewer than 100 unreserved executions.

    It is the only concurrency setting. The mapping's scaling_config cannot
    go below 2, and AWS refuses to create a mapping whose maximum exceeds the
    function's reservation, so there is none. When two messages arrive
    together the mapping can invoke twice and the second invocation is
    throttled. A throttled message goes back on the queue
    after the visibility timeout and the attempt counts toward
    max_receive_count, which is why that is 5 rather than 3: a burst must not
    dead-letter good work. Two messages at once is rare for this cog.
  DESC
  type        = number
  default     = 1
}

variable "max_receive_count" {
  description = "Deliveries before a message goes to the DLQ. Not automatic — without a redrive policy a poison message retries forever. 5, not 3, because reserved_concurrency = 1 means a burst can throttle a good message, and a throttled attempt still counts."
  type        = number
  default     = 5
}

variable "log_retention_days" {
  description = "CloudWatch Logs is inherited whether you want it or not; an explicit group means it does not retain forever by default."
  type        = number
  default     = 30
}

# ── Worker environment ───────────────────────────────────────────────────
#
# What the two routed flows read, and nothing else. Lambda caps the whole
# environment at 4 KB, and the Google service-account JSON is most of that
# — see README.md before adding anything.

variable "kaiano_api_base_url" {
  description = "Base URL for api-kaianolevine-com."
  type        = string
}

variable "deejay_cog_api_key" {
  description = "This cog's own named API key (CD-019), DEEJAY_COG_API_KEY. No fallback — unset or wrong means 401 on every ingest."
  type        = string
  sensitive   = true

  # The first apply shipped the example's "..." and every call 401'd,
  # including the failure report that would have said so.
  validation {
    condition     = length(var.deejay_cog_api_key) >= 20 && !strcontains(var.deejay_cog_api_key, "...")
    error_message = "deejay_cog_api_key looks like the placeholder from terraform.tfvars.example."
  }
}

variable "google_credentials_json" {
  description = "Service-account JSON as a string, GOOGLE_CREDENTIALS_JSON. Drive and Sheets for both flows."
  type        = string
  sensitive   = true

  # The first apply shipped the example's placeholder, which the Google
  # client rejected before falling back to a credentials.json that does not
  # exist on Lambda. Parsing here fails the plan instead of every run.
  validation {
    condition = (
      can(jsondecode(var.google_credentials_json)) &&
      try(jsondecode(var.google_credentials_json).type, "") == "service_account" &&
      can(jsondecode(var.google_credentials_json).private_key)
    )
    error_message = "google_credentials_json must be a service-account JSON document (type = service_account, with a private_key)."
  }
}

variable "spotipy_client_id" {
  description = "SPOTIPY_CLIENT_ID. Unset skips the playlist sync and says so in the run report."
  type        = string
  sensitive   = true
  default     = ""
}

variable "spotipy_client_secret" {
  description = "SPOTIPY_CLIENT_SECRET."
  type        = string
  sensitive   = true
  default     = ""
}

variable "spotipy_refresh_token" {
  description = "SPOTIPY_REFRESH_TOKEN."
  type        = string
  sensitive   = true
  default     = ""
}

variable "spotify_radio_playlist_id" {
  description = "SPOTIFY_RADIO_PLAYLIST_ID. Unset leaves the radio playlist untouched and reports it."
  type        = string
  default     = ""
}

variable "vdj_history_folder_id" {
  description = <<-DESC
    VDJ_HISTORY_FOLDER_ID, for ingest-live-history. Required even though
    deejay_cog.config has a default: the Drive helper reads it from
    mini_app_polis.config, which has none.
  DESC
  type        = string
  default     = "1HGxEr5ocY9JLtXcJqDRIOD95rXU6QLUW"
}

variable "sentry_dsn" {
  description = "Sentry DSN for the worker, SENTRY_DSN."
  type        = string
  sensitive   = true
  default     = ""
}

variable "create_github_oidc_provider" {
  description = <<-DESC
    False when the account already has the GitHub OIDC provider — there can
    only be one per account, and a second `terraform apply` in a different
    cog's directory would otherwise fail on a resource that already exists.
    True for the first cog, false for every one after — so false here, by
    default rather than by remembering to pass it.
  DESC
  type        = bool
  default     = false
}

variable "create_api_producer" {
  description = <<-DESC
    Whether this state owns the API's sending identity.

    True for the first cog, false for every one after — the same shape as
    create_github_oidc_provider, and for the same reason. False here: the
    API's one producer user lives in evaluator-cog's state, and its
    `*-jobs` wildcard already covers deejay-jobs. There is one
    api-kaianolevine-com, so there should be one IAM user for it, holding
    one access key. A producer per cog means the API carries five
    credentials, five Doppler entries and five client configurations by
    the fifth cog, all saying the same thing.

    Its policy is a wildcard over `*-jobs`, so a new cog's queue is covered
    the moment it exists without a cross-state reference back to here.
  DESC
  type        = bool
  default     = false
}
