# In this directory this creates nothing: create_api_producer is false, and
# the API's one producer user lives in evaluator-cog's state. Kept so every
# cog's infra/ is the same template. What follows is that template's text.
#
# api-kaianolevine-com, as a caller. The fleet's only producer, and its one
# long-lived credential.
#
# **This is a fleet-wide resource living in the first cog's state**, like
# the GitHub OIDC provider above it. Every cog after the first sets
# `create_api_producer = false` — there is one API, so there should be one
# identity for it. Create one per cog and by the fifth the API holds five
# access keys, five Doppler entries and five boto3 client configurations,
# all expressing "the API may send to the fleet's queues".
#
# The name says `evaluator` because renaming an IAM user destroys and
# recreates it, which would invalidate the key the API is using and stop
# every enqueue until a new one reached Doppler. Not worth an outage for a
# cosmetic fix; see the migration doc if it is ever worth doing
# deliberately.

# The account this is applied to, for the queue ARN pattern below. Read
# rather than configured, so the policy cannot name the wrong account.
data "aws_caller_identity" "current" {}

resource "aws_iam_user" "producer" {
  count = var.create_api_producer ? 1 : 0
  name  = "${var.name_prefix}-producer"
}

data "aws_iam_policy_document" "producer" {
  # Send, and nothing else. It cannot read any queue, cannot delete from
  # one, and cannot see a dead-letter queue.
  #
  # The resource is a wildcard over `*-jobs` rather than a list of ARNs,
  # and that is deliberate. Every cog's queue is `<name_prefix>-jobs` in
  # this account, so one statement covers the fleet as it grows — no
  # cross-state reference from a cog's infra back to this policy, and no
  # step where adding a cog means remembering to widen a permission
  # somewhere else.
  #
  # What the wildcard gives up: a queue created later is writable by the
  # API without anyone deciding. That is the intended state — the API is
  # the only legitimate producer for any of them — and it is bounded by
  # the naming convention and by SendMessage being the only action. A
  # compromised key can cause work to happen; it cannot read a queue,
  # drain one, or forge the result of anything.
  statement {
    actions   = ["sqs:SendMessage"]
    resources = ["arn:aws:sqs:${var.region}:${data.aws_caller_identity.current.account_id}:*-jobs"]
  }
}

resource "aws_iam_user_policy" "producer" {
  count = var.create_api_producer ? 1 : 0
  # Keeps the name_prefix form, for the same reason the user keeps its
  # name: changing it forces a replacement, and Terraform replaces an
  # inline policy by deleting it and then creating the new one — a window,
  # however short, where the API cannot enqueue. The policy's contents are
  # what matter and those change in place.
  name   = "${var.name_prefix}-producer-send"
  user   = aws_iam_user.producer[0].name
  policy = data.aws_iam_policy_document.producer.json
}

# No aws_iam_access_key here, deliberately.
#
# Terraform would hold the secret in state, and this state is a plaintext
# file on a workstation. Mint the key once, by hand, and put it straight
# into Doppler:
#
#     aws iam create-access-key --user-name evaluator-producer
#
# There is no OIDC path from Railway, so this is a long-lived credential
# and it is the thing in this plan most worth a rotation reminder.
