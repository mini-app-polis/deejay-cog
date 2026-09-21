# The account budget (owned elsewhere — see var.create_account_budget), and
# this cog's alert topic and DLQ alarm, which are per cog.
#
# From evaluator-cog's copy: account-level, not evaluator-level.
#
# When the fleet generalises, everything else in this directory becomes one
# module per cog and this file does not: there is one budget and one OIDC
# provider per account. Kept separate so lifting it out later is a move
# rather than an untangling.

# Owned by evaluator-cog's state; see var.create_account_budget.
resource "aws_budgets_budget" "monthly" {
  count        = var.create_account_budget ? 1 : 0
  name         = "${var.name_prefix}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # Actual spend, not forecast. A forecast alarm on a near-zero baseline
  # cries wolf on the first day of every month.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
}

# Somewhere for an alarm to go.
#
# An alarm with no action is decoration. It changes state, nobody hears,
# and the console reads as coverage — which is the same shape as a green
# run that delivered nothing, arrived at from a different direction. This
# was exactly the state of the alarm below until a test message sat in the
# DLQ for twenty minutes and told no one.
resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email

  # AWS emails a confirmation link and the subscription stays
  # "PendingConfirmation" until someone clicks it. Terraform cannot do
  # that, and an unconfirmed subscription delivers nothing — so this
  # resource applying cleanly is NOT the same as the alarm being wired up.
  # Check with:
  #   aws sns list-subscriptions-by-topic --topic-arn <arn>
}

# A DLQ nobody has watched a message enter is not yet a DLQ — and one
# nobody is told about is not much better.
resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${var.name_prefix}-dlq-not-empty"
  alarm_description   = "A job failed every retry and is sitting in the dead-letter queue."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.dlq.name
  }

  alarm_actions = [aws_sns_topic.alerts.arn]

  # Also say when it clears. Without this the only signal is the first
  # one, and an operator has no way to tell "still broken" from "someone
  # fixed it and I never heard".
  ok_actions = [aws_sns_topic.alerts.arn]
}
