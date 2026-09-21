# The work queue, and the thing that catches what it cannot deliver.

resource "aws_sqs_queue" "dlq" {
  name = "${var.name_prefix}-jobs-dlq"

  # The maximum. A message reaches here because something was wrong enough
  # to survive every retry, which usually means a person has to look at it
  # — and a person is slower than four days.
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_sqs_queue" "jobs" {
  name = "${var.name_prefix}-jobs"

  # Must exceed the function timeout. If SQS redelivers while the function
  # is still working, the same job runs twice — which is the duplicate
  # findings failure PIPE-002's idempotency guard exists to absorb, and
  # relying on that absorption for something preventable is the wrong trade.
  #
  # Derived rather than configured so the invariant cannot be broken by
  # changing one of the two and forgetting the other.
  visibility_timeout_seconds = var.worker_timeout_seconds + 60

  # Long polling. Short polling bills empty receives and adds latency.
  receive_wait_time_seconds = 20

  message_retention_seconds = 345600 # 4 days

  # Not automatic. Without this a poison message retries forever.
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })
}

# Lets a message be moved back to the work queue once its cause is fixed,
# from the console's DLQ redrive, without hand-rolling a consumer.
resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.jobs.arn]
  })
}
