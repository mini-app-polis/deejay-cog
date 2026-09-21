# What the code ticket needs back from this one.

output "queue_url" {
  description = "For probing by hand. The API derives this from the cog name and does not need it configured."
  value       = aws_sqs_queue.jobs.url
}

output "queue_arn" {
  value = aws_sqs_queue.jobs.arn
}

output "dlq_arn" {
  value = aws_sqs_queue.dlq.arn
}

output "dlq_url" {
  description = "For watching a poison message arrive, and for redriving it once fixed."
  value       = aws_sqs_queue.dlq.url
}

output "function_name" {
  description = "For the CI deploy step."
  value       = aws_lambda_function.worker.function_name
}

output "region" {
  value = var.region
}

output "deploy_role_arn" {
  description = "Set as the AWS_DEPLOY_ROLE_ARN repository variable in GitHub."
  value       = aws_iam_role.deploy.arn
}

output "producer_user_name" {
  description = "Mint its access key by hand and put it in Doppler — see producer.tf. Empty on a cog that does not own the API's identity."
  value       = one(aws_iam_user.producer[*].name)
}

output "alerts_topic_arn" {
  description = "Check the email subscription is CONFIRMED, not PendingConfirmation."
  value       = aws_sns_topic.alerts.arn
}


