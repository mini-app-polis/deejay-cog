# How CI updates the function without holding a long-lived key.

data "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc_provider ? 0 : 1
  url   = "https://token.actions.githubusercontent.com"
}

resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc_provider ? 1 : 0

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]

  # AWS stopped validating these for this issuer in 2023 and now uses its
  # own trust store, but the argument is still required. Both of GitHub's
  # published values, so a rotation of either does not break the provider.
  thumbprint_list = [
    "6938fd4d98bab03faadb97b34396831e3780aea1",
    "1c58a3a8518e8759bf075b76b750d4f2df264fcd",
  ]
}

locals {
  github_oidc_arn = var.create_github_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : data.aws_iam_openid_connect_provider.github[0].arn
}

data "aws_iam_policy_document" "deploy_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.github_oidc_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Scoped to this repository. Without the sub condition any repository
    # on GitHub could assume this role.
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:*"]
    }
  }
}

resource "aws_iam_role" "deploy" {
  name               = "${var.name_prefix}-ci-deploy"
  assume_role_policy = data.aws_iam_policy_document.deploy_assume.json
}

data "aws_iam_policy_document" "deploy" {
  # Code only, plus the reads a deploy needs to confirm itself.
  #
  # CI cannot change the function's configuration, its role, its environment
  # or its concurrency — those are Terraform's, applied from a workstation,
  # and a deploy that could change them would be able to point the worker at
  # a different API without anyone reviewing a .tf file. Note the asymmetry
  # that makes that safe: UpdateFunctionConfiguration is absent while
  # GetFunctionConfiguration is present, because reading is not writing.
  #
  # GetFunctionConfiguration is a separate IAM action from GetFunction and
  # is not implied by it. `aws lambda wait function-updated` polls it, so
  # without it the deploy lands and then fails on the wait — the worst of
  # both: the code is live and the run is red.
  statement {
    actions = [
      "lambda:UpdateFunctionCode",
      "lambda:GetFunction",
      "lambda:GetFunctionConfiguration",
    ]
    resources = [aws_lambda_function.worker.arn]
  }
}

resource "aws_iam_role_policy" "deploy" {
  name   = "${var.name_prefix}-ci-deploy"
  role   = aws_iam_role.deploy.id
  policy = data.aws_iam_policy_document.deploy.json
}
