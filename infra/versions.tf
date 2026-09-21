terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  # No remote backend, deliberately. See README.md: Terraform is applied
  # from a workstation and owns infrastructure; CI owns the function's code
  # and needs no state at all. A solo operator with one workstation buys
  # little from an S3 backend and pays a bootstrap chicken-and-egg for it.
  # Revisit when a second person or a second machine applies this.
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = "mini-app-polis"
      Component = "deejay-worker"
      ManagedBy = "terraform"
      Repo      = "mini-app-polis/deejay-cog"
    }
  }
}
