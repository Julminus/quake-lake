terraform {
  required_version = ">= 1.9"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.74"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.8"
    }
  }
  # State stays local (plan only, see the README). For a real deployment, keep it in a
  # versioned S3 bucket of its own: backend "s3" { bucket = "...", key = "quake-lake.tfstate" }.
}

provider "aws" {
  region = var.region

  # plan_without_credentials: plan a new deployment on a machine with no AWS account.
  # Nothing exists yet, so the plan needs no API call; these turn off the ones the
  # provider makes on start-up (checking the keys, looking up the account id).
  access_key                  = var.plan_without_credentials ? "plan-only" : null
  secret_key                  = var.plan_without_credentials ? "plan-only" : null
  skip_credentials_validation = var.plan_without_credentials
  skip_requesting_account_id  = var.plan_without_credentials
  skip_metadata_api_check     = var.plan_without_credentials

  default_tags {
    tags = {
      project    = var.name
      managed-by = "terraform"
    }
  }
}
