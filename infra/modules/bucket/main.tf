# One private S3 bucket of the lake. New buckets already get SSE-S3 encryption and
# BucketOwnerEnforced ownership (ACLs off) by default, so neither is repeated here.

terraform {
  required_version = ">= 1.9"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.74"
    }
  }
}

variable "name_prefix" {
  description = "Bucket name prefix; AWS appends a unique suffix (names are global)."
  type        = string
}

variable "versioned" {
  description = "Keep old versions of overwritten or deleted objects, for noncurrent_days."
  type        = bool
  default     = false
}

variable "noncurrent_days" {
  description = "Days an old version is kept, when versioned."
  type        = number
  default     = 30
}

variable "expire_days" {
  description = "Delete every object this many days after it was written (null: never)."
  type        = number
  default     = null
}

resource "aws_s3_bucket" "this" {
  bucket_prefix = var.name_prefix
}

resource "aws_s3_bucket_public_access_block" "this" {
  bucket                  = aws_s3_bucket.this.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Policies are jsonencode()d rather than aws_iam_policy_document data sources, so the
# offline tests (tests/*.tftest.hcl, a mocked provider) can read what they grant.
resource "aws_s3_bucket_policy" "this" {
  bucket = aws_s3_bucket.this.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyPlainHTTP"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.this.arn, "${aws_s3_bucket.this.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  # S3 refuses a bucket policy while the public access block is being written.
  depends_on = [aws_s3_bucket_public_access_block.this]
}

resource "aws_s3_bucket_versioning" "this" {
  count  = var.versioned ? 1 : 0
  bucket = aws_s3_bucket.this.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  dynamic "rule" {
    for_each = var.versioned ? [1] : []
    content {
      id     = "expire-old-versions"
      status = "Enabled"
      filter {}
      noncurrent_version_expiration {
        noncurrent_days = var.noncurrent_days
      }
      expiration {
        expired_object_delete_marker = true
      }
    }
  }

  dynamic "rule" {
    for_each = var.expire_days == null ? [] : [var.expire_days]
    content {
      id     = "expire-objects"
      status = "Enabled"
      filter {}
      expiration {
        days = rule.value
      }
    }
  }

  # Lifecycle rules on a versioned bucket need versioning switched on first.
  depends_on = [aws_s3_bucket_versioning.this]
}

output "bucket" {
  description = "The bucket's name."
  value       = aws_s3_bucket.this.bucket
}

output "arn" {
  description = "The bucket's ARN."
  value       = aws_s3_bucket.this.arn
}
