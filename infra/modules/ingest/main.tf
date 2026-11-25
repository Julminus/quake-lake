# The ingest Lambda (quake_lake.handler.lambda_handler) and the EventBridge schedules
# that invoke it. The package is src/ zipped: the handler needs only the standard
# library and boto3, which the Python runtime provides.

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
}

variable "function_name" {
  description = "Name of the Lambda function (and of its log group and role)."
  type        = string
}

variable "source_dir" {
  description = "The folder holding the quake_lake package (the repository's src/)."
  type        = string
}

variable "package_path" {
  description = "Where to write the zip the function is deployed from."
  type        = string
}

variable "bronze" {
  description = "The bucket the handler writes to (a bucket module), its name and ARN together."
  type        = object({ bucket = string, arn = string })
}

variable "schedules" {
  description = "EventBridge schedule expression and handler event (JSON) for each schedule."
  type = map(object({
    expression = string
    event      = string
  }))
}

variable "memory_mb" {
  description = "Memory for the function; Lambda's CPU share grows with it."
  type        = number
  default     = 256
}

variable "timeout_s" {
  description = "Hard cap on one invocation (a stalled USGS request has no shorter bound)."
  type        = number
  default     = 300
}

variable "log_retention_days" {
  description = "Days CloudWatch keeps the function's logs."
  type        = number
  default     = 30
}

data "archive_file" "package" {
  type             = "zip"
  source_dir       = var.source_dir
  output_path      = var.package_path
  excludes         = ["**/__pycache__/**"]
  output_file_mode = "0644" # the same zip, and hash, from Windows and Linux checkouts
  # shortcut: any other untracked file under src/ is packaged too, so the hash matches only
  # a clean checkout; package from an export of the commit if that ever matters.
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_iam_role" "this" {
  name = var.function_name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# What the handler does and nothing more: write objects into bronze, and its own logs.
# No list, read or delete: it never needs them, so a bug can't delete bronze (and bronze
# is versioned, so an overwrite can be undone for 30 days).
resource "aws_iam_role_policy" "this" {
  name = "ingest"
  role = aws_iam_role.this.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "WriteBronze"
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${var.bronze.arn}/*"
      },
      {
        Sid      = "WriteOwnLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.this.arn}:*"
      },
    ]
  })
}

resource "aws_lambda_function" "this" {
  function_name    = var.function_name
  role             = aws_iam_role.this.arn
  runtime          = "python3.12"
  handler          = "quake_lake.handler.lambda_handler"
  filename         = data.archive_file.package.output_path
  source_code_hash = data.archive_file.package.output_base64sha256
  memory_size      = var.memory_mb
  timeout          = var.timeout_s

  environment {
    variables = {
      BRONZE_URI = "s3://${var.bronze.bucket}"
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.this.name
  }

  # The role's policy must exist before the first invocation can write anything.
  depends_on = [aws_iam_role_policy.this]
}

# A failed invocation is retried twice, about 1 and 3 minutes later (Lambda's default for
# EventBridge's asynchronous calls, written down so the tests hold it). fetch()'s own
# retries span about 7 s; these outlast a short BMKG outage, and BMKG's feeds forget.
# The cost: the handler fails only after writing every feed that worked, so a retry
# writes those again (about 10 KB; silver drops the repeats).
resource "aws_lambda_function_event_invoke_config" "this" {
  function_name          = aws_lambda_function.this.function_name
  maximum_retry_attempts = 2
}

resource "aws_cloudwatch_event_rule" "this" {
  for_each            = var.schedules
  name                = "${var.function_name}-${each.key}"
  schedule_expression = each.value.expression
}

resource "aws_cloudwatch_event_target" "this" {
  for_each = var.schedules
  rule     = aws_cloudwatch_event_rule.this[each.key].name
  arn      = aws_lambda_function.this.arn
  input    = each.value.event
}

resource "aws_lambda_permission" "this" {
  for_each      = var.schedules
  statement_id  = "events-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.this.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.this[each.key].arn
}

output "function" {
  description = "The function as built: name, bucket, memory, timeout, retries, logs, zip size."
  value = {
    name               = aws_lambda_function.this.function_name
    bronze_uri         = aws_lambda_function.this.environment[0].variables.BRONZE_URI
    retries            = aws_lambda_function_event_invoke_config.this.maximum_retry_attempts
    memory_mb          = aws_lambda_function.this.memory_size
    timeout_s          = aws_lambda_function.this.timeout
    log_retention_days = aws_cloudwatch_log_group.this.retention_in_days
    package_bytes      = data.archive_file.package.output_size
  }
}

output "schedules" {
  description = "Each schedule's expression and the event it sends."
  value = {
    for k, r in aws_cloudwatch_event_rule.this :
    k => { expression = r.schedule_expression, event = aws_cloudwatch_event_target.this[k].input }
  }
}
