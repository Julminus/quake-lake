# The Glue Data Catalog table over silver's Parquet, and an Athena workgroup to query it.
# Partition projection computes silver's year=/month= partitions from the table's
# properties, so no crawler runs and nothing has to register a new month.

terraform {
  required_version = ">= 1.9"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.74"
    }
  }
}

variable "database" {
  description = "Glue database name (letters, digits and underscores, for Athena)."
  type        = string
}

variable "region" {
  description = "AWS region of the catalog, for its ARNs."
  type        = string
}

variable "silver_bucket" {
  description = "Bucket silver is written to (its SILVER_URI is s3://<bucket>)."
  type        = string
}

variable "silver_bucket_arn" {
  description = "ARN of the silver bucket."
  type        = string
}

variable "results_bucket" {
  description = "Bucket for Athena's query results."
  type        = string
}

variable "results_bucket_arn" {
  description = "ARN of the results bucket."
  type        = string
}

variable "silver_years" {
  description = "First and last year Athena looks for in silver."
  type        = tuple([number, number])
}

variable "bytes_scanned_cutoff" {
  description = "Athena cancels a query in this workgroup once it has scanned this much."
  type        = number
  default     = 1000000000 # 1 GB, $0.005; silver is about 3 MB
}

resource "aws_glue_catalog_database" "this" {
  name = var.database
}

resource "aws_glue_catalog_table" "silver_events" {
  database_name = aws_glue_catalog_database.this.name
  name          = "silver_events"
  description   = "Silver events (quake_lake.silver.EVENTS): one row per event, USGS and BMKG."
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                  = "TRUE"
    classification            = "parquet"
    "projection.enabled"      = "true"
    "projection.year.type"    = "integer"
    "projection.year.range"   = "${var.silver_years[0]},${var.silver_years[1]}"
    "projection.month.type"   = "integer"
    "projection.month.range"  = "1,12"
    "projection.month.digits" = "2" # silver writes month=01, not month=1
  }

  partition_keys {
    name = "year"
    type = "int"
  }
  partition_keys {
    name = "month"
    type = "int"
  }

  storage_descriptor {
    location      = "s3://${var.silver_bucket}/events/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    # The same list as silver's schema; tests/test_infra.py keeps the two in step.
    dynamic "columns" {
      for_each = jsondecode(file("${path.module}/silver_events_columns.json"))
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

resource "aws_athena_workgroup" "this" {
  name = replace(var.database, "_", "-")

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query     = var.bytes_scanned_cutoff
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
    result_configuration {
      output_location = "s3://${var.results_bucket}/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

locals {
  # The catalog's id is the account id, known once the database exists.
  glue_arn = "arn:aws:glue:${var.region}:${aws_glue_catalog_database.this.catalog_id}"
}

# For whoever queries silver with Athena: this workgroup, this table, silver's events
# (read only) and the results bucket. Attach it to your user or role.
resource "aws_iam_policy" "query" {
  name        = "${aws_athena_workgroup.this.name}-query"
  description = "Query silver_events with Athena in the ${aws_athena_workgroup.this.name} workgroup."
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RunQueries"
        Effect = "Allow"
        Action = [
          "athena:GetWorkGroup", "athena:StartQueryExecution", "athena:StopQueryExecution",
          "athena:GetQueryExecution", "athena:GetQueryResults", "athena:ListQueryExecutions",
        ]
        Resource = aws_athena_workgroup.this.arn
      },
      {
        Sid      = "ReadCatalog"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
        Resource = ["${local.glue_arn}:catalog", aws_glue_catalog_database.this.arn, "${local.glue_arn}:table/${var.database}/*"]
      },
      {
        Sid       = "ListSilverEvents"
        Effect    = "Allow"
        Action    = "s3:ListBucket"
        Resource  = var.silver_bucket_arn
        Condition = { StringLike = { "s3:prefix" = ["events/*"] } }
      },
      {
        Sid      = "ReadSilverEvents"
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${var.silver_bucket_arn}/events/*"
      },
      {
        Sid      = "ListQueryResults"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = var.results_bucket_arn
      },
      {
        Sid      = "WriteQueryResults"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
        Resource = "${var.results_bucket_arn}/*"
      },
    ]
  })
}

output "database" {
  description = "Glue database holding silver_events."
  value       = aws_glue_catalog_database.this.name
}

output "workgroup" {
  description = "Athena workgroup to query in."
  value       = aws_athena_workgroup.this.name
}

output "query_policy_arn" {
  description = "IAM policy for querying silver with Athena."
  value       = aws_iam_policy.query.arn
}
