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

variable "silver" {
  description = "The bucket silver is written to (SILVER_URI s3://<bucket>), a bucket module."
  type        = object({ bucket = string, arn = string })
}

variable "results" {
  description = "The bucket for Athena's query results, a bucket module."
  type        = object({ bucket = string, arn = string })
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
    location      = "s3://${var.silver.bucket}/events/"
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
      output_location = "s3://${var.results.bucket}/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

locals {
  # "arn:<partition>:glue:<region>:<account>", from the database's own ARN.
  glue_arn = split(":database/", aws_glue_catalog_database.this.arn)[0]
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
        Sid    = "ReadCatalog"
        Effect = "Allow"
        Action = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
        # AWS's example Athena policies grant the `default` database too (read only here).
        Resource = [
          "${local.glue_arn}:catalog",
          "${local.glue_arn}:database/default",
          aws_glue_catalog_database.this.arn,
          "${local.glue_arn}:table/${var.database}/*",
        ]
      },
      {
        Sid       = "ListSilverEvents"
        Effect    = "Allow"
        Action    = "s3:ListBucket"
        Resource  = var.silver.arn
        Condition = { StringLike = { "s3:prefix" = ["events/*"] } }
      },
      {
        Sid      = "ReadSilverEvents"
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${var.silver.arn}/events/*"
      },
      {
        Sid      = "ListQueryResults"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = var.results.arn
      },
      {
        Sid      = "WriteQueryResults"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
        Resource = "${var.results.arn}/*"
      },
    ]
  })
}

output "athena" {
  description = "Where to query: the database and workgroup, what the table reads, where results go."
  value = {
    database         = aws_glue_catalog_database.this.name
    workgroup        = aws_athena_workgroup.this.name
    table_location   = aws_glue_catalog_table.silver_events.storage_descriptor[0].location
    results_location = aws_athena_workgroup.this.configuration[0].result_configuration[0].output_location
  }
}

output "query_policy_arn" {
  description = "IAM policy for querying silver with Athena."
  value       = aws_iam_policy.query.arn
}
