# Offline tests: `terraform test` from infra/. The AWS provider is mocked, so "apply" here
# creates nothing anywhere: computed attributes (ARNs, bucket names) get made-up values
# and the assertions read what Terraform built. The policies are compared whole, so any
# widening (an extra action, a wildcard, a lost condition) fails a test.

mock_provider "aws" {
  # The provider checks that these are ARNs where another resource takes them as input.
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::123456789012:role/test-ingest" }
  }
  mock_resource "aws_lambda_function" {
    defaults = { arn = "arn:aws:lambda:ap-southeast-1:123456789012:function:test-ingest" }
  }
  mock_resource "aws_cloudwatch_event_rule" {
    defaults = { arn = "arn:aws:events:ap-southeast-1:123456789012:rule/test" }
  }
  mock_resource "aws_glue_catalog_database" {
    defaults = { arn = "arn:aws:glue:ap-southeast-1:123456789012:database/test_lake" }
  }
}

run "a_plain_bucket_is_private_and_tls_only" {
  command = apply
  module {
    source = "./modules/bucket"
  }
  variables {
    name_prefix = "test-plain-"
  }

  assert {
    condition = alltrue([
      aws_s3_bucket_public_access_block.this.block_public_acls,
      aws_s3_bucket_public_access_block.this.block_public_policy,
      aws_s3_bucket_public_access_block.this.ignore_public_acls,
      aws_s3_bucket_public_access_block.this.restrict_public_buckets,
    ])
    error_message = "Every public-access switch must be on."
  }
  assert {
    condition = jsondecode(aws_s3_bucket_policy.this.policy).Statement == [{
      Sid       = "DenyPlainHTTP"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.this.arn, "${aws_s3_bucket.this.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
    error_message = "The bucket policy must deny every request that isn't TLS, and nothing else."
  }
  assert {
    condition     = length(aws_s3_bucket_versioning.this) == 0
    error_message = "Versioning is off unless asked for."
  }
  assert {
    condition     = [for r in aws_s3_bucket_lifecycle_configuration.this.rule : r.id] == ["abort-incomplete-uploads"]
    error_message = "A plain bucket only cleans up unfinished uploads; nothing expires."
  }
  assert {
    condition     = aws_s3_bucket_lifecycle_configuration.this.rule[0].abort_incomplete_multipart_upload[0].days_after_initiation == 1
    error_message = "Unfinished uploads are cleaned up after a day."
  }
}

run "a_versioned_expiring_bucket_gets_both_rules" {
  command = apply
  module {
    source = "./modules/bucket"
  }
  variables {
    name_prefix = "test-both-"
    versioned   = true
    expire_days = 7
  }

  assert {
    condition     = aws_s3_bucket_versioning.this[0].versioning_configuration[0].status == "Enabled"
    error_message = "versioned = true must switch versioning on."
  }
  assert {
    condition = [for r in aws_s3_bucket_lifecycle_configuration.this.rule : r.id] == [
      "abort-incomplete-uploads", "expire-old-versions", "expire-objects",
    ]
    error_message = "Expected the upload, old-version and expiry rules, in that order."
  }
  assert {
    condition = (
      aws_s3_bucket_lifecycle_configuration.this.rule[1].noncurrent_version_expiration[0].noncurrent_days == 30
      && aws_s3_bucket_lifecycle_configuration.this.rule[1].expiration[0].expired_object_delete_marker
    )
    error_message = "Old versions are kept 30 days, and then their delete markers go too."
  }
  assert {
    condition     = aws_s3_bucket_lifecycle_configuration.this.rule[2].expiration[0].days == 7
    error_message = "expire_days must reach the expiry rule."
  }
}

run "the_ingest_role_can_only_write_bronze_and_its_logs" {
  command = apply
  module {
    source = "./modules/ingest"
  }
  variables {
    function_name = "test-ingest"
    source_dir    = "../src"
    package_path  = ".build/test-ingest.zip"
    bronze        = { bucket = "test-bronze", arn = "arn:aws:s3:::test-bronze" }
    schedules = {
      hourly = { expression = "rate(1 hour)", event = "{\"sources\":[\"bmkg\"]}" }
      daily  = { expression = "cron(0 0 * * ? *)", event = "{\"days\":7,\"sources\":[\"usgs\"]}" }
    }
  }
  # Distinct ARNs per rule, so a permission or target wired to the wrong rule fails.
  override_resource {
    target = aws_cloudwatch_event_rule.this["hourly"]
    values = { arn = "arn:aws:events:ap-southeast-1:123456789012:rule/test-ingest-hourly" }
  }
  override_resource {
    target = aws_cloudwatch_event_rule.this["daily"]
    values = { arn = "arn:aws:events:ap-southeast-1:123456789012:rule/test-ingest-daily" }
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.this.policy).Statement == [
      { Sid = "WriteBronze", Effect = "Allow", Action = "s3:PutObject", Resource = "arn:aws:s3:::test-bronze/*" },
      {
        Sid      = "WriteOwnLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.this.arn}:*"
      },
    ]
    error_message = "The role may put objects into bronze and write its own logs, nothing else."
  }
  assert {
    condition = jsondecode(aws_iam_role.this.assume_role_policy).Statement == [
      { Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" },
    ]
    error_message = "Only Lambda may assume the role, and only assume it."
  }
  assert {
    condition     = aws_lambda_function.this.source_code_hash == data.archive_file.package.output_base64sha256
    error_message = "A change to src/ must change the deployed code."
  }
  assert {
    condition     = aws_lambda_function.this.environment[0].variables == tomap({ BRONZE_URI = "s3://test-bronze" })
    error_message = "The handler reads its bucket from BRONZE_URI."
  }
  assert {
    condition     = aws_lambda_function.this.handler == "quake_lake.handler.lambda_handler" && aws_lambda_function.this.runtime == "python3.12"
    error_message = "The handler and runtime CI's Lambda-image test uses."
  }
  assert {
    condition     = aws_lambda_function.this.logging_config[0].log_group == "/aws/lambda/test-ingest"
    error_message = "The function logs to the group Terraform creates (with a retention), not one Lambda would make."
  }
  assert {
    condition = (
      aws_lambda_function_event_invoke_config.this.function_name == aws_lambda_function.this.function_name
      && aws_lambda_function_event_invoke_config.this.maximum_retry_attempts == 2
    )
    error_message = "A failed invocation of this function is retried twice (BMKG's feeds forget)."
  }
  assert {
    condition = { for k, t in aws_cloudwatch_event_target.this : k => [t.rule, t.arn, t.input] } == {
      for k, s in var.schedules : k => [aws_cloudwatch_event_rule.this[k].name, aws_lambda_function.this.arn, s.event]
    }
    error_message = "Each rule must invoke the function with its own schedule's event."
  }
  assert {
    condition = { for k, r in aws_cloudwatch_event_rule.this : k => r.schedule_expression } == {
      for k, s in var.schedules : k => s.expression
    }
    error_message = "Each rule runs on its own schedule."
  }
  assert {
    condition = { for k, p in aws_lambda_permission.this : k => [p.function_name, p.source_arn, p.principal, p.action] } == {
      for k, r in aws_cloudwatch_event_rule.this : k => [aws_lambda_function.this.function_name, r.arn, "events.amazonaws.com", "lambda:InvokeFunction"]
    }
    error_message = "Each rule, and only EventBridge, may invoke the function."
  }
}

run "the_catalog_reads_silver_and_the_query_policy_is_scoped" {
  command = apply
  module {
    source = "./modules/catalog"
  }
  variables {
    database     = "test_lake"
    silver       = { bucket = "test-silver", arn = "arn:aws:s3:::test-silver" }
    results      = { bucket = "test-results", arn = "arn:aws:s3:::test-results" }
    silver_years = [2015, 2035]
  }
  # Another partition, region and account than anywhere else here, so the Glue ARNs must
  # really come from the database's ARN.
  override_resource {
    target = aws_glue_catalog_database.this
    values = { arn = "arn:aws-cn:glue:cn-north-1:210987654321:database/test_lake" }
  }

  assert {
    condition     = aws_glue_catalog_table.silver_events.storage_descriptor[0].location == "s3://test-silver/events/"
    error_message = "The table reads silver's events/ folder."
  }
  assert {
    condition = (
      aws_glue_catalog_table.silver_events.parameters.classification == "parquet"
      && aws_glue_catalog_table.silver_events.storage_descriptor[0].ser_de_info[0].serialization_library == "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      && aws_glue_catalog_table.silver_events.storage_descriptor[0].input_format == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    )
    error_message = "Silver is Parquet: the table must read it with the Parquet SerDe and input format."
  }
  assert {
    condition     = [for k in aws_glue_catalog_table.silver_events.partition_keys : [k.name, k.type]] == [["year", "int"], ["month", "int"]]
    error_message = "Partitions are silver's year=/month= folders, in that order, as integers."
  }
  assert {
    condition = { for k, v in aws_glue_catalog_table.silver_events.parameters : k => v if startswith(k, "projection.") } == {
      "projection.enabled"      = "true"
      "projection.year.type"    = "integer"
      "projection.year.range"   = "2015,2035"
      "projection.month.type"   = "integer"
      "projection.month.range"  = "1,12"
      "projection.month.digits" = "2"
    }
    error_message = "Partition projection must cover silver_years and every zero-padded month."
  }
  assert {
    condition = (
      aws_athena_workgroup.this.configuration[0].result_configuration[0].output_location == "s3://test-results/"
      && aws_athena_workgroup.this.configuration[0].enforce_workgroup_configuration
      && aws_athena_workgroup.this.configuration[0].bytes_scanned_cutoff_per_query == 1000000000
      && aws_athena_workgroup.this.configuration[0].result_configuration[0].encryption_configuration[0].encryption_option == "SSE_S3"
    )
    error_message = "Results go, encrypted, to the results bucket, a query stops at 1 GB, and clients can't override either."
  }
  assert {
    condition = jsondecode(aws_iam_policy.query.policy).Statement == [
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
        Resource = [
          "arn:aws-cn:glue:cn-north-1:210987654321:catalog",
          "arn:aws-cn:glue:cn-north-1:210987654321:database/default",
          "arn:aws-cn:glue:cn-north-1:210987654321:database/test_lake",
          "arn:aws-cn:glue:cn-north-1:210987654321:table/test_lake/*",
        ]
      },
      {
        Sid       = "ListSilverEvents"
        Effect    = "Allow"
        Action    = "s3:ListBucket"
        Resource  = "arn:aws:s3:::test-silver"
        Condition = { StringLike = { "s3:prefix" = ["events/*"] } }
      },
      { Sid = "ReadSilverEvents", Effect = "Allow", Action = "s3:GetObject", Resource = "arn:aws:s3:::test-silver/events/*" },
      {
        Sid      = "ListQueryResults"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = "arn:aws:s3:::test-results"
      },
      {
        Sid      = "WriteQueryResults"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
        Resource = "arn:aws:s3:::test-results/*"
      },
    ]
    error_message = "The query policy reaches this workgroup, this database, silver's events (read only) and the results bucket, nothing else."
  }
}

run "the_whole_configuration" {
  command = apply

  assert {
    condition = jsondecode(aws_iam_policy.pipeline.policy).Statement == [
      { Sid = "ListLake", Effect = "Allow", Action = "s3:ListBucket", Resource = [module.bronze.arn, module.silver.arn] },
      { Sid = "ReadBronze", Effect = "Allow", Action = "s3:GetObject", Resource = "${module.bronze.arn}/*" },
      {
        Sid      = "RewriteSilver"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "${module.silver.arn}/*"
      },
    ]
    error_message = "The pipeline may read bronze and rewrite silver, nothing else."
  }
  assert {
    condition = output.retention == {
      bronze         = { versioned = true, expire_days = null }
      silver         = { versioned = false, expire_days = null }
      gold           = { versioned = false, expire_days = null }
      athena_results = { versioned = false, expire_days = 7 }
    }
    error_message = "Bronze, the only copy of BMKG's snapshots, keeps old versions; only query results expire, after 7 days."
  }
  # The wiring between modules: each reads and writes the right bucket. (Each module takes
  # a whole bucket module, so a name and an ARN can't come from two different buckets.)
  assert {
    condition     = output.ingest.function.bronze_uri == "s3://${module.bronze.bucket}"
    error_message = "The Lambda writes into bronze."
  }
  assert {
    condition = (
      output.athena.table_location == "s3://${module.silver.bucket}/events/"
      && output.athena.results_location == "s3://${module.athena_results.bucket}/"
    )
    error_message = "Athena reads silver and writes its results to the results bucket."
  }
  assert {
    condition     = output.ingest.function.retries == 2
    error_message = "A failed invocation is retried twice."
  }
  assert {
    condition = (
      output.ingest.schedules.bmkg.expression == "rate(1 hour)"
      && jsondecode(output.ingest.schedules.bmkg.event) == { sources = ["bmkg"] }
    )
    error_message = "BMKG's feeds only hold the latest events: poll them every hour, BMKG only."
  }
  assert {
    condition = (
      output.ingest.schedules.usgs.expression == "cron(0 0 * * ? *)"
      && jsondecode(output.ingest.schedules.usgs.event) == { sources = ["usgs"], days = 7 }
    )
    error_message = "USGS once a day, 7 days back (the handler's look-back)."
  }
  assert {
    condition     = keys(output.ingest.schedules) == ["bmkg", "usgs"]
    error_message = "Two schedules, no more."
  }
  assert {
    condition = (
      output.ingest.function.memory_mb == 256
      && output.ingest.function.timeout_s == 300
      && output.ingest.function.log_retention_days == 30
    )
    error_message = "256 MB (measured: docs/results/lambda-s11.log), a 5-minute cap, 30 days of logs."
  }
  assert {
    condition     = output.athena.database == "quake_lake" && output.athena.workgroup == "quake-lake"
    error_message = "Athena names come from var.name, with an underscore for Glue."
  }
}
