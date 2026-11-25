# Offline tests of the modules' logic: `terraform test` from infra/. The AWS provider is
# mocked, so "apply" here creates nothing anywhere: computed attributes (ARNs, bucket
# names) get made-up values and the assertions read the configuration Terraform built.

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
    defaults = { catalog_id = "123456789012" }
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
    condition     = aws_s3_bucket_lifecycle_configuration.this.rule[1].noncurrent_version_expiration[0].noncurrent_days == 30
    error_message = "Old versions are kept 30 days by default."
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
    function_name     = "test-ingest"
    source_dir        = "../src"
    package_path      = ".build/test-ingest.zip"
    bronze_bucket     = "test-bronze"
    bronze_bucket_arn = "arn:aws:s3:::test-bronze"
    schedules = {
      hourly = { expression = "rate(1 hour)", event = "{\"sources\":[\"bmkg\"]}" }
      daily  = { expression = "cron(0 0 * * ? *)", event = "{\"days\":7,\"sources\":[\"usgs\"]}" }
    }
  }

  assert {
    condition     = jsondecode(aws_iam_role_policy.this.policy).Statement[*].Action == ["s3:PutObject", ["logs:CreateLogStream", "logs:PutLogEvents"]]
    error_message = "The role may put objects and write logs, nothing else (no read, list or delete)."
  }
  assert {
    condition = jsondecode(aws_iam_role_policy.this.policy).Statement[*].Resource == [
      "arn:aws:s3:::test-bronze/*", "${aws_cloudwatch_log_group.this.arn}:*",
    ]
    error_message = "Writes are limited to the bronze bucket and the function's own log group."
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
    condition     = { for k, t in aws_cloudwatch_event_target.this : k => t.input } == { for k, s in var.schedules : k => s.event }
    error_message = "Each schedule must send its own event."
  }
  assert {
    condition = alltrue([for k, p in aws_lambda_permission.this :
    p.source_arn == aws_cloudwatch_event_rule.this[k].arn && p.principal == "events.amazonaws.com"])
    error_message = "Each rule, and only EventBridge, may invoke the function."
  }
}

run "the_catalog_reads_silver_and_the_query_policy_is_scoped" {
  command = apply
  module {
    source = "./modules/catalog"
  }
  variables {
    database           = "test_lake"
    region             = "ap-southeast-1"
    silver_bucket      = "test-silver"
    silver_bucket_arn  = "arn:aws:s3:::test-silver"
    results_bucket     = "test-results"
    results_bucket_arn = "arn:aws:s3:::test-results"
    silver_years       = [2015, 2040]
  }

  assert {
    condition     = aws_glue_catalog_table.silver_events.storage_descriptor[0].location == "s3://test-silver/events/"
    error_message = "The table reads silver's events/ folder."
  }
  assert {
    condition     = aws_glue_catalog_table.silver_events.partition_keys[*].name == tolist(["year", "month"])
    error_message = "Partitions are silver's year=/month= folders, in that order."
  }
  assert {
    condition = { for k, v in aws_glue_catalog_table.silver_events.parameters : k => v if startswith(k, "projection.") } == {
      "projection.enabled"      = "true"
      "projection.year.type"    = "integer"
      "projection.year.range"   = "2015,2040"
      "projection.month.type"   = "integer"
      "projection.month.range"  = "1,12"
      "projection.month.digits" = "2"
    }
    error_message = "Partition projection must cover silver_years and every zero-padded month."
  }
  assert {
    condition     = aws_athena_workgroup.this.configuration[0].result_configuration[0].output_location == "s3://test-results/"
    error_message = "Query results go to the results bucket."
  }
  assert {
    condition     = aws_athena_workgroup.this.configuration[0].enforce_workgroup_configuration
    error_message = "Clients must not be able to override the result location or the scan cutoff."
  }
  assert {
    condition = flatten(jsondecode(aws_iam_policy.query.policy).Statement[*].Resource) == [
      aws_athena_workgroup.this.arn,
      "arn:aws:glue:ap-southeast-1:${aws_glue_catalog_database.this.catalog_id}:catalog",
      aws_glue_catalog_database.this.arn,
      "arn:aws:glue:ap-southeast-1:${aws_glue_catalog_database.this.catalog_id}:table/test_lake/*",
      "arn:aws:s3:::test-silver",
      "arn:aws:s3:::test-silver/events/*",
      "arn:aws:s3:::test-results",
      "arn:aws:s3:::test-results/*",
    ]
    error_message = "The query policy reaches this workgroup, this database, silver's events and the results bucket only."
  }
  assert {
    condition     = !anytrue([for s in jsondecode(aws_iam_policy.query.policy).Statement : contains(flatten([s.Action]), "s3:PutObject") && s.Resource != "arn:aws:s3:::test-results/*"])
    error_message = "Querying may write only to the results bucket, never to silver."
  }
}

run "the_whole_configuration_plans" {
  command = plan

  assert {
    condition     = output.athena == { database = "quake_lake", workgroup = "quake-lake" }
    error_message = "Athena names come from var.name, with an underscore for Glue."
  }
  assert {
    condition     = output.ingest_function == "quake-lake-ingest"
    error_message = "The function is named after var.name."
  }
}
