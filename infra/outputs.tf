output "buckets" {
  description = "Bucket names: BRONZE_URI is s3://<bronze>, SILVER_URI s3://<silver>."
  value = {
    bronze         = module.bronze.bucket
    silver         = module.silver.bucket
    gold           = module.gold.bucket
    athena_results = module.athena_results.bucket
  }
}

output "ingest_function" {
  description = "The ingest Lambda: aws lambda invoke --function-name <this> out.json"
  value       = module.ingest.function_name
}

output "ingest_package_bytes" {
  description = "Size of the Lambda's deployment zip."
  value       = module.ingest.package_bytes
}

output "athena" {
  description = "Where to query silver: database.silver_events in this workgroup."
  value = {
    database  = module.catalog.database
    workgroup = module.catalog.workgroup
  }
}

output "policies" {
  description = "IAM policies to attach: pipeline (build silver), query (Athena over silver)."
  value = {
    pipeline = aws_iam_policy.pipeline.arn
    query    = module.catalog.query_policy_arn
  }
}
