# quake-lake on AWS: the ingest Lambda writes bronze on a schedule; silver is built
# from bronze by the CLI (or the Airflow stack) with the pipeline policy below; Athena
# queries silver through the Glue table. Plan only: see the README.

module "bronze" {
  source      = "./modules/bucket"
  name_prefix = "${var.name}-bronze-"
  # Bronze is the only copy of BMKG's snapshots (its feeds forget), so an overwrite or
  # delete stays recoverable for 30 days.
  versioned = true
}

module "silver" {
  source      = "./modules/bucket"
  name_prefix = "${var.name}-silver-"
}

# Reserved for the gold marts as Parquet, which nothing writes yet (README, What's next).
module "gold" {
  source      = "./modules/bucket"
  name_prefix = "${var.name}-gold-"
}

module "athena_results" {
  source      = "./modules/bucket"
  name_prefix = "${var.name}-athena-"
  expire_days = 7
}

module "ingest" {
  source        = "./modules/ingest"
  function_name = "${var.name}-ingest"
  source_dir    = "${path.root}/../src"
  package_path  = "${path.root}/.build/ingest.zip"
  bronze        = module.bronze # name and ARN of one bucket, so they can't come from two
  schedules = {
    # BMKG's feeds only hold the latest events, so they are polled every hour;
    # USGS once a day, 7 days back, which also picks up a week of revisions.
    bmkg = {
      expression = "rate(1 hour)"
      event      = jsonencode({ sources = ["bmkg"] })
    }
    usgs = {
      expression = "cron(0 0 * * ? *)"
      event      = jsonencode({ sources = ["usgs"], days = 7 })
    }
  }
}

module "catalog" {
  source   = "./modules/catalog"
  database = replace(var.name, "-", "_")
  silver   = module.silver
  results  = module.athena_results
  # shortcut: Athena only sees these years of silver; widen it before backfilling earlier
  # or before 2035. AWS advises against projection when over half the projected
  # partitions are empty: 109 of these 252 months are, today. Each year more costs
  # 12 more S3 LISTs per query without a year filter.
  silver_years = [2015, 2035]
}

# For whoever builds silver (`quake-lake silver` with AWS credentials): read bronze,
# rewrite silver. Attach it to that user or role.
resource "aws_iam_policy" "pipeline" {
  name        = "${var.name}-pipeline"
  description = "Build quake-lake's silver from bronze."
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListLake"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = [module.bronze.arn, module.silver.arn]
      },
      {
        Sid      = "ReadBronze"
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${module.bronze.arn}/*"
      },
      {
        Sid      = "RewriteSilver"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "${module.silver.arn}/*"
      },
    ]
  })
}
