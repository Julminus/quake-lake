# What the AWS deployment would cost

**About 3 cents a month as deployed, and about $0.60 to $0.90 with a daily silver rebuild
and 300 Athena queries a month.** Nothing here has run on AWS: `infra/` is planned, never
applied. These are estimates from AWS's own price list, not a bill.

Numbers below come from [`results/aws-cost-s11.json`](results/aws-cost-s11.json), written
by `uv run python scripts/aws_cost.py`. The script downloads the on-demand prices for
`ap-southeast-1` (Singapore, the default region in `infra/variables.tf`) from the
[AWS Price List bulk API](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/using-ppslong.html)
(published 2026-11-19 to 2026-11-25), takes usage from earlier measurements in
`docs/results/`, and applies no free tier. `--region ap-southeast-3` prices Jakarta.

## The estimate

USD per 30-day month. Month 1 is the first month after loading the 2015-to-today backfill;
month 12 has a year of hourly BMKG snapshots in bronze.

| Item | What it is | Month 1 | Month 12 |
|---|---|---:|---:|
| Lambda | 750 invocations (720 BMKG, 30 USGS), each billed as a cold start of 4.35 s at 256 MB | 0.0137 | 0.0137 |
| Bronze writes | 2,190 S3 PUTs (3 BMKG feeds an hour, 1 USGS object a day) | 0.0110 | 0.0110 |
| Storage | bronze (19.0 MB backfill + 7.4 MB a month) and silver (3.1 MB) | 0.0007 | 0.0026 |
| Logs | about 1 KB of CloudWatch Logs per invocation | 0.0005 | 0.0005 |
| **As deployed** | what runs with nobody using it | **0.0259** | **0.0278** |
| Silver rebuild, requests | a daily rebuild from outside AWS: a GET of every bronze object, silver's 144 PUTs | 0.0499 | 0.3426 |
| Silver rebuild, transfer | the same rebuild downloading all of bronze, 30 times | 0.0884 | 0.3599 |
| Athena, scans | 300 queries, each billed Athena's 10 MB minimum (silver is 3.1 MB) | 0.0136 | 0.0136 |
| Athena, S3 requests | the same queries' S3 requests, mostly listing projected partitions (below) | 0.5056 | 0.5056 |
| Glue | 600 catalog requests, 2 catalog objects | 0.0006 | 0.0006 |
| **With use** | | **0.6840** | **1.2501** |

**Free tiers** would take most of this away: AWS's 100 GB a month of free transfer to the
internet covers the silver download (leaving **$0.60 and $0.89**), Lambda's always-free
1 million requests and 400,000 GB-seconds cover the function, and Glue's first million
catalog objects and requests are free. The table applies none of them.

## Where the usage numbers come from

- **Lambda duration:** [`results/lambda-s11.log`](results/lambda-s11.log)
  (`scripts/measure_lambda.sh`): the handler in AWS's Python 3.12 Lambda image, at the CPU
  share Lambda gives 256 MB (`docker --cpus 0.1447`), writing to MinIO. The slowest of three
  cold starts was 0.81 s of init plus a 3.55 s first invocation, fetching all three BMKG
  feeds and 7 days of USGS. Every invocation is priced at that (an hourly function is
  usually cold; init is billed). Peak memory was 51.3 MiB, so 256 MB is for CPU, not
  memory: at 128 MB the same cold start took 1.75 to 1.86 s of init and 4.0 to 4.2 s.
- **Bronze growth:** one BMKG snapshot is 3 objects of 9,614 bytes in all, a 7-day USGS
  fetch 14,701 bytes (both in the same log); the backfill is 143 objects of 18,990,379
  bytes ([`results/backfill-s7.json`](results/backfill-s7.json)).
- **Silver:** 143 monthly Parquet files of 3,135,062 bytes plus the quarantine file
  ([`results/silver-s8.json`](results/silver-s8.json)).
- **Assumptions** (not measured; in the JSON's `assumptions`): every invocation cold; 1 KB of
  logs per invocation; silver rebuilt once a day by the Airflow stack on a PC, reading
  bronze over the internet; 300 Athena queries a month, none filtered on `year`; Athena
  lists each projected partition once per query and reads each Parquet file with 2 GETs.

## What drives it

- **Listing partitions that don't exist.** The Glue table projects silver's partitions
  from `silver_years = [2015, 2040]`, so a query with no `year` filter makes Athena look at
  312 months, 143 of them with data so far, and each look is an S3 LIST at $0.005 per 1,000.
  That is $0.0017 per query, against $0.00005 for the data it scans. `where year = 2026`
  lists 12. A narrower range is cheaper but stops seeing new years when it runs out, which
  is why `infra/main.tf` marks it as a shortcut.
- **Rebuilding silver from all of bronze.** Hourly BMKG polls add 2,160 objects a month,
  and a rebuild GETs every one: 26,423 objects a run by month 12. Reading only new
  `ingest_date` partitions is the upgrade (README, "Silver is rebuilt from all of bronze").
  Running the rebuild inside AWS would remove the transfer, not the requests.
- **Nothing runs by the hour.** No NAT gateway (the function isn't in a VPC: it needs the
  public internet and S3, and a NAT gateway is billed for every hour it exists, more than
  everything above together), no KMS keys
  (SSE-S3), no crawler (partition projection), no always-on compute.

## Not counted

- **EventBridge:** the 750 scheduled events a month. Rules on a schedule aren't priced in
  the EventBridge price list (it prices custom, partner and cross-account events, Pipes and
  EventBridge Scheduler); at the custom-event price, $1.00 per million, they would cost
  $0.00075.
- **Old versions in bronze:** bronze keys are written once, except a backfill re-run on
  the same UTC day, which overwrites its 143 objects; the 19 MB of old versions stay 30 days.
- **A Terraform state bucket**, once state moves to S3 (a few KB), and tax.

Before an apply (which `PROGRESS.md` doesn't allow yet: `AWS_APPLY_ALLOWED: false`), set
an AWS Budgets alert at $1 a month in the console; the first two budgets are free.
