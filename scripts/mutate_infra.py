"""Plant one bug at a time in infra/ and check that `terraform test` fails on each.

Every file is restored afterwards, even on Ctrl-C. Needs an initialised infra/
(`make infra-check` once). docs/results/mutations-s11.txt is one run:

    uv run python scripts/mutate_infra.py > docs/results/mutations-s11.txt

The unmutated tests must pass first, and a mutation counts as caught only when a test run
fails (`run "..."... fail`), not when Terraform can't run at all.
"""

import re
import subprocess
import sys
from pathlib import Path

INFRA = Path(__file__).resolve().parents[1] / "infra"
ING, CAT, BKT, ROOT = (
    "modules/ingest/main.tf",
    "modules/catalog/main.tf",
    "modules/bucket/main.tf",
    "main.tf",
)
MUTATIONS = [
    # the ingest module
    (ING, 'Action   = "s3:PutObject"', 'Action   = ["s3:PutObject", "s3:GetObject"]'),
    (ING, 'Resource = "${var.bronze.arn}/*"', 'Resource = "*"'),
    (ING, "input    = each.value.event", 'input    = "{}"'),
    (ING, "log_group  = aws_cloudwatch_log_group.this.name", "log_group  = null"),
    (ING, "maximum_retry_attempts = 2", "maximum_retry_attempts = 0"),
    (
        ING,
        "function_name          = aws_lambda_function.this.function_name",
        'function_name          = "other"',
    ),
    (
        ING,
        "function_name = aws_lambda_function.this.function_name",
        'function_name = "other"',
    ),
    (
        ING,
        "source_arn    = aws_cloudwatch_event_rule.this[each.key].arn",
        "source_arn    = values(aws_cloudwatch_event_rule.this)[0].arn",
    ),
    (
        ING,
        "rule     = aws_cloudwatch_event_rule.this[each.key].name",
        "rule     = values(aws_cloudwatch_event_rule.this)[0].name",
    ),
    (
        ING,
        "arn      = aws_lambda_function.this.arn",
        'arn      = "arn:aws:lambda:ap-southeast-1:123456789012:function:other"',
    ),
    (ING, 'principal     = "events.amazonaws.com"', 'principal     = "s3.amazonaws.com"'),
    (
        ING,
        'Principal = { Service = "lambda.amazonaws.com" }',
        'Principal = { Service = "ec2.amazonaws.com" }',
    ),
    (ING, 'Action    = "sts:AssumeRole"', 'Action    = ["sts:AssumeRole", "sts:TagSession"]'),
    (ING, 'BRONZE_URI = "s3://${var.bronze.bucket}"', 'BRONZE_URI = "s3://${var.bronze.bucket}/x"'),
    (
        ING,
        "source_code_hash = data.archive_file.package.output_base64sha256",
        "source_code_hash = null",
    ),
    (ING, "default     = 256", "default     = 10240"),
    (ING, "default     = 300", "default     = 900"),
    (ING, "  default     = 30\n", "  default     = 3653\n"),
    # the catalog module
    (CAT, '"projection.month.digits" = "2"', '"projection.month.digits" = "1"'),
    (CAT, 'Resource = "${var.silver.arn}/events/*"', 'Resource = "${var.silver.arn}/*"'),
    (
        CAT,
        'Action   = "s3:GetObject"\n        Resource = "${var.silver.arn}/events/*"',
        'Action   = ["s3:GetObject", "s3:PutObject"]\n'
        '        Resource = "${var.silver.arn}/events/*"',
    ),
    (
        CAT,
        "enforce_workgroup_configuration    = true",
        "enforce_workgroup_configuration    = false",
    ),
    (
        CAT,
        'Action = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]',
        'Action = "glue:*"',
    ),
    (
        CAT,
        '"athena:GetWorkGroup", "athena:StartQueryExecution"',
        '"athena:*", "athena:StartQueryExecution"',
    ),
    (CAT, '        Condition = { StringLike = { "s3:prefix" = ["events/*"] } }\n', ""),
    (
        CAT,
        "bytes_scanned_cutoff_per_query     = var.bytes_scanned_cutoff",
        "bytes_scanned_cutoff_per_query     = null",
    ),
    (CAT, 'type = "int"', 'type = "string"'),
    (
        CAT,
        'location      = "s3://${var.silver.bucket}/events/"',
        'location      = "s3://${var.silver.bucket}/"',
    ),
    (
        CAT,
        'output_location = "s3://${var.results.bucket}/"',
        'output_location = "s3://${var.silver.bucket}/"',
    ),
    (
        CAT,
        'glue_arn = split(":database/", aws_glue_catalog_database.this.arn)[0]',
        'glue_arn = "arn:aws:glue:ap-southeast-1:123456789012"',
    ),
    (CAT, 'classification            = "parquet"', 'classification            = "json"'),
    (CAT, "parquet.serde.ParquetHiveSerDe", "lazy.LazySimpleSerDe"),
    (CAT, 'encryption_option = "SSE_S3"', 'encryption_option = "SSE_KMS"'),
    # the bucket module
    (BKT, "restrict_public_buckets = true", "restrict_public_buckets = false"),
    (BKT, '"aws:SecureTransport" = "false"', '"aws:SecureTransport" = "true"'),
    (BKT, "days_after_initiation = 1", "days_after_initiation = 7"),
    (BKT, "for_each = var.versioned ? [1] : []", "for_each = []"),
    (BKT, "expired_object_delete_marker = true", "expired_object_delete_marker = false"),
    # the root: settings and the wiring between modules
    (ROOT, 'expression = "rate(1 hour)"', 'expression = "rate(1 day)"'),
    (
        ROOT,
        'jsonencode({ sources = ["usgs"], days = 7 })',
        'jsonencode({ sources = ["usgs"], days = 30 })',
    ),
    (ROOT, 'Resource = "${module.bronze.arn}/*"', 'Resource = "*"'),
    (
        ROOT,
        'Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]',
        'Action   = "s3:*"',
    ),
    (ROOT, "versioned = true", "versioned = false"),
    (ROOT, "expire_days = 7", "expire_days = 1"),
    (ROOT, "bronze        = module.bronze", "bronze        = module.silver"),
    (ROOT, "silver   = module.silver", "silver   = module.bronze"),
    (ROOT, "results  = module.athena_results", "results  = module.silver"),
    (ROOT, "silver_years = [2015, 2035]", "silver_years = [2015, 2025]"),
]
FAILED_RUN = re.compile(r'run "[^"]+"\.\.\. fail')


def test_runs_fail() -> bool:
    out = subprocess.run(
        ["terraform", "test", "-no-color"], cwd=INFRA, capture_output=True, text=True
    )
    if out.returncode != 0 and not FAILED_RUN.search(out.stdout):
        sys.exit(f"terraform test couldn't run:\n{out.stdout}{out.stderr}")
    return out.returncode != 0


def main() -> int:
    if test_runs_fail():
        sys.exit("the unmutated tests fail; fix them first")
    missed = []
    for rel, old, new in MUTATIONS:
        path = INFRA / rel
        text = path.read_text(encoding="utf-8")
        if old not in text:
            sys.exit(f"{rel} no longer contains {old!r}: update the mutation")
        try:
            path.write_text(text.replace(old, new, 1), encoding="utf-8", newline="\n")
            caught = test_runs_fail()
        finally:
            path.write_text(text, encoding="utf-8", newline="\n")
        print(f"{'caught' if caught else 'MISSED'}: {rel}: {old.strip()[:70]!r}", flush=True)
        if not caught:
            missed.append(old)
    print(f"{len(MUTATIONS) - len(missed)} of {len(MUTATIONS)} caught")
    return 1 if missed else 0


if __name__ == "__main__":
    sys.exit(main())
