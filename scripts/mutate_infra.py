"""Plant one bug at a time in infra/ and check that `terraform test` fails on each.

Every file is restored afterwards, even on Ctrl-C. Needs an initialised infra/
(`make infra-check` once). docs/results/mutations-s11.txt is one run:

    uv run python scripts/mutate_infra.py > docs/results/mutations-s11.txt
"""

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
    (ING, 'Resource = "${var.bronze_bucket_arn}/*"', 'Resource = "*"'),
    (ING, "input    = each.value.event", 'input    = "{}"'),
    (ING, "log_group  = aws_cloudwatch_log_group.this.name", "log_group  = null"),
    (ING, "maximum_retry_attempts = 0", "maximum_retry_attempts = 2"),
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
    (ING, 'BRONZE_URI = "s3://${var.bronze_bucket}"', 'BRONZE_URI = "s3://${var.bronze_bucket}/x"'),
    (ING, "default     = 256", "default     = 10240"),
    (ING, "default     = 300", "default     = 900"),
    (ING, "  default     = 30\n", "  default     = 3653\n"),
    # the catalog module
    (CAT, '"projection.month.digits" = "2"', '"projection.month.digits" = "1"'),
    (
        CAT,
        'Resource = "${var.silver_bucket_arn}/events/*"',
        'Resource = "${var.silver_bucket_arn}/*"',
    ),
    (
        CAT,
        'Action   = "s3:GetObject"\n        Resource = "${var.silver_bucket_arn}/events/*"',
        'Action   = ["s3:GetObject", "s3:PutObject"]\n'
        '        Resource = "${var.silver_bucket_arn}/events/*"',
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
        'location      = "s3://${var.silver_bucket}/events/"',
        'location      = "s3://${var.silver_bucket}/"',
    ),
    (
        CAT,
        'output_location = "s3://${var.results_bucket}/"',
        'output_location = "s3://${var.silver_bucket}/"',
    ),
    # the bucket module
    (BKT, "restrict_public_buckets = true", "restrict_public_buckets = false"),
    (BKT, '"aws:SecureTransport" = "false"', '"aws:SecureTransport" = "true"'),
    (BKT, "days_after_initiation = 1", "days_after_initiation = 7"),
    (BKT, "for_each = var.versioned ? [1] : []", "for_each = []"),
    (BKT, "expired_object_delete_marker = true", "expired_object_delete_marker = false"),
    # the root
    (ROOT, 'expression = "rate(1 hour)"', 'expression = "rate(1 day)"'),
    (
        ROOT,
        'jsonencode({ sources = ["usgs"], days = 7 })',
        'jsonencode({ sources = ["usgs"], days = 30 })',
    ),
    (ROOT, 'Resource = "${module.bronze.arn}/*"', 'Resource = "*"'),
    (ROOT, 'Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]', 'Action   = "s3:*"'),
    (ROOT, "versioned = true", "versioned = false"),
    (ROOT, "silver_years = [2015, 2035]", "silver_years = [2015, 2025]"),
]

missed = []
for rel, old, new in MUTATIONS:
    path = INFRA / rel
    text = path.read_text(encoding="utf-8")
    if old not in text:
        print(f"NOT FOUND {rel}: {old!r}")
        missed.append((rel, old))
        continue
    try:
        path.write_text(text.replace(old, new, 1), encoding="utf-8", newline="\n")
        r = subprocess.run(
            ["terraform", "test", "-no-color"], cwd=INFRA, capture_output=True, text=True
        )
        caught = r.returncode != 0
        print(f"{'caught' if caught else 'MISSED'}: {rel}: {old.strip()[:70]!r}")
        if not caught:
            missed.append((rel, old))
    finally:
        path.write_text(text, encoding="utf-8", newline="\n")
print(f"{len(MUTATIONS) - len(missed)} of {len(MUTATIONS)} caught")
sys.exit(1 if missed else 0)
