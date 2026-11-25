"""Monthly AWS cost of the Terraform in infra/, from AWS's public price list.

Fetches the on-demand prices for the region from the AWS Price List bulk API (no account
needed), takes usage from measurements in docs/results/ plus the assumptions below, and
writes docs/results/aws-cost-s11.json. docs/aws-cost-estimate.md explains the result.

    uv run python scripts/aws_cost.py [--region ap-southeast-1] [--out PATH]
"""

from __future__ import annotations

import argparse
import json
import re
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "docs" / "results"
OFFER_URL = (
    "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/{offer}/current/{region}/index.json"
)
REGION_CODE = {"ap-southeast-1": "APS1", "ap-southeast-3": "APS4"}  # usage-type prefixes
GB = 2**30  # objects are counted in bytes; AWS bills GB as 2^30 bytes

# (offer, usage type after the region code, a word of the price's description, unit)
PRICES = {
    "lambda_gb_second": ("AWSLambda", "Lambda-GB-Second", "Tier-1", "GB-second"),
    "lambda_request": ("AWSLambda", "Request", "Requests", "request"),
    "s3_storage_gb_month": ("AmazonS3", "TimedStorage-ByteHrs", "first 50 TB", "GB-month"),
    "s3_put_list": ("AmazonS3", "Requests-Tier1", "PUT", "request"),
    "s3_get": ("AmazonS3", "Requests-Tier2", "GET", "request"),
    "internet_out_gb": ("AWSDataTransfer", "DataTransfer-Out-Bytes", "first 10 TB", "GB"),
    "athena_tb_scanned": ("AmazonAthena", "DataScannedInTB", "Terabytes", "TB"),
    "glue_request": ("AWSGlue", "Catalog-Request", "requests", "request"),
    "glue_object_month": ("AWSGlue", "Catalog-Storage", "objects", "object-month"),
    # Lambda's logs may be billed as vended logs; their first tier is the same $0.70 here.
    "logs_ingest_gb": ("AmazonCloudWatch", "DataProcessing-Bytes", "Standard log class", "GB"),
}

# Assumptions: a 30-day month, the schedules in infra/main.tf (checked in deployment()),
# and use beyond them.
DAYS = 30
BMKG_POLLS = 24 * DAYS  # rate(1 hour)
USGS_RUNS = DAYS  # cron(0 0 * * ? *)
LOG_BYTES_PER_INVOCATION = 1_000  # START/END/REPORT plus one line per object written
SILVER_RUNS = DAYS  # a daily `quake-lake silver` from outside AWS (a PC)
ATHENA_QUERIES = 300  # ten a day
ATHENA_MIN_BYTES = 10 * 2**20  # Athena bills at least 10 MB per query


def deployment(main: str, ingest: str) -> dict[str, int]:
    """The numbers the estimate takes from infra/main.tf and modules/ingest/main.tf.

    Only whole lines of HCL count (a commented-out line starts with `#`), as
    `terraform fmt` writes them. shortcut: a regex, not an HCL parser; it refuses what it
    can't read rather than guess.
    """
    schedules = re.findall(r'^\s*expression\s*=\s*"([^"]+)"\s*$', main, re.M)
    if schedules != ["rate(1 hour)", "cron(0 0 * * ? *)"]:
        raise SystemExit(f"infra/main.tf's schedules are {schedules}: update BMKG_POLLS/USGS_RUNS")
    years = re.search(r"^\s*silver_years\s*=\s*\[(\d+),\s*(\d+)\]\s*$", main, re.M)
    # The root's memory_mb, inside its module "ingest" block, if it sets one; else the default.
    block = re.search(r'^module "ingest" \{\n(.*?)^\}', main, re.M | re.S)
    if block is None:
        raise SystemExit('infra/main.tf has no module "ingest" block')
    root = re.search(r"^\s*memory_mb\s*=\s*(.+?)\s*$", block[1], re.M)
    default = re.search(
        r'^variable "memory_mb" \{\n[^}]*?^\s*default\s*=\s*(\d+)\s*$', ingest, re.M
    )
    memory = root[1] if root else default[1] if default else None
    if not years or memory is None or not memory.isdigit():
        raise SystemExit(f"can't read silver_years or memory_mb (got {memory!r}) from infra/")
    return {
        "lambda_memory_mb": int(memory),
        # Athena projects every month of these years, data or not: one LIST each (assumed).
        "athena_partitions": (int(years[2]) - int(years[1]) + 1) * 12,
    }


def fetch_prices(region: str) -> dict[str, dict[str, object]]:
    code, offers, out = REGION_CODE[region], {}, {}
    for key, (offer, usage, word, unit) in PRICES.items():
        if offer not in offers:
            url = OFFER_URL.format(offer=offer, region=region)
            with urllib.request.urlopen(url, timeout=60) as r:
                offers[offer] = (url, json.load(r))
        url, data = offers[offer]
        found = [
            (d["pricePerUnit"]["USD"], d["description"])
            for sku, p in data["products"].items()
            if p["attributes"].get("usagetype") == f"{code}-{usage}"
            for t in data["terms"]["OnDemand"].get(sku, {}).values()
            for d in t["priceDimensions"].values()
            if word in d["description"] and d["beginRange"] == "0"
        ]
        if len(found) != 1:
            raise SystemExit(f"{key}: expected one price for {code}-{usage}, found {found}")
        usd, description = found[0]
        out[key] = {
            "usd": float(usd),
            "per": unit,
            "description": description,
            "source": url,
            "published": data["publicationDate"],
        }
    return out


def measured(memory_mb: int) -> dict[str, float]:
    log = (RESULTS / "lambda-s11.log").read_text(encoding="utf-8")
    runs = {
        json.loads(line)["event"].get("sources", ["both"])[0]: json.loads(line)
        for line in log.splitlines()
        if line.startswith("{")
    }
    block = log.split(f"## memory {memory_mb} MB")[1:]  # the cold starts at that memory
    if not block:
        raise SystemExit(f"lambda-s11.log has no cold starts at {memory_mb} MB")
    cold = [
        float(re.search(r"INIT REPORT\(durationMs: ([\d.]+)", b)[1])
        + float(re.search(r"\tDuration: ([\d.]+) ms", b)[1])
        for b in block
    ]
    backfill = json.loads((RESULTS / "backfill-s7.json").read_text(encoding="utf-8"))
    silver = json.loads((RESULTS / "silver-s8.json").read_text(encoding="utf-8"))["silver"]
    return {
        "lambda_memory_gb": memory_mb / 1024,
        "lambda_cold_seconds_max": max(cold) / 1000,
        "bmkg_bytes_per_poll": runs["bmkg"]["bytes"],
        "bmkg_objects_per_poll": runs["bmkg"]["objects"],
        "usgs_bytes_per_run": runs["usgs"]["bytes"],
        "backfill_bytes": backfill["bytes"],
        "backfill_objects": backfill["objects"],
        "silver_objects": silver["partitions"] + 1,  # + quarantine/events.jsonl
        "silver_bytes": silver["parquet_bytes"],
    }


def month(
    n: int, m: dict[str, float], partitions: int, p: dict[str, dict[str, object]]
) -> dict[str, float]:
    """Cost of month n after deploying (the backfill loaded first), with no free tier."""
    usd = {k: float(v["usd"]) for k, v in p.items()}  # type: ignore[arg-type]
    invocations = BMKG_POLLS + USGS_RUNS
    puts = BMKG_POLLS * m["bmkg_objects_per_poll"] + USGS_RUNS
    month_bytes = BMKG_POLLS * m["bmkg_bytes_per_poll"] + USGS_RUNS * m["usgs_bytes_per_run"]
    bronze_objects = m["backfill_objects"] + n * puts  # at the end of month n
    bronze_bytes = m["backfill_bytes"] + n * month_bytes
    gb_seconds = invocations * m["lambda_cold_seconds_max"] * m["lambda_memory_gb"]
    return {
        "lambda": gb_seconds * usd["lambda_gb_second"] + invocations * usd["lambda_request"],
        "bronze_puts": puts * usd["s3_put_list"],
        "storage": (bronze_bytes + m["silver_bytes"]) / GB * usd["s3_storage_gb_month"],
        "logs": invocations * LOG_BYTES_PER_INVOCATION / GB * usd["logs_ingest_gb"],
        # Every rebuild reads every bronze object and rewrites silver's.
        "silver_rebuild_requests": SILVER_RUNS
        * (
            bronze_objects * usd["s3_get"]
            + (bronze_objects / 1000 + m["silver_objects"]) * usd["s3_put_list"]
        ),
        "silver_rebuild_transfer_out": SILVER_RUNS * bronze_bytes / GB * usd["internet_out_gb"],
        "athena_scans": ATHENA_QUERIES * ATHENA_MIN_BYTES / 2**40 * usd["athena_tb_scanned"],
        # Partition projection: every projected month is a LIST, data or not; 2 PUTs of results.
        "athena_s3_requests": ATHENA_QUERIES
        * ((partitions + 2) * usd["s3_put_list"] + 2 * m["silver_objects"] * usd["s3_get"]),
        "glue": ATHENA_QUERIES * 2 * usd["glue_request"] + 2 * usd["glue_object_month"],
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--region", default="ap-southeast-1", choices=sorted(REGION_CODE))
    ap.add_argument("--out", type=Path, default=RESULTS / "aws-cost-s11.json")
    a = ap.parse_args()
    infra = ROOT / "infra"
    d = deployment(
        (infra / "main.tf").read_text(encoding="utf-8"),
        (infra / "modules" / "ingest" / "main.tf").read_text(encoding="utf-8"),
    )
    prices, m = fetch_prices(a.region), measured(d["lambda_memory_mb"])
    months = {n: month(n, m, d["athena_partitions"], prices) for n in (1, 12)}
    deployed = ("lambda", "bronze_puts", "storage", "logs")  # what runs with no one using it
    result = {
        "region": a.region,
        "deployment": d,
        "assumptions": {
            "days": DAYS,
            "bmkg_polls": BMKG_POLLS,
            "usgs_runs": USGS_RUNS,
            "every_invocation_cold": True,
            "log_bytes_per_invocation": LOG_BYTES_PER_INVOCATION,
            "silver_runs_from_outside_aws": SILVER_RUNS,
            "athena_queries": ATHENA_QUERIES,
            "athena_lists_each_projected_partition": True,
            "free_tier": "none applied",
        },
        "measured": m,
        "prices": prices,
        "months": {
            str(n): {
                "items_usd": {k: round(v, 6) for k, v in items.items()},
                "deployed_only_usd": round(sum(items[k] for k in deployed), 4),
                "total_usd": round(sum(items.values()), 4),
            }
            for n, items in months.items()
        },
    }
    a.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8", newline="\n")
    for n, items in months.items():
        print(f"month {n}: deployed only ${sum(items[k] for k in deployed):.4f}, ", end="")
        print(f"with silver rebuilds and Athena ${sum(items.values()):.4f}")
        for k, v in items.items():
            print(f"  {k:28} ${v:.6f}")


if __name__ == "__main__":
    main()
