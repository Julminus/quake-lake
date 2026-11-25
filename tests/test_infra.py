"""infra/ and what reads it: the Glue table must match silver's schema, and the cost
estimate (scripts/aws_cost.py) must read the deployment's numbers right."""

import importlib.util
import json
from pathlib import Path

import pyarrow as pa
import pytest

from quake_lake.silver import EVENTS

ROOT = Path(__file__).parents[1]
COLUMNS = ROOT / "infra" / "modules" / "catalog" / "silver_events_columns.json"
GLUE_TYPES = {
    pa.string(): "string",
    pa.float64(): "double",
    pa.timestamp("ms", tz="UTC"): "timestamp",
}

_spec = importlib.util.spec_from_file_location("aws_cost", ROOT / "scripts" / "aws_cost.py")
assert _spec
assert _spec.loader
aws_cost = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(aws_cost)

MAIN = """
module "ingest" {
  source = "./modules/ingest"
  # memory_mb = 128
  schedules = {
    bmkg = {
      expression = "rate(1 hour)"
    }
    usgs = {
      expression = "cron(0 0 * * ? *)"
    }
  }
}

module "catalog" {
  # silver_years = [2000, 2001]
  silver_years       = [2015, 2035]
}
"""
INGEST = """
variable "memory_mb" {
  description = "Memory."
  type        = number
  default     = 256
}
"""


def test_the_glue_table_lists_silvers_columns_in_order_with_their_types() -> None:
    expected = [{"name": f.name, "type": GLUE_TYPES[f.type]} for f in EVENTS]
    assert json.loads(COLUMNS.read_text(encoding="utf-8")) == expected


def test_partition_columns_are_not_table_columns() -> None:
    # Athena refuses a table whose partition key is also a column; silver keeps
    # year and month in the folder names only.
    assert not {"year", "month"} & set(EVENTS.names)


def test_the_cost_estimate_reads_the_real_infra() -> None:
    main = (ROOT / "infra" / "main.tf").read_text(encoding="utf-8")
    ingest = (ROOT / "infra" / "modules" / "ingest" / "main.tf").read_text(encoding="utf-8")
    assert aws_cost.deployment(main, ingest) == {"lambda_memory_mb": 256, "athena_partitions": 252}


def test_the_cost_estimate_skips_comments_and_takes_the_roots_memory() -> None:
    assert aws_cost.deployment(MAIN, INGEST) == {"lambda_memory_mb": 256, "athena_partitions": 252}
    root = MAIN.replace("  # memory_mb = 128\n", "  memory_mb = 512\n")
    assert aws_cost.deployment(root, INGEST)["lambda_memory_mb"] == 512


@pytest.mark.parametrize(
    ("main", "problem"),
    [
        (MAIN.replace("  # memory_mb = 128\n", "  memory_mb = var.memory\n"), "memory_mb"),
        (MAIN.replace("rate(1 hour)", "rate(1 day)"), "schedules"),
        (MAIN.replace("  silver_years       = [2015, 2035]\n", ""), "silver_years"),
        (MAIN.replace('module "ingest"', 'module "fetch"'), "ingest"),
    ],
)
def test_the_cost_estimate_refuses_what_it_cant_read(main: str, problem: str) -> None:
    with pytest.raises(SystemExit, match=problem):
        aws_cost.deployment(main, INGEST)
