"""The Glue table Terraform declares over silver (infra/) must match silver's schema."""

import json
from pathlib import Path

import pyarrow as pa

from quake_lake.silver import EVENTS

COLUMNS = Path(__file__).parents[1] / "infra" / "modules" / "catalog" / "silver_events_columns.json"
GLUE_TYPES = {
    pa.string(): "string",
    pa.float64(): "double",
    pa.timestamp("ms", tz="UTC"): "timestamp",
}


def test_the_glue_table_lists_silvers_columns_in_order_with_their_types() -> None:
    expected = [{"name": f.name, "type": GLUE_TYPES[f.type]} for f in EVENTS]
    assert json.loads(COLUMNS.read_text(encoding="utf-8")) == expected


def test_partition_columns_are_not_table_columns() -> None:
    # Athena refuses a table whose partition key is also a column; silver keeps
    # year and month in the folder names only.
    assert not {"year", "month"} & set(EVENTS.names)
