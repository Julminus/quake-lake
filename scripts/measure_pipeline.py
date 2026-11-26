"""Run the pipeline from an empty folder to the dashboard; write docs/results/pipeline-s12.json.

    uv run python scripts/measure_pipeline.py --lake <empty or new folder>

Runs the commands the quickstart runs, as separate processes, each timed: a BMKG snapshot,
the USGS backfill since 2015 (network, about 2.5 minutes), silver, `dbt build` and `dbt
source freshness`. Then counts what each layer holds, which data-quality checks ran, and
renders the dashboard headless (Streamlit's AppTest) on the new gold. Bronze, silver and
gold stay in --lake, so the dashboard can be opened on them (GOLD_DB=<lake>/gold.duckdb).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from streamlit.testing.v1 import AppTest

from quake_lake import dashboard, silver
from quake_lake.storage import LocalStorage

ROOT = Path(__file__).parent.parent
MARTS = ("fct_earthquakes", "dim_province", "agg_daily_activity", "agg_province_magnitude")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--lake", required=True, help="an empty or new folder")
    p.add_argument("--since", default="2015-01-01")
    p.add_argument("--out", default="docs/results/pipeline-s12.json")
    args = p.parse_args()
    lake, out = Path(args.lake).resolve(), Path(args.out).resolve()
    if lake.exists() and any(lake.iterdir()):
        raise SystemExit(f"{lake} is not empty")
    lake.mkdir(parents=True, exist_ok=True)
    env = {
        **os.environ,
        "BRONZE_URI": str(lake / "bronze"),
        "SILVER_URI": str(lake / "silver"),
        "GOLD_DB": str(lake / "gold.duckdb"),
        "DBT_PROJECT_DIR": "transform",
        "DBT_PROFILES_DIR": "transform",
        "DBT_TARGET_PATH": str(lake / "target"),
        "DBT_LOG_PATH": str(lake / "logs"),
        "DBT_USE_COLORS": "false",
    }
    started_at = datetime.now(UTC).replace(microsecond=0)
    steps = {
        "bmkg": ["quake-lake", "bmkg"],
        "usgs": ["quake-lake", "usgs", "--since", args.since],
        "silver": ["quake-lake", "silver"],
        "gold": ["dbt", "build"],
        "freshness": ["dbt", "source", "freshness"],
    }
    seconds, outputs = {}, {}
    for name, cmd in steps.items():
        t = time.perf_counter()
        run = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True, check=False)
        seconds[name] = round(time.perf_counter() - t, 2)
        if run.returncode != 0:
            raise SystemExit(f"{' '.join(cmd)} failed:\n{run.stdout}\n{run.stderr}")
        lines = run.stdout.replace(str(lake), "<lake>").strip().splitlines()
        outputs[name] = lines[-6:]  # the summary lines, without this machine's path
        print(f"{name}: {seconds[name]} s", file=sys.stderr)
        if name == "gold":  # before freshness writes its own run_results.json
            dbt_results = json.loads((lake / "target" / "run_results.json").read_text("utf-8"))

    with tempfile.TemporaryDirectory() as tmp:  # the same build, for its counts
        r = silver.build_silver(LocalStorage(lake / "bronze"), LocalStorage(Path(tmp)))
    bronze_files = list((lake / "bronze").rglob("*.json"))
    kinds = Counter(x["unique_id"].split(".")[0] for x in dbt_results["results"])
    statuses = Counter(x["status"] for x in dbt_results["results"])
    with duckdb.connect(env["GOLD_DB"], read_only=True) as con:
        marts = {m: con.execute(f"select count(*) from {m}").fetchone()[0] for m in MARTS}
        indonesian = con.execute("select count(*) from fct_earthquakes where in_indonesia")
        marts["fct_earthquakes_in_indonesia"] = indonesian.fetchone()[0]

    os.environ["GOLD_DB"] = env["GOLD_DB"]
    renders = []
    for _ in range(3):  # the first reads gold; the others are served from Streamlit's cache
        t = time.perf_counter()
        at = AppTest.from_file(dashboard.__file__, default_timeout=120).run()
        renders.append(round(time.perf_counter() - t, 2))
        if at.exception:
            raise SystemExit(f"the dashboard failed: {at.exception}")
    t = time.perf_counter()
    with duckdb.connect(env["GOLD_DB"], read_only=True) as con:
        rows = len(con.sql(dashboard.QUERY).df())
    load_seconds = round(time.perf_counter() - t, 3)

    record_checks = sum(len(c.checks) for c in silver.RECORD.columns.values())
    report = {
        "started_at": started_at.isoformat(),
        "since": args.since,
        "seconds": {**seconds, "total": round(sum(seconds.values()), 2)},
        "cli_output": outputs,
        "bronze": {
            "objects": len(bronze_files),
            "bytes": sum(f.stat().st_size for f in bronze_files),
            "usgs_objects": sum("source=usgs" in f.as_posix() for f in bronze_files),
        },
        "silver": {
            "records": r.records,
            "quarantined": r.quarantined,
            "repeats_dropped": r.repeats,
            "bmkg_merged": r.merged,
            "events": r.events,
            "partitions": r.partitions,
            "records_not_kept_as_events_pct": round(100 * (r.records - r.events) / r.records, 2),
            "parquet_bytes": sum(f.stat().st_size for f in (lake / "silver").rglob("*.parquet")),
        },
        "gold": marts,
        "checks": {
            "bronze_shape_check_per_object": 1,
            "silver_typed_columns": len(silver.RECORD.columns),
            "silver_value_checks": record_checks + len(silver.RECORD.checks),
            "dbt_nodes": dict(kinds),
            "dbt_statuses": dict(statuses),
            "dbt_elapsed_seconds": round(dbt_results["elapsed_time"], 2),
        },
        "dashboard": {
            "rows_loaded": rows,
            "query_seconds": load_seconds,
            "apptest_render_seconds": renders,
            "metrics": {m.label: m.value for m in at.metric},
            "top_provinces": at.dataframe[0].value.head(5).to_dict("records"),
        },
    }
    out.write_text(json.dumps(report, indent=2, default=str) + "\n", "utf-8", newline="\n")
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
