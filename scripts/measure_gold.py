"""Measure gold (the dbt project) on a real bronze; write docs/results/gold-s9.json.

    uv run python scripts/measure_gold.py --bronze lake/bronze

Builds silver from the bronze into a temporary folder and gold from that silver (three
full builds, timed), counts the marts, then fetches a BMKG snapshot and the last 7 days
of USGS into the bronze (network), rebuilds silver, and runs fct_earthquakes
incrementally and then in full, to compare the two. Only the fetch writes to --bronze.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from dbt.cli.main import dbtRunner

from quake_lake.cli import main as cli
from quake_lake.silver import build_silver
from quake_lake.storage import LocalStorage

PROJECT = Path(__file__).parent.parent / "transform"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bronze", default="lake/bronze")
    p.add_argument("--out", default="docs/results/gold-s9.json")
    args = p.parse_args()
    bronze = LocalStorage(Path(args.bronze))

    # dbt keeps its log and the DuckDB file open until the process ends, which Windows
    # won't delete under it; the leftovers are a few MB in the temp folder.
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        os.environ["SILVER_URI"] = f"{tmp}/silver"
        os.environ["GOLD_DB"] = f"{tmp}/gold.duckdb"
        db = Path(tmp) / "gold.duckdb"
        silver = LocalStorage(Path(tmp) / "silver")
        first = build_silver(bronze, silver)

        builds = [_timed(tmp, "build", "--full-refresh") for _ in range(3)]
        _, res = builds[-1]
        counts = _counts(db)
        before = {r[0]: r[1] for r in _rows(db, "select event_id, row_hash from fct_earthquakes")}
        _, fresh = _timed(tmp, "source", "freshness")

        fetched_at = datetime.now(UTC).replace(microsecond=0)
        for command in ("bmkg", "usgs"):  # the 7-day look-back the schedule will run
            if cli([command, "--bronze", args.bronze]) != 0:
                raise SystemExit(f"quake-lake {command} failed")
        second = build_silver(bronze, silver)

        _, shown = _timed(tmp, "show", "--select", "fct_earthquakes", "--limit", "1000000")
        written = [r["event_id"] for r in shown.result.results[0].agate_table]
        incremental_s, inc = _timed(tmp, "run", "--select", "fct_earthquakes")
        after_incremental = _rows(db, "select * from fct_earthquakes order by event_id")
        full_s, full = _timed(tmp, "run", "--select", "fct_earthquakes", "--full-refresh")
        after_full = _rows(db, "select * from fct_earthquakes order by event_id")
        now = {r[0] for r in after_full}

    report = {
        "silver": {"before_fetch": first.events, "after_fetch": second.events},
        "dbt": {
            "nodes": _kinds(res),
            "statuses": _statuses(res),
            "build_seconds": [round(s, 2) for s, _ in builds],
            "dbt_elapsed_seconds": [round(r.result.elapsed_time, 2) for _, r in builds],
            "slowest_nodes": _slowest(res),
        },
        "marts": counts,
        "freshness": {
            "status": str(fresh.result.results[0].status),
            "max_loaded_at": str(fresh.result.results[0].max_loaded_at),
            "age_hours": round(fresh.result.results[0].age / 3600, 1),
        },
        "incremental": {
            "fetched_at": fetched_at.isoformat(),
            "rows_before": len(before),
            "rows_after": len(now),
            "rows_written": len(written),
            "new_events": sum(e not in before for e in written),
            "changed_events": sum(e in before for e in written),
            "deleted_events": len(before.keys() - now),
            "fct_seconds_incremental": round(_node_seconds(inc, "fct_earthquakes"), 3),
            "fct_seconds_full_refresh": round(_node_seconds(full, "fct_earthquakes"), 3),
            "run_seconds_incremental": round(incremental_s, 2),
            "run_seconds_full_refresh": round(full_s, 2),
            "identical_to_full_refresh": after_incremental == after_full,
        },
    }
    Path(args.out).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps(report, indent=2))


def _timed(tmp: str, *args: str):
    started = time.perf_counter()
    paths = ["--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
    paths += ["--target-path", f"{tmp}/target", "--log-path", f"{tmp}/logs"]
    res = dbtRunner().invoke([*args, *paths])
    seconds = time.perf_counter() - started
    if res.exception is not None or (not res.success and args[0] != "source"):
        raise SystemExit(f"dbt {' '.join(args)} failed: {res.exception or _statuses(res)}")
    return seconds, res


def _rows(db: Path, sql: str) -> list[tuple]:
    with duckdb.connect(str(db)) as c:
        return c.sql(sql).fetchall()


def _counts(db: Path) -> dict[str, object]:
    def one(sql: str) -> dict[str, object]:
        with duckdb.connect(str(db)) as c:
            cur = c.execute(sql)
            names = [d[0] for d in cur.description]
            return {k: _plain(v) for k, v in zip(names, cur.fetchone() or (), strict=True)}

    def by(sql: str) -> dict[str, int]:
        return {str(k): int(v) for k, v in _rows(db, sql + " group by 1 order by 2 desc")}

    indonesian = "from fct_earthquakes where in_indonesia"
    per_province = "from agg_province_magnitude join dim_province using (province_code)"
    return {
        "fct_earthquakes": {
            **one(
                "select count(*) as rows, count(*) filter (where in_indonesia) as in_indonesia,"
                " count(*) filter (where row_source = 'bmkg') as bmkg_only,"
                " count(*) filter (where bmkg_listed) as bmkg_listed from fct_earthquakes"
            ),
            "by_place_country": by(
                "select coalesce(place_country_code, '(none)'), count(*) from fct_earthquakes"
            ),
            "indonesian_by_magnitude_class": by(f"select magnitude_class, count(*) {indonesian}"),
            "indonesian_by_depth_class": by(f"select depth_class, count(*) {indonesian}"),
        },
        "agg_daily_activity": {
            **one(
                "select count(*) as rows,"
                " count(*) filter (where earthquakes = 0) as days_with_none,"
                " min(activity_date) as first, max(activity_date) as last,"
                " median(earthquakes) as median_per_day from agg_daily_activity"
            ),
            "busiest_day": one(
                "select activity_date, earthquakes, max_magnitude from agg_daily_activity"
                " order by earthquakes desc, activity_date limit 1"
            ),
        },
        "agg_province_magnitude": {
            **one(
                "select count(*) as rows, count(distinct province_code) as provinces"
                " from agg_province_magnitude"
            ),
            "by_province": by(f"select province, sum(earthquakes) {per_province}"),
            "by_region": by(f"select region, sum(earthquakes) {per_province}"),
        },
        "dim_province": one("select count(*) as rows from dim_province"),
        "sulawesi_utara": one(
            "select count(*) as all, count(*) filter (where in_indonesia) as in_indonesia"
            " from fct_earthquakes where province_code = 'ID-SA'"
        ),
    }


def _plain(v: object) -> object:
    """A value json can write: dates as text."""
    return v if isinstance(v, int | float | str) or v is None else str(v)


def _kinds(res) -> dict[str, int]:
    kinds: dict[str, int] = {}
    for r in res.result.results:
        kind = r.node.resource_type
        if kind == "model":
            kind = f"model ({r.node.config.materialized})"
        kinds[str(kind)] = kinds.get(str(kind), 0) + 1
    return kinds


def _statuses(res) -> dict[str, int]:
    out: dict[str, int] = {}
    for r in res.result.results:
        out[str(r.status)] = out.get(str(r.status), 0) + 1
    return out


def _slowest(res) -> dict[str, float]:
    times = sorted(((r.execution_time, r.node.name) for r in res.result.results), reverse=True)
    return {name: round(t, 3) for t, name in times[:5]}


def _node_seconds(res, name: str) -> float:
    (t,) = [r.execution_time for r in res.result.results if r.node.name == name]
    return t


if __name__ == "__main__":
    main()
