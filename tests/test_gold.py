"""Gold: the dbt project in transform/, built with dbt-duckdb over a silver made in the test."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from dbt.cli.main import dbtRunner

from conftest import fixture
from quake_lake.silver import build_silver
from quake_lake.storage import LocalStorage
from test_silver import bmkg, feature, gempa, usgs

ROOT = Path(__file__).parent.parent
PROJECT = ROOT / "transform"
PALU_KEY = "source=usgs/ingest_date=2026-11-20/20180928T000000Z_20180929T000000Z.json"
USGS_KEY = "source=usgs/ingest_date=2026-11-20/20261101T000000Z_20261120T000000Z.json"


@pytest.fixture
def lake(tmp_path, monkeypatch):
    """A bronze, silver and gold under tmp_path, and dbt pointed at them."""
    monkeypatch.chdir(ROOT)  # the project's relative paths (the province table) start here
    monkeypatch.setenv("SILVER_URI", str(tmp_path / "silver"))
    monkeypatch.setenv("GOLD_DB", str(tmp_path / "gold.duckdb"))
    return tmp_path


def silver(lake: Path, objects: dict[str, bytes]) -> None:
    bronze = LocalStorage(lake / "bronze")
    for key, body in objects.items():
        bronze.put(key, body)
    build_silver(bronze, LocalStorage(lake / "silver"))


def dbt(lake: Path, *args: str):
    paths = ["--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
    paths += ["--target-path", str(lake / "target"), "--log-path", str(lake / "logs")]
    res = dbtRunner().invoke([*args, *paths])
    assert res.exception is None, res.exception
    return res


def query(lake: Path, sql: str) -> list[tuple]:
    # Not read_only: dbt-duckdb keeps its connection open, and DuckDB refuses a second
    # connection to the same file with a different configuration.
    with duckdb.connect(str(lake / "gold.duckdb")) as c:
        return c.sql(sql).fetchall()


def statuses(res) -> dict[str, str]:
    return {r.node.name: str(r.status) for r in res.result.results}


def test_gold_builds_from_the_recorded_feeds(lake):
    silver(
        lake,
        {
            PALU_KEY: fixture("usgs-palu-2018-09-28.json"),
            "source=bmkg/ingest_date=2026-11-20/gempaterkini_20261120T181200Z.json": fixture(
                "bmkg-gempaterkini.json"
            ),
        },
    )
    res = dbt(lake, "build")

    assert res.success
    assert set(statuses(res).values()) == {"pass", "success"}
    # The Palu day: 81 USGS events, one of them placed in the Philippines by USGS.
    assert query(lake, "select count(*), count_if(in_indonesia) from fct_earthquakes") == [
        (81 + 15, 80 + 15)
    ]
    assert query(
        lake,
        "select magnitude_class, province_code, in_indonesia from fct_earthquakes"
        " where event_id = 'us1000h3p4'",
    ) == [("major", "ID-ST", True)]
    assert query(
        lake,
        "select earthquakes, m6_plus from agg_daily_activity where activity_date = '2018-09-28'",
    ) == [(80, 2)]
    # Every day from 2018-09-28 to the last BMKG event, quiet ones as 0.
    days = (datetime(2026, 11, 20) - datetime(2018, 9, 28)).days + 1
    assert query(lake, "select count(*), sum(earthquakes) from agg_daily_activity") == [(days, 95)]
    assert query(lake, "select count(*), count_if(region is null) from dim_province") == [(38, 0)]


def test_a_usgs_region_missing_from_the_seed_warns(lake):
    silver(lake, {USGS_KEY: usgs(feature("us1", place="60 km W of Gizo, Solomon Islands"))})
    res = dbt(lake, "build")

    assert res.success
    warned = {n: s for n, s in statuses(res).items() if s == "warn"}
    assert list(warned) == [
        "relationships_int_earthquakes_place_region__place_region__ref_usgs_place_regions_"
    ]
    # It counts as Indonesian until someone adds it.
    assert query(lake, "select place_region, in_indonesia from fct_earthquakes") == [
        ("Solomon Islands", True)
    ]


def fct(lake: Path) -> list[tuple]:
    return query(lake, "select * from fct_earthquakes order by event_id")


def test_an_incremental_run_matches_a_full_refresh(lake):
    t0 = datetime(2026, 11, 10, tzinfo=UTC)
    day1 = USGS_KEY
    day2 = "source=usgs/ingest_date=2026-11-21/20261102T000000Z_20261121T000000Z.json"
    place = {"place": "10 km N of Palu, Indonesia"}
    first = {
        day1: usgs(
            feature("same", t=t0, **place),
            feature("revised", t=t0 + timedelta(hours=1), mag=5.0, **place),
            feature("renamed", t=t0 + timedelta(hours=2), **place),
        ),
        # BMKG-only for now; USGS lists it in the next fetch.
        "source=bmkg/ingest_date=2026-11-20/gempaterkini_20261120T180000Z.json": bmkg(
            gempa(t=t0 + timedelta(hours=3), lat=-1.0, lon=120.0)
        ),
    }
    silver(lake, first)
    assert dbt(lake, "build").success
    before = {row[0] for row in fct(lake)}
    assert before == {"same", "revised", "renamed", "bmkg-20261110T030000Z"}

    later = t0 + timedelta(days=1)
    second = {
        day2: usgs(
            feature("same", t=t0, **place),
            feature("revised", t=t0 + timedelta(hours=1), mag=5.4, updated=later, **place),
            feature(
                "new-id", t=t0 + timedelta(hours=2), updated=later, ids=",renamed,new-id,", **place
            ),
            feature(
                "matches-bmkg", t=t0 + timedelta(hours=3, seconds=2), lat=-1.1, lon=120.0, **place
            ),
            feature("new", t=t0 + timedelta(hours=4), **place),
        )
    }
    silver(lake, second)
    # The rows the incremental run will write: the new and the changed, not "same".
    shown = dbt(lake, "show", "--select", "fct_earthquakes", "--limit", "100")
    written = {r["event_id"] for r in shown.result.results[0].agate_table}
    assert written == {"revised", "new-id", "matches-bmkg", "new"}
    assert dbt(lake, "run", "--select", "fct_earthquakes").success
    incremental = fct(lake)

    assert {row[0] for row in incremental} == {"same", "revised", "new-id", "matches-bmkg", "new"}
    assert dbt(lake, "run", "--select", "fct_earthquakes", "--full-refresh").success
    assert incremental == fct(lake)


def test_freshness_follows_the_newest_origin_time(lake):
    now = datetime.now(UTC).replace(microsecond=0)
    key = f"source=usgs/ingest_date={now:%Y-%m-%d}/x_{now:%Y%m%dT%H%M%SZ}.json"
    silver(lake, {PALU_KEY: fixture("usgs-palu-2018-09-28.json")})
    assert dbt(lake, "source", "freshness").result.results[0].status == "error"

    silver(lake, {key: usgs(feature("now", t=now - timedelta(hours=1)))})
    assert dbt(lake, "source", "freshness").result.results[0].status == "pass"
