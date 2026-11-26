"""Gold: the dbt project in transform/, built with dbt-duckdb over a silver made in the test."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import duckdb
import pytest
from dbt.cli.main import dbtRunner

from conftest import fixture
from quake_lake import dashboard
from quake_lake.silver import build_silver
from quake_lake.storage import LocalStorage
from test_silver import T0, bmkg, feature, gempa, usgs

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
    # The aggregates, against the recorded features themselves.
    palu = json.loads(fixture("usgs-palu-2018-09-28.json"))["features"]
    ours = [f for f in palu if f["properties"]["place"].endswith(", Indonesia")]
    mags = [f["properties"]["mag"] for f in ours]
    shallow = sum(f["geometry"]["coordinates"][2] < 70 for f in ours)
    assert query(
        lake,
        "select earthquakes, m5_plus, m6_plus, shallow, max_magnitude from agg_daily_activity"
        " where activity_date = '2018-09-28'",
    ) == [(80, sum(m >= 5 for m in mags), sum(m >= 6 for m in mags), shallow, max(mags))]
    # Every province and class, recomputed from the fact's rows (groups of up to dozens).
    groups: dict[tuple, list[tuple]] = {}
    for p, cls, mag, depth, t in query(
        lake,
        "select province_code, magnitude_class, magnitude, depth_km, origin_time_utc"
        " from fct_earthquakes where in_indonesia",
    ):
        groups.setdefault((p, cls), []).append((mag, depth, t))
    expected = {
        k: (len(v), max(m for m, _, _ in v), min(t for *_, t in v), max(t for *_, t in v))
        for k, v in groups.items()
    }
    # The exact mean, rounded half away from zero as SQL rounds.
    depths = {
        k: float(
            (sum(Decimal(repr(d)) for _, d, _ in v) / len(v)).quantize(
                Decimal("0.1"), ROUND_HALF_UP
            )
        )
        for k, v in groups.items()
    }
    rows = query(
        lake,
        "select province_code, magnitude_class, earthquakes, max_magnitude, first_utc, last_utc,"
        " mean_depth_km from agg_province_magnitude",
    )
    assert {(p, c): (n, mx, first, last) for p, c, n, mx, first, last, _ in rows} == expected
    assert {(p, c): depth for p, c, *_, depth in rows} == depths
    assert max(len(v) for v in groups.values()) > 1
    # Every day from 2018-09-28 to the last BMKG event, quiet ones as 0.
    days = (datetime(2026, 11, 20) - datetime(2018, 9, 28)).days + 1
    assert query(lake, "select count(*), sum(earthquakes) from agg_daily_activity") == [(days, 95)]
    assert query(lake, "select count(*), count_if(region is null) from dim_province") == [(38, 0)]
    # The dashboard computes from the fact; its province and month counts match the marts.
    with duckdb.connect(str(lake / "gold.duckdb")) as c:
        quakes = c.sql(dashboard.QUERY).df()
    assert len(quakes) == 81 + 15
    ours = quakes[quakes.in_indonesia]
    board = dashboard.leaderboard(ours)[["province", "earthquakes", "m5_plus", "largest"]]
    assert set(board.itertuples(index=False)) == set(
        query(
            lake,
            "select province, sum(earthquakes), coalesce(sum(earthquakes) filter (where"
            " magnitude_class in ('moderate', 'strong', 'major', 'great')), 0),"
            " max(max_magnitude) from agg_province_magnitude join dim_province"
            " using (province_code) group by province",
        )
    )
    months = dashboard.monthly(ours)
    assert {(m.date(), n) for m, n in months.itertuples(index=False)} == set(
        query(
            lake,
            "select date_trunc('month', activity_date)::date, sum(earthquakes)"
            " from agg_daily_activity group by 1",
        )
    )


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


def test_the_mean_depth_is_exact_whatever_the_row_order(lake):
    # Papua Barat Daya's 7 minor earthquakes in the real data: their mean depth is 27.35
    # exactly. Summed as floats it was 27.3 in a full build and 27.4 in an incremental one.
    # And two light ones at 100.05 km: the double nearest 100.05 is 100.0499... And two
    # micro ones with mean 27.3495, which rounding twice (to 27.35, then 27.4) gets wrong.
    depths = [(3.5, d) for d in (38.8, 39.01, 39.51, 15.04, 10.0, 39.09, 10.0)]
    depths += [(4.5, 100.05), (4.5, 100.05), (2.5, 27.349), (2.5, 27.35)]
    near_sorong = [
        feature(f"us{i}", T0 + timedelta(hours=i), -0.9, 131.3, d, m, place="Sorong, Indonesia")
        for i, (m, d) in enumerate(depths)
    ]
    silver(lake, {USGS_KEY: usgs(*near_sorong)})

    assert dbt(lake, "build").success
    assert query(
        lake,
        "select province_code, magnitude_class, earthquakes, mean_depth_km"
        " from agg_province_magnitude order by magnitude_class",
    ) == [("ID-PD", "light", 2, 100.1), ("ID-PD", "micro", 2, 27.3), ("ID-PD", "minor", 7, 27.4)]


def fct(lake: Path) -> list[tuple]:
    return query(lake, "select * from fct_earthquakes order by event_id")


def test_an_incremental_run_matches_a_full_refresh(lake):
    t0 = datetime(2026, 11, 10, tzinfo=UTC)
    day1 = USGS_KEY
    day2 = "source=usgs/ingest_date=2026-11-21/20261102T000000Z_20261121T000000Z.json"
    place = {"place": "10 km N of Palu, Indonesia"}
    quiet = t0 + timedelta(hours=5)  # changes without a new `updated`
    first = {
        day1: usgs(
            feature("same", t=t0, **place),
            feature("revised", t=t0 + timedelta(hours=1), mag=5.0, **place),
            feature("renamed", t=t0 + timedelta(hours=2), **place),
            feature("quiet", t=quiet, mag=4.0, **place),
        ),
        "source=bmkg/ingest_date=2026-11-20/gempaterkini_20261120T180000Z.json": bmkg(
            # BMKG-only for now; USGS lists it in the next fetch.
            gempa(t=t0 + timedelta(hours=3), lat=-1.0, lon=120.0),
            # BMKG-only for good; BMKG revises its magnitude, and BMKG rows have no `updated`.
            gempa(t=quiet, lat=-5.0, lon=105.0, mag="3.0"),
        ),
    }
    silver(lake, first)
    assert dbt(lake, "build").success
    before = fct(lake)
    assert {row[0] for row in before} == {
        "same",
        "revised",
        "renamed",
        "quiet",
        "bmkg-20261110T030000Z",
        "bmkg-20261110T050000Z",
    }
    # --empty runs the models on no rows and leaves int_earthquakes an empty view; the
    # clean-up hook must not empty the table, then or on the next run of the fact alone.
    assert dbt(lake, "run", "--empty").success
    assert fct(lake) == before
    assert dbt(lake, "run", "--select", "fct_earthquakes").success
    assert fct(lake) == before
    assert dbt(lake, "run").success  # rebuilds the views from silver
    assert fct(lake) == before

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
            feature("quiet", t=quiet, mag=4.6, **place),  # same `updated` as before
        ),
        "source=bmkg/ingest_date=2026-11-21/gempaterkini_20261121T180000Z.json": bmkg(
            gempa(t=quiet, lat=-5.0, lon=105.0, mag="3.4")
        ),
    }
    silver(lake, second)
    # The rows the incremental run will write: the new and the changed, not "same".
    shown = dbt(lake, "show", "--select", "fct_earthquakes", "--limit", "100")
    written = {r["event_id"] for r in shown.result.results[0].agate_table}
    assert written == {"revised", "new-id", "matches-bmkg", "new", "quiet", "bmkg-20261110T050000Z"}
    assert dbt(lake, "run", "--select", "fct_earthquakes").success
    incremental = fct(lake)

    assert {row[0] for row in incremental} == {
        "same",
        "revised",
        "new-id",
        "matches-bmkg",
        "new",
        "quiet",
        "bmkg-20261110T050000Z",
    }
    assert dbt(lake, "run", "--select", "fct_earthquakes", "--full-refresh").success
    assert incremental == fct(lake)


def test_freshness_follows_the_newest_origin_time(lake):
    now = datetime.now(UTC).replace(microsecond=0)
    key = f"source=usgs/ingest_date={now:%Y-%m-%d}/x_{now:%Y%m%dT%H%M%SZ}.json"
    silver(lake, {PALU_KEY: fixture("usgs-palu-2018-09-28.json")})
    assert dbt(lake, "source", "freshness").result.results[0].status == "error"

    silver(lake, {key: usgs(feature("now", t=now - timedelta(hours=1)))})
    assert dbt(lake, "source", "freshness").result.results[0].status == "pass"
