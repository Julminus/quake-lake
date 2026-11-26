"""The Streamlit dashboard, run headless (AppTest) on a small gold built here.

tests/test_gold.py runs the dashboard's query on a gold dbt built and compares its counts
with the marts.
"""

from __future__ import annotations

from datetime import datetime

import duckdb
import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from quake_lake import dashboard

COLUMNS = "event_id, origin_time_utc, latitude, longitude, depth_km, depth_class, magnitude,"
COLUMNS += " place, in_indonesia, province_code"
QUAKES = [
    ("a", datetime(2018, 9, 28, 10, 2), -0.2, 119.8, 10.0, "shallow", 7.5, "Palu", True, "ID-ST"),
    ("b", datetime(2019, 1, 5), -3.6, 128.2, 30.0, "shallow", 4.6, "Ambon", True, "ID-MA"),
    ("c", datetime(2019, 3, 1), -3.7, 128.1, 120.0, "intermediate", 5.2, "Ambon", True, "ID-MA"),
    ("d", datetime(2020, 6, 1), 5.0, 126.5, 600.0, "deep", 6.1, "Mindanao", False, "ID-SA"),
    ("e", datetime(2020, 7, 1), -7.9, 110.4, 10.0, "shallow", None, "Bantul", True, "ID-YO"),
]
PROVINCES = [
    ("ID-ST", "Sulawesi Tengah", "Sulawesi"),
    ("ID-MA", "Maluku", "Maluku"),
    ("ID-SA", "Sulawesi Utara", "Sulawesi"),
    ("ID-YO", "DI Yogyakarta", "Jawa"),
]


def add(gold, rows) -> None:
    with duckdb.connect(str(gold)) as c:
        c.executemany(f"insert into fct_earthquakes ({COLUMNS}) values (?{', ?' * 9})", rows)


@pytest.fixture
def gold(tmp_path, monkeypatch):
    path = tmp_path / "gold.duckdb"
    with duckdb.connect(str(path)) as c:
        c.execute(
            "create table fct_earthquakes (event_id varchar, origin_time_utc timestamp,"
            " latitude double, longitude double, depth_km double, depth_class varchar,"
            " magnitude double, place varchar, in_indonesia boolean, province_code varchar)"
        )
        c.execute(
            "create table dim_province (province_code varchar, province varchar, region varchar)"
        )
        c.executemany("insert into dim_province values (?, ?, ?)", PROVINCES)
    add(path, QUAKES)
    monkeypatch.setenv("GOLD_DB", str(path))
    return path


def app() -> AppTest:
    return AppTest.from_file(dashboard.__file__, default_timeout=60).run()


def metrics(at: AppTest) -> dict[str, str]:
    return {m.label: m.value for m in at.metric}


def test_the_default_view_counts_indonesian_earthquakes(gold):
    at = app()

    assert not at.exception
    assert metrics(at) == {
        "Earthquakes": "4",  # d is placed abroad; e (no magnitude) stays
        "M5 and up": "2",
        "M6 and up": "1",
        "Largest": "M7.5",
    }
    board = at.dataframe[0].value
    assert board.province.tolist() == ["Maluku", "DI Yogyakarta", "Sulawesi Tengah"]
    assert board.earthquakes.tolist() == [2, 1, 1]
    assert board.m5_plus.tolist() == [1, 0, 1]
    assert "Newest event in gold: 2020-07-01 00:00 UTC" in at.caption[0].value


def test_the_filters_drive_every_number(gold):
    at = app()

    at.sidebar.toggle[0].set_value(True).run()  # with the one USGS places abroad
    assert metrics(at)["Earthquakes"] == "5"
    assert metrics(at)["Largest"] == "M7.5"

    at.sidebar.slider[1].set_value(5.0).run()  # a magnitude filter drops the unknown one
    assert metrics(at)["Earthquakes"] == "3"
    assert at.dataframe[0].value.province.tolist() == [
        "Maluku",
        "Sulawesi Tengah",
        "Sulawesi Utara",
    ]

    at.sidebar.slider[0].set_value((2020, 2020)).run()
    assert metrics(at) == {
        "Earthquakes": "1",
        "M5 and up": "1",
        "M6 and up": "1",
        "Largest": "M6.1",
    }

    at.sidebar.slider[1].set_value(8.0).run()
    assert not at.metric
    assert at.info[0].value == "No earthquake matches these filters."


def test_a_rebuilt_gold_is_read_again(gold):
    at = app()
    assert metrics(at)["Earthquakes"] == "4"

    add(gold, [("f", datetime(2021, 1, 1), -1.0, 120.0, 5.0, "shallow", 4.1, "x", True, "ID-ST")])
    at.run()

    assert metrics(at)["Earthquakes"] == "5"
    assert "Newest event in gold: 2021-01-01" in at.caption[0].value


def test_no_gold_says_how_to_build_it(tmp_path, monkeypatch):
    monkeypatch.setenv("GOLD_DB", str(tmp_path / "missing.duckdb"))
    at = app()

    assert not at.exception
    assert "make gold" in at.error[0].value
    assert not at.metric


def test_a_gold_without_the_marts_is_an_error_not_a_traceback(tmp_path, monkeypatch):
    path = tmp_path / "gold.duckdb"
    duckdb.connect(str(path)).close()
    monkeypatch.setenv("GOLD_DB", str(path))
    at = app()

    assert not at.exception
    assert "fct_earthquakes" in at.error[0].value


def test_an_empty_gold_says_so(gold):
    with duckdb.connect(str(gold)) as c:
        c.execute("delete from fct_earthquakes")
    at = app()

    assert not at.exception
    assert "has no earthquakes" in at.info[0].value


def test_magnitudes_are_counted_to_the_nearest_tenth():
    quakes = pd.DataFrame({"magnitude": [4.3, 4.34, 4.36, 5.0, None]})

    got = dashboard.magnitudes(quakes)

    assert got.to_dict("list") == {"magnitude": [4.3, 4.4, 5.0], "earthquakes": [2, 1, 1]}


def test_a_quiet_month_is_a_zero():
    quakes = pd.DataFrame(
        {"event_id": ["a", "b"], "origin_time_utc": [datetime(2019, 1, 5), datetime(2019, 3, 1)]}
    )

    got = dashboard.monthly(quakes)

    assert got.earthquakes.tolist() == [1, 0, 1]
    assert got.month.tolist() == [pd.Timestamp(f"2019-0{m}-01") for m in (1, 2, 3)]
