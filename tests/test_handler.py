from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlsplit

import pytest

from conftest import PALU_EVENTS
from quake_lake.handler import lambda_handler
from quake_lake.storage import LocalStorage


@pytest.fixture
def bronze(tmp_path, monkeypatch) -> LocalStorage:
    monkeypatch.setenv("BRONZE_URI", str(tmp_path / "bronze"))
    return LocalStorage(tmp_path / "bronze")


def test_default_event_ingests_both_sources(net, bronze):
    result = lambda_handler({}, None)

    keys = bronze.list()
    assert [k.split("/")[0] for k in keys] == ["source=bmkg"] * 3 + ["source=usgs"]
    assert sorted(o["key"] for o in result["objects"]) == keys
    assert result["objects"][0]["key"].startswith("source=usgs/")  # in the order sources ran
    assert result["events"] == PALU_EVENTS + 1 + 15 + 15
    assert result["bronze"] == bronze.uri
    ingested = datetime.fromisoformat(result["ingested_at"])
    assert all(f"ingest_date={ingested.date()}" in k for k in keys)


def test_usgs_window_is_the_last_days(net, bronze):
    lambda_handler({"sources": ["usgs"], "days": 2}, None)
    (url,) = net.urls
    q = {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}
    start = datetime.fromisoformat(q["starttime"]).replace(tzinfo=UTC)
    end = datetime.fromisoformat(q["endtime"]).replace(tzinfo=UTC)
    assert end - start == timedelta(days=2)
    assert abs(datetime.now(UTC) - end) < timedelta(minutes=1)


def test_bmkg_only(net, bronze):
    result = lambda_handler({"sources": ["bmkg"]}, None)
    assert len(result["objects"]) == 3
    assert not any("usgs" in u for u in net.urls)


def test_none_event_is_the_default(net, bronze):
    assert len(lambda_handler(None)["objects"]) == 4


def test_a_failing_source_does_not_cost_the_other_its_snapshot(net, bronze):
    net.routes["https://earthquake.usgs.gov/"] = 404
    with pytest.raises(RuntimeError, match=r"ingest failed for usgs: .*HTTP 404"):
        lambda_handler({}, None)
    assert [k.split("/")[0] for k in bronze.list()] == ["source=bmkg"] * 3


@pytest.mark.parametrize(
    "event",
    [
        {"days": 0},
        {"days": "7"},
        {"days": True},
        {"days": 1.5},
        {"sources": "usgs"},
        {"sources": []},
        {"sources": ["usgs", "emsc"]},
        {"sources": [["usgs"]]},
    ],
)
def test_bad_events_are_refused_before_any_request(net, bronze, event):
    with pytest.raises(ValueError, match=r"^'(days|sources)' must be"):
        lambda_handler(event, None)
    assert net.urls == []


def test_needs_bronze_uri(net, monkeypatch):
    monkeypatch.delenv("BRONZE_URI", raising=False)
    with pytest.raises(KeyError, match="BRONZE_URI"):
        lambda_handler({}, None)
