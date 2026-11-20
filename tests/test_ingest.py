from __future__ import annotations

import itertools
from datetime import UTC, datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

import pytest

from conftest import PALU_EVENTS, fixture
from quake_lake.ingest import (
    BMKG_FEEDS,
    IngestError,
    Written,
    ingest_bmkg,
    ingest_usgs,
    month_windows,
    usgs_url,
)
from quake_lake.storage import LocalStorage

WIB = timezone(timedelta(hours=7))
NOW = datetime(2026, 11, 20, 18, 4, 32, tzinfo=UTC)


def utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=UTC)


def test_month_windows_cover_the_range_without_gaps():
    windows = month_windows(utc(2015, 11, 15), utc(2016, 2, 10, 6))
    assert windows == [
        (utc(2015, 11, 15), utc(2015, 12, 1)),
        (utc(2015, 12, 1), utc(2016, 1, 1)),
        (utc(2016, 1, 1), utc(2016, 2, 1)),
        (utc(2016, 2, 1), utc(2016, 2, 10, 6)),
    ]


def test_backfill_since_2015_is_one_query_per_month():
    windows = month_windows(utc(2015, 1, 1), NOW)
    assert len(windows) == 12 * 11 + 11  # 2015-01 .. 2026-11
    assert all(a < b for a, b in windows)
    assert all(b == c for (_, b), (c, _) in itertools.pairwise(windows))


def test_month_windows_empty_and_zone_aware():
    assert month_windows(NOW, NOW) == []
    assert month_windows(NOW, NOW - timedelta(days=1)) == []
    # 05:00 WIB on 1 March is still 28 February in UTC, so the first window is the
    # last two hours of February: months are UTC months, not the caller's.
    windows = month_windows(datetime(2026, 3, 1, 5, tzinfo=WIB), utc(2026, 5, 1))
    assert windows[0] == (utc(2026, 2, 28, 22), utc(2026, 3, 1))
    assert len(windows) == 3


def test_naive_times_are_refused():
    with pytest.raises(ValueError, match="naive datetime"):
        usgs_url(datetime(2026, 1, 1), NOW)


def test_usgs_url_asks_for_the_indonesia_box_in_utc():
    url = usgs_url(datetime(2018, 9, 28, 7, tzinfo=WIB), utc(2018, 9, 29))
    q = {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}
    assert url.startswith("https://earthquake.usgs.gov/fdsnws/event/1/query?")
    assert q == {
        "format": "geojson",
        "starttime": "2018-09-28T00:00:00",
        "endtime": "2018-09-29T00:00:00",
        "minlatitude": "-12",
        "maxlatitude": "7",
        "minlongitude": "94",
        "maxlongitude": "142",
        "orderby": "time-asc",
    }


def test_ingest_usgs_stores_the_raw_bytes_under_source_and_ingest_date(tmp_path):
    body = fixture("usgs-palu-2018-09-28.json")
    urls: list[str] = []
    storage = LocalStorage(tmp_path)
    window = (utc(2018, 9, 28), utc(2018, 9, 29))

    written = ingest_usgs(storage, [window], now=NOW, fetch=lambda u: urls.append(u) or body)

    key = "source=usgs/ingest_date=2026-11-20/20180928T000000Z_20180929T000000Z.json"
    assert written == [Written(key, PALU_EVENTS, len(body))]
    assert storage.get(key) == body  # byte for byte
    assert urls == [usgs_url(*window)]


def test_ingest_date_is_the_utc_date(tmp_path):
    # 01:00 WIB on the 21st is still the 20th in UTC.
    now = datetime(2026, 11, 21, 1, 0, tzinfo=WIB)
    w = ingest_usgs(
        LocalStorage(tmp_path),
        [(now - timedelta(days=7), now)],
        now=now,
        fetch=lambda u: fixture("usgs-empty.json"),
    )
    assert w[0].key == "source=usgs/ingest_date=2026-11-20/20261113T180000Z_20261120T180000Z.json"
    assert w[0].events == 0


@pytest.mark.parametrize(
    "body",
    [
        b"<html>503 Service Unavailable</html>",
        b"[]",
        b'{"type": "Feature", "features": []}',
        b'{"type": "FeatureCollection", "features": {"a": 1}}',
        b'{"type": "FeatureCollection"}',
        b"",
    ],
)
def test_ingest_usgs_refuses_what_is_not_a_feature_collection(tmp_path, body):
    storage = LocalStorage(tmp_path)
    with pytest.raises(IngestError, match="USGS answer"):
        ingest_usgs(storage, [(utc(2018, 9, 28), utc(2018, 9, 29))], now=NOW, fetch=lambda u: body)
    assert storage.list() == []


def test_ingest_bmkg_snapshots_each_feed(tmp_path):
    storage = LocalStorage(tmp_path)
    urls: list[str] = []

    def fetch(url: str) -> bytes:
        urls.append(url)
        return fixture("bmkg-" + url.rsplit("/", 1)[1])

    written = ingest_bmkg(storage, now=NOW, fetch=fetch)

    assert [(w.key, w.events) for w in written] == [
        ("source=bmkg/ingest_date=2026-11-20/autogempa_20261120T180432Z.json", 1),
        ("source=bmkg/ingest_date=2026-11-20/gempaterkini_20261120T180432Z.json", 15),
        ("source=bmkg/ingest_date=2026-11-20/gempadirasakan_20261120T180432Z.json", 15),
    ]
    assert urls == [f"https://data.bmkg.go.id/DataMKG/TEWS/{f}.json" for f in BMKG_FEEDS]
    for w in written:
        assert storage.get(w.key) == fixture("bmkg-" + w.key.split("/")[-1].split("_")[0] + ".json")
        assert w.size == len(storage.get(w.key))


def test_ingest_bmkg_one_feed(tmp_path):
    written = ingest_bmkg(
        LocalStorage(tmp_path),
        now=NOW,
        feeds=["autogempa"],
        fetch=lambda u: fixture("bmkg-autogempa.json"),
    )
    assert len(written) == 1


@pytest.mark.parametrize(
    "body",
    [
        b"<html></html>",
        b'{"Infogempa": "x"}',
        b'{"Infogempa": {"gempa": "x"}}',
        b'{"other": 1}',
        b"[1]",
    ],
)
def test_ingest_bmkg_refuses_what_is_not_the_feed(tmp_path, body):
    storage = LocalStorage(tmp_path)
    with pytest.raises(IngestError, match="BMKG"):
        ingest_bmkg(storage, now=NOW, fetch=lambda u: body)
    assert storage.list() == []
