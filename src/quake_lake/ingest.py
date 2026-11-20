"""Bronze ingestion: fetch raw JSON from USGS and BMKG and store it unchanged.

Layout under the bronze root (one ``ingest_date`` per run, in UTC):

    source=usgs/ingest_date=2026-11-20/20150101T000000Z_20150201T000000Z.json
    source=bmkg/ingest_date=2026-11-20/autogempa_20261120T180432Z.json

Bronze keeps exactly the bytes the source sent. A payload is only checked for
the shape that proves it is the real feed (not an error page) and counted;
parsing, typing and deduplication are the silver layer's job.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode

from quake_lake.fetch import Fetch, fetch
from quake_lake.storage import Storage

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
# Indonesia spans about 6°N to 11°S and 95°E to 141°E; one degree of margin on each side.
INDONESIA_BBOX = {"minlatitude": -12, "maxlatitude": 7, "minlongitude": 94, "maxlongitude": 142}
BMKG_URL = "https://data.bmkg.go.id/DataMKG/TEWS/{feed}.json"
# Latest event, latest 15 events of M5.0+, latest 15 felt events.
BMKG_FEEDS = ("autogempa", "gempaterkini", "gempadirasakan")

Window = tuple[datetime, datetime]


class IngestError(RuntimeError):
    """A source answered with something that is not its feed."""


@dataclass(frozen=True)
class Written:
    key: str
    events: int
    size: int


def month_windows(start: datetime, end: datetime) -> list[Window]:
    """Calendar-month windows from ``start`` to ``end`` (the first and last may be partial).

    USGS answers at most 20,000 events per query; the busiest month since 2015
    in the Indonesia box had 606 (August 2026), so one query per month is safe.
    """
    windows: list[Window] = []
    lo = start
    while lo < end:
        hi = min(_next_month(lo), end)
        windows.append((lo, hi))
        lo = hi
    return windows


def usgs_url(start: datetime, end: datetime) -> str:
    # USGS treats both ends as inclusive, so an event exactly on a boundary can
    # appear in two windows; silver deduplicates on the event id.
    query = {
        "format": "geojson",
        "starttime": _iso(start),
        "endtime": _iso(end),
        **INDONESIA_BBOX,
        "orderby": "time-asc",
    }
    return f"{USGS_URL}?{urlencode(query)}"


def ingest_usgs(
    storage: Storage, windows: Iterable[Window], *, now: datetime, fetch: Fetch = fetch
) -> list[Written]:
    """One request and one bronze object per window."""
    written = []
    for start, end in windows:
        body = fetch(usgs_url(start, end))
        doc = _json(body, "USGS")
        features = doc.get("features") if doc.get("type") == "FeatureCollection" else None
        if not isinstance(features, list):
            raise IngestError(f"USGS answer is not a GeoJSON FeatureCollection: {body[:200]!r}")
        key = f"{_prefix('usgs', now)}/{_stamp(start)}_{_stamp(end)}.json"
        storage.put(key, body)
        written.append(Written(key, len(features), len(body)))
    return written


def ingest_bmkg(
    storage: Storage,
    *,
    now: datetime,
    feeds: Sequence[str] = BMKG_FEEDS,
    fetch: Fetch = fetch,
) -> list[Written]:
    """One snapshot per feed. The feeds only hold the latest events, so poll them often."""
    written = []
    for feed in feeds:
        body = fetch(BMKG_URL.format(feed=feed))
        gempa = _json(body, "BMKG").get("Infogempa", {})
        gempa = gempa.get("gempa") if isinstance(gempa, dict) else None
        if not isinstance(gempa, dict | list):
            raise IngestError(f"BMKG {feed} answer has no Infogempa.gempa: {body[:200]!r}")
        key = f"{_prefix('bmkg', now)}/{feed}_{_stamp(now)}.json"
        storage.put(key, body)
        written.append(Written(key, 1 if isinstance(gempa, dict) else len(gempa), len(body)))
    return written


def _json(body: bytes, source: str) -> dict[str, Any]:
    try:
        doc = json.loads(body)
    except ValueError:
        raise IngestError(f"{source} answer is not JSON: {body[:200]!r}") from None
    if not isinstance(doc, dict):
        raise IngestError(f"{source} answer is not a JSON object: {body[:200]!r}")
    return doc


def _prefix(source: str, now: datetime) -> str:
    return f"source={source}/ingest_date={_utc(now).date().isoformat()}"


def _stamp(t: datetime) -> str:
    return _utc(t).strftime("%Y%m%dT%H%M%SZ")


def _iso(t: datetime) -> str:
    return _utc(t).strftime("%Y-%m-%dT%H:%M:%S")


def _next_month(t: datetime) -> datetime:
    """Midnight UTC on the first day of the month after ``t``."""
    t = _utc(t)
    year, month = (t.year + 1, 1) if t.month == 12 else (t.year, t.month + 1)
    return datetime(year, month, 1, tzinfo=UTC)


def _utc(t: datetime) -> datetime:
    # astimezone() would read a naive time as this machine's local time (WIB here).
    if t.tzinfo is None:
        raise ValueError(f"naive datetime {t.isoformat()}: pass a timezone-aware time")
    return t.astimezone(UTC)
