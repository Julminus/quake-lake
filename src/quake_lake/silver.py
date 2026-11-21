"""Silver: every bronze object, typed, validated and deduplicated, as partitioned Parquet.

Layout under the silver root:

    events/year=2026/month=11/part-0.parquet   one file per month of origin time (UTC)
    quarantine/events.jsonl                    records that failed validation, with reasons

Each run rebuilds silver from all of bronze, so it is idempotent: the same bronze
gives byte-identical files, an event revised in a later fetch replaces its older
version, and a BMKG event that USGS only lists later is merged on the next run.
"""

from __future__ import annotations

import csv
import io
import itertools
import json
import math
import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import cache
from importlib.resources import files
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pandera.errors import SchemaErrors
from pandera.pandas import Check, Column, DataFrameSchema

from quake_lake.ingest import INDONESIA_BBOX
from quake_lake.storage import Storage

# Measured on 2026-11-21: the 15 BMKG events that USGS also lists were 0 to 5 s and
# 8 to 36 km from their USGS match; distinct USGS events this close are rare (6 pairs
# within 10 s and 100 km in 12 years). Time tells events apart far better than place.
MATCH_SECONDS = 10
MATCH_KM = 100
EARTH_KM = 6371.0088
# The source's clock or ours may be a little off; a time zone slip (WIB read as UTC) is 7 h.
CLOCK_SLACK = timedelta(hours=1)
# Objects read and written at a time: on S3 each one is a request, and they wait on the network.
IO_THREADS = 8

_STAMP = re.compile(r"(\d{8}T\d{6}Z)\.json$")

EVENTS = pa.schema(
    [
        ("event_id", pa.string()),
        ("source", pa.string()),
        ("time", pa.timestamp("ms", tz="UTC")),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("depth_km", pa.float64()),
        ("magnitude", pa.float64()),
        ("mag_type", pa.string()),
        ("place", pa.string()),
        ("event_type", pa.string()),
        ("updated", pa.timestamp("ms", tz="UTC")),
        ("bmkg_id", pa.string()),
        ("province_code", pa.string()),
        ("province", pa.string()),
        ("province_km", pa.float64()),
        ("bronze_key", pa.string()),
    ]
)

RECORD = DataFrameSchema(
    {
        "event_id": Column(str, Check.str_length(min_value=1)),
        "source": Column(str, Check.isin(["usgs", "bmkg"])),
        # Instrumental catalogues start around 1900; earlier is a broken timestamp.
        "time": Column("datetime64[ms, UTC]", Check.ge(pd.Timestamp("1900-01-01", tz="UTC"))),
        "latitude": Column(
            float, Check.in_range(INDONESIA_BBOX["minlatitude"], INDONESIA_BBOX["maxlatitude"])
        ),
        "longitude": Column(
            float, Check.in_range(INDONESIA_BBOX["minlongitude"], INDONESIA_BBOX["maxlongitude"])
        ),
        # USGS puts shallow volcanic events a little above sea level; the deepest
        # earthquakes are about 700 km down.
        "depth_km": Column(float, Check.in_range(-10, 800)),
        "magnitude": Column(float, Check.in_range(-2, 10), nullable=True),
        "mag_type": Column(str, nullable=True),
        "place": Column(str, nullable=True),
        "event_type": Column(str),
        "updated": Column("datetime64[ms, UTC]", nullable=True),
        "ids": Column(str, nullable=True),  # USGS: every id the event has had, ",a,b,"
        "fetched": Column("datetime64[ms, UTC]"),
    },
    checks=Check(
        lambda df: df["time"] <= df["fetched"] + CLOCK_SLACK,
        name="time is not after the fetch",
    ),
)


@dataclass(frozen=True)
class Result:
    objects: int
    records: int
    quarantined: int
    repeats: int  # rows dropped: a newer row had the same event id, or lists it in `ids`
    merged: int  # BMKG events merged into their USGS match
    events: int
    partitions: int
    deleted: int  # stale partitions from an earlier run


def build_silver(bronze: Storage, silver: Storage) -> Result:
    """Rebuild silver from every object in bronze."""
    keys = [
        k
        for source in ("usgs", "bmkg")
        for k in bronze.list(f"source={source}/")
        if k.endswith(".json")
    ]
    rows: list[dict[str, Any]] = []
    raw: list[Any] = []
    bad: list[dict[str, Any]] = []
    records = 0
    with ThreadPoolExecutor(IO_THREADS) as pool:
        bodies = list(pool.map(bronze.get, keys))
    for key, body in zip(keys, bodies, strict=True):
        try:
            fetched, parse, items = _open(key, body)
        except _UNREADABLE as e:
            bad.append({"bronze_key": key, "errors": [f"unreadable object: {e}"], "record": None})
            continue
        for item in items:
            records += 1
            try:
                row = parse(item)
            except _UNREADABLE as e:
                error = f"unreadable record: {type(e).__name__}: {e}"
                bad.append({"bronze_key": key, "errors": [error], "record": item})
            else:
                rows.append({**row, "fetched": fetched, "bronze_key": key})
                raw.append(item)

    df = _frame(rows)
    failed = _validate(df)
    for i in sorted(failed):
        bad.append({"bronze_key": df.at[i, "bronze_key"], "errors": failed[i], "record": raw[i]})
    df = df.drop(index=list(failed))
    if df.empty:
        # A mistyped URI, or a bronze of error pages, would otherwise delete all of silver.
        why = f"; the first problem: {bad[0]['errors'][0]}" if bad else ""
        raise ValueError(
            f"no valid records in {len(keys):,} bronze objects under {bronze.uri}{why}; "
            "silver is left as it was"
        )

    before = len(df)
    # The newest version of each event wins: USGS's own `updated`, then the latest fetch.
    # A sort on several columns is stable in pandas, so with all three equal the later row wins.
    df = df.sort_values(["updated", "fetched", "bronze_key"], na_position="first")
    df = df.drop_duplicates("event_id", keep="last")
    # USGS sometimes gives an event a new preferred id; the newer row still lists the old
    # one in `ids`. Rows are sorted oldest first, so a lower position is an older version.
    position = {eid: n for n, eid in enumerate(df["event_id"])}
    replaced = {
        alias
        for n, (eid, ids) in enumerate(zip(df["event_id"], df["ids"], strict=True))
        if isinstance(ids, str)
        for alias in ids.split(",")
        if alias and alias != eid and position.get(alias, n) < n
    }
    df = df[~df["event_id"].isin(replaced)]
    repeats = before - len(df)
    df, merged = _merge_sources(df)
    df = df.sort_values(["time", "event_id"], ignore_index=True)
    df["province_code"], df["province"], df["province_km"] = _nearest_province(
        df["latitude"].to_numpy(), df["longitude"].to_numpy()
    )

    # One conversion to Arrow; rows are sorted by time, so each month is one slice of it.
    table = pa.Table.from_pandas(df[EVENTS.names], schema=EVENTS, preserve_index=False)
    parts, start = {}, 0
    for m, run in itertools.groupby(df["time"].dt.strftime("year=%Y/month=%m")):
        n = sum(1 for _ in run)
        parts[f"events/{m}/part-0.parquet"] = _parquet(table.slice(start, n))
        start += n
    with ThreadPoolExecutor(IO_THREADS) as pool:
        list(pool.map(silver.put, parts, parts.values()))
        stale = [k for k in silver.list("events/") if k.endswith(".parquet") and k not in parts]
        list(pool.map(silver.delete, stale))
    silver.put("quarantine/events.jsonl", "".join(json.dumps(b) + "\n" for b in bad).encode())
    return Result(
        objects=len(keys),
        records=records,
        quarantined=len(bad),
        repeats=repeats,
        merged=merged,
        events=len(df),
        partitions=len(parts),
        deleted=len(stale),
    )


# Overflow: a time or number too large for a datetime or a float; recursion: deep nesting.
_UNREADABLE = (
    ValueError,
    TypeError,
    AttributeError,
    KeyError,
    IndexError,
    OverflowError,
    RecursionError,
)


def _open(key: str, body: bytes) -> tuple[datetime, Callable[[Any], dict[str, Any]], list[Any]]:
    """The fetch time (from the key), the record parser and the records of one object."""
    stamp = _STAMP.search(key)
    if stamp is None:
        raise ValueError("the key has no fetch time (…_YYYYMMDDTHHMMSSZ.json)")
    fetched = datetime.strptime(stamp.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    # NaN, Infinity and 1e400 become _NotFinite: every field refuses them, and quarantine
    # (which writes them back as strings) stays strict JSON.
    doc = json.loads(body, parse_constant=_NotFinite, parse_float=_float_or_text)
    if key.startswith("source=usgs/"):
        items = doc["features"]
        if not isinstance(items, list):
            raise TypeError("features is not a list")
        return fetched, _usgs, items
    gempa = doc["Infogempa"]["gempa"]
    if not isinstance(gempa, dict | list):
        raise TypeError(f"Infogempa.gempa is not an object or a list: {gempa!r:.80}")
    return fetched, _bmkg, [gempa] if isinstance(gempa, dict) else gempa


def _usgs(f: dict[str, Any]) -> dict[str, Any]:
    p = f["properties"]
    lon, lat, depth = f["geometry"]["coordinates"][:3]
    return {
        "event_id": _text(f["id"]),
        "source": "usgs",
        "time": _epoch_ms(p["time"]),
        "latitude": _number(lat),
        "longitude": _number(lon),
        "depth_km": _number(depth),
        "magnitude": _number(p.get("mag")),
        "mag_type": _text(p.get("magType")),
        "place": _text(p.get("place")),
        "event_type": _text(p.get("type")),
        "updated": _epoch_ms(p.get("updated")),
        "ids": _text(p.get("ids")),
    }


def _bmkg(r: dict[str, Any]) -> dict[str, Any]:
    time = datetime.fromisoformat(r["DateTime"])
    if time.tzinfo is None:
        raise ValueError(f"DateTime {r['DateTime']!r} has no time zone")
    time = time.astimezone(UTC)
    lat, lon = r["Coordinates"].split(",")
    return {
        # BMKG gives no id; its feeds name an event by its origin time.
        "event_id": f"bmkg-{time:%Y%m%dT%H%M%SZ}",
        "source": "bmkg",
        "time": time,
        "latitude": _finite(lat),
        "longitude": _finite(lon),
        "depth_km": _finite(r["Kedalaman"].lower().removesuffix("km")),
        "magnitude": _finite(r["Magnitude"]),
        "mag_type": None,  # the feeds don't say which magnitude
        "place": _text(r.get("Wilayah")),
        "event_type": "earthquake",
        "updated": None,
        "ids": None,
    }


def _number(x: Any) -> float | None:
    if x is None:
        return None
    if isinstance(x, bool) or not isinstance(x, int | float):
        raise TypeError(f"expected a number, got {x!r:.80}")
    return _finite(x)


class _NotFinite(str):
    """NaN, Infinity or 1e400 as written in the JSON; neither a number nor a string here."""

    def __repr__(self) -> str:
        return str.__str__(self)


def _float_or_text(text: str) -> float | str:
    f = float(text)
    return f if math.isfinite(f) else _NotFinite(text)


def _finite(x: str | float) -> float:
    """``float(x)``, refusing NaN and infinity (``1e400`` in JSON, ``"nan"`` in BMKG's strings)."""
    f = float(x)
    if not math.isfinite(f):
        raise ValueError(f"not a finite number: {x!r:.80}")
    return f


def _text(x: Any) -> str | None:
    if x is None or (isinstance(x, str) and not isinstance(x, _NotFinite)):
        return x
    raise TypeError(f"expected a string, got {x!r}")


def _epoch_ms(x: Any) -> datetime | None:
    ms = _number(x)
    return None if ms is None else datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=ms)


def _frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [*RECORD.columns, "bronze_key"]
    df = pd.DataFrame.from_records(rows, columns=columns)
    for col in ("time", "updated", "fetched"):
        df[col] = pd.to_datetime(df[col], utc=True).astype("datetime64[ms, UTC]")
    for col in ("latitude", "longitude", "depth_km", "magnitude"):
        df[col] = df[col].astype("float64")
    for col in ("event_id", "source", "mag_type", "place", "event_type", "ids", "bronze_key"):
        df[col] = df[col].astype("str")
    return df


def _validate(df: pd.DataFrame) -> dict[int, list[str]]:
    """Row index -> what is wrong with it."""
    try:
        RECORD.validate(df, lazy=True)
    except SchemaErrors as e:
        cases = e.failure_cases
    else:
        return {}
    if cases["index"].isna().any():
        # A failure that isn't tied to a row (a wrong dtype) is a bug in _frame, not bad data.
        raise RuntimeError(f"silver schema failed as a whole:\n{cases.to_string()}")
    failed: dict[int, list[str]] = {}
    for _, c in cases.iterrows():
        what = f"{c['column']}: {c['check']}" if c["schema_context"] == "Column" else c["check"]
        if c["schema_context"] == "Column" and c["check"] != "not_nullable":
            what += f" (got {c['failure_case']!r})"
        reasons = failed.setdefault(int(c["index"]), [])
        if what not in reasons:
            reasons.append(what)
    return failed


def _merge_sources(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Fold each BMKG event into the USGS event it matches; ``bmkg_id`` keeps the link."""
    usgs = df[df["source"] == "usgs"].sort_values("time")
    bmkg = df[df["source"] == "bmkg"]
    t = usgs["time"].astype("int64").to_numpy()  # ms since the epoch
    pairs = []
    for b, bt, blat, blon in zip(
        bmkg.index, bmkg["time"].astype("int64"), bmkg["latitude"], bmkg["longitude"], strict=True
    ):
        lo, hi = np.searchsorted(t, [bt - MATCH_SECONDS * 1000, bt + MATCH_SECONDS * 1000 + 1])
        near = usgs.iloc[lo:hi]
        km = _km(blat, blon, near["latitude"].to_numpy(), near["longitude"].to_numpy())
        for u, ut, d in zip(near.index, t[lo:hi], km, strict=True):
            if d <= MATCH_KM:
                pairs.append((abs(int(ut) - int(bt)), float(d), b, u))
    # Closest in time first, each event matched at most once: in a swarm, two BMKG
    # events can't both claim the same USGS event.
    df = df.assign(bmkg_id=df["event_id"].where(df["source"] == "bmkg"))
    taken_b, taken_u = set(), set()
    for _, _, b, u in sorted(pairs):
        if b not in taken_b and u not in taken_u:
            taken_b.add(b)
            taken_u.add(u)
            df.at[u, "bmkg_id"] = df.at[b, "event_id"]
    return df.drop(index=list(taken_b)), len(taken_b)


def _km(lat: float, lon: float, lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
    """Great-circle distance from one point to many, in km."""
    p1, p2 = np.radians(lat), np.radians(lats)
    dlon = np.radians(lons - lon)
    h = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlon / 2) ** 2
    km: np.ndarray = 2 * EARTH_KM * np.arcsin(np.sqrt(h))
    return km


@cache
def _province_points() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    with (files("quake_lake") / "provinces.csv").open(encoding="utf-8") as f:
        table = list(csv.DictReader(f))
    codes = np.array([r["province_code"] for r in table])
    names = np.array([r["province"] for r in table])
    return (
        codes,
        names,
        _unit(
            np.array([float(r["latitude"]) for r in table]),
            np.array([float(r["longitude"]) for r in table]),
        ),
    )


def _nearest_province(lats: np.ndarray, lons: np.ndarray) -> tuple[Any, Any, Any]:
    """The province of the nearest of 522 regency reference points, and its distance in km.

    shortcut: nearest reference point, not point-in-polygon, so an event near a
    border can get the neighbour; bundle province polygons if that matters.
    """
    codes, names, points = _province_points()
    best = np.empty(len(lats), dtype=np.int64)
    cos = np.empty(len(lats))
    events = _unit(lats, lons)
    for i in range(0, len(lats), 4096):  # 4096 x 522 dot products per step, about 17 MB
        dots = events[i : i + 4096] @ points.T
        best[i : i + 4096] = dots.argmax(axis=1)
        cos[i : i + 4096] = dots.max(axis=1)
    km = EARTH_KM * np.arccos(np.clip(cos, -1.0, 1.0))
    return codes[best], names[best], km.round(1)


def _unit(lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
    lat, lon = np.radians(lats), np.radians(lons)
    return np.column_stack([np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat)])


def _parquet(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()
