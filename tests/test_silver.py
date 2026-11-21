from __future__ import annotations

import csv
import io
import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from importlib.resources import files

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from conftest import PALU_EVENTS, fixture
from quake_lake import silver
from quake_lake.cli import main
from quake_lake.silver import EVENTS, build_silver
from quake_lake.storage import LocalStorage, open_storage

T0 = datetime(2026, 11, 10, tzinfo=UTC)
USGS_KEY = "source=usgs/ingest_date=2026-11-20/20261101T000000Z_20261120T000000Z.json"
BMKG_KEY = "source=bmkg/ingest_date=2026-11-20/gempaterkini_20261120T180000Z.json"


def feature(id="us1", t=T0, lat=-7.0, lon=110.0, depth=10.0, mag=5.0, updated=None, **props):
    ms = int(t.timestamp() * 1000)
    return {
        "type": "Feature",
        "id": id,
        "properties": {
            "mag": mag,
            "magType": "mb",
            "place": "somewhere",
            "time": ms,
            "updated": ms if updated is None else int(updated.timestamp() * 1000),
            "type": "earthquake",
            **props,
        },
        "geometry": {"type": "Point", "coordinates": [lon, lat, depth]},
    }


def usgs(*features) -> bytes:
    return json.dumps({"type": "FeatureCollection", "features": list(features)}).encode()


def gempa(t=T0, lat=-7.0, lon=110.0, mag="5.0", depth="10 km"):
    return {
        "DateTime": t.isoformat(),
        "Coordinates": f"{lat},{lon}",
        "Magnitude": mag,
        "Kedalaman": depth,
        "Wilayah": "somewhere",
    }


def bmkg(*records) -> bytes:
    return json.dumps({"Infogempa": {"gempa": list(records)}}).encode()


def build(tmp_path, objects):
    bronze = LocalStorage(tmp_path / "bronze")
    for key, body in objects.items():
        bronze.put(key, body)
    out = LocalStorage(tmp_path / "silver")
    return build_silver(bronze, out), out


def events(out) -> pd.DataFrame:
    if not (out.root / "events").exists():
        return pd.DataFrame(columns=EVENTS.names)
    table = ds.dataset(out.root / "events", format="parquet", partitioning="hive").to_table()
    return table.to_pandas().sort_values(["time", "event_id"], ignore_index=True)


def quarantine(out) -> list[dict]:
    return [json.loads(line) for line in out.get("quarantine/events.jsonl").splitlines()]


def files_of(out) -> dict[str, bytes]:
    return {k: out.get(k) for k in out.list()}


def test_recorded_feeds_become_typed_events(tmp_path):
    objects = {
        "source=usgs/ingest_date=2026-11-20/20180928T000000Z_20180929T000000Z.json": fixture(
            "usgs-palu-2018-09-28.json"
        ),
        **{
            f"source=bmkg/ingest_date=2026-11-20/{feed}_20261120T181200Z.json": fixture(
                f"bmkg-{feed}.json"
            )
            for feed in ("autogempa", "gempaterkini", "gempadirasakan")
        },
    }
    result, out = build(tmp_path, objects)

    df = events(out)
    bmkg_times = {
        r["DateTime"]
        for feed in ("autogempa", "gempaterkini", "gempadirasakan")
        for r in silver._open(f"x_{feed}_20261120T181200Z.json", fixture(f"bmkg-{feed}.json"))[2]
    }
    assert result.records == PALU_EVENTS + 31
    assert result.events == PALU_EVENTS + len(bmkg_times) == len(df)
    assert result.repeats == 31 - len(bmkg_times)  # the latest event is in two feeds
    assert result.quarantined == 0
    assert quarantine(out) == []
    first = pq.read_schema(out.root / "events/year=2018/month=09/part-0.parquet")
    assert first.remove_metadata() == EVENTS
    palu = df.set_index("event_id").loc["us1000h3p4"]  # the M7.5 Palu earthquake
    assert (palu["magnitude"], palu["province"], palu["province_code"]) == (
        7.5,
        "Sulawesi Tengah",
        "ID-ST",
    )
    (raw,) = [
        f
        for f in json.loads(fixture("usgs-palu-2018-09-28.json"))["features"]
        if f["id"] == "us1000h3p4"
    ]
    assert palu["time"] == pd.Timestamp(raw["properties"]["time"], unit="ms", tz="UTC")
    assert palu["time"].microsecond > 0  # the milliseconds are kept
    latest = df.set_index("event_id").loc["bmkg-20261120T175026Z"]
    assert (latest["source"], latest["depth_km"], latest["magnitude"]) == ("bmkg", 17.0, 4.6)
    assert latest["bmkg_id"] == "bmkg-20261120T175026Z"


def test_a_rebuild_writes_the_same_bytes(tmp_path):
    objects = {
        USGS_KEY: usgs(
            feature("us1"), feature("us2", t=T0 - timedelta(days=40)), feature("x", lat=9.0)
        ),
        BMKG_KEY: bmkg(gempa(t=T0 + timedelta(seconds=3)), gempa(lat=-3.0), gempa(mag="abc")),
    }
    result, out = build(tmp_path, objects)
    assert (result.merged, result.quarantined, result.events) == (1, 2, 3)
    first = files_of(out)
    build_silver(LocalStorage(tmp_path / "bronze"), out)
    assert files_of(out) == first
    assert sorted(first) == [
        "events/year=2026/month=10/part-0.parquet",
        "events/year=2026/month=11/part-0.parquet",
        "quarantine/events.jsonl",
    ]


def test_the_newest_usgs_version_wins_wherever_it_is_stored(tmp_path):
    later = T0 + timedelta(days=2)
    objects = {
        # The revision sorts first by key but carries the later `updated`.
        "source=usgs/ingest_date=2026-11-19/20261101T000000Z_20261119T000000Z.json": usgs(
            feature("us1", mag=5.1, updated=later)
        ),
        USGS_KEY: usgs(feature("us1", mag=5.0)),
    }
    result, out = build(tmp_path, objects)
    assert result.repeats == 1
    assert events(out)["magnitude"].tolist() == [5.1]


def test_with_the_same_updated_the_later_fetch_wins(tmp_path):
    objects = {
        "source=usgs/ingest_date=2026-11-20/20261101T000000Z_20261120T000000Z.json": usgs(
            feature("us1", place="old")
        ),
        "source=usgs/ingest_date=2026-11-21/20261101T000000Z_20261121T000000Z.json": usgs(
            feature("us1", place="new")
        ),
    }
    _, out = build(tmp_path, objects)
    assert events(out)["place"].tolist() == ["new"]


def test_a_replaced_usgs_id_is_dropped_for_the_newer_one(tmp_path):
    objects = {
        USGS_KEY: usgs(feature("us7000aaaa", mag=5.0, ids=",us7000aaaa,")),
        "source=usgs/ingest_date=2026-11-21/20261101T000000Z_20261121T000000Z.json": usgs(
            feature("at00bbbb", mag=5.3, ids=",at00bbbb,us7000aaaa,", updated=T0 + timedelta(1))
        ),
    }
    result, out = build(tmp_path, objects)
    assert events(out)[["event_id", "magnitude"]].values.tolist() == [["at00bbbb", 5.3]]
    assert result.repeats == 1


def test_an_older_row_listing_a_newer_id_drops_nothing(tmp_path):
    # The newer row no longer lists the old id: USGS split them, so both are events.
    objects = {
        USGS_KEY: usgs(feature("at00bbbb", ids=",at00bbbb,us7000aaaa,")),
        "source=usgs/ingest_date=2026-11-21/20261101T000000Z_20261121T000000Z.json": usgs(
            feature("us7000aaaa", ids=",us7000aaaa,", updated=T0 + timedelta(1))
        ),
    }
    _, out = build(tmp_path, objects)
    assert sorted(events(out)["event_id"]) == ["at00bbbb", "us7000aaaa"]


def test_a_bmkg_event_listed_by_usgs_is_merged_into_the_usgs_row(tmp_path):
    objects = {
        USGS_KEY: usgs(feature("us1", mag=5.0)),
        # 5 s later and about 36 km north: the largest offsets measured on real pairs.
        BMKG_KEY: bmkg(gempa(t=T0 + timedelta(seconds=5), lat=-6.68, mag="5.4")),
    }
    result, out = build(tmp_path, objects)
    df = events(out)
    assert (result.merged, result.events) == (1, 1)
    assert df[["event_id", "source", "magnitude", "bmkg_id"]].values.tolist() == [
        ["us1", "usgs", 5.0, "bmkg-20261110T000005Z"]
    ]


@pytest.mark.parametrize(
    ("seconds", "lat", "merged"),
    [
        (10, -7.0, 1),  # the time tolerance is inclusive
        (-10, -7.0, 1),
        (11, -7.0, 0),
        (0, -7.85, 1),  # 94.5 km
        (0, -7.95, 0),  # 105.6 km
    ],
)
def test_merge_tolerance(tmp_path, seconds, lat, merged):
    objects = {
        USGS_KEY: usgs(feature("us1")),
        BMKG_KEY: bmkg(gempa(t=T0 + timedelta(seconds=seconds), lat=lat)),
    }
    result, out = build(tmp_path, objects)
    assert result.merged == merged
    assert len(events(out)) == 2 - merged
    assert events(out)["bmkg_id"].notna().sum() == 1  # the link, or the BMKG row itself


def test_merging_is_one_to_one_and_closest_in_time_first(tmp_path):
    objects = {
        USGS_KEY: usgs(feature("us1"), feature("us2", t=T0 + timedelta(seconds=6))),
        # Both are within 10 s of both USGS events.
        BMKG_KEY: bmkg(
            gempa(t=T0 + timedelta(seconds=1)),
            gempa(t=T0 + timedelta(seconds=4)),
            gempa(t=T0 + timedelta(seconds=2)),
        ),
    }
    result, out = build(tmp_path, objects)
    df = events(out).set_index("event_id")
    assert result.merged == 2
    assert df.loc["us1", "bmkg_id"] == "bmkg-20261110T000001Z"
    assert df.loc["us2", "bmkg_id"] == "bmkg-20261110T000004Z"
    assert df.loc["bmkg-20261110T000002Z", "source"] == "bmkg"


@pytest.mark.parametrize(
    ("objects", "reason"),
    [
        ({USGS_KEY: usgs(feature("bad", lat=8.5))}, "latitude: in_range(-12, 7) (got 8.5)"),
        ({USGS_KEY: usgs(feature("bad", lon=150.0))}, "longitude: in_range(94, 142)"),
        ({USGS_KEY: usgs(feature("bad", depth=None))}, "depth_km: not_nullable"),
        ({USGS_KEY: usgs(feature("bad", depth=900.0))}, "depth_km: in_range(-10, 800)"),
        ({USGS_KEY: usgs(feature("bad", mag=12.0))}, "magnitude: in_range(-2, 10)"),
        ({USGS_KEY: usgs(feature("bad", type=None))}, "event_type: not_nullable"),
        ({USGS_KEY: usgs(feature("", place="no id"))}, "event_id: str_length"),
        (
            {USGS_KEY: usgs(feature("bad", t=datetime(2026, 11, 20, 1, 1, tzinfo=UTC)))},
            "time is not after the fetch",
        ),
        ({USGS_KEY: usgs(feature("bad", mag="5.0"))}, "unreadable record: TypeError"),
        ({USGS_KEY: usgs(feature("bad", place=["a"]))}, "expected a string, got ['a']"),
        ({USGS_KEY: usgs(feature("bad", mag=True))}, "expected a number, got True"),
        ({USGS_KEY: usgs({"id": "bad", "properties": {}})}, "unreadable record: KeyError"),
        ({BMKG_KEY: bmkg(gempa(mag="abc"))}, "unreadable record: ValueError"),
        ({BMKG_KEY: bmkg({**gempa(), "DateTime": "2026-11-10T00:00:00"})}, "has no time zone"),
        ({BMKG_KEY: bmkg({**gempa(), "Coordinates": "-7.0"})}, "unreadable record: ValueError"),
        ({USGS_KEY: usgs(feature("bad", time=float("inf")))}, "expected a number, got Infinity"),
        ({USGS_KEY: usgs(feature("bad", mag=float("nan")))}, "expected a number, got NaN"),
        (
            {USGS_KEY: usgs(feature("bad", lat=-7.123)).replace(b"-7.123", b"-1e400")},
            "expected a number, got -1e400",
        ),
        ({USGS_KEY: usgs(feature("bad", time=10**20))}, "unreadable record: OverflowError"),
        ({USGS_KEY: usgs(feature("bad", lon=10**400))}, "unreadable record: OverflowError"),
        ({USGS_KEY: usgs(feature("bad", time=-(10**14)))}, "unreadable record: OverflowError"),
        (
            {USGS_KEY: usgs(feature("bad", t=datetime(1899, 12, 31, tzinfo=UTC)))},
            "time: greater_than_or_equal_to(1900-01-01 00:00:00+00:00)",
        ),
        ({BMKG_KEY: bmkg(gempa(mag="nan"))}, "not a finite number: 'nan'"),
        ({USGS_KEY: usgs(feature("bad", place=float("nan")))}, "expected a string, got NaN"),
        ({USGS_KEY: usgs(feature("bad", magType=float("inf")))}, "expected a string, got Infinity"),
        (
            {USGS_KEY: usgs(feature("x")).replace(b'"id": "x"', b'"id": NaN')},
            "expected a string, got NaN",
        ),
        ({BMKG_KEY: bmkg({**gempa(), "Wilayah": float("nan")})}, "expected a string, got NaN"),
        ({BMKG_KEY: bmkg(gempa(depth="inf km"))}, "not a finite number"),
        (
            {BMKG_KEY: bmkg({**gempa(), "DateTime": "0001-01-01T00:00:00+07:00"})},
            "unreadable record: OverflowError",
        ),
    ],
)
def test_bad_records_are_quarantined_and_the_rest_still_lands(tmp_path, objects, reason):
    ((key, body),) = objects.items()
    doc = json.loads(body, parse_constant=str, parse_float=silver._float_or_text)
    raw = doc["features"][0] if key == USGS_KEY else doc["Infogempa"]["gempa"][0]
    good = "source=usgs/ingest_date=2026-11-21/a_20261121T000000Z.json"
    result, out = build(tmp_path, {key: body, good: usgs(feature("good", lat=-3.0))})

    (line,) = out.get("quarantine/events.jsonl").splitlines()
    bad = json.loads(line, parse_constant=pytest.fail)  # strict JSON: no NaN or Infinity
    assert result.quarantined == 1
    assert any(reason in e for e in bad["errors"]), bad["errors"]
    assert (bad["bronze_key"], bad["record"]) == (key, raw)
    assert events(out)["event_id"].tolist() == ["good"]


def test_a_bad_revision_does_not_replace_the_good_version(tmp_path):
    objects = {
        USGS_KEY: usgs(feature("us1", mag=5.0)),
        "source=usgs/ingest_date=2026-11-21/20261101T000000Z_20261121T000000Z.json": usgs(
            feature("us1", mag=99.0, updated=T0 + timedelta(days=1))
        ),
    }
    result, out = build(tmp_path, objects)
    assert events(out)["magnitude"].tolist() == [5.0]
    assert result.quarantined == 1
    assert result.repeats == 0


@pytest.mark.parametrize(
    ("key", "body", "reason"),
    [
        (USGS_KEY, b"<html>oops</html>", "unreadable object: Expecting value"),
        (USGS_KEY, b'{"type": "FeatureCollection"}', "unreadable object: 'features'"),
        (USGS_KEY, b'{"features": {}}', "features is not a list"),
        (BMKG_KEY, b'{"Infogempa": "x"}', "unreadable object"),
        (BMKG_KEY, b'{"Infogempa": {"gempa": "Service Unavailable"}}', "not an object or a list"),
        (BMKG_KEY, b'{"Infogempa": {"gempa": 5}}', "not an object or a list"),
        (USGS_KEY, b'{"features": ' + b"[" * 100_000, "unreadable object: maximum recursion"),
        ("source=usgs/ingest_date=2026-11-20/by-hand.json", usgs(feature()), "no fetch time"),
    ],
)
def test_an_unreadable_object_is_quarantined_whole(tmp_path, key, body, reason):
    other = "source=usgs/ingest_date=2026-11-21/a_20261121T000000Z.json"
    result, out = build(tmp_path, {key: body, other: usgs(feature("ok"))})
    (bad,) = quarantine(out)
    assert reason in bad["errors"][0]
    assert (bad["bronze_key"], bad["record"]) == (key, None)
    assert events(out)["event_id"].tolist() == ["ok"]
    assert result.objects == 2


def test_a_partition_left_empty_is_deleted_and_nothing_else(tmp_path):
    first = {USGS_KEY: usgs(feature("us1", t=datetime(2026, 10, 31, 23, 59, tzinfo=UTC)))}
    _, out = build(tmp_path, first)
    out.put("events/README.txt", b"keep me")
    out.put("elsewhere/x.parquet", b"keep me too")
    # USGS revises the origin time across the month boundary.
    revised = usgs(feature("us1", t=datetime(2026, 11, 1, 0, 1, tzinfo=UTC), updated=T0))
    LocalStorage(tmp_path / "bronze").put(
        "source=usgs/ingest_date=2026-11-21/20261101T000000Z_20261121T000000Z.json", revised
    )
    result = build_silver(LocalStorage(tmp_path / "bronze"), out)
    assert result.deleted == 1
    assert out.list() == [
        "elsewhere/x.parquet",
        "events/README.txt",
        "events/year=2026/month=11/part-0.parquet",
        "quarantine/events.jsonl",
    ]


@pytest.mark.parametrize(
    ("objects", "message"),
    [
        ({}, r"no valid record under .* \(objects read: 0, records: 0\); silver is left as it was"),
        (
            {USGS_KEY: b"<html>503 Service Unavailable</html>"},
            r"\(objects read: 1, records: 0; the first problem: unreadable object",
        ),
        (
            {USGS_KEY: usgs(feature(lat=9.0), feature(depth=None, place="x" * 10**6))},
            r"\(objects read: 1, records: 2; the first problem: latitude: in_range\(-12, 7\) \(got 9.0\)\)",
        ),
        (
            {USGS_KEY: usgs(feature(place=["x"] * 10**6))},
            r"the first problem: unreadable record: TypeError: expected a string, got \['x', [^)]{0,80}\)",
        ),
    ],
)
def test_a_bronze_with_no_valid_record_is_refused_and_silver_is_left_alone(
    tmp_path, objects, message
):
    _, out = build(tmp_path / "before", {USGS_KEY: usgs(feature())})
    before = files_of(out)
    bronze = LocalStorage(tmp_path / "bronze")
    for key, body in objects.items():
        bronze.put(key, body)
    with pytest.raises(ValueError, match=message):
        build_silver(bronze, out)
    assert files_of(out) == before


def test_other_event_types_are_kept_and_typed(tmp_path):
    _, out = build(tmp_path, {USGS_KEY: usgs(feature("v1", type="volcanic eruption", mag=None))})
    df = events(out)
    assert df["event_type"].tolist() == ["volcanic eruption"]
    assert df["magnitude"].isna().all()


@pytest.mark.parametrize(
    ("lat", "lon", "code"),
    [
        (-6.1754, 106.8272, "ID-JK"),  # Monas, Jakarta
        (-0.8917, 119.8707, "ID-ST"),  # Palu
        (-2.5337, 140.7181, "ID-PA"),  # Jayapura
        (-8.4932, 140.4018, "ID-PS"),  # Merauke, Papua Selatan since 2022
        (-0.8762, 131.2558, "ID-PD"),  # Kota Sorong, Papua Barat Daya since 2022
        (-8.6500, 115.2167, "ID-BA"),  # Denpasar
        (5.5483, 95.3238, "ID-AC"),  # Banda Aceh
        (-7.7956, 110.3695, "ID-YO"),  # Yogyakarta
    ],
)
def test_nearest_province_of_known_places(lat, lon, code):
    codes, _, km = silver._nearest_province(np.array([lat]), np.array([lon]))
    assert codes[0] == code
    # The distance is to a regency's reference point, not to a border: Merauke town is
    # 82 km from the centre of Merauke regency, which is the size of Belgium.
    assert km[0] < 100


def test_nearest_province_reports_the_distance_at_sea():
    # Deep in the Indian Ocean, well south of Java.
    _, names, km = silver._nearest_province(np.array([-11.5]), np.array([110.0]))
    assert names[0] in {"Jawa Tengah", "DI Yogyakarta", "Jawa Timur"}
    assert 350 < km[0] < 500


def test_the_province_table_has_the_38_provinces():
    with (files("quake_lake") / "provinces.csv").open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 522
    names = {(r["province_code"], r["province"]) for r in rows}
    assert len(names) == len({c for c, _ in names}) == 38
    assert {"ID-PS", "ID-PT", "ID-PE", "ID-PD"} <= {c for c, _ in names}


def test_a_failure_not_tied_to_a_row_is_a_bug(tmp_path, monkeypatch):
    real = silver._frame

    def broken(rows):
        df = real(rows)
        df["latitude"] = df["latitude"].astype("str")
        return df

    monkeypatch.setattr(silver, "_frame", broken)
    with pytest.raises(RuntimeError, match="silver schema failed as a whole"):
        build(tmp_path, {USGS_KEY: usgs(feature())})


def test_cli_silver(tmp_path, capsys):
    bronze = LocalStorage(tmp_path / "bronze")
    bronze.put(USGS_KEY, usgs(feature("us1"), feature("us1"), feature("bad", lat=9.0)))
    bronze.put(BMKG_KEY, bmkg(gempa(t=T0 + timedelta(seconds=2))))
    code = main(["silver", "--bronze", str(bronze.root), "--silver", str(tmp_path / "silver")])
    out = capsys.readouterr().out.splitlines()
    assert code == 0
    assert out[:4] == [
        "read 2 bronze objects, 4 records",
        "quarantined 1 record (quarantine/events.jsonl)",
        "dropped 1 repeat of an event id",
        "merged 1 BMKG event into the USGS event it matches",
    ]
    assert out[4].startswith(f"wrote 1 event in 1 monthly partition to {tmp_path / 'silver'} in ")


def test_cli_silver_with_no_bronze_is_an_error(tmp_path, capsys):
    assert main(["silver", "--bronze", str(tmp_path / "typo"), "--silver", str(tmp_path)]) == 1
    assert "no valid record under" in capsys.readouterr().err


def test_cli_silver_uses_the_environment_and_reports_errors(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("BRONZE_URI", str(tmp_path / "bronze"))
    monkeypatch.setenv("SILVER_URI", " ")
    assert main(["silver"]) == 1
    assert "empty storage URI" in capsys.readouterr().err


@pytest.mark.minio
@pytest.mark.skipif(
    not os.environ.get("QUAKE_LAKE_MINIO"), reason="set QUAKE_LAKE_MINIO=1 with MinIO up"
)
def test_minio_silver_round_trip():
    """Bronze and silver on a real MinIO (``make minio``): reads, writes and the stale delete."""
    root = f"s3://lake/test-{uuid.uuid4().hex}"
    bronze, out = open_storage(f"{root}/bronze"), open_storage(f"{root}/silver")
    bronze.put(USGS_KEY, usgs(feature("us1", t=datetime(2026, 10, 31, 23, 59, tzinfo=UTC))))
    assert build_silver(bronze, out).partitions == 1
    revised = usgs(feature("us1", t=datetime(2026, 11, 1, 0, 1, tzinfo=UTC), updated=T0))
    bronze.put("source=usgs/ingest_date=2026-11-21/a_20261121T000000Z.json", revised)
    assert build_silver(bronze, out).deleted == 1
    assert out.list() == ["events/year=2026/month=11/part-0.parquet", "quarantine/events.jsonl"]
    table = pq.read_table(io.BytesIO(out.get("events/year=2026/month=11/part-0.parquet")))
    assert table.column("event_id").to_pylist() == ["us1"]


def test_an_id_twice_in_one_object_keeps_the_later_copy(tmp_path):
    _, out = build(tmp_path, {USGS_KEY: usgs(feature("us1", place="a"), feature("us1", place="b"))})
    assert events(out)["place"].tolist() == ["b"]


def test_a_failed_write_deletes_no_stale_partition(tmp_path, monkeypatch):
    _, out = build(tmp_path, {USGS_KEY: usgs(feature("us1", t=datetime(2026, 10, 31, tzinfo=UTC)))})
    revised = usgs(feature("us1", t=datetime(2026, 11, 1, 1, tzinfo=UTC), updated=T0))
    LocalStorage(tmp_path / "bronze").put(
        "source=usgs/ingest_date=2026-11-21/20261101T000000Z_20261121T000000Z.json", revised
    )
    real = LocalStorage.put

    def put(self, key, data):
        if "month=11" in key:
            raise OSError("disk full")
        real(self, key, data)

    monkeypatch.setattr(LocalStorage, "put", put)
    with pytest.raises(OSError, match="disk full"):
        build_silver(LocalStorage(tmp_path / "bronze"), out)
    assert "events/year=2026/month=10/part-0.parquet" in out.list()


def test_a_replaced_id_is_judged_by_updated_not_by_fetch_order(tmp_path):
    objects = {
        # Fetched first, but USGS revised it later: this is the newer version.
        USGS_KEY: usgs(feature("at00bbbb", ids=",at00bbbb,us7000aaaa,", updated=T0 + timedelta(1))),
        "source=usgs/ingest_date=2026-11-21/20261101T000000Z_20261121T000000Z.json": usgs(
            feature("us7000aaaa", ids=",us7000aaaa,")
        ),
    }
    _, out = build(tmp_path, objects)
    assert events(out)["event_id"].tolist() == ["at00bbbb"]
