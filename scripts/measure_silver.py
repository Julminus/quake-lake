"""Measure the silver build on a real bronze folder; write docs/results/silver-s8.json.

    uv run python scripts/measure_silver.py --bronze lake/bronze
    uv run --with shapely python scripts/measure_silver.py --polygons idn-adm1.geojson

``--polygons`` also scores the nearest-reference-point province against province
polygons (OCHA's 34-province ADM1, simplified, from geoBoundaries' gbHumanitarian
release): the province that contains the epicentre, or for an event at sea the
nearest one. The polygons predate the 2022 split of Papua, so both sides are
compared at the 34-province level.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import tempfile
import time
from pathlib import Path

import numpy as np
import pyarrow.dataset as ds

from quake_lake import silver
from quake_lake.storage import LocalStorage

# 2022 provinces -> the 2020 province they were cut from.
SPLIT = {"ID-PS": "ID-PA", "ID-PT": "ID-PA", "ID-PE": "ID-PA", "ID-PD": "ID-PB"}
POLYGON_ISO = {  # shapeName in the OCHA polygons -> ISO 3166-2
    "Daerah Istimewa Yogyakarta": "ID-YO",
    "Dki Jakarta": "ID-JK",
}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bronze", default="lake/bronze")
    p.add_argument("--out", default="docs/results/silver-s8.json")
    p.add_argument("--polygons", help="province polygons (GeoJSON) to score the province step")
    args = p.parse_args()
    bronze = LocalStorage(Path(args.bronze))

    with tempfile.TemporaryDirectory() as tmp:
        out = LocalStorage(Path(tmp))
        seconds, digests = [], []
        for _ in range(3):  # the 2nd and 3rd runs rebuild over the 1st one's files
            started = time.perf_counter()
            result = silver.build_silver(bronze, out)
            seconds.append(round(time.perf_counter() - started, 2))
            digests.append(_digest(Path(tmp)))
        df = ds.dataset(Path(tmp) / "events", format="parquet", partitioning="hive")
        df = df.to_table().to_pandas()
        size = sum(f.stat().st_size for f in Path(tmp).rglob("*.parquet"))

    usgs_keys = [k for k in bronze.list("source=usgs/") if k.endswith(".json")]
    report = {
        "bronze": {
            "objects": result.objects,
            "usgs_objects": len(usgs_keys),
            "ingest_dates": sorted({k.split("/")[1] for k in bronze.list() if "/" in k}),
            "bytes": sum(len(bronze.get(k)) for k in bronze.list() if k.endswith(".json")),
            "records": result.records,
            "usgs_ids_fetched_twice": _usgs_repeats(bronze, usgs_keys),
            "bmkg_vs_recorded_fixtures": _bmkg_revisions(bronze),
        },
        "silver": {
            "events": result.events,
            "by_source": df["source"].value_counts().to_dict(),
            "quarantined": result.quarantined,
            "repeats_dropped": result.repeats,
            "bmkg_merged_into_usgs": result.merged,
            "partitions": result.partitions,
            "parquet_bytes": size,
            "event_types": df["event_type"].value_counts().to_dict(),
        },
        "seconds_per_build": seconds,
        "byte_identical_rebuilds": len(set(digests)) == 1,
        "matches": _matches(bronze, df),
        "distinct_usgs_pairs_within_tolerance": _close_pairs(df[df["source"] == "usgs"]),
        "province": _provinces(df),
    }
    if args.polygons:
        report["province_vs_polygons"] = _against_polygons(df, Path(args.polygons))
    Path(args.out).write_text(json.dumps(report, indent=2) + "\n", newline="\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


def _usgs_repeats(bronze: LocalStorage, keys: list[str]) -> dict[str, int]:
    """USGS ids found in more than one object, how many USGS had revised, and renames."""
    versions: dict[str, list[int]] = {}
    aliases: dict[str, set[str]] = {}
    for key in keys:
        for f in json.loads(bronze.get(key))["features"]:
            versions.setdefault(f["id"], []).append(f["properties"]["updated"])
            for alias in f["properties"]["ids"].split(","):
                if alias:
                    aliases.setdefault(alias, set()).add(f["id"])
    twice = [v for v in versions.values() if len(v) > 1]
    return {
        "ids": len(twice),
        "revised": sum(len(set(v)) > 1 for v in twice),
        "ids_listed_by_another_event": sum(len(events) > 1 for events in aliases.values()),
    }


def _bmkg_revisions(bronze: LocalStorage) -> dict[str, int]:
    """BMKG events in both the recorded fixtures (2026-11-20) and bronze: did any change?

    BMKG has no ids, so silver names an event by its origin time; a revised time
    would make it a second event.
    """
    fixtures = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
    old, new = {}, {}
    for target, keys in ((old, fixtures.glob("bmkg-*.json")), (new, bronze.list("source=bmkg/"))):
        for k in keys:
            body = k.read_bytes() if isinstance(k, Path) else bronze.get(k)
            for item in silver._open(f"x_{Path(str(k)).name[:-5]}_20000101T000000Z.json", body)[2]:
                target[item["DateTime"]] = (
                    item["Coordinates"],
                    item["Magnitude"],
                    item["Kedalaman"],
                )
    shared = set(old) & set(new)
    near = {
        t
        for t in set(old) - set(new)
        for n in new
        if abs(silver.datetime.fromisoformat(t) - silver.datetime.fromisoformat(n)).total_seconds()
        < 60
    }
    return {
        "shared": len(shared),
        "changed": sum(old[t] != new[t] for t in shared),
        "missing_with_a_time_within_60_s": len(near),
    }


def _digest(root: Path) -> str:
    h = hashlib.sha256()
    for f in sorted(root.rglob("*")):
        if f.is_file():
            h.update(f.relative_to(root).as_posix().encode() + f.read_bytes())
    return h.hexdigest()


def _matches(bronze: LocalStorage, df) -> dict:
    """Time and distance between each merged BMKG event and its USGS match."""
    bmkg = {}
    for key in bronze.list("source=bmkg/"):
        _, parse, items = silver._open(key, bronze.get(key))
        for item in items:
            row = parse(item)
            bmkg[row["event_id"]] = row
    pairs = []
    for _, u in df[(df["source"] == "usgs") & df["bmkg_id"].notna()].iterrows():
        b = bmkg[u["bmkg_id"]]
        dt = abs((u["time"] - b["time"]).total_seconds())
        km = float(
            silver._km(
                b["latitude"], b["longitude"], np.array([u["latitude"]]), np.array([u["longitude"]])
            )[0]
        )
        pairs.append(
            {
                "usgs": u["event_id"],
                "bmkg": u["bmkg_id"],
                "seconds": round(dt, 1),
                "km": round(km, 1),
                "usgs_mag": u["magnitude"],
                "bmkg_mag": b["magnitude"],
            }
        )
    unmatched = df[df["source"] == "bmkg"]
    usgs_t = df.loc[df["source"] == "usgs", "time"].astype("int64").to_numpy() / 1000

    def within(t: object, seconds: float) -> int:
        return int((np.abs(usgs_t - t.timestamp()) <= seconds).sum())  # type: ignore[attr-defined]

    gaps = [float(np.abs(usgs_t - t.timestamp()).min()) for t in unmatched["time"]]
    return {
        "bmkg_events": len(bmkg),
        "matched": len(pairs),
        "max_seconds": max((p["seconds"] for p in pairs), default=None),
        "max_km": max((p["km"] for p in pairs), default=None),
        "median_km": statistics.median(p["km"] for p in pairs) if pairs else None,
        "bmkg_minus_usgs_magnitude": sorted(round(p["bmkg_mag"] - p["usgs_mag"], 1) for p in pairs),
        "unmatched_bmkg_magnitudes": sorted(unmatched["magnitude"].tolist()),
        # Any distance: how clear-cut each match is in time alone.
        "usgs_events_within_300_s_of_each_matched_bmkg": sorted(
            within(bmkg[p["bmkg"]]["time"], 300) for p in pairs
        ),
        "unmatched_seconds_to_nearest_usgs_min": round(min(gaps), 1) if gaps else None,
        "pairs": pairs,
    }


def _close_pairs(usgs) -> dict:
    """Pairs of distinct USGS events within the match tolerance: how often a merge could err."""
    u = usgs.sort_values("time")
    t = u["time"].astype("int64").to_numpy() / 1000
    lat, lon = u["latitude"].to_numpy(), u["longitude"].to_numpy()
    km = []  # distance of every pair within the time tolerance
    for i in range(len(t)):
        j = i + 1
        while j < len(t) and t[j] - t[i] <= silver.MATCH_SECONDS:
            km.append(float(silver._km(lat[i], lon[i], lat[j : j + 1], lon[j : j + 1])[0]))
            j += 1
    return {
        "seconds": silver.MATCH_SECONDS,
        "km": silver.MATCH_KM,
        "pairs": sum(d <= silver.MATCH_KM for d in km),
        "pairs_by_km": {d: sum(x <= d for x in km) for d in (50, 100, 150)},
        "pairs_any_distance": len(km),
        "events": len(t),
    }


def _provinces(df) -> dict:
    km = df["province_km"]
    # USGS names the nearest town; its country says how much of the box is not Indonesia.
    country = df.loc[df["source"] == "usgs", "place"].str.rsplit(", ", n=1).str[-1]
    foreign = country.isin(
        [
            "Philippines",
            "Malaysia",
            "Timor Leste",
            "Papua New Guinea",
            "Palau",
            "Australia",
            "Brunei",
        ]
    )
    sulut = df[(df["province_code"] == "ID-SA") & (df["source"] == "usgs")]
    return {
        "provinces_used": int(df["province_code"].nunique()),
        "top": df["province"].value_counts().head(10).to_dict(),
        "km_quantiles": {q: round(float(km.quantile(q)), 1) for q in (0.25, 0.5, 0.75, 0.95)},
        "km_max": float(km.max()),
        "usgs_place_country": country.value_counts().head(12).to_dict(),
        "usgs_foreign_place_share": round(float(foreign.mean()), 3),
        "sulawesi_utara_usgs_events": len(sulut),
        "sulawesi_utara_usgs_place_in_philippines": int(
            sulut["place"].str.endswith("Philippines").sum()
        ),
    }


def _against_polygons(df, path: Path) -> dict:
    import shapely  # only for this measurement: uv run --with shapely

    features = json.loads(path.read_text(encoding="utf-8"))["features"]
    names = {r[1]: r[0] for r in _table()}
    iso = [
        POLYGON_ISO.get(f["properties"]["shapeName"]) or names[f["properties"]["shapeName"]]
        for f in features
    ]
    geoms = [shapely.geometry.shape(f["geometry"]) for f in features]
    tree = shapely.STRtree(geoms)
    points = shapely.points(df["longitude"].to_numpy(), df["latitude"].to_numpy())
    inside_pt, inside_poly = tree.query(points, predicate="within")
    truth = np.full(len(df), "", dtype=object)
    truth[inside_pt] = np.array(iso)[inside_poly]
    on_land = truth != ""
    near_pt, near_poly = tree.query_nearest(points[~on_land], all_matches=False)
    truth[np.flatnonzero(~on_land)[near_pt]] = np.array(iso)[near_poly]
    ours = df["province_code"].map(lambda c: SPLIT.get(c, c)).to_numpy()
    ok = ours == truth
    km = df["province_km"].to_numpy()
    bands = {}
    for lo, hi in ((0, 50), (50, 100), (100, 200), (200, 10_000)):
        m = ~on_land & (km >= lo) & (km < hi)
        bands[f"at_sea_{lo}_{hi}_km_from_a_reference_point"] = {
            "events": int(m.sum()),
            "agree": round(float(ok[m].mean()), 3),
        }
    return {
        "polygons": path.name,
        "events": len(df),
        "agree": round(float(ok.mean()), 3),
        "on_land": {"events": int(on_land.sum()), "agree": round(float(ok[on_land].mean()), 3)},
        "at_sea": {"events": int((~on_land).sum()), "agree": round(float(ok[~on_land].mean()), 3)},
        **bands,
        "note": "at sea, the reference is the nearest polygon in degrees (planar), not km",
    }


def _table() -> list[tuple[str, str]]:
    import csv
    from importlib.resources import files

    with (files("quake_lake") / "provinces.csv").open(encoding="utf-8") as f:
        return sorted({(r["province_code"], r["province"]) for r in csv.DictReader(f)})


if __name__ == "__main__":
    main()
