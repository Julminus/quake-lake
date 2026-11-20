"""Measure a full USGS backfill and describe what landed in bronze (needs the network).

Runs the same per-month ingest the CLI runs, times every request, then reads
the objects back from storage and counts what is in them. The README's
backfill numbers come from the JSON this writes.

    uv run python scripts/measure_backfill.py \\
        --bronze lake/bronze --out docs/results/backfill-s7.json
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quake_lake import __version__
from quake_lake.ingest import ingest_usgs, month_windows
from quake_lake.storage import Storage, open_storage


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bronze", required=True, help="an empty bronze root (local path or s3://)")
    p.add_argument("--since", default="2015-01-01")
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()

    storage = TimedStorage(open_storage(args.bronze))
    if storage.list("source=usgs/"):
        raise SystemExit(f"{args.bronze} already holds USGS objects; use an empty root")
    now = datetime.now(UTC).replace(microsecond=0)
    since = datetime.fromisoformat(args.since).replace(tzinfo=UTC)
    windows = month_windows(since, now)

    seconds: list[float] = []
    started = time.perf_counter()
    for window in windows:
        t = time.perf_counter()
        ingest_usgs(storage, [window], now=now)
        seconds.append(time.perf_counter() - t)
    wall = time.perf_counter() - started

    record = {
        "measured_at": now.isoformat(),
        "quake_lake": __version__,
        "machine": f"{platform.system()} {platform.release()}, Python {platform.python_version()}",
        "bronze": storage.uri,
        "since": since.isoformat(),
        "until": now.isoformat(),
        "requests": len(windows),
        "wall_seconds": round(wall, 1),
        "request_seconds": {
            "median": round(statistics.median(seconds), 3),
            "p95": round(statistics.quantiles(seconds, n=20)[-1], 3),
            "max": round(max(seconds), 3),
        },
        "storage_put_seconds_total": round(storage.put_seconds, 3),
        **describe(storage),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps({k: v for k, v in record.items() if k != "events_per_month"}, indent=2))


class TimedStorage:
    """Passes everything through and adds up the time spent in ``put``."""

    def __init__(self, inner: Storage) -> None:
        self.inner, self.uri, self.put_seconds = inner, inner.uri, 0.0

    def put(self, key: str, data: bytes) -> None:
        t = time.perf_counter()
        self.inner.put(key, data)
        self.put_seconds += time.perf_counter() - t

    def get(self, key: str) -> bytes:
        return self.inner.get(key)

    def list(self, prefix: str = "") -> list[str]:
        return self.inner.list(prefix)


def describe(storage: Storage) -> dict[str, Any]:
    """Counts over every USGS object in bronze."""
    ids: Counter[str] = Counter()
    per_month: dict[str, int] = {}
    kinds: Counter[str] = Counter()
    statuses: Counter[str] = Counter()
    mag_types: Counter[str] = Counter()
    mags: list[float] = []
    size = 0
    keys = storage.list("source=usgs/")
    for key in keys:
        body = storage.get(key)
        size += len(body)
        features = json.loads(body)["features"]
        per_month[key.rsplit("/", 1)[1][:6]] = len(features)
        for f in features:
            props = f["properties"]
            ids[f["id"]] += 1
            kinds[props["type"]] += 1
            statuses[props["status"]] += 1
            mag_types[props["magType"]] += 1
            if props["mag"] is not None:
                mags.append(props["mag"])
    busiest = max(per_month, key=per_month.__getitem__)
    return {
        "objects": len(keys),
        "bytes": size,
        "events": sum(ids.values()),
        "unique_event_ids": len(ids),
        "ids_in_more_than_one_window": sum(1 for c in ids.values() if c > 1),
        "busiest_month": {"month": busiest, "events": per_month[busiest]},
        "magnitude": {
            "min": min(mags),
            "median": statistics.median(mags),
            "max": max(mags),
            "missing": sum(ids.values()) - len(mags),
            "at_least": {str(m): sum(1 for x in mags if x >= m) for m in (4.0, 4.5, 5.0, 6.0, 7.0)},
        },
        "event_types": dict(kinds.most_common()),
        "review_status": dict(statuses.most_common()),
        "magnitude_types": dict(mag_types.most_common()),
        "events_per_month": per_month,
    }


if __name__ == "__main__":
    main()
