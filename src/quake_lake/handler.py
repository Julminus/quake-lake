"""AWS Lambda entry point: ``quake_lake.handler.lambda_handler``.

Environment: ``BRONZE_URI`` (e.g. ``s3://quake-lake-bronze``). Event, all keys optional:
``{"sources": ["usgs", "bmkg"], "days": 7}``. USGS is fetched for the last ``days``
days, so a daily schedule overlaps the previous runs and picks up revised events;
BMKG's feeds only hold the latest events, so it suits an hourly schedule of its own.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any

from quake_lake.ingest import BMKG_FEEDS, Written, ingest_bmkg, ingest_usgs
from quake_lake.storage import open_storage

DEFAULT_DAYS = 7
MAX_DAYS = 36_500  # a century: anything longer is a typo, and a backfill is the CLI's job
SOURCES = ("usgs", "bmkg")

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)  # Lambda's root logger is at WARNING; keep the per-object lines


def lambda_handler(event: dict[str, Any] | None, context: object = None) -> dict[str, Any]:
    event = event or {}
    now = datetime.now(UTC).replace(microsecond=0)
    days = event.get("days", DEFAULT_DAYS)
    if not isinstance(days, int) or isinstance(days, bool) or not 1 <= days <= MAX_DAYS:
        raise ValueError(f"'days' must be a whole number from 1 to {MAX_DAYS}, got {days!r}")
    sources = event.get("sources", list(SOURCES))
    if (
        not isinstance(sources, list)
        or not sources
        or any(s not in SOURCES for s in sources)
        or len(set(sources)) < len(sources)
    ):
        raise ValueError(f"'sources' must list some of {list(SOURCES)} once each, got {sources!r}")

    storage = open_storage(os.environ["BRONZE_URI"])
    # BMKG first and one feed at a time: its feeds only hold the latest events, so
    # neither a broken feed nor a slow USGS may cost the others their snapshot.
    # USGS can be fetched again at any time.
    steps: list[tuple[str, Callable[[], list[Written]]]] = []
    if "bmkg" in sources:
        steps += [
            (f"bmkg {f}", partial(ingest_bmkg, storage, now=now, feeds=[f])) for f in BMKG_FEEDS
        ]
    if "usgs" in sources:
        window = (now - timedelta(days=days), now)
        steps.append(("usgs", partial(ingest_usgs, storage, [window], now=now)))

    written: list[Written] = []
    errors: dict[str, str] = {}
    for name, step in steps:
        try:
            for w in step():
                log.info("wrote %s (%d events, %d bytes)", w.key, w.events, w.size)
                written.append(w)
        except Exception as e:
            log.exception("%s failed", name)
            errors[name] = f"{type(e).__name__}: {e}"
    if errors:
        # Raising marks the invocation failed, so Lambda's retry and alarms see it.
        raise RuntimeError(f"ingest failed for {', '.join(errors)}: {errors}")
    return {
        "bronze": storage.uri,
        "ingested_at": now.isoformat(),
        "objects": [asdict(w) for w in written],
        "events": sum(w.events for w in written),
    }
