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
from typing import Any

from quake_lake.ingest import Written, ingest_bmkg, ingest_usgs
from quake_lake.storage import open_storage

DEFAULT_DAYS = 7

log = logging.getLogger(__name__)


def lambda_handler(event: dict[str, Any] | None, context: object = None) -> dict[str, Any]:
    event = event or {}
    now = datetime.now(UTC).replace(microsecond=0)
    days = event.get("days", DEFAULT_DAYS)
    if not isinstance(days, int) or isinstance(days, bool) or days < 1:
        raise ValueError(f"'days' must be a whole number of days >= 1, got {days!r}")
    storage = open_storage(os.environ["BRONZE_URI"])
    jobs: dict[str, Callable[[], list[Written]]] = {
        "usgs": lambda: ingest_usgs(storage, [(now - timedelta(days=days), now)], now=now),
        "bmkg": lambda: ingest_bmkg(storage, now=now),
    }
    names = list(jobs)
    sources = event.get("sources", names)
    if not isinstance(sources, list) or not sources or any(s not in names for s in sources):
        raise ValueError(f"'sources' must be a non-empty list from {names}, got {sources!r}")

    written: list[Written] = []
    errors: dict[str, str] = {}
    for source in sources:
        # One source failing must not cost the other its snapshot: BMKG's feeds
        # move on, so a missed poll is data lost for good.
        try:
            written += jobs[source]()
        except Exception as e:
            log.exception("%s ingest failed", source)
            errors[source] = f"{type(e).__name__}: {e}"
    for w in written:
        log.info("wrote %s (%d events, %d bytes)", w.key, w.events, w.size)
    if errors:
        # Raising marks the invocation failed, so Lambda's retry and alarms see it.
        raise RuntimeError(f"ingest failed for {', '.join(errors)}: {errors}")
    return {
        "bronze": storage.uri,
        "ingested_at": now.isoformat(),
        "objects": [asdict(w) for w in written],
        "events": sum(w.events for w in written),
    }
