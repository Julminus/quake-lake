"""quake-lake: fetch Indonesian earthquake data into the bronze layer."""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from functools import partial

from botocore.exceptions import BotoCoreError, ClientError

from quake_lake.fetch import FetchError
from quake_lake.handler import DEFAULT_DAYS
from quake_lake.ingest import BMKG_FEEDS, IngestError, ingest_bmkg, ingest_usgs, month_windows
from quake_lake.storage import open_storage

DEFAULT_BRONZE = "lake/bronze"


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if getattr(args, "until", None) is not None and args.since is None:
        parser.error("--until needs --since")
    now = datetime.now(UTC).replace(microsecond=0)
    started = time.perf_counter()
    objects = events = size = 0
    try:
        storage = open_storage(args.bronze)
        if args.command == "usgs":
            if args.since is None:
                windows = [(now - timedelta(days=args.days), now)]
            else:
                windows = month_windows(args.since, args.until or now)
                if not windows:
                    raise ValueError("--since must be before --until")
            steps = [partial(ingest_usgs, storage, [w], now=now) for w in windows]
        else:
            steps = [partial(ingest_bmkg, storage, now=now, feeds=[f]) for f in BMKG_FEEDS]
        # One request at a time, printed as it lands, so a failure part-way
        # still shows what reached bronze.
        for step in steps:
            for w in step():
                print(f"{w.key}  {_count(w.events, 'event')}  {w.size:,} bytes", flush=True)
                objects, events, size = objects + 1, events + w.events, size + w.size
    except (FetchError, IngestError, ValueError, OSError, BotoCoreError, ClientError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    seconds = time.perf_counter() - started
    print(f"wrote {_count(objects, 'object')} to {storage.uri}: ", end="")
    print(f"{_count(events, 'event')}, {size:,} bytes in {seconds:.1f} s")
    return 0


def _count(n: int, noun: str) -> str:
    return f"{n:,} {noun}" if n == 1 else f"{n:,} {noun}s"


def _parser() -> argparse.ArgumentParser:
    bronze = argparse.ArgumentParser(add_help=False)
    bronze.add_argument(
        "--bronze",
        default=os.environ.get("BRONZE_URI", DEFAULT_BRONZE),
        help="bronze root: a local folder or s3://bucket/prefix "
        "(default: $BRONZE_URI, else %(default)s)",
    )
    parser = argparse.ArgumentParser(prog="quake-lake", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    usgs = sub.add_parser(
        "usgs",
        parents=[bronze],
        help="fetch USGS events in the Indonesia box",
        description="Without --since: one query for the last --days days. "
        "With --since: a backfill, one query per calendar month.",
    )
    when = usgs.add_mutually_exclusive_group()
    when.add_argument(
        "--days", type=_days, default=DEFAULT_DAYS, help="look back this many days (default: 7)"
    )
    when.add_argument("--since", type=_utc_time, help="backfill start, e.g. 2015-01-01 (UTC)")
    usgs.add_argument("--until", type=_utc_time, help="backfill end (default: now)")

    sub.add_parser("bmkg", parents=[bronze], help="snapshot BMKG's three latest-quake feeds")
    return parser


def _days(text: str) -> int:
    if not text.isdigit() or int(text) < 1:
        raise argparse.ArgumentTypeError(f"expected a whole number of days >= 1, got {text!r}")
    return int(text)


def _utc_time(text: str) -> datetime:
    """An ISO date or time; one without a timezone is read as UTC."""
    try:
        t = datetime.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"expected an ISO date like 2015-01-01, got {text!r}"
        ) from None
    return t.replace(tzinfo=UTC) if t.tzinfo is None else t.astimezone(UTC)
