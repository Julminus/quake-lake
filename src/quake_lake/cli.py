"""quake-lake: fetch Indonesian earthquake data into bronze, and build silver from it."""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from functools import partial

from botocore.exceptions import BotoCoreError, ClientError

from quake_lake.fetch import FetchError
from quake_lake.handler import DEFAULT_DAYS, MAX_DAYS
from quake_lake.ingest import (
    BMKG_FEEDS,
    IngestError,
    Written,
    ingest_bmkg,
    ingest_usgs,
    month_windows,
)
from quake_lake.storage import Storage, open_storage

DEFAULT_BRONZE = "lake/bronze"
DEFAULT_SILVER = "lake/silver"


ERRORS = (FetchError, IngestError, ValueError, OSError, BotoCoreError, ClientError)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if getattr(args, "until", None) is not None and args.since is None:
        parser.error("--until needs --since")
    now = datetime.now(UTC).replace(microsecond=0)
    started = time.perf_counter()
    if args.command == "silver":
        return _silver(args, started)
    try:
        storage = open_storage(args.bronze)
        steps = _steps(args, storage, now)
    except ERRORS as e:  # e.g. a bad URI, or boto3 finding no such AWS profile
        print(f"error: {e}", file=sys.stderr)
        return 1

    objects = events = size = 0
    failed = False
    # One request at a time, printed as it lands, so a failure part-way still
    # shows what reached bronze.
    for step in steps:
        try:
            for w in step():
                print(f"{w.key}  {_count(w.events, 'event')}  {w.size:,} bytes", flush=True)
                objects, events, size = objects + 1, events + w.events, size + w.size
        except ERRORS as e:
            print(f"error: {e}", file=sys.stderr, flush=True)
            failed = True
            # A backfill stops (re-running it is cheap); BMKG goes on to the next
            # feed, because a feed that isn't snapshotted now may be gone later.
            if args.command == "usgs":
                break
    seconds = time.perf_counter() - started
    print(f"wrote {_count(objects, 'object')} to {storage.uri}: ", end="")
    print(f"{_count(events, 'event')}, {size:,} bytes in {seconds:.1f} s")
    return 1 if failed else 0


def _steps(
    args: argparse.Namespace, storage: Storage, now: datetime
) -> list[Callable[[], list[Written]]]:
    if args.command == "bmkg":
        return [partial(ingest_bmkg, storage, now=now, feeds=[f]) for f in BMKG_FEEDS]
    if args.since is None:
        windows = [(now - timedelta(days=args.days), now)]
    else:
        if args.since > now:
            raise ValueError(f"--since ({args.since.isoformat()}) is in the future")
        # A window that ends in the future would be named as if it were complete.
        until = min(args.until or now, now)
        windows = month_windows(args.since, until)
        if not windows:
            raise ValueError(
                f"--since ({args.since.isoformat()}) must be before --until ({until.isoformat()})"
            )
    return [partial(ingest_usgs, storage, [w], now=now) for w in windows]


def _silver(args: argparse.Namespace, started: float) -> int:
    from quake_lake.silver import build_silver  # pandas and pyarrow: only this command needs them

    try:
        silver = open_storage(args.silver)
        r = build_silver(open_storage(args.bronze), silver)
    except ERRORS as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    seconds = time.perf_counter() - started
    stale = f", deleted {_count(r.deleted, 'stale partition')}" if r.deleted else ""
    print(f"read {_count(r.objects, 'bronze object')}, {_count(r.records, 'record')}")
    print(f"quarantined {_count(r.quarantined, 'record')} (quarantine/events.jsonl)")
    print(f"dropped {_count(r.repeats, 'repeat')} of an event id")
    print(f"merged {_count(r.merged, 'BMKG event')} into the USGS event it matches")
    print(
        f"wrote {_count(r.events, 'event')} in {_count(r.partitions, 'monthly partition')} "
        f"to {silver.uri}{stale} in {seconds:.1f} s"
    )
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

    silver = sub.add_parser(
        "silver",
        parents=[bronze],
        help="rebuild the silver layer (typed Parquet) from all of bronze",
        description="Reads every bronze object, validates and deduplicates the events, and "
        "rewrites events/year=/month=/part-0.parquet and quarantine/events.jsonl.",
    )
    silver.add_argument(
        "--silver",
        default=os.environ.get("SILVER_URI", DEFAULT_SILVER),
        help="silver root (default: $SILVER_URI, else %(default)s)",
    )
    return parser


def _days(text: str) -> int:
    if not text.isdigit() or not 1 <= int(text) <= MAX_DAYS:
        raise argparse.ArgumentTypeError(
            f"expected a whole number of days from 1 to {MAX_DAYS}, got {text!r}"
        )
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
