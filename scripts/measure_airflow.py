"""Measure the compose stack (MinIO + Airflow) end to end; write docs/results/airflow-s10.json.

    uv run python scripts/measure_airflow.py --bronze lake/bronze

Deletes the stack's volumes first (`docker compose down -v`: MinIO's bronze and silver,
Airflow's database and logs, gold), times `docker compose up -d --wait`, and waits for the
catch-up runs from --start. Then copies --bronze into MinIO and triggers a run over the
whole history, stops MinIO during another run to watch the retries, and finally rebuilds
silver and gold locally from a copy of MinIO's bronze to compare every mart with the gold
Airflow built. Network: the DAG's BMKG and USGS fetches. --bronze is only read.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
import threading
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

import duckdb
from dbt.cli.main import dbtRunner

from quake_lake.silver import build_silver
from quake_lake.storage import LocalStorage

ROOT = Path(__file__).parent.parent
API = "http://127.0.0.1:8080/api/v2/dags/quake_lake"
MARTS = ("fct_earthquakes", "dim_province", "agg_daily_activity", "agg_province_magnitude")
DONE = {"success", "failed"}
TEXT = {"encoding": "utf-8", "errors": "replace"}  # docker's output, whatever Windows' code page


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bronze", default="lake/bronze")
    three_days_ago = (datetime.now(UTC) - timedelta(days=3)).date()
    p.add_argument("--start", type=date.fromisoformat, default=three_days_ago)
    p.add_argument("--out", default="docs/results/airflow-s10.json")
    args = p.parse_args()
    bronze, out = Path(args.bronze).resolve(), Path(args.out).resolve()
    os.chdir(ROOT)
    os.environ["QUAKE_LAKE_START_DATE"] = args.start.isoformat()

    compose("down", "-v")
    compose("build")
    started = time.perf_counter()
    compose("up", "-d", "--wait")
    up_seconds = time.perf_counter() - started
    expected = (datetime.now(UTC).date() - args.start).days + 1  # a run at each 00:00 UTC
    catch_up = wait_for(lambda runs: len(runs) >= expected and all_done(runs))
    last_done = max(r["end_date"] for r in catch_up)
    idle = docker_stats()

    mc("mirror", "--quiet", "/host", "local/lake/bronze", mount=bronze, read_only=True)
    with Sampler() as peak:
        history = run_once()

    compose("stop", "minio")
    retried = trigger()
    wait_for(lambda _: any(t["state"] == "up_for_retry" for t in tasks(retried)))
    compose("start", "minio")
    retry = wait_run(retried)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        here = Path(tmp)
        mc("mirror", "--quiet", "local/lake/bronze", "/host/bronze", mount=here)
        compose("cp", "airflow-scheduler:/app/lake/gold.duckdb", str(here / "airflow.duckdb"))
        silver = build_silver(LocalStorage(here / "bronze"), LocalStorage(here / "silver"))
        os.environ.update(SILVER_URI=str(here / "silver"), GOLD_DB=str(here / "local.duckdb"))
        dirs = ["--project-dir", "transform", "--profiles-dir", "transform"]
        res = dbtRunner().invoke(["build", *dirs, "--target-path", f"{tmp}/target"])
        if not res.success:
            raise SystemExit(f"local dbt build failed: {res.exception}")
        same = compare(here / "airflow.duckdb", here / "local.duckdb")

    report = {
        "airflow": "3.3.2 (apache/airflow:slim-3.3.2-python3.12), LocalExecutor, Postgres 16",
        "start_date": args.start.isoformat(),
        "images": images(),
        "compose_up_seconds": round(up_seconds, 1),
        "catch_up": {
            "runs": len(catch_up),
            "states": count(r["state"] for r in catch_up),
            "seconds_from_up_to_last_run_done": round(seconds_between(started, last_done), 1),
            "runs_detail": [run_record(r) for r in catch_up],
        },
        "memory_mib_idle": idle,
        "full_history_run": {**run_record(history), "memory_mib_peak": peak.max},
        "retry_run_with_minio_stopped": run_record(retry),
        "local_rebuild_of_the_same_bronze": {
            "bronze_objects": silver.objects,
            "records": silver.records,
            "events": silver.events,
            "marts": same,
        },
    }
    out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps(report, indent=2))


def compose(*args: str) -> str:
    done = subprocess.run(["docker", "compose", *args], capture_output=True, **TEXT)
    if done.returncode != 0:
        raise SystemExit(f"docker compose {' '.join(args)} failed:\n{done.stderr}")
    return done.stdout


def mc(*args: str, mount: Path, read_only: bool = False) -> None:
    volume = f"{mount}:/host" + (":ro" if read_only else "")
    compose("run", "--rm", "-v", volume, "--entrypoint", "mc", "createbucket", *args)


def api(path: str = "", body: dict | None = None) -> dict:
    data = None if body is None else json.dumps(body).encode()
    req = Request(API + path, data=data, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=30) as r:
        return json.load(r)


def runs() -> list[dict]:
    return api("/dagRuns?order_by=run_after&limit=100")["dag_runs"]


def tasks(run_id: str) -> list[dict]:
    return api(f"/dagRuns/{quote(run_id)}/taskInstances")["task_instances"]


def all_done(rs: list[dict]) -> bool:
    return all(r["state"] in DONE for r in rs)


def wait_for(ready, timeout: float = 1800) -> list[dict]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rs = runs()
        if ready(rs):
            return rs
        time.sleep(2)
    raise SystemExit("timed out waiting for the DAG")


def trigger() -> str:
    return api("/dagRuns", {"logical_date": None})["dag_run_id"]


def wait_run(run_id: str) -> dict:
    (run,) = [r for r in wait_for(lambda rs: done(rs, run_id)) if r["dag_run_id"] == run_id]
    return run


def done(rs: list[dict], run_id: str) -> bool:
    return any(r["dag_run_id"] == run_id and r["state"] in DONE for r in rs)


def run_once() -> dict:
    return wait_run(trigger())


def run_record(run: dict) -> dict:
    keep = ("task_id", "state", "try_number", "duration")
    return {
        "run_id": run["dag_run_id"],
        "run_type": run["run_type"],
        "state": run["state"],
        "seconds": round(run["duration"], 1),
        "tasks": {
            t["task_id"]: {k: _round(t[k]) for k in keep[1:]}
            for t in sorted(tasks(run["dag_run_id"]), key=lambda t: t["task_id"])
        },
    }


def _round(v):
    return round(v, 2) if isinstance(v, float) else v


def seconds_between(perf_start: float, iso_end: str) -> float:
    wall_start = time.time() - (time.perf_counter() - perf_start)
    return datetime.fromisoformat(iso_end).timestamp() - wall_start


def count(values) -> dict[str, int]:
    out: dict[str, int] = {}
    for v in values:
        out[v] = out.get(v, 0) + 1
    return out


def docker_stats() -> dict[str, float]:
    lines = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}"],
        capture_output=True,
        check=True,
        **TEXT,
    ).stdout.splitlines()
    return {
        name.removeprefix("quake-lake-").removesuffix("-1"): _mib(usage.split(" / ")[0])
        for name, usage in (line.split("\t") for line in lines)
        if name.startswith("quake-lake-")
    }


def _mib(text: str) -> float:
    for unit, factor in (("GiB", 1024), ("MiB", 1), ("KiB", 1 / 1024)):
        if text.endswith(unit):
            return round(float(text.removesuffix(unit)) * factor, 1)
    raise ValueError(text)


class Sampler:
    """Each container's highest memory use while the block runs, sampled about once a second."""

    def __enter__(self) -> Sampler:
        self.max: dict[str, float] = {}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample)
        self._thread.start()
        return self

    def _sample(self) -> None:
        while not self._stop.is_set():
            for name, mib in docker_stats().items():
                self.max[name] = max(mib, self.max.get(name, 0))

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join()


def images() -> dict[str, str]:
    names = ("quake-lake-airflow:dev", "apache/airflow:slim-3.3.2-python3.12", "quake-lake:dev")
    sizes = {}
    for name in names:
        out = subprocess.run(
            ["docker", "images", name, "--format", "{{.Size}}"], capture_output=True, **TEXT
        )
        sizes[name] = out.stdout.strip()
    return sizes


def compare(a: Path, local: Path) -> dict[str, dict[str, int]]:
    """Rows per mart in each gold, and rows in one that the other lacks (duplicates count)."""
    # dbt in this process still holds the local file: open it as dbt did (its catalog is
    # then named after the file, `b` below), and attach Airflow's gold to it.
    with duckdb.connect(str(local)) as c:
        c.execute(f"attach '{a}' as a (read_only)")
        b = local.stem

        def n(sql: str) -> int:
            (v,) = c.execute(sql).fetchone() or (0,)
            return int(v)

        return {
            t: {
                "airflow_rows": n(f"select count(*) from a.{t}"),
                "local_rows": n(f"select count(*) from {b}.{t}"),
                "only_in_airflow": n(f"select count(*) from (from a.{t} except all from {b}.{t})"),
                "only_in_local": n(f"select count(*) from (from {b}.{t} except all from a.{t})"),
            }
            for t in MARTS
        }


if __name__ == "__main__":
    main()
