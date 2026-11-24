"""quake_lake: the daily pipeline, BMKG and USGS to gold, on the Airflow from compose.yaml.

Each task but latest_only is one step of `make run` or `make freshness`, run with the
quake-lake and dbt executables from the image's own virtualenv (/app/.venv), so Airflow's
dependencies and the pipeline's never meet. Bronze and silver are on MinIO (BRONZE_URI,
SILVER_URI); gold is /app/lake/gold.duckdb.

- usgs fetches the 7 days before the run's date, so every run, a catch-up or backfill run
  included, covers its own week and picks up USGS's revisions to it.
- bmkg snapshots BMKG's feeds, which only hold the latest events, so only the latest run
  polls them; latest_only skips it in catch-up runs.
- gold is `dbt build`: the models, and dbt's data tests between them, so a failing test
  stops the marts downstream of it from being replaced.
- freshness (`dbt source freshness`, latest run only) fails when silver's newest event is
  7 days old: ingestion has stopped.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.sdk import DAG, DeadlineAlert, DeadlineReference, SyncCallback
from quake_lake_alerts import missed_deadline

QUAKE_LAKE = "/app/.venv/bin/quake-lake"
DBT = "/app/.venv/bin/dbt"
# Catch-up creates one run per day from here; each takes about 10 s.
START = (os.environ.get("QUAKE_LAKE_START_DATE") or "2026-11-20").strip()
try:
    start = datetime.fromisoformat(START)
except ValueError:
    raise ValueError(f"QUAKE_LAKE_START_DATE={START!r} is not a date like 2026-11-20") from None

with DAG(
    dag_id="quake_lake",
    description="BMKG and USGS earthquakes into bronze, silver and gold, daily",
    schedule="@daily",  # 00:00 UTC, 07:00 in Jakarta
    start_date=start.astimezone(UTC) if start.tzinfo else start.replace(tzinfo=UTC),
    catchup=True,
    # Silver is rebuilt from all of bronze and gold is one DuckDB file: one run at a time.
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        # A stalled USGS can hold one request for 11 minutes (see the README).
        "execution_timeout": timedelta(minutes=30),
    },
    # Airflow 3's replacement for SLAs: alert when a run hasn't succeeded an hour after
    # it was queued (a normal run takes about 10 s).
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(hours=1),
        callback=SyncCallback(missed_deadline),
    ),
    doc_md=__doc__,
    tags=["quake-lake"],
):
    latest_only = LatestOnlyOperator(task_id="latest_only")
    bmkg = BashOperator(task_id="bmkg", bash_command=f"{QUAKE_LAKE} bmkg")
    # The run's date: its logical date (00:00 UTC for a scheduled or backfill run, the date
    # asked for in a manual one), else, for a manual run without one, when it was triggered.
    usgs = BashOperator(
        task_id="usgs",
        bash_command=f"{QUAKE_LAKE} usgs"
        "{% set day = dag_run.logical_date or dag_run.run_after %}"
        " --since {{ (day - macros.timedelta(days=7)).isoformat() }}"
        " --until {{ day.isoformat() }}",
    )
    # Runs when bmkg was skipped (a catch-up run), not when it failed.
    silver = BashOperator(
        task_id="silver", bash_command=f"{QUAKE_LAKE} silver", trigger_rule="none_failed"
    )
    gold = BashOperator(task_id="gold", bash_command=f"{DBT} build", cwd="/app")
    freshness = BashOperator(
        task_id="freshness", bash_command=f"{DBT} source freshness", cwd="/app"
    )

    latest_only >> [bmkg, freshness]
    [bmkg, usgs] >> silver >> gold >> freshness
