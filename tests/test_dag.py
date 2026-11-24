"""The Airflow DAG (dags/) and its deadline callback (plugins/).

Airflow isn't a dev dependency (it doesn't run on Windows, and its pins would have to
agree with dbt's), so the DAG tests skip without it; CI runs them in the Airflow image
(`docker compose run airflow-cli`) with QUAKE_LAKE_AIRFLOW=1, which makes a missing or
broken Airflow fail them instead. The callback is plain Python and runs everywhere.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "plugins"))  # Airflow puts its plugins folder on the path too

from quake_lake_alerts import missed_deadline  # noqa: E402


def test_the_deadline_callback_logs_the_run_and_the_deadline(caplog):
    # The context Airflow passes: the REST API's fields for the run, and the deadline.
    context = {
        "dag_run": {"dag_run_id": "scheduled__2026-11-24T00:00:00+00:00", "state": "running"},
        "deadline": {"id": "x", "deadline_time": "2026-11-24 01:00:00+00:00"},
    }
    with caplog.at_level(logging.ERROR, logger="quake_lake_alerts"):
        missed_deadline(context)
    assert caplog.messages == [
        "quake_lake run scheduled__2026-11-24T00:00:00+00:00 missed its deadline of "
        "2026-11-24 01:00:00+00:00 (state: running)"
    ]


def test_the_deadline_callback_survives_an_empty_context(caplog):
    with caplog.at_level(logging.ERROR, logger="quake_lake_alerts"):
        missed_deadline({})
    assert caplog.messages == ["quake_lake run None missed its deadline of None (state: None)"]


def load_dags():
    if not os.environ.get("QUAKE_LAKE_AIRFLOW"):
        pytest.importorskip("airflow")
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(str(ROOT / "dags"))


@pytest.fixture(scope="module")
def dag():
    bag = load_dags()
    assert bag.import_errors == {}
    return bag.dags["quake_lake"]


def test_the_tasks_run_in_pipeline_order(dag):
    downstream = {t.task_id: t.downstream_task_ids for t in dag.tasks}
    assert downstream == {
        "latest_only": {"bmkg", "freshness"},
        "bmkg": {"silver"},
        "usgs": {"silver"},
        "silver": {"gold"},
        "gold": {"freshness"},
        "freshness": set(),
    }
    # In a catch-up run latest_only skips bmkg; silver must run anyway, but not after a failure.
    assert dag.get_task("silver").trigger_rule == "none_failed"
    assert {dag.get_task(t).trigger_rule for t in ("bmkg", "gold", "freshness")} == {"all_success"}


def test_daily_with_catch_up_one_run_at_a_time(dag):
    # Airflow 3's default for a cron schedule: no data interval, a run at 00:00 UTC.
    assert type(dag.timetable).__name__ == "CronTriggerTimetable"
    assert dag.timetable.expression == "0 0 * * *"
    assert dag.catchup
    assert dag.max_active_runs == 1


def utc(text: str | None) -> datetime | None:
    return text and datetime.fromisoformat(text).replace(tzinfo=UTC)


@pytest.mark.parametrize(
    ("logical_date", "run_after", "since", "until"),
    [
        # A scheduled (or catch-up, or backfill) run: 00:00 UTC.
        (
            "2026-11-24T00:00:00",
            "2026-11-24T00:00:00",
            "2026-11-17T00:00:00+00:00",
            "2026-11-24T00:00:00+00:00",
        ),
        # A manual run for a past time fetches the week before it, not the week before now.
        (
            "2026-11-21T12:00:00",
            "2026-11-24T19:56:11",
            "2026-11-14T12:00:00+00:00",
            "2026-11-21T12:00:00+00:00",
        ),
        # A manual run without a logical date: the week before it was triggered.
        (
            None,
            "2026-11-23T18:26:47",
            "2026-11-16T18:26:47+00:00",
            "2026-11-23T18:26:47+00:00",
        ),
    ],
)
def test_usgs_fetches_the_seven_days_before_the_run(dag, logical_date, run_after, since, until):
    from airflow.sdk.execution_time import macros

    task = dag.get_task("usgs")
    run = SimpleNamespace(logical_date=utc(logical_date), run_after=utc(run_after))
    command = task.render_template(task.bash_command, {"dag_run": run, "macros": macros})
    assert command == f"/app/.venv/bin/quake-lake usgs --since {since} --until {until}"


@pytest.mark.parametrize(
    ("value", "start"),
    [
        (" 2026-11-01 ", datetime(2026, 11, 1, tzinfo=UTC)),  # a date: 00:00 UTC
        ("2026-11-01T07:00+07:00", datetime(2026, 11, 1, tzinfo=UTC)),  # converted to UTC
        ("  ", datetime(2026, 11, 20, tzinfo=UTC)),  # blank: the default
    ],
)
def test_the_start_date_is_read_as_utc(monkeypatch, value, start):
    monkeypatch.setenv("QUAKE_LAKE_START_DATE", value)
    bag = load_dags()
    assert bag.import_errors == {}
    assert bag.dags["quake_lake"].start_date == start


def test_a_bad_start_date_names_the_variable(monkeypatch):
    monkeypatch.setenv("QUAKE_LAKE_START_DATE", "20-11-2026")
    (error,) = load_dags().import_errors.values()
    assert "QUAKE_LAKE_START_DATE='20-11-2026' is not a date like 2026-11-20" in error


def test_every_task_retries_and_has_a_timeout(dag):
    for task in dag.tasks:
        assert (task.retries, task.retry_delay) == (2, timedelta(minutes=2)), task.task_id
        assert task.execution_timeout == timedelta(minutes=30), task.task_id


def test_a_run_not_done_an_hour_after_it_was_queued_calls_the_callback(dag):
    (alert,) = dag.deadline
    assert type(alert.reference).__name__ == "DagRunQueuedAtDeadline"
    assert alert.interval == timedelta(hours=1)
    assert alert.callback.path == "quake_lake_alerts.missed_deadline"


def test_dbt_runs_from_the_image_root(dag):
    # transform/'s relative paths (src/quake_lake/provinces.csv, lake/gold.duckdb) need it.
    for task_id in ("gold", "freshness"):
        task = dag.get_task(task_id)
        assert task.cwd == "/app"
        assert task.bash_command.startswith("/app/.venv/bin/dbt ")
