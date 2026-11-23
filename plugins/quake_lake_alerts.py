"""The quake_lake DAG's deadline callback, in a module of its own: Airflow stores a callback
as an import path, and a DAG file isn't importable by its name, a plugin module is."""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


def missed_deadline(context: dict[str, Any]) -> None:
    """Log the miss. With a Slack or email connection, this is where the alert would go.

    The scheduler's log gets the line; `dag_run` holds the REST API's fields for the run.
    """
    run = context.get("dag_run") or {}
    deadline = context.get("deadline") or {}
    log.error(
        "quake_lake run %s missed its deadline of %s (state: %s)",
        run.get("dag_run_id"),
        deadline.get("deadline_time"),
        run.get("state"),
    )
