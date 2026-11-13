"""Weekly Delta housekeeping for the Silver layer.

Time-based rather than Asset-driven: this is not part of producing data, it
is upkeep, and it must run whether or not a batch arrived that week. It is
also the one DAG that is safe to run at any moment -- OPTIMIZE and VACUUM
only touch files the transaction log has already superseded.
"""

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from src.common.lake import VACUUM_RETAIN_HOURS
from src.common.observability import on_failure_alert

log = logging.getLogger("airflow.task")


@dag(
    dag_id="lake_maintenance",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["silver", "maintenance"],
    # Lowering the retention below 168 hours makes Delta refuse the VACUUM,
    # because a long-running reader could still need those files.
    params={"retain_hours": VACUUM_RETAIN_HOURS},
    default_args={"on_failure_callback": on_failure_alert,
                  "retries": 1, "retry_delay": timedelta(minutes=10)},
)
def lake_maintenance():
    @task
    def maintain(retain_hours: str) -> dict:
        from src.transform.maintenance_job import run

        metrics = run(int(retain_hours))
        log.info("lake maintenance: %s", metrics)
        return metrics

    maintain(retain_hours="{{ params.retain_hours }}")


lake_maintenance()
