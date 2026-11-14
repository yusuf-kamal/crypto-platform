"""Silver + staging for source type 2, triggered by the Bronze asset."""

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from src.common.observability import on_failure_alert
from src.common.assets import (
    BRONZE_API_DAILY,
    SILVER_API_DAILY,
    WAREHOUSE_STAGING,
)

log = logging.getLogger("airflow.task")


@dag(
    dag_id="api_daily_warehouse",
    start_date=datetime(2026, 1, 1),
    schedule=[BRONZE_API_DAILY],
    catchup=False,
    max_active_runs=1,
    tags=["silver", "gold", "api"],
    default_args={"on_failure_callback": on_failure_alert,
                  "retries": 1, "retry_delay": timedelta(minutes=2)},
)
def api_daily_warehouse():
    @task(outlets=[SILVER_API_DAILY, WAREHOUSE_STAGING])
    def build(ds: str) -> dict:
        from src.transform.api_job import run

        # Empty unless someone triggers this DAG with a config, which is the
        # normal case: an Asset-triggered run has no data interval, so there
        # is no `{{ ds }}` to render -- referencing it fails the task before
        # it starts. run() then resolves the day from what Bronze holds.
        metrics = run(ds)
        log.info("api silver + staging built: %s", metrics)
        return metrics

    build(ds="{{ dag_run.conf.get('ds', '') }}")


api_daily_warehouse()
