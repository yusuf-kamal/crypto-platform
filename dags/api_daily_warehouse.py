"""Silver + staging for source type 2, triggered by the Bronze asset."""

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from src.common.assets import BRONZE_API_DAILY, SILVER_API_DAILY

log = logging.getLogger("airflow.task")


@dag(
    dag_id="api_daily_warehouse",
    start_date=datetime(2026, 1, 1),
    schedule=[BRONZE_API_DAILY],
    catchup=False,
    max_active_runs=1,
    tags=["silver", "gold", "api"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
)
def api_daily_warehouse():
    @task(outlets=[SILVER_API_DAILY])
    def build(ds: str) -> dict:
        from src.transform.api_job import run

        metrics = run(ds)
        log.info("api silver + staging built: %s", metrics)
        return metrics

    build(ds="{{ ds }}")


api_daily_warehouse()
