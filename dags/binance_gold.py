import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from src.common.assets import SILVER_AGGTRADES
from src.common.config import MONTHS, SYMBOLS

log = logging.getLogger("airflow.task")


@dag(
    dag_id="binance_gold",
    start_date=datetime(2026, 1, 1),
    schedule=[SILVER_AGGTRADES],
    catchup=False,
    max_active_runs=1,
    tags=["gold"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
)
def binance_gold():
    @task
    def build_staging() -> dict:
        from src.transform.gold_job import run

        metrics = run(SYMBOLS, MONTHS)
        log.info("staging built: %s", metrics)
        return metrics

    build_staging()


binance_gold()
