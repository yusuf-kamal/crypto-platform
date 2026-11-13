import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from src.common.observability import on_failure_alert
from src.common.assets import SILVER_AGGTRADES, WAREHOUSE_STAGING
from src.common.config import MONTHS, SYMBOLS

log = logging.getLogger("airflow.task")


@dag(
    dag_id="binance_gold",
    start_date=datetime(2026, 1, 1),
    schedule=[SILVER_AGGTRADES],
    catchup=False,
    max_active_runs=1,
    tags=["gold"],
    # Trigger with full_refresh=true to recompute the whole configured scope,
    # for example after changing the candle logic.
    params={"full_refresh": False},
    default_args={"on_failure_callback": on_failure_alert,
                  "retries": 1, "retry_delay": timedelta(minutes=5)},
)
def binance_gold():
    @task(outlets=[WAREHOUSE_STAGING])
    def build_staging(full_refresh: str) -> dict:
        from src.transform.gold_job import run

        # Params arrive as a rendered template string, not as a bool.
        full = str(full_refresh).strip().lower() in ("1", "true", "yes")
        metrics = run(SYMBOLS, MONTHS, full_refresh=full)
        log.info("staging built: %s", metrics)
        return metrics

    build_staging(full_refresh="{{ params.full_refresh }}")


binance_gold()
