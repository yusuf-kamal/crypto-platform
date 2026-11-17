"""Bronze ingestion for source type 2: two public REST APIs.

Unlike the Binance DAG this one is scheduled daily, because both APIs only
expose the data as it is now. The response body is stored untouched under the
run's logical date, so a re-run overwrites its own day and nothing else.
"""

import json
import logging
from datetime import date, datetime, timedelta

from airflow.sdk import dag, task

from src.common.observability import on_failure_alert
from src.common.assets import BRONZE_API_DAILY
from src.common.config import COINGECKO_DAYS, COINGECKO_IDS, FEAR_GREED_LIMIT
from src.common.data_quality import DataQualityError, record_failure, validate_payload
from src.transform.api_daily import (
    fear_greed_rows, market_chart_rows, split_valid_rejected, validate_api_batch,
)

log = logging.getLogger("airflow.task")

BUCKET = "bronze"
CONN_ID = "minio_s3"


def check_payload(hook, payload, field, dataset, ds, symbol=None, coin_id=None):
    try:
        validate_payload(payload, field, dataset)
        rows = (market_chart_rows(payload, symbol, coin_id) if symbol
                else fear_greed_rows(payload))
        valid, rejected = split_valid_rejected(rows)
        validate_api_batch(dataset, valid, rejected, symbols=(symbol,) if symbol else (),
                           through=date.fromisoformat(ds))
    except DataQualityError as exc:
        record_failure(exc, lambda key, body: hook.load_string(
            string_data=body, key=key, bucket_name=BUCKET, replace=False),
            rejected_payload=repr(payload))
        raise


@dag(
    dag_id="api_daily_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "api"],
    default_args={"on_failure_callback": on_failure_alert,
                  "retries": 3, "retry_delay": timedelta(minutes=5)},
)
def api_daily_ingestion():
    @task
    def ingest_market_charts(ds: str) -> dict:
        """One call per coin, sequentially: CoinGecko rate-limits by IP."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        from src.ingestion.api_sources import fetch_market_chart

        hook = S3Hook(aws_conn_id=CONN_ID)
        written = {}
        payloads = {}
        for symbol, coin_id in COINGECKO_IDS.items():
            payload = fetch_market_chart(coin_id, COINGECKO_DAYS)
            check_payload(hook, payload, "prices", f"coingecko:{symbol}", ds, symbol, coin_id)
            payloads[symbol] = payload
        # Validate every configured coin before replacing any of this task's
        # Bronze objects. Repeat validation at the Silver boundary for replays.
        for symbol, coin_id in COINGECKO_IDS.items():
            payload = payloads[symbol]
            key = f"coingecko/market_chart/coin_id={coin_id}/{ds}.json"
            hook.load_string(string_data=json.dumps(payload), key=key,
                             bucket_name=BUCKET, replace=True)
            written[symbol] = len(payload.get("prices") or [])
            log.info("bronze %s: %d daily points", key, written[symbol])
        return {"points_per_symbol": written}

    @task
    def ingest_fear_greed(ds: str) -> dict:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        from src.ingestion.api_sources import fetch_fear_greed

        hook = S3Hook(aws_conn_id=CONN_ID)
        payload = fetch_fear_greed(FEAR_GREED_LIMIT)
        check_payload(hook, payload, "data", "fear_greed", ds)
        key = f"alternative_me/fear_greed/{ds}.json"
        hook.load_string(string_data=json.dumps(payload), key=key,
                         bucket_name=BUCKET, replace=True)
        days = len(payload.get("data") or [])
        log.info("bronze %s: %d daily values", key, days)
        return {"days": days}

    @task(outlets=[BRONZE_API_DAILY])
    def publish(market: dict, fear_greed: dict) -> None:
        log.info("api bronze ready: %s, fear_greed %s", market, fear_greed)

    publish(ingest_market_charts(ds="{{ ds }}"),
            ingest_fear_greed(ds="{{ ds }}"))


api_daily_ingestion()
