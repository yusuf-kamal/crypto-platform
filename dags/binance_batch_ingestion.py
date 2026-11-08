import hashlib
import logging
import os
import tempfile
import time
from datetime import datetime, timedelta

import requests
from airflow.sdk import dag, task

log = logging.getLogger("airflow.task")

BASE_URL = "https://data.binance.vision/data/spot/monthly/aggTrades"
BUCKET = "bronze"
CONN_ID = "minio_s3"

SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
MONTHS = ["2026-05", "2026-06", "2026-07", "2026-08", "2026-09", "2026-10"]


@dag(
    dag_id="binance_batch_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "batch"],
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
)
def binance_batch_ingestion():
    @task(max_active_tis_per_dag=2)
    def ingest(symbol: str, month: str) -> dict:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id=CONN_ID)
        name = f"{symbol}-aggTrades-{month}.zip"
        key = f"binance/aggTrades/symbol={symbol}/{name}"

        if hook.check_for_key(key, bucket_name=BUCKET):
            log.info("skip, already in bronze: %s", key)
            return {"symbol": symbol, "month": month, "status": "skipped"}

        url = f"{BASE_URL}/{symbol}/{name}"
        resp = requests.get(url + ".CHECKSUM", timeout=30)
        resp.raise_for_status()
        expected_sha = resp.text.split()[0]

        start = time.time()
        sha = hashlib.sha256()
        size = 0
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, name)
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
                        sha.update(chunk)
                        size += len(chunk)

            if sha.hexdigest() != expected_sha:
                raise ValueError(f"checksum mismatch for {name}")

            hook.load_file(filename=path, key=key,
                           bucket_name=BUCKET, replace=True)

        seconds = round(time.time() - start, 1)
        log.info("loaded %s: %d bytes in %ss", key, size, seconds)
        return {"symbol": symbol, "month": month, "status": "loaded",
                "bytes": size, "seconds": seconds}

    ingest.expand(symbol=SYMBOLS, month=MONTHS)


binance_batch_ingestion()
