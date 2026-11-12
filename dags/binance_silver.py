import json
import logging
import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from src.common.assets import BRONZE_AGGTRADES, SILVER_AGGTRADES
from src.common.config import MONTHS, SYMBOLS

log = logging.getLogger("airflow.task")


@dag(
    dag_id="binance_silver",
    start_date=datetime(2026, 1, 1),
    schedule=[BRONZE_AGGTRADES],  
    catchup=False,
    tags=["silver"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
)
def binance_silver():
    @task(max_active_tis_per_dag=1)
    def transform(symbol: str, month: str) -> dict:
        import boto3
        from botocore.exceptions import ClientError

        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT"],
            aws_access_key_id=os.environ["MINIO_ROOT_USER"],
            aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
            region_name="us-east-1",
        )
        manifest = f"_manifests/aggtrades/symbol={symbol}/{month}.json"
        try:
            s3.head_object(Bucket="silver", Key=manifest)
            log.info("skip, already processed: %s %s", symbol, month)
            return {"symbol": symbol, "month": month, "status": "skipped"}
        except ClientError:
            pass

        from src.transform.silver_job import run

        metrics = run(symbol, month)
        s3.put_object(Bucket="silver", Key=manifest, Body=json.dumps(metrics))
        return {**metrics, "status": "processed"}

    results = transform.expand(symbol=SYMBOLS, month=MONTHS)

    @task(outlets=[SILVER_AGGTRADES])
    def publish(results):
        done = [r for r in results if r["status"] == "processed"]
        log.info("silver ready: %d files processed", len(done))

    publish(results)


binance_silver()
