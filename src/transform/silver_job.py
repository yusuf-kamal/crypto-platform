import os
import sys
import tempfile
import time
import zipfile

import boto3
from pyspark import StorageLevel
from pyspark.sql import functions as F

from src.common.observability import json_log, write_run_metrics
from src.common.spark import get_spark
from src.transform.silver_aggtrades import RAW_COLUMNS, parse_raw, tag_rejects

SILVER = "s3a://silver/aggtrades"
QUARANTINE = "s3a://silver/_quarantine/aggtrades"


def run(symbol: str, month: str) -> dict:
    started = time.time()
    timings = {}
    key = f"binance/aggTrades/symbol={symbol}/{symbol}-aggTrades-{month}.zip"
    spark = get_spark(f"silver_{symbol}_{month}")
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
    )

    def lap(name, since):
        now = time.time()
        timings[name] = round(now - since, 1)
        return now

    with tempfile.TemporaryDirectory() as tmp:
        t = time.time()
        zip_path = os.path.join(tmp, "in.zip")
        s3.download_file("bronze", key, zip_path)
        with zipfile.ZipFile(zip_path) as z:
            csv_name = z.namelist()[0]
            z.extract(csv_name, tmp)
        t = lap("download_unzip_s", t)

        raw = (
            spark.read.option("header", "false")
            .csv(f"file://{os.path.join(tmp, csv_name)}")
            .toDF(*RAW_COLUMNS)
        )
        tagged = (
            tag_rejects(parse_raw(raw), month)
            .withColumn("symbol", F.lit(symbol))
            .withColumn("file_month", F.lit(month))
            .withColumn("source_file", F.lit(key))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        total = tagged.count()
        t = lap("read_parse_count_s", t)

        rejected = tagged.filter(F.col("reject_reason").isNotNull())
        n_rejected = rejected.count()
        rejected.write.mode("overwrite").partitionBy(
            "symbol", "file_month").parquet(QUARANTINE)
        t = lap("quarantine_s", t)

        valid = (
            tagged.filter(F.col("reject_reason").isNull())
            .drop("reject_reason", "raw_record", "transact_micros")
            .dropDuplicates(["agg_trade_id"])
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        n_valid = valid.count()
        t = lap("dedupe_count_s", t)
        valid.repartition(F.col("trade_date")).write.mode("overwrite").partitionBy(
            "symbol", "trade_date").parquet(SILVER)
        lap("write_valid_s", t)

        tagged.unpersist()
        valid.unpersist()

    metrics = {
        "symbol": symbol, "month": month, "rows_in": total,
        "rows_valid": n_valid, "rows_rejected": n_rejected,
        "rows_duplicate": total - n_rejected - n_valid,
        "error_rate": round(n_rejected / total, 6) if total else 0,
        "seconds": round(time.time() - started, 1),
        "timings": timings,
    }
    json_log("silver_file_processed", **metrics)
    write_run_metrics(spark, "silver", [{
        "entity": f"{symbol}|{month}",
        "status": "processed",
        "rows_in": total,
        "rows_valid": n_valid,
        "rows_rejected": n_rejected,
        "seconds": metrics["seconds"],
        "details": {"timings": timings, "error_rate": metrics["error_rate"]},
    }])
    return metrics


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2])
