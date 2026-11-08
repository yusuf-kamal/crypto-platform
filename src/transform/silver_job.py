import json
import os
import sys
import tempfile
import time
import zipfile

import boto3
from pyspark import StorageLevel
from pyspark.sql import functions as F

from src.common.spark import get_spark
from src.transform.silver_aggtrades import RAW_COLUMNS, parse_raw, tag_rejects

SILVER = "s3a://silver/aggtrades"
QUARANTINE = "s3a://silver/_quarantine/aggtrades"


def run(symbol: str, month: str) -> dict:
    started = time.time()
    key = f"binance/aggTrades/symbol={symbol}/{symbol}-aggTrades-{month}.zip"
    spark = get_spark(f"silver_{symbol}_{month}")
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
    )

    with tempfile.TemporaryDirectory() as tmp:
        zip_path = os.path.join(tmp, "in.zip")
        s3.download_file("bronze", key, zip_path)
        with zipfile.ZipFile(zip_path) as z:
            csv_name = z.namelist()[0]
            z.extract(csv_name, tmp)

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

        rejected = tagged.filter(F.col("reject_reason").isNotNull())
        n_rejected = rejected.count()
        rejected.write.mode("overwrite").partitionBy(
            "symbol", "file_month").parquet(QUARANTINE)

        valid = (
            tagged.filter(F.col("reject_reason").isNull())
            .drop("reject_reason", "raw_record", "transact_micros")
            .dropDuplicates(["agg_trade_id"])
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        n_valid = valid.count()
        valid.write.mode("overwrite").partitionBy(
            "symbol", "trade_date").parquet(SILVER)

        tagged.unpersist()
        valid.unpersist()

    metrics = {
        "symbol": symbol, "month": month, "rows_in": total,
        "rows_valid": n_valid, "rows_rejected": n_rejected,
        "rows_duplicate": total - n_rejected - n_valid,
        "error_rate": round(n_rejected / total, 6) if total else 0,
        "seconds": round(time.time() - started, 1),
    }
    print(json.dumps(metrics))
    return metrics


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2])
