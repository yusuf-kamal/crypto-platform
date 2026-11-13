"""Silver + staging for the REST API source.

Reads the untouched JSON payloads from Bronze, parses them with the pure
functions in api_daily.py, writes the valid rows to Silver as Parquet and to
SQL Server staging, and parks the rejected rows in the quarantine prefix.

Values arrive as JSON floats, so Spark keeps them as doubles here; the
warehouse columns are DECIMAL so that downstream arithmetic stays consistent
with the Binance fact table. (Trade prices are decimal end to end because
those are exact in the source; API aggregates never were.)
"""

import json
import os
import sys
import time

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.common.config import COINGECKO_IDS
from src.common.observability import json_log, write_run_metrics
from src.common.spark import get_spark
from src.transform.api_daily import (
    fear_greed_rows,
    market_chart_rows,
    split_valid_rejected,
)

BRONZE_BUCKET = "bronze"
MARKET_KEY = "coingecko/market_chart/coin_id={coin_id}/{ds}.json"
FNG_KEY = "alternative_me/fear_greed/{ds}.json"

SILVER_MARKET = "s3a://silver/coin_market_daily"
SILVER_FNG = "s3a://silver/fear_greed_daily"
QUARANTINE_MARKET = "s3a://silver/_quarantine/coin_market_daily"
QUARANTINE_FNG = "s3a://silver/_quarantine/fear_greed_daily"

STAGING_MARKET = "staging.coin_market_daily"
STAGING_FNG = "staging.fear_greed_daily"

MARKET_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("coin_id", StringType(), False),
    StructField("snapshot_date", DateType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("market_cap_usd", DoubleType(), True),
    StructField("total_volume_usd", DoubleType(), True),
    StructField("source_ts_ms", LongType(), True),
])

FNG_SCHEMA = StructType([
    StructField("snapshot_date", DateType(), True),
    StructField("fng_value", IntegerType(), True),
    StructField("fng_classification", StringType(), True),
    StructField("source_ts_ms", LongType(), True),
])


def _with_reason(schema: StructType) -> StructType:
    return StructType(list(schema.fields)
                      + [StructField("reject_reason", StringType(), True)])


def _s3():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
    )


def _read_bronze_json(s3, key: str) -> dict:
    body = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read()
    return json.loads(body)


def _jdbc_url() -> str:
    return (
        f"jdbc:sqlserver://{os.environ['MSSQL_HOST']}:{os.environ['MSSQL_PORT']};"
        f"databaseName={os.environ['MSSQL_DB']};encrypt=true;trustServerCertificate=true"
    )


def _write_staging(df, table: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", _jdbc_url())
        .option("dbtable", table)
        .option("user", os.environ["MSSQL_USER"])
        .option("password", os.environ["MSSQL_PASSWORD"])
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


def run(ds: str) -> dict:
    started = time.time()
    spark = get_spark(f"api_daily_{ds}")
    s3 = _s3()

    market_rows: list[dict] = []
    for symbol, coin_id in COINGECKO_IDS.items():
        payload = _read_bronze_json(
            s3, MARKET_KEY.format(coin_id=coin_id, ds=ds))
        market_rows += market_chart_rows(payload, symbol, coin_id)

    fng_rows = fear_greed_rows(
        _read_bronze_json(s3, FNG_KEY.format(ds=ds)))

    market_valid, market_rejected = split_valid_rejected(market_rows)
    fng_valid, fng_rejected = split_valid_rejected(fng_rows)

    # These are daily aggregates: a few thousand rows, not millions. One
    # partition per table keeps the object store free of tiny files.
    for rows, schema, silver_path, staging in (
        (market_valid, MARKET_SCHEMA, SILVER_MARKET, STAGING_MARKET),
        (fng_valid, FNG_SCHEMA, SILVER_FNG, STAGING_FNG),
    ):
        df = spark.createDataFrame(rows, schema).coalesce(1)
        df.write.mode("overwrite").parquet(silver_path)
        _write_staging(df.drop("source_ts_ms"), staging)

    for rows, schema, path in (
        (market_rejected, MARKET_SCHEMA, QUARANTINE_MARKET),
        (fng_rejected, FNG_SCHEMA, QUARANTINE_FNG),
    ):
        if rows:
            (spark.createDataFrame(rows, _with_reason(schema))
             .coalesce(1).write.mode("overwrite").parquet(path))

    metrics = {
        "ds": ds,
        "market_rows_valid": len(market_valid),
        "market_rows_rejected": len(market_rejected),
        "fear_greed_rows_valid": len(fng_valid),
        "fear_greed_rows_rejected": len(fng_rejected),
        "seconds": round(time.time() - started, 1),
    }
    json_log("api_daily_built", **metrics)
    write_run_metrics(spark, "api", [
        {"entity": "coingecko_market_chart", "status": "processed",
         "rows_valid": len(market_valid),
         "rows_rejected": len(market_rejected),
         "seconds": metrics["seconds"], "details": {"ds": ds}},
        {"entity": "alternative_me_fear_greed", "status": "processed",
         "rows_valid": len(fng_valid),
         "rows_rejected": len(fng_rejected),
         "seconds": metrics["seconds"], "details": {"ds": ds}},
    ])
    return metrics


if __name__ == "__main__":
    run(sys.argv[1])
