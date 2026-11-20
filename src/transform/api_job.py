"""Silver + staging for the REST API source.

Reads the untouched JSON payloads from Bronze, parses them with the pure
functions in api_daily.py, writes the valid rows to the configured lake format and to
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
from datetime import date

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
from src.common.data_quality import DataQualityError, record_failure
from src.common.lake import (
    QUARANTINE_COIN_MARKET,
    QUARANTINE_FEAR_GREED,
    SILVER_COIN_MARKET,
    SILVER_FEAR_GREED,
    write_lake,
)
from src.common.observability import json_log, write_run_metrics
from src.common.spark import get_spark
from src.transform.api_daily import (
    fear_greed_rows,
    market_chart_rows,
    split_valid_rejected,
    validate_api_batch,
)

BRONZE_BUCKET = "bronze"
MARKET_KEY = "coingecko/market_chart/coin_id={coin_id}/{ds}.json"
FNG_KEY = "alternative_me/fear_greed/{ds}.json"
FNG_PREFIX = "alternative_me/fear_greed/"

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


def latest_bronze_ds(s3) -> str:
    """The most recent day that Bronze actually holds.

    This job is triggered by an Asset, and an Asset-triggered DAG run has no
    data interval -- so `{{ ds }}` is not merely empty there, it is undefined,
    and rendering it fails the task before it starts. The day therefore comes
    from the data rather than from the scheduler, which is also the more
    honest source: it is the day whose file is really there to process.
    """
    days = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=FNG_PREFIX):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if name.endswith(".json"):
                days.append(name.removesuffix(".json"))
    if not days:
        raise RuntimeError(
            f"no Fear & Greed payload under s3://{BRONZE_BUCKET}/{FNG_PREFIX} "
            "- run api_daily_ingestion first"
        )
    return max(days)


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


def run(ds: str | None = None) -> dict:
    """Build Silver and staging for one day of API payloads.

    `ds` is optional on purpose: the DAG that calls this is Asset-triggered
    and has no data interval to take a date from, so with no argument the
    day is resolved from Bronze instead.
    """
    started = time.time()
    s3 = _s3()
    ds = (ds or "").strip() or latest_bronze_ds(s3)

    market_rows: list[dict] = []
    for symbol, coin_id in COINGECKO_IDS.items():
        payload = _read_bronze_json(
            s3, MARKET_KEY.format(coin_id=coin_id, ds=ds))
        market_rows += market_chart_rows(payload, symbol, coin_id)

    fng_rows = fear_greed_rows(
        _read_bronze_json(s3, FNG_KEY.format(ds=ds)))

    market_valid, market_rejected = split_valid_rejected(market_rows)
    fng_valid, fng_rejected = split_valid_rejected(fng_rows)

    # Validate BOTH datasets before creating any publication writer. Raw Bronze
    # remains replayable; immutable diagnostics also work for uncastable rows.
    try:
        through = date.fromisoformat(ds)
        validate_api_batch("coin_market_daily", market_valid, market_rejected,
                           symbols=tuple(COINGECKO_IDS), through=through)
        validate_api_batch("fear_greed_daily", fng_valid, fng_rejected, through=through)
    except DataQualityError as exc:
        record_failure(exc, lambda key, body: s3.put_object(
            Bucket="silver", Key=key, Body=body), ds=ds,
            rejected_sample=repr((market_rejected + fng_rejected)[:10]))
        raise

    spark = get_spark(f"api_daily_{ds}")

    # Quarantine can contain values that cannot fit the accepted Spark schema.
    # Raw values remain replayable in Bronze; null unsafe quarantine cells.
    for rows, schema, path in (
        (market_rejected, MARKET_SCHEMA, QUARANTINE_COIN_MARKET),
        (fng_rejected, FNG_SCHEMA, QUARANTINE_FEAR_GREED),
    ):
        if rows:
            safe_rows = []
            for row in rows:
                safe = dict(row)
                for field in schema.fields:
                    value = safe.get(field.name)
                    if isinstance(field.dataType, DoubleType):
                        try:
                            safe[field.name] = (float(value)
                                                if isinstance(value, (int, float)) else None)
                        except OverflowError:
                            safe[field.name] = None
                    elif isinstance(field.dataType, (IntegerType, LongType)):
                        bits = 32 if isinstance(field.dataType, IntegerType) else 64
                        if not isinstance(value, int) or not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
                            safe[field.name] = None
                safe_rows.append(safe)
            write_lake(spark.createDataFrame(safe_rows, _with_reason(schema)).coalesce(1), path)

    # Construct both accepted frames before the first overwrite, catching schema
    # errors in the second dataset without publishing the first one.
    market_valid = [{**r, **{k: float(r[k]) if r.get(k) is not None else None
                           for k in ("price_usd", "market_cap_usd", "total_volume_usd")}}
                    for r in market_valid]
    frames = [spark.createDataFrame(rows, schema).coalesce(1) for rows, schema in (
        (market_valid, MARKET_SCHEMA), (fng_valid, FNG_SCHEMA))]

    # These are daily aggregates: a few thousand rows, not millions. One
    # partition per table keeps the object store free of tiny files.
    for df, silver_path, staging in (
        (frames[0], SILVER_COIN_MARKET, STAGING_MARKET),
        (frames[1], SILVER_FEAR_GREED, STAGING_FNG),
    ):
        # The API returns its whole lookback window every run, so the
        # dataset is replaced wholesale rather than sliced. In Delta that
        # replacement is a single atomic commit.
        write_lake(df, silver_path)
        _write_staging(df.drop("source_ts_ms"), staging)

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
