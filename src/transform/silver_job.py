import os
import sys
import tempfile
import time
import zipfile

from pyspark import StorageLevel
from pyspark.sql import functions as F

from src.common.lake import (
    QUARANTINE_AGGTRADES,
    SILVER_AGGTRADES,
    month_bounds,
    symbol_date_predicate,
    write_lake,
)
from src.common.observability import json_log, write_run_metrics
from src.common.data_quality import (
    DataQualityError, record_failure, validate_summary,
)
from src.common.spark import get_spark
from src.transform.silver_aggtrades import RAW_COLUMNS, parse_raw, tag_rejects


def run(symbol: str, month: str) -> dict:
    import boto3

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
        )
        if len(raw.columns) != len(RAW_COLUMNS):
            exc = DataQualityError(f"aggtrades:{symbol}|{month}", [
                f"expected {len(RAW_COLUMNS)} raw fields, got {len(raw.columns)}"])
            record_failure(exc, lambda key, body: s3.put_object(
                Bucket="silver", Key=key, Body=body), source_file=key)
            raise exc
        raw = raw.toDF(*RAW_COLUMNS)
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
        write_lake(
            rejected,
            QUARANTINE_AGGTRADES,
            partition_by=["symbol", "file_month"],
            replace_where=(
                f"symbol = '{symbol}' and file_month = '{month}'"
            ),
        )
        t = lap("quarantine_s", t)

        valid = (
            tagged.filter(F.col("reject_reason").isNull())
            .drop("reject_reason", "raw_record", "transact_micros")
            # agg_trade_id is unique per *symbol*, not globally. One job
            # handles one symbol, so the single-column version happened to
            # be correct -- but it would silently delete real trades the
            # moment a job ever covered two symbols.
            .dropDuplicates(["symbol", "agg_trade_id"])
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        n_valid = valid.count()
        # Count repeated parseable keys across rejected rows too: a malformed
        # version of an existing trade must not hide behind the reject budget.
        duplicate_groups = (
            tagged.filter(F.col("agg_trade_id").isNotNull())
            .groupBy("symbol", "agg_trade_id").count()
            .filter(F.col("count") > 1)
        )
        n_duplicates = duplicate_groups.agg(
            F.sum(F.col("count") - 1).alias("duplicates")).first()["duplicates"] or 0
        t = lap("dedupe_count_s", t)
        # repartition by trade_date keeps one output file per day instead of
        # the 200 tiny ones the default shuffle produced (section 10).
        # replaceWhere states the slice this file owns, so a re-run can only
        # ever replace its own symbol and days.
        try:
            publish_valid_trades(valid, symbol, month, total, n_rejected, n_valid,
                                 n_duplicates=n_duplicates)
        except DataQualityError as exc:
            record_failure(exc, lambda diagnostic_key, body: s3.put_object(
                Bucket="silver", Key=diagnostic_key, Body=body), source_file=key)
            raise
        finally:
            tagged.unpersist()
            valid.unpersist()
        lap("write_valid_s", t)

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


def publish_valid_trades(valid, symbol, month, total, n_rejected, n_valid,
                         n_duplicates=None, expected_dates=()):
    """Do not infer full-month coverage for sparse or partial historical data."""
    start, end = month_bounds(month)
    dates = {r["trade_date"] for r in valid.select("trade_date").distinct().collect()}
    validate_summary(
        f"aggtrades:{symbol}|{month}", rows_in=total, rows_valid=n_valid,
        rows_rejected=n_rejected,
        duplicates=(total - n_rejected - n_valid if n_duplicates is None else n_duplicates),
        observed_symbols=[symbol] if n_valid and symbol else [], required_symbols=[symbol],
        observed_dates=dates,
        expected_dates=expected_dates,
    )
    write_lake(valid.repartition(F.col("trade_date")), SILVER_AGGTRADES,
               partition_by=["symbol", "trade_date"],
               replace_where=symbol_date_predicate(symbol, start, end))


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2])
