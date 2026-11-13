"""Gold: 1-minute candles from Silver into SQL Server staging.

Incremental by default. The Silver DAG writes one manifest per processed
(symbol, month) file; this job keeps its own list of the pairs it has already
turned into candles and only recomputes the difference. Staging then holds
just that difference, which the dbt incremental models merge into the fact on
its composite key -- so a partial load is still correct, and a re-run of an
unchanged scope does no Spark work at all.
"""

import json
import os
import sys
import time
from functools import reduce

from pyspark.sql import functions as F

from src.common.observability import json_log, write_run_metrics
from src.common.spark import get_spark
from src.transform.gold_candles import build_candles_1m

STAGING_TABLE = "staging.candles_1m"
SILVER_BUCKET = "silver"
SILVER_MANIFEST_PREFIX = "_manifests/aggtrades/"
GOLD_STATE_KEY = "_manifests/gold/candles_1m.json"


def _s3():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
    )


def pair_from_manifest_key(key: str) -> tuple[str, str] | None:
    """`_manifests/aggtrades/symbol=BTCUSDT/2026-06.json` -> ("BTCUSDT", "2026-06").

    Returns None for anything that is not a manifest, so a stray object under
    the prefix cannot be mistaken for a processed file.
    """
    parts = key.split("/")
    if len(parts) != 4 or not parts[-1].endswith(".json"):
        return None
    if not parts[2].startswith("symbol="):
        return None
    symbol = parts[2].removeprefix("symbol=")
    month = parts[3].removesuffix(".json")
    if not symbol or not month:
        return None
    return symbol, month


def silver_pairs(s3) -> set[tuple[str, str]]:
    """(symbol, month) pairs that the Silver DAG has finished writing.

    Manifest keys look like
    `_manifests/aggtrades/symbol=BTCUSDT/2026-06.json`.
    """
    pairs = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=SILVER_BUCKET,
                                   Prefix=SILVER_MANIFEST_PREFIX):
        for obj in page.get("Contents", []):
            pair = pair_from_manifest_key(obj["Key"])
            if pair:
                pairs.add(pair)
    return pairs


def _load_state(s3) -> set[tuple[str, str]]:
    from botocore.exceptions import ClientError

    try:
        body = s3.get_object(Bucket=SILVER_BUCKET, Key=GOLD_STATE_KEY)["Body"]
    except ClientError:
        return set()
    done = json.loads(body.read()).get("processed", [])
    return {tuple(item.split("|", 1)) for item in done if "|" in item}


def _save_state(s3, pairs: set[tuple[str, str]]) -> None:
    payload = {"processed": sorted(f"{s}|{m}" for s, m in pairs)}
    s3.put_object(Bucket=SILVER_BUCKET, Key=GOLD_STATE_KEY,
                  Body=json.dumps(payload))


def jdbc_url() -> str:
    return (
        f"jdbc:sqlserver://{os.environ['MSSQL_HOST']}:{os.environ['MSSQL_PORT']};"
        f"databaseName={os.environ['MSSQL_DB']};encrypt=true;trustServerCertificate=true"
    )


def run(symbols: list[str], months: list[str],
        full_refresh: bool = False) -> dict:
    started = time.time()
    s3 = _s3()

    in_scope = {(s, m) for s in symbols for m in months}
    available = silver_pairs(s3) & in_scope
    already_done = set() if full_refresh else _load_state(s3)
    todo = sorted(available - already_done)

    if not todo:
        metrics = {"status": "skipped", "reason": "no new silver files",
                   "pairs_available": len(available), "seconds": 0.0}
        json_log("gold_skipped", **metrics)
        return metrics

    spark = get_spark("gold_candles")

    # An exact per-pair filter, not symbol IN (...) AND month IN (...), which
    # would pull in combinations that were never processed.
    pair_filter = reduce(
        lambda a, b: a | b,
        [(F.col("symbol") == s) & (F.col("file_month") == m) for s, m in todo],
    )

    trades = spark.read.parquet("s3a://silver/aggtrades").filter(pair_filter)

    candles = build_candles_1m(trades).select(
        "symbol",
        F.col("candle_ts").cast("timestamp"),
        "open_price", "high_price", "low_price", "close_price",
        F.col("volume").cast("decimal(28,8)"),
        F.col("quote_volume").cast("decimal(28,8)"),
        F.col("trade_count").cast("int"),
        F.col("buy_volume").cast("decimal(28,8)"),
        F.col("sell_volume").cast("decimal(28,8)"),
    )

    # Staging is a hand-off buffer, not history: it is truncated and refilled
    # with this batch only. The dbt models merge it into the fact.
    (
        candles.write.format("jdbc")
        .option("url", jdbc_url())
        .option("dbtable", STAGING_TABLE)
        .option("user", os.environ["MSSQL_USER"])
        .option("password", os.environ["MSSQL_PASSWORD"])
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("truncate", "true")
        .option("batchsize", "10000")
        .mode("overwrite")
        .save()
    )

    _save_state(s3, already_done | set(todo))

    metrics = {
        "status": "processed",
        "pairs": [f"{s}|{m}" for s, m in todo],
        "pairs_processed": len(todo),
        "pairs_skipped": len(available) - len(todo),
        "full_refresh": full_refresh,
        "seconds": round(time.time() - started, 1),
    }
    json_log("gold_staging_built", **metrics)
    write_run_metrics(spark, "gold", [
        {"entity": f"{s}|{m}", "status": "processed",
         "seconds": metrics["seconds"],
         "details": {"full_refresh": full_refresh, "batch_pairs": len(todo)}}
        for s, m in todo
    ])
    return metrics


if __name__ == "__main__":
    full = len(sys.argv) > 3 and sys.argv[3].lower() in ("1", "true", "yes")
    run(sys.argv[1].split(","), sys.argv[2].split(","), full)
