import json
import os
import sys
import time

from pyspark.sql import functions as F

from src.common.spark import get_spark
from src.transform.gold_candles import build_candles_1m

STAGING_TABLE = "staging.candles_1m"


def jdbc_url() -> str:
    return (
        f"jdbc:sqlserver://{os.environ['MSSQL_HOST']}:{os.environ['MSSQL_PORT']};"
        f"databaseName={os.environ['MSSQL_DB']};encrypt=true;trustServerCertificate=true"
    )


def run(symbols: list[str], months: list[str]) -> dict:
    started = time.time()
    spark = get_spark("gold_candles")

    trades = (
        spark.read.parquet("s3a://silver/aggtrades")
        .filter(F.col("symbol").isin(symbols) & F.col("file_month").isin(months))
    )

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

    metrics = {"symbols": symbols, "months": months,
               "seconds": round(time.time() - started, 1)}
    print(json.dumps(metrics))
    return metrics


if __name__ == "__main__":
    run(sys.argv[1].split(","), sys.argv[2].split(","))