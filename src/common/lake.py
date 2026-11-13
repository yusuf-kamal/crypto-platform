"""One place that knows where the lake lives and in what format it is written.

The Silver layer is Delta Lake. Parquet got the project a long way, but two
problems were documented limitations rather than solved ones:

* **Partial writes.** Committer algorithm v2 is fast on object storage but
  leaves half-written output behind if a job dies mid-write. A reader cannot
  tell. Delta's transaction log makes a write atomic: until the commit, no
  reader sees any of it.
* **Blind re-processing.** `mode("overwrite")` with dynamic partition
  overwrite replaces whichever partitions the batch happens to contain. With
  `replaceWhere` the job states up front exactly which slice it is allowed to
  replace, and Delta fails the write if the data does not match -- a typo in
  the predicate becomes an error instead of silent data loss.

Delta also gives time travel (`versionAsOf`) for free, which is what makes a
bad run inspectable after the fact rather than merely gone.

`LAKE_FORMAT` is an escape hatch: set it to `parquet` and every read and
write here falls back to the old behaviour, so the switch can be reverted
without touching the jobs.
"""

import os

LAKE_FORMAT = os.environ.get("LAKE_FORMAT", "delta").lower()

# Silver
SILVER_AGGTRADES = "s3a://silver/aggtrades"
SILVER_COIN_MARKET = "s3a://silver/coin_market_daily"
SILVER_FEAR_GREED = "s3a://silver/fear_greed_daily"
SILVER_STREAM_CANDLES = "s3a://silver/stream_candles_1m"

# Quarantine and bookkeeping
QUARANTINE_AGGTRADES = "s3a://silver/_quarantine/aggtrades"
QUARANTINE_COIN_MARKET = "s3a://silver/_quarantine/coin_market_daily"
QUARANTINE_FEAR_GREED = "s3a://silver/_quarantine/fear_greed_daily"
STREAM_CHECKPOINT = "s3a://silver/_checkpoints/stream_candles_1m"


def read_lake(spark, path: str):
    """Read a Silver dataset, whichever format the lake is written in."""
    return spark.read.format(LAKE_FORMAT).load(path)


def write_lake(df, path: str, partition_by: list[str] | None = None,
               replace_where: str | None = None, mode: str = "overwrite"):
    """Write a Silver dataset.

    `replace_where` is the point of this helper. It names the exact slice the
    write may replace, so re-running one file can never touch another file's
    rows. It is Delta-only; on the parquet fallback the call degrades to the
    previous dynamic-partition overwrite, which is close enough in effect but
    not atomic.
    """
    writer = df.write.format(LAKE_FORMAT).mode(mode)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if replace_where and LAKE_FORMAT == "delta" and mode == "overwrite":
        writer = writer.option("replaceWhere", replace_where)

    writer.save(path)


def sql_literal(value: str) -> str:
    """Quote a value for a replaceWhere predicate.

    The predicates are built from symbols and months that come from this
    project's own config, not from user input, but a value that contains a
    quote would still produce a broken predicate rather than a wrong one.
    """
    return "'" + str(value).replace("'", "''") + "'"


def symbol_date_predicate(symbol: str, start_date: str, end_date: str) -> str:
    """The slice one (symbol, month) file owns: its symbol and its days.

    `trade_date` is a partition column, so Delta can satisfy this by
    rewriting whole partitions instead of scanning the table.
    """
    return (
        f"symbol = {sql_literal(symbol)} "
        f"and trade_date >= {sql_literal(start_date)} "
        f"and trade_date <= {sql_literal(end_date)}"
    )


def month_bounds(month: str) -> tuple[str, str]:
    """'2026-06' -> ('2026-06-01', '2026-06-30'), without a calendar table."""
    from calendar import monthrange
    from datetime import date

    year, mon = (int(part) for part in month.split("-"))
    last_day = monthrange(year, mon)[1]
    return (date(year, mon, 1).isoformat(),
            date(year, mon, last_day).isoformat())


# --- Maintenance -----------------------------------------------------------
# Delta keeps superseded files around so that time travel works. Nothing
# reclaims them on its own, so disk use grows with every re-run and small
# files accumulate. These two statements are what the maintenance DAG runs.

SILVER_TABLES = (
    SILVER_AGGTRADES,
    SILVER_COIN_MARKET,
    SILVER_FEAR_GREED,
    SILVER_STREAM_CANDLES,
    QUARANTINE_AGGTRADES,
    QUARANTINE_COIN_MARKET,
    QUARANTINE_FEAR_GREED,
)

# One week. Shorter than this and a `VACUUM` could delete files a long-running
# reader still needs; Delta refuses anything under 168 hours by default for
# exactly that reason.
VACUUM_RETAIN_HOURS = 168


def delta_relation(path: str) -> str:
    """`delta.`s3a://...`` -- how Spark SQL names a Delta table by path."""
    return f"delta.`{path}`"


def table_exists(spark, path: str) -> bool:
    """True if the path holds a readable table of the configured format.

    Maintenance must not fail because the streaming path has never run, or
    because a quarantine prefix is still empty.
    """
    try:
        read_lake(spark, path).limit(0).count()
        return True
    except Exception:                                         # noqa: BLE001
        return False


def optimize(spark, path: str) -> None:
    """Compact the many small files a partitioned write leaves behind."""
    spark.sql(f"OPTIMIZE {delta_relation(path)}")


def vacuum(spark, path: str, retain_hours: int = VACUUM_RETAIN_HOURS) -> None:
    """Delete files that no version inside the retention window references."""
    spark.sql(
        f"VACUUM {delta_relation(path)} RETAIN {int(retain_hours)} HOURS")


def table_detail(spark, path: str) -> dict:
    """Size, file count and current version, for the maintenance metrics."""
    row = spark.sql(f"DESCRIBE DETAIL {delta_relation(path)}").collect()[0]
    history = spark.sql(
        f"DESCRIBE HISTORY {delta_relation(path)} LIMIT 1").collect()
    return {
        "num_files": row["numFiles"],
        "size_bytes": row["sizeInBytes"],
        "version": history[0]["version"] if history else None,
    }
