"""Pure transformations for the Binance trade stream (no I/O).

The streaming path mirrors the batch path on purpose: the same typing rules,
the same `side` derivation and the same 1-minute grain, so a streamed candle
and a batch candle are comparable. What differs is only *when* the data
arrives, which is why the aggregation here needs a watermark.

A raw combined-stream message looks like:

    {"stream": "btcusdt@trade",
     "data": {"e": "trade", "E": 1700000000123, "s": "BTCUSDT",
              "t": 12345, "p": "0.001", "q": "100",
              "T": 1700000000120, "m": true, "M": true}}

Note `t` (trade id) and `T` (trade time): see parse_trade_events for why that
pair rules out the obvious `from_json` approach.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# JSON paths into the combined-stream message, with the type each value has
# to be read as. Only the fields the warehouse needs are listed; anything
# else Binance adds is ignored rather than breaking the parse.
TRADE_FIELDS = {
    "symbol": ("$.data.s", "string"),
    "trade_id": ("$.data.t", "long"),
    "price": ("$.data.p", "decimal(18,8)"),
    "quantity": ("$.data.q", "decimal(18,8)"),
    "trade_ms": ("$.data.T", "long"),
    "is_buyer_maker": ("$.data.m", "boolean"),
}


def parse_trade_events(kafka_df: DataFrame) -> DataFrame:
    """Kafka `value` bytes -> flat, typed trade rows.

    This uses `get_json_object` rather than `from_json` with a struct schema,
    and the reason is the payload itself: Binance names the trade id `t` and
    the trade time `T`. Spark resolves column and struct-field names
    **case-insensitively** by default, so a struct holding both fields makes
    every reference to either one fail with AMBIGUOUS_REFERENCE. JSON paths
    are matched case-sensitively, which sidesteps the collision without
    turning on `spark.sql.caseSensitive` for the whole session.

    The cost is that the JSON is scanned once per field instead of once per
    row. At this volume -- a few hundred messages a second across five
    symbols -- that is not measurable, and the alternative is a global Spark
    setting that would change behaviour far away from here.

    Prices and quantities arrive as strings and are cast to the same
    decimal(18,8) the batch Silver layer uses, so no floating-point error
    creeps in on the streaming side either.
    """
    value = F.col("value").cast("string")

    parsed = kafka_df.select(*[
        F.get_json_object(value, path).cast(sql_type).alias(name)
        for name, (path, sql_type) in TRADE_FIELDS.items()
    ])

    return (
        parsed
        .withColumn("trade_ts", F.expr("timestamp_millis(trade_ms)"))
        # Same rule as the batch layer: if the buyer was the maker, the
        # aggressor was the seller.
        .withColumn(
            "side",
            F.when(F.col("is_buyer_maker"), F.lit("SELL"))
            .when(~F.col("is_buyer_maker"), F.lit("BUY")),
        )
        .withColumn("quote_value", F.col("price") * F.col("quantity"))
    )


def valid_trades(df: DataFrame) -> DataFrame:
    """Drop rows the warehouse cannot use.

    There is no quarantine prefix here: a stream has no file to replay, and
    stopping an unbounded query over one malformed message would lose live
    data. The dropped count is exposed through the query's progress metrics.
    """
    return df.filter(
        F.col("symbol").isNotNull()
        & F.col("trade_id").isNotNull()
        & F.col("trade_ts").isNotNull()
        & F.col("side").isNotNull()
        & (F.col("price") > 0)
        & (F.col("quantity") > 0)
    )


def stream_candles_1m(df: DataFrame, watermark: str = "2 minutes") -> DataFrame:
    """1-minute candles from the live trade stream.

    The watermark is what makes this bounded: Spark may close a minute and
    forget its state once no trade newer than `watermark` can still belong to
    it. Trades that arrive later than that are dropped rather than silently
    reopening a published candle.
    """
    return (
        df.withWatermark("trade_ts", watermark)
        .groupBy(F.window("trade_ts", "1 minute"), F.col("symbol"))
        .agg(
            F.min_by("price", "trade_id").alias("open_price"),
            F.max_by("price", "trade_id").alias("close_price"),
            F.max("price").alias("high_price"),
            F.min("price").alias("low_price"),
            F.sum("quantity").alias("volume"),
            F.sum("quote_value").alias("quote_volume"),
            F.count("*").alias("trade_count"),
            F.sum(F.when(F.col("side") == "BUY", F.col("quantity"))
                  .otherwise(0)).alias("buy_volume"),
            F.sum(F.when(F.col("side") == "SELL", F.col("quantity"))
                  .otherwise(0)).alias("sell_volume"),
        )
        .select(
            F.col("symbol"),
            F.col("window.start").alias("candle_ts"),
            "open_price", "high_price", "low_price", "close_price",
            "volume", "quote_volume", "trade_count",
            "buy_volume", "sell_volume",
        )
    )
