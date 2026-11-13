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
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Only the fields the warehouse needs; anything else in the payload is
# ignored instead of failing, so Binance can add fields freely.
TRADE_DATA_SCHEMA = StructType([
    StructField("e", StringType(), True),    # event type
    StructField("s", StringType(), True),    # symbol
    StructField("t", LongType(), True),      # trade id
    StructField("p", StringType(), True),    # price   (string in the payload)
    StructField("q", StringType(), True),    # quantity(string in the payload)
    StructField("T", LongType(), True),      # trade time, epoch milliseconds
    StructField("m", BooleanType(), True),   # is the buyer the maker?
])

TRADE_EVENT_SCHEMA = StructType([
    StructField("stream", StringType(), True),
    StructField("data", TRADE_DATA_SCHEMA, True),
])


def parse_trade_events(kafka_df: DataFrame) -> DataFrame:
    """Kafka `value` bytes -> flat, typed trade rows.

    Prices and quantities arrive as strings and are cast to the same
    decimal(18,8) the batch Silver layer uses, so no floating-point error
    creeps in on the streaming side either.
    """
    parsed = kafka_df.select(
        F.from_json(F.col("value").cast("string"),
                    TRADE_EVENT_SCHEMA).alias("msg")
    ).select("msg.data.*")

    return (
        parsed.select(
            F.col("s").alias("symbol"),
            F.col("t").cast("long").alias("trade_id"),
            F.col("p").cast("decimal(18,8)").alias("price"),
            F.col("q").cast("decimal(18,8)").alias("quantity"),
            F.col("T").alias("trade_ms"),
            F.col("m").alias("is_buyer_maker"),
        )
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
