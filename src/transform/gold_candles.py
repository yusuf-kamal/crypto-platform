from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_candles_1m(trades: DataFrame) -> DataFrame:
    return (
        trades.withColumn("candle_ts", F.date_trunc("minute", "trade_ts"))
        .groupBy("symbol", "candle_ts")
        .agg(
            F.min_by("price", "agg_trade_id").alias("open_price"),
            F.max_by("price", "agg_trade_id").alias("close_price"),
            F.max("price").alias("high_price"),
            F.min("price").alias("low_price"),
            F.sum("quantity").alias("volume"),
            F.sum("quote_value").alias("quote_volume"),
            F.count("*").alias("trade_count"),
            F.sum(F.when(F.col("side") == "BUY", F.col(
                "quantity")).otherwise(0)).alias("buy_volume"),
            F.sum(F.when(F.col("side") == "SELL", F.col(
                "quantity")).otherwise(0)).alias("sell_volume"),
        )
    )
