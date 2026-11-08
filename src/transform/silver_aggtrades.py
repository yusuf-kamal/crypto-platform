from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RAW_COLUMNS = [
    "agg_trade_id", "price", "quantity", "first_trade_id",
    "last_trade_id", "transact_time", "is_buyer_maker", "is_best_match",
]


def parse_raw(df_raw: DataFrame) -> DataFrame:
    t = F.col("transact_time")
    micros = (
        F.when(F.length(t) == 13, t.cast("long") * 1000)
        .when(F.length(t) == 16, t.cast("long"))
    )
    return (
        df_raw.select(
            F.col("agg_trade_id").cast("long").alias("agg_trade_id"),
            F.col("price").cast("decimal(18,8)").alias("price"),
            F.col("quantity").cast("decimal(18,8)").alias("quantity"),
            F.col("first_trade_id").cast("long").alias("first_trade_id"),
            F.col("last_trade_id").cast("long").alias("last_trade_id"),
            micros.alias("transact_micros"),
            F.col("is_buyer_maker").cast("boolean").alias("is_buyer_maker"),
            F.col("is_best_match").cast("boolean").alias("is_best_match"),
            F.concat_ws(",", *[F.col(c)
                        for c in RAW_COLUMNS]).alias("raw_record"),
        )
        .withColumn("trade_ts", F.expr("timestamp_micros(transact_micros)"))
        .withColumn("trade_date", F.to_date("trade_ts"))
        .withColumn(
            "side",
            F.when(F.col("is_buyer_maker") == True, "SELL")  
            .when(F.col("is_buyer_maker") == False, "BUY"),  
        )
        .withColumn("quote_value", F.col("price") * F.col("quantity"))
    )


def tag_rejects(df: DataFrame, expected_month: str) -> DataFrame:
    reason = (
        F.when(
            F.col("agg_trade_id").isNull() | F.col("price").isNull()
            | F.col("quantity").isNull() | F.col("trade_ts").isNull()
            | F.col("is_buyer_maker").isNull(),
            "null_or_unparseable_field",
        )
        .when((F.col("price") <= 0) | (F.col("quantity") <= 0),
              "non_positive_price_or_quantity")
        .when(F.col("last_trade_id") < F.col("first_trade_id"),
              "invalid_trade_id_range")
        .when(F.date_format("trade_ts", "yyyy-MM") != F.lit(expected_month),
              "outside_file_month")
    )
    return df.withColumn("reject_reason", reason)
