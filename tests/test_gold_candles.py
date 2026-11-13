"""Unit tests for the 1-minute candle aggregation (src/transform/gold_candles.py).

These run on a tiny in-memory DataFrame shaped like the Silver layer, so they
need no S3 store and no SQL Server. The interesting property under test is
that open/close follow *trade order* (agg_trade_id), not the order in which
Spark happens to read the rows.
"""

from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.transform.gold_candles import build_candles_1m

# Mirrors the Silver schema that gold_job.py reads back from Parquet.
SILVER_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("agg_trade_id", LongType(), False),
    StructField("price", DecimalType(18, 8), False),
    StructField("quantity", DecimalType(18, 8), False),
    StructField("quote_value", DecimalType(38, 16), False),
    StructField("side", StringType(), False),
    StructField("is_buyer_maker", BooleanType(), False),
    StructField("trade_ts", TimestampType(), False),
])


def trade(symbol, agg_trade_id, price, quantity, side, ts):
    price, quantity = Decimal(price), Decimal(quantity)
    ts = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    return (
        symbol,
        agg_trade_id,
        price,
        quantity,
        price * quantity,
        side,
        side == "SELL",
        ts,
    )


# One BTC minute with three deliberately out-of-order trades, a second BTC
# minute, and one ETH trade in the first minute -> 3 candles in total.
TRADES = [
    trade("BTCUSDT", 3, "102", "1", "BUY", "2026-06-01 00:00:45"),
    trade("BTCUSDT", 1, "100", "2", "SELL", "2026-06-01 00:00:10"),
    trade("BTCUSDT", 2, "105", "3", "BUY", "2026-06-01 00:00:30"),
    trade("BTCUSDT", 4, "101", "1", "BUY", "2026-06-01 00:01:00"),
    trade("ETHUSDT", 10, "2000", "0.5", "SELL", "2026-06-01 00:00:05"),
]


@pytest.fixture(scope="module")
def candles(spark):
    df = spark.createDataFrame(TRADES, SILVER_SCHEMA)
    rows = build_candles_1m(df).collect()
    return {(r.symbol, str(r.candle_ts)): r for r in rows}


def test_one_row_per_symbol_and_minute(candles):
    assert sorted(candles) == [
        ("BTCUSDT", "2026-06-01 00:00:00"),
        ("BTCUSDT", "2026-06-01 00:01:00"),
        ("ETHUSDT", "2026-06-01 00:00:00"),
    ]


def test_open_and_close_follow_trade_id_not_row_order(candles):
    c = candles[("BTCUSDT", "2026-06-01 00:00:00")]
    # agg_trade_id 1 is the first trade of the minute, 3 the last -- even though
    # the input listed id 3 first.
    assert float(c.open_price) == 100.0
    assert float(c.close_price) == 102.0


def test_high_and_low_are_the_price_extremes(candles):
    c = candles[("BTCUSDT", "2026-06-01 00:00:00")]
    assert float(c.high_price) == 105.0
    assert float(c.low_price) == 100.0
    assert c.low_price <= c.open_price <= c.high_price
    assert c.low_price <= c.close_price <= c.high_price


def test_volume_quote_volume_and_trade_count(candles):
    c = candles[("BTCUSDT", "2026-06-01 00:00:00")]
    assert float(c.volume) == 6.0                      # 1 + 2 + 3
    assert float(c.quote_volume) == 617.0              # 102 + 200 + 315
    assert c.trade_count == 3


def test_a_single_trade_makes_a_flat_candle(candles):
    c = candles[("ETHUSDT", "2026-06-01 00:00:00")]
    assert {float(c.open_price), float(c.high_price),
            float(c.low_price), float(c.close_price)} == {2000.0}
    assert c.trade_count == 1


def test_buy_and_sell_volume_split(candles):
    c = candles[("BTCUSDT", "2026-06-01 00:00:00")]
    # Concrete values guard against a regression in the when/otherwise split:
    # id 2 BUY 3 + id 3 BUY 1, against id 1 SELL 2.
    assert float(c.buy_volume) == 4.0
    assert float(c.sell_volume) == 2.0
    # The invariant that must hold for every candle. It also catches a null
    # `side`, which otherwise(0) would silently drop from both buckets.
    assert float(c.buy_volume) + float(c.sell_volume) == float(c.volume)
