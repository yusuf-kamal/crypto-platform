"""Unit tests for the streaming transformations (src/streaming/trade_events.py).

The functions are ordinary DataFrame -> DataFrame transformations, so they can
be tested on a static DataFrame: no Kafka, no running query. That is the whole
reason the streaming logic lives outside trades_stream_job.py.
"""

import json

import pytest
from pyspark.sql import functions as F

from src.streaming.trade_events import (
    parse_trade_events,
    stream_candles_1m,
    valid_trades,
)


def event(symbol="BTCUSDT", trade_id=1, price="100.0", qty="2.0",
          trade_ms=1764547200000, is_buyer_maker=False):
    """One combined-stream message, as Binance sends it."""
    return json.dumps({
        "stream": f"{symbol.lower()}@trade",
        "data": {"e": "trade", "s": symbol, "t": trade_id, "p": price,
                 "q": qty, "T": trade_ms, "m": is_buyer_maker, "M": True},
    })


def kafka_frame(spark, messages):
    """Imitates the Kafka source: a single binary `value` column."""
    rows = [(m,) for m in messages]
    return (spark.createDataFrame(rows, "value_str string")
            .select(F.col("value_str").cast("binary").alias("value")))


MINUTE = 1764547200000          # 2025-12-01 00:00:00 UTC
NEXT_MINUTE = MINUTE + 60_000


def test_parse_types_strings_and_derives_side_and_quote_value(spark):
    df = parse_trade_events(kafka_frame(spark, [event()]))
    row = df.collect()[0]
    assert row.symbol == "BTCUSDT"
    assert row.trade_id == 1
    assert float(row.price) == 100.0
    assert float(row.quantity) == 2.0
    assert float(row.quote_value) == 200.0
    assert row.side == "BUY"
    assert str(row.trade_ts) == "2025-12-01 00:00:00"


def test_buyer_as_maker_means_the_seller_was_the_aggressor(spark):
    df = parse_trade_events(kafka_frame(spark, [event(is_buyer_maker=True)]))
    assert df.collect()[0].side == "SELL"


def test_a_malformed_message_becomes_nulls_and_is_dropped(spark):
    # from_json returns null for unparseable input instead of raising, which
    # is what keeps an unbounded query alive on one bad message.
    messages = [event(), "not json at all", json.dumps({"stream": "x"})]
    parsed = parse_trade_events(kafka_frame(spark, messages))
    assert parsed.count() == 3
    assert valid_trades(parsed).count() == 1


@pytest.mark.parametrize("bad", [
    {"price": "0"},
    {"price": "-1"},
    {"qty": "0"},
])
def test_non_positive_values_are_dropped(spark, bad):
    parsed = parse_trade_events(kafka_frame(spark, [event(**bad)]))
    assert valid_trades(parsed).count() == 0


def test_stream_candles_group_by_minute_and_symbol(spark):
    messages = [
        event(trade_id=3, price="102", qty="1", trade_ms=MINUTE + 45_000),
        event(trade_id=1, price="100", qty="2", trade_ms=MINUTE + 10_000,
              is_buyer_maker=True),
        event(trade_id=2, price="105", qty="3", trade_ms=MINUTE + 30_000),
        event(trade_id=4, price="101", qty="1", trade_ms=NEXT_MINUTE),
        event(symbol="ETHUSDT", trade_id=9, price="2000", qty="0.5",
              trade_ms=MINUTE + 5_000),
    ]
    parsed = valid_trades(parse_trade_events(kafka_frame(spark, messages)))
    candles = {(r.symbol, str(r.candle_ts)): r
               for r in stream_candles_1m(parsed).collect()}

    assert sorted(candles) == [
        ("BTCUSDT", "2025-12-01 00:00:00"),
        ("BTCUSDT", "2025-12-01 00:01:00"),
        ("ETHUSDT", "2025-12-01 00:00:00"),
    ]

    c = candles[("BTCUSDT", "2025-12-01 00:00:00")]
    # Same invariants as the batch candle: open/close follow trade id.
    assert float(c.open_price) == 100.0
    assert float(c.close_price) == 102.0
    assert float(c.high_price) == 105.0
    assert float(c.low_price) == 100.0
    assert float(c.volume) == 6.0
    assert c.trade_count == 3
    assert float(c.buy_volume) == 4.0
    assert float(c.sell_volume) == 2.0
    assert float(c.buy_volume) + float(c.sell_volume) == float(c.volume)
