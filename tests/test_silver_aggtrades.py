import pytest
from pyspark.sql import functions as F

from src.transform.silver_aggtrades import RAW_COLUMNS, parse_raw, tag_rejects

GOOD = ("1", "1.36800000", "982.4", "10", "12",
        "1777593600386207", "False", "True")


def run(spark, rows, month="2026-05"):
    df = spark.createDataFrame(rows, RAW_COLUMNS)
    return tag_rejects(parse_raw(df), month)


def test_valid_row_is_not_rejected(spark):
    row = run(spark, [GOOD]).collect()[0]
    assert row.reject_reason is None
    assert row.side == "BUY"
    assert float(row.quote_value) == pytest.approx(1343.9232)


def test_sell_side_when_buyer_is_maker(spark):
    row = run(spark, [GOOD[:6] + ("True", "True")]).collect()[0]
    assert row.side == "SELL"


def test_timestamp_unit_detected_from_digit_count(spark):
    ms_row = GOOD[:5] + ("1777593600386",) + GOOD[6:]  # 13 خانة
    df = run(spark, [ms_row, GOOD]).select(
        F.date_format("trade_ts", "yyyy-MM-dd HH:mm:ss.SSS").alias("t")
    )
    assert [r.t for r in df.collect()] == ["2026-05-01 00:00:00.386"] * 2


REJECT_CASES = [
    ("price_zero", GOOD[:1] + ("0",) + GOOD[2:],
     "non_positive_price_or_quantity"),
    ("price_not_a_number", GOOD[:1] + ("abc",) +
     GOOD[2:], "null_or_unparseable_field"),
    ("bad_trade_id_range", GOOD[:3] + ("20",
     "10") + GOOD[5:], "invalid_trade_id_range"),
    ("wrong_month", GOOD[:5] + ("1790812800302801",) +
     GOOD[6:], "outside_file_month"),
    ("bad_boolean", GOOD[:6] + ("maybe",) +
     GOOD[7:], "null_or_unparseable_field"),
]


@pytest.mark.parametrize("name,row,expected", REJECT_CASES)
def test_bad_rows_get_a_reject_reason(spark, name, row, expected):
    assert run(spark, [row]).collect()[0].reject_reason == expected
