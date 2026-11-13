"""Unit tests for the REST API transforms (src/transform/api_daily.py).

These are plain Python -- no Spark, no network -- because the functions under
test only reshape JSON. The payload fixtures are trimmed copies of real
responses from both APIs.
"""

from datetime import date

import pytest

from src.transform.api_daily import (
    fear_greed_rows,
    market_chart_rows,
    reject_reason,
    split_valid_rejected,
)

DAY_1 = 1763078400000  # 2025-11-14 00:00 UTC
DAY_2 = 1763164800000  # 2025-11-15 00:00 UTC

MARKET_PAYLOAD = {
    "prices": [[DAY_1, 924.53], [DAY_2, 915.91]],
    "market_caps": [[DAY_1, 1.28e11], [DAY_2, 1.27e11]],
    "total_volumes": [[DAY_1, 1.5e9], [DAY_2, 1.6e9]],
}

FNG_PAYLOAD = {
    "name": "Fear and Greed Index",
    "data": [
        {"value": "20", "value_classification": "Extreme Fear",
         "timestamp": "1794528000", "time_until_update": "45438"},
        {"value": "28", "value_classification": "Fear",
         "timestamp": "1794441600"},
    ],
    "metadata": {"error": None},
}


def test_market_chart_rows_one_row_per_day():
    rows = market_chart_rows(MARKET_PAYLOAD, "BNBUSDT", "binancecoin")
    assert [r["snapshot_date"] for r in rows] == [
        date(2025, 11, 14), date(2025, 11, 15)]
    assert all(r["symbol"] == "BNBUSDT" for r in rows)
    assert all(r["coin_id"] == "binancecoin" for r in rows)


def test_market_chart_rows_carry_all_three_measures():
    first = market_chart_rows(MARKET_PAYLOAD, "BNBUSDT", "binancecoin")[0]
    assert first["price_usd"] == 924.53
    assert first["market_cap_usd"] == 1.28e11
    assert first["total_volume_usd"] == 1.5e9


def test_market_measures_are_matched_by_timestamp_not_position():
    # market_caps is missing DAY_1 and lists the days in the other order.
    # Zipping by position would attach DAY_2's cap to DAY_1.
    payload = {
        "prices": [[DAY_1, 924.53], [DAY_2, 915.91]],
        "market_caps": [[DAY_2, 1.27e11]],
        "total_volumes": [],
    }
    rows = market_chart_rows(payload, "BNBUSDT", "binancecoin")
    assert rows[0]["market_cap_usd"] is None
    assert rows[1]["market_cap_usd"] == 1.27e11
    assert rows[0]["total_volume_usd"] is None


def test_market_chart_rows_are_sorted_and_tolerate_an_empty_payload():
    reversed_payload = {"prices": [[DAY_2, 915.91], [DAY_1, 924.53]]}
    rows = market_chart_rows(reversed_payload, "BNBUSDT", "binancecoin")
    assert [r["snapshot_date"] for r in rows] == [
        date(2025, 11, 14), date(2025, 11, 15)]
    assert market_chart_rows({}, "BNBUSDT", "binancecoin") == []


def test_fear_greed_rows_parse_strings_and_second_precision_timestamps():
    rows = fear_greed_rows(FNG_PAYLOAD)
    assert rows[0] == {
        "snapshot_date": date(2026, 11, 13),
        "fng_value": 20,
        "fng_classification": "Extreme Fear",
        "source_ts_ms": 1794528000000,
    }
    assert rows[1]["fng_value"] == 28
    assert isinstance(rows[0]["fng_value"], int)


def test_fear_greed_rows_keep_unparseable_values_as_none():
    # A bad row must survive parsing so that reject_reason() can quarantine it
    # with a reason, instead of the job dying on a ValueError.
    payload = {"data": [{"value": "n/a", "value_classification": None,
                         "timestamp": "not-a-number"}]}
    row = fear_greed_rows(payload)[0]
    assert row["fng_value"] is None
    assert row["snapshot_date"] is None


# --- quality gate -----------------------------------------------------------

GOOD_MARKET = {
    "symbol": "BNBUSDT", "coin_id": "binancecoin",
    "snapshot_date": date(2025, 11, 14), "price_usd": 924.53,
    "market_cap_usd": 1.28e11, "total_volume_usd": 1.5e9,
    "source_ts_ms": DAY_1,
}

GOOD_FNG = {
    "snapshot_date": date(2026, 11, 13), "fng_value": 20,
    "fng_classification": "Extreme Fear", "source_ts_ms": 1794528000000,
}


def test_good_rows_of_both_shapes_pass():
    assert reject_reason(GOOD_MARKET) is None
    assert reject_reason(GOOD_FNG) is None


def test_market_row_keeps_its_price_when_only_the_aggregates_are_missing():
    # The deliberate exception: a missing market cap or volume must not cost
    # us the valid price for that day.
    row = {**GOOD_MARKET, "market_cap_usd": None, "total_volume_usd": None}
    assert reject_reason(row) is None


def test_the_running_unfinished_day_is_quarantined():
    # CoinGecko stamps the last point at "now", not at midnight.
    row = {**GOOD_MARKET, "source_ts_ms": DAY_1 + 37 * 60 * 1000}
    assert reject_reason(row) == "partial_or_unparseable_day"


@pytest.mark.parametrize("patch,expected", [
    ({"snapshot_date": None}, "null_or_unparseable_date"),
    ({"source_ts_ms": None}, "partial_or_unparseable_day"),
    ({"price_usd": None}, "null_price"),
    ({"price_usd": 0}, "non_positive_price"),
    ({"price_usd": -1.0}, "non_positive_price"),
])
def test_bad_market_rows_get_a_reason(patch, expected):
    assert reject_reason({**GOOD_MARKET, **patch}) == expected


@pytest.mark.parametrize("patch,expected", [
    ({"fng_value": None}, "null_or_unparseable_fng_value"),
    ({"fng_value": 101}, "fng_value_out_of_range"),
    ({"fng_value": -1}, "fng_value_out_of_range"),
    ({"fng_classification": None}, "missing_fng_classification"),
    ({"fng_classification": ""}, "missing_fng_classification"),
])
def test_bad_fear_greed_rows_get_a_reason(patch, expected):
    assert reject_reason({**GOOD_FNG, **patch}) == expected


def test_split_attaches_the_reason_and_never_raises():
    bad = {**GOOD_FNG, "fng_value": 500}
    valid, rejected = split_valid_rejected([GOOD_FNG, bad])
    assert valid == [GOOD_FNG]
    assert rejected[0]["reject_reason"] == "fng_value_out_of_range"
    # The original rows are left untouched.
    assert "reject_reason" not in bad
