"""Unit tests for src/common/lake.py.

The predicate helpers are small but load-bearing: `replaceWhere` decides
which rows a write is allowed to delete. A predicate that is too wide would
quietly remove another file's data, and one that does not match the written
rows makes Delta reject the write. Both are worth pinning down.
"""

import pytest

from src.common.lake import month_bounds, sql_literal, symbol_date_predicate


@pytest.mark.parametrize("month,expected", [
    ("2026-01", ("2026-01-01", "2026-01-31")),
    ("2026-06", ("2026-06-01", "2026-06-30")),
    ("2026-02", ("2026-02-01", "2026-02-28")),
    # 2028 is a leap year: a hard-coded 28 would cut a day of trades.
    ("2028-02", ("2028-02-01", "2028-02-29")),
    ("2026-12", ("2026-12-01", "2026-12-31")),
])
def test_month_bounds(month, expected):
    assert month_bounds(month) == expected


def test_sql_literal_quotes_and_escapes():
    assert sql_literal("BTCUSDT") == "'BTCUSDT'"
    # A quote in the value would otherwise end the literal early and leave a
    # broken predicate rather than a wrong one.
    assert sql_literal("O'Hara") == "'O''Hara'"


def test_symbol_date_predicate_covers_exactly_one_symbol_and_month():
    start, end = month_bounds("2026-06")
    predicate = symbol_date_predicate("BTCUSDT", start, end)
    assert predicate == (
        "symbol = 'BTCUSDT' "
        "and trade_date >= '2026-06-01' "
        "and trade_date <= '2026-06-30'"
    )


def test_predicate_names_the_partition_columns():
    # Both columns are Silver partition columns, which is what lets Delta
    # replace whole partitions instead of scanning the table.
    predicate = symbol_date_predicate("ETHUSDT", "2026-07-01", "2026-07-31")
    assert "symbol" in predicate
    assert "trade_date" in predicate
