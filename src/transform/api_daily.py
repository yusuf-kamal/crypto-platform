"""Pure transformations for the REST API source (no I/O, unit-testable).

Both APIs deliver one value per day, so everything here lands on a daily
grain and joins to the existing `gold.dim_date`. That is deliberately a
different grain from the Binance path (one minute), which is what makes the
two sources interesting to compare: minute-level trading activity against
daily market cap and daily market sentiment.
"""

from datetime import datetime, timezone



MS_PER_DAY = 86_400_000


def _utc_date(ms: int):
    """Epoch milliseconds -> UTC date. All layers of this project are UTC."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date()


def _as_int(value):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def market_chart_rows(payload: dict, symbol: str, coin_id: str) -> list[dict]:
    """CoinGecko /coins/{id}/market_chart -> one row per day.

    The payload holds three independent arrays of [epoch_ms, value] pairs
    (`prices`, `market_caps`, `total_volumes`). They normally share the same
    timestamps, but nothing guarantees it, so they are indexed by timestamp
    and joined rather than zipped by position.
    """
    def by_ts(key: str) -> dict:
        return {int(point[0]): point[1]
                for point in (payload.get(key) or [])
                if isinstance(point, (list, tuple)) and len(point) == 2}

    prices = by_ts("prices")
    caps = by_ts("market_caps")
    volumes = by_ts("total_volumes")

    return [
        {
            "symbol": symbol,
            "coin_id": coin_id,
            "snapshot_date": _utc_date(ms),
            "price_usd": prices[ms],
            "market_cap_usd": caps.get(ms),
            "total_volume_usd": volumes.get(ms),
            "source_ts_ms": ms,
        }
        for ms in sorted(prices)
    ]


def fear_greed_rows(payload: dict) -> list[dict]:
    """alternative.me /fng/ -> one row per day.

    The API returns every field as a string, newest day first, and the
    `timestamp` is in epoch *seconds*.
    """
    rows = []
    for item in payload.get("data") or []:
        seconds = _as_int(item.get("timestamp"))
        ms = seconds * 1000 if seconds is not None else None
        rows.append({
            "snapshot_date": _utc_date(ms) if ms is not None else None,
            "fng_value": _as_int(item.get("value")),
            "fng_classification": item.get("value_classification"),
            "source_ts_ms": ms,
        })
    return rows


def reject_reason(row: dict) -> str | None:
    """Return a short reason string if this API row must be quarantined.

    Mirrors tag_rejects() in silver_aggtrades.py: a bad row is never allowed
    to fail the job, it is written to the quarantine prefix with a reason.
    """
    if row.get("snapshot_date") is None:
        return "null_or_unparseable_date"

    # CoinGecko's last point is the running, unfinished day: it is stamped at
    # "now" instead of midnight. Keeping it would make the latest row change
    # on every run, so it waits in quarantine until the day is closed.
    source_ms = row.get("source_ts_ms")
    if source_ms is None or source_ms % MS_PER_DAY != 0:
        return "partial_or_unparseable_day"

    if "fng_value" in row:          # Fear & Greed row
        if row["fng_value"] is None:
            return "null_or_unparseable_fng_value"
        if not 0 <= row["fng_value"] <= 100:
            return "fng_value_out_of_range"
        if not row.get("fng_classification"):
            return "missing_fng_classification"
        return None

    price = row.get("price_usd")    # CoinGecko market row
    if price is None:
        return "null_price"
    if price <= 0:
        return "non_positive_price"

    # market_cap_usd and total_volume_usd are deliberately *not* required.
    # CoinGecko leaves them out now and then, and rejecting the row would
    # throw away that day's valid price as well. The fact table carries the
    # nulls instead, which the BI layer can handle.
    return None


def split_valid_rejected(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Partition rows, attaching `reject_reason` to the rejected ones."""
    valid, rejected = [], []
    for row in rows:
        reason = reject_reason(row)
        if reason is None:
            valid.append(row)
        else:
            rejected.append({**row, "reject_reason": reason})
    return valid, rejected
