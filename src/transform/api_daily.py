"""Pure transformations for the REST API source (no I/O, unit-testable).

Both APIs deliver one value per day, so everything here lands on a daily
grain and joins to the existing `gold.dim_date`. That is deliberately a
different grain from the Binance path (one minute), which is what makes the
two sources interesting to compare: minute-level trading activity against
daily market cap and daily market sentiment.
"""

import math
from datetime import date, datetime, timezone

from src.common.data_quality import validate_rows



MS_PER_DAY = 86_400_000


def _utc_date(ms: int):
    """Epoch milliseconds -> UTC date. All layers of this project are UTC."""
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date()
    except (TypeError, ValueError, OverflowError, OSError):
        return None


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
    # Preserve repeated price points: dict-by-timestamp silently discarded them.
    # Optional arrays may be absent, but malformed/duplicate entries are errors.
    payload = payload if isinstance(payload, dict) else {}
    malformed_optional = False

    def by_ts(key: str) -> dict:
        nonlocal malformed_optional
        result = {}
        points = payload.get(key) or []
        if not isinstance(points, list):
            malformed_optional = True
            return result
        for point in points:
            if not isinstance(point, (list, tuple)) or len(point) != 2:
                malformed_optional = True
                continue
            ms = _as_int(point[0])
            if ms is None or ms in result:
                malformed_optional = True
            result[ms] = point[1]
        return result

    caps, volumes = by_ts("market_caps"), by_ts("total_volumes")
    points = payload.get("prices") or []
    if not isinstance(points, list):
        points = [None]
    rows = []
    for point in points:
        well_formed = isinstance(point, (list, tuple)) and len(point) == 2
        ms = _as_int(point[0]) if well_formed else None
        row = {
            "symbol": symbol,
            "coin_id": coin_id,
            "snapshot_date": _utc_date(ms),
            "price_usd": point[1] if well_formed else None,
            "market_cap_usd": caps.get(ms),
            "total_volume_usd": volumes.get(ms),
            "source_ts_ms": ms,
        }
        if malformed_optional:
            row["parse_error"] = "malformed_or_duplicate_optional_points"
        rows.append(row)
    return sorted(rows, key=lambda r: r["source_ts_ms"] or 0)


def fear_greed_rows(payload: dict) -> list[dict]:
    """alternative.me /fng/ -> one row per day.

    The API returns every field as a string, newest day first, and the
    `timestamp` is in epoch *seconds*.
    """
    rows = []
    payload = payload if isinstance(payload, dict) else {}
    items = payload.get("data") or []
    if not isinstance(items, list):
        items = [None]
    for item in items:
        item = item if isinstance(item, dict) else {}
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
    if row.get("parse_error"):
        return row["parse_error"]
    if row.get("snapshot_date") is None:
        return "null_or_unparseable_date"

    # CoinGecko's last point is the running, unfinished day: it is stamped at
    # "now" instead of midnight. Keeping it would make the latest row change
    # on every run, so it waits in quarantine until the day is closed.
    source_ms = row.get("source_ts_ms")
    partial = source_ms is None or source_ms % MS_PER_DAY != 0

    if "fng_value" in row:          # Fear & Greed row
        if row["fng_value"] is None:
            return "null_or_unparseable_fng_value"
        if not 0 <= row["fng_value"] <= 100:
            return "fng_value_out_of_range"
        label = row.get("fng_classification")
        if not isinstance(label, str) or not label.strip():
            return "missing_fng_classification"
        if len(label) > 30:
            return "oversized_fng_classification"
        if partial:
            return "partial_or_unparseable_day"
        return None

    price = row.get("price_usd")    # CoinGecko market row
    if not row.get("symbol") or not row.get("coin_id"):
        return "missing_symbol_or_coin_id"
    if price is None:
        return "null_price"
    for name in ("price_usd", "market_cap_usd", "total_volume_usd"):
        value = row.get(name)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return f"invalid_numeric_{name}"
        try:
            finite = math.isfinite(value)
        except OverflowError:
            finite = False
        if not finite:
            return f"non_finite_{name}"
        # Match SQL Server DECIMAL capacity before a JDBC overwrite starts.
        ceiling = 10 ** (20 if name == "price_usd" else 26)
        if abs(value) >= ceiling:
            return f"out_of_range_{name}"
        if value < 0:
            return "non_positive_price" if name == "price_usd" else f"negative_{name}"
    if price <= 0:
        return "non_positive_price"
    if partial:
        return "partial_or_unparseable_day"

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


def validate_api_batch(dataset, valid, rejected, *, symbols=(), through: date,
                       expected_dates=()):
    """Validate the supplied batch, with coverage only when explicitly declared.

    The expected unfinished intraday point stays quarantined, but does not
    consume the bad-data budget or conflict with a midnight business key.
    `through` is the Bronze logical date, not a guaranteed payload boundary:
    retries fetch current API data and historical extracts may be partial.
    """
    last_day = max((r["snapshot_date"] for r in valid + rejected
                    if r.get("snapshot_date") is not None), default=None)
    bad = [r for r in rejected if not (
        symbols and r["reject_reason"] == "partial_or_unparseable_day"
        and r.get("snapshot_date") == last_day)]
    fields = ("symbol", "coin_id", "snapshot_date", "price_usd") if symbols else (
        "snapshot_date", "fng_value", "fng_classification")
    keys = ("symbol", "snapshot_date") if symbols else ("snapshot_date",)
    validate_rows(dataset, valid, bad, key_fields=keys, required_fields=fields,
                  required_symbols=symbols)
    for symbol in symbols or (None,):
        group = [r for r in valid if symbol is None or r["symbol"] == symbol]
        validate_rows(f"{dataset}:{symbol or 'daily'}", group,
                      [r for r in bad if symbol is None or r.get("symbol") == symbol],
                      key_fields=keys, required_fields=fields, expected_dates=expected_dates)
