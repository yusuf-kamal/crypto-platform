"""Pre-publication contracts. Pure Python; Spark jobs pass bounded summaries.

Quarantine is diagnostic output, not publication. Call these gates before any
overwrite of accepted data, and let DataQualityError fail the Airflow task.
"""

import json
import math
import os
from collections import Counter
from datetime import date, timedelta
from uuid import uuid4

from src.common.observability import json_log


class DataQualityError(ValueError):
    def __init__(self, dataset: str, failures: list[str], summary: dict | None = None):
        self.report = {"dataset": dataset, "failures": failures, **(summary or {})}
        super().__init__(f"Data quality failed for {dataset}: " + "; ".join(failures))


def max_rejection_rate() -> float:
    """Read at task runtime, so DAG parsing does not depend on configuration."""
    value = float(os.environ.get("DQ_MAX_REJECTION_RATE", "0.01"))
    if not math.isfinite(value) or not 0 <= value <= 1:
        raise ValueError("DQ_MAX_REJECTION_RATE must be finite and between 0 and 1")
    return value


def date_range(start: date, end: date) -> set[date]:
    return {start + timedelta(days=i) for i in range((end - start).days + 1)}


def validate_summary(dataset: str, *, rows_in: int, rows_valid: int,
                     rows_rejected: int, duplicates: int = 0,
                     observed_symbols=(), required_symbols=(),
                     observed_dates=(), expected_dates=(),
                     rejection_limit: float | None = None) -> dict:
    limit = max_rejection_rate() if rejection_limit is None else rejection_limit
    if not math.isfinite(limit) or not 0 <= limit <= 1:
        raise ValueError("rejection_limit must be finite and between 0 and 1")
    rate = rows_rejected / rows_in if rows_in else 0.0
    missing_symbols = sorted(set(required_symbols) - set(observed_symbols))
    missing_dates = sorted(set(expected_dates) - set(observed_dates))
    failures = []
    if rows_in <= 0:
        failures.append("empty input batch")
    if rows_valid <= 0:
        failures.append("no valid rows")
    if duplicates:
        failures.append(f"duplicate or conflicting business keys: {duplicates}")
    if missing_symbols:
        failures.append(f"missing required symbols: {missing_symbols}")
    if missing_dates:
        failures.append(f"missing expected dates: {len(missing_dates)}")
    if rate > limit:
        failures.append(f"rejection rate {rate:.6f} exceeds {limit:.6f}")
    summary = dict(rows_in=rows_in, rows_valid=rows_valid,
                   rows_rejected=rows_rejected, duplicates=duplicates,
                   rejection_rate=rate, rejection_limit=limit,
                   missing_symbols=missing_symbols,
                   missing_dates=[str(d) for d in missing_dates[:31]])
    if failures:
        raise DataQualityError(dataset, failures, summary)
    return summary


def validate_rows(dataset: str, valid: list[dict], rejected: list[dict], *,
                  key_fields: tuple[str, ...], required_fields: tuple[str, ...],
                  required_symbols=(), expected_dates=(), rejection_limit=None) -> dict:
    """Small API batches only. Large Spark batches use validate_summary."""
    missing = sum(any(r.get(k) is None or r.get(k) == "" for k in required_fields)
                  for r in valid)
    if missing:
        raise DataQualityError(dataset, [f"required fields missing in {missing} rows"])
    keys = Counter(tuple(r.get(k) for k in key_fields) for r in valid + rejected
                   if all(r.get(k) is not None for k in key_fields))
    return validate_summary(
        dataset, rows_in=len(valid) + len(rejected), rows_valid=len(valid),
        rows_rejected=len(rejected), duplicates=sum(n - 1 for n in keys.values()),
        observed_symbols={r.get("symbol") for r in valid},
        required_symbols=required_symbols,
        observed_dates={r.get("snapshot_date") for r in valid},
        expected_dates=expected_dates, rejection_limit=rejection_limit,
    )


def validate_payload(payload, field: str, dataset: str) -> None:
    """Envelope checks before replacing a Bronze API object."""
    if not isinstance(payload, dict) or not isinstance(payload.get(field), list):
        raise DataQualityError(dataset, [f"missing or invalid required array: {field}"])
    if not payload[field]:
        raise DataQualityError(dataset, ["empty input batch"])


def record_failure(error: DataQualityError, write_diagnostic, **context) -> None:
    """Log first, then best-effort immutable JSON. Never mask the gate failure.

    write_diagnostic(key, body) is supplied by S3Hook or boto3 callers. Raw
    payloads remain in Bronze; callers may include a bounded rejected sample.
    """
    report = {**error.report, **context}
    json_log("data_quality_failed", **report)
    key = f"_quarantine/_quality_failures/{uuid4().hex}.json"
    try:
        write_diagnostic(key, json.dumps(report, default=str))
    except Exception as exc:  # diagnostics must not replace the original failure
        json_log("quality_diagnostic_write_failed", error=str(exc), dataset=report["dataset"])
