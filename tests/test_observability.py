"""Unit tests for src/common/observability.py.

This module makes two promises the rest of the platform relies on, and
neither had a test:

  1. a metrics write that fails must NOT fail the pipeline
  2. an alert that cannot be delivered must NOT mask the real failure

Both are "swallow the exception" paths, which is exactly the kind of code
that looks fine and silently stops working. No Spark session and no network
are involved: the writer chain and requests.post are both faked.
"""

import json
import logging

import pytest
import requests

from src.common import observability


class FakeWriter:
    """Accepts the whole .write.format().option()...save() chain."""

    def __init__(self, calls):
        self.calls = calls

    def format(self, name):
        self.calls.append(("format", name))
        return self

    def option(self, key, value):
        self.calls.append(("option", key))
        return self

    def mode(self, name):
        self.calls.append(("mode", name))
        return self

    def save(self):
        self.calls.append(("save", None))


class FakeDataFrame:
    def __init__(self, calls):
        self.calls = calls
        self.write = FakeWriter(calls)

    def coalesce(self, n):
        self.calls.append(("coalesce", n))
        return self


class FakeSpark:
    def __init__(self, calls, fail=False):
        self.calls = calls
        self.fail = fail
        self.rows = None
        self.schema = None

    def createDataFrame(self, rows, schema):  # noqa: N802 - Spark's spelling
        self.rows = rows
        self.schema = schema
        if self.fail:
            raise RuntimeError("warehouse unreachable")
        return FakeDataFrame(self.calls)


@pytest.fixture
def warehouse_env(monkeypatch):
    for key, value in {
        "MSSQL_HOST": "mssql", "MSSQL_PORT": "1433", "MSSQL_DB": "CryptoDWH",
        "MSSQL_USER": "crypto_app", "MSSQL_PASSWORD": "secret",
    }.items():
        monkeypatch.setenv(key, value)


# --- json_log ---------------------------------------------------------------

def test_json_log_emits_one_parseable_line(caplog):
    with caplog.at_level(logging.INFO, logger="airflow.task"):
        payload = observability.json_log("silver_done", symbol="BTCUSDT", rows=5)

    assert len(caplog.records) == 1
    logged = json.loads(caplog.records[0].getMessage())
    assert logged == payload
    assert logged["event"] == "silver_done"
    assert logged["symbol"] == "BTCUSDT"
    assert logged["rows"] == 5
    # A fixed envelope is the point: every line carries a timestamp.
    assert logged["ts"].endswith("+00:00")


def test_json_log_survives_a_value_json_cannot_encode(caplog):
    # Metrics dicts carry dates and Decimals. default=str is what keeps a log
    # call from raising inside a job that otherwise succeeded.
    from decimal import Decimal

    with caplog.at_level(logging.INFO, logger="airflow.task"):
        observability.json_log("x", amount=Decimal("1.5"))

    assert json.loads(caplog.records[0].getMessage())["amount"] == "1.5"


# --- write_run_metrics ------------------------------------------------------

def test_no_rows_writes_nothing(warehouse_env):
    calls = []
    assert observability.write_run_metrics(FakeSpark(calls), "silver", []) == 0
    assert calls == []


def test_rows_are_written_and_counted(warehouse_env):
    calls = []
    spark = FakeSpark(calls)
    rows = [
        {"entity": "BTCUSDT|2026-06", "rows_in": 10, "rows_valid": 9,
         "rows_rejected": 1, "seconds": 1.5, "details": {"a": 1}},
        {"entity": "ETHUSDT|2026-06", "status": "skipped"},
    ]

    assert observability.write_run_metrics(spark, "silver", rows) == 2
    assert ("save", None) in calls
    assert ("mode", "append") in calls

    # The layer is stamped on every row, and details is serialised to a string
    # because the column is NVARCHAR, not a struct.
    assert [r["layer"] for r in spark.rows] == ["silver", "silver"]
    assert spark.rows[0]["details"] == '{"a": 1}'
    # A row that does not say otherwise counts as processed.
    assert spark.rows[0]["status"] == "processed"
    assert spark.rows[1]["status"] == "skipped"
    # Missing counts stay null rather than becoming 0, which would read as
    # "nothing was rejected" instead of "not measured".
    assert spark.rows[1]["rows_in"] is None


def test_a_failing_metrics_write_is_swallowed(caplog):
    # The promise: observability must never fail the pipeline. Note there is
    # no warehouse_env here -- the write fails before the URL is even built.
    calls = []
    with caplog.at_level(logging.WARNING, logger="airflow.task"):
        written = observability.write_run_metrics(
            FakeSpark(calls, fail=True), "silver",
            [{"entity": "BTCUSDT|2026-06"}])

    assert written == 0
    assert ("save", None) not in calls
    logged = json.loads(caplog.records[-1].getMessage())
    assert logged["event"] == "metrics_write_failed"
    assert "warehouse unreachable" in logged["error"]


# --- on_failure_alert -------------------------------------------------------

class FakeTaskInstance:
    dag_id = "binance_silver"
    task_id = "transform"
    try_number = 2
    log_url = "http://127.0.0.1:8085/log"


def failure_context():
    return {"task_instance": FakeTaskInstance(),
            "exception": RuntimeError("spark died")}


def test_a_failure_is_logged_even_without_a_webhook(monkeypatch, caplog):
    monkeypatch.delenv(observability.WEBHOOK_ENV, raising=False)

    def explode(*args, **kwargs):
        raise AssertionError("must not post without a webhook configured")

    monkeypatch.setattr(requests, "post", explode)

    with caplog.at_level(logging.ERROR, logger="airflow.task"):
        observability.on_failure_alert(failure_context())

    logged = json.loads(caplog.records[0].getMessage())
    assert logged["event"] == "task_failed"
    assert logged["dag_id"] == "binance_silver"
    assert logged["task_id"] == "transform"
    assert "spark died" in logged["error"]


def test_a_webhook_is_posted_when_configured(monkeypatch, caplog):
    monkeypatch.setenv(observability.WEBHOOK_ENV, "https://hooks.example/abc")
    posted = {}

    def capture(url, json=None, timeout=None):
        posted.update({"url": url, "json": json, "timeout": timeout})

    monkeypatch.setattr(requests, "post", capture)

    with caplog.at_level(logging.ERROR, logger="airflow.task"):
        observability.on_failure_alert(failure_context())

    assert posted["url"] == "https://hooks.example/abc"
    assert "binance_silver" in posted["json"]["text"]
    assert posted["timeout"] == 10


def test_an_undeliverable_alert_does_not_mask_the_real_failure(
        monkeypatch, caplog):
    monkeypatch.setenv(observability.WEBHOOK_ENV, "https://hooks.example/abc")

    def refuse(*args, **kwargs):
        raise requests.ConnectionError("no route to host")

    monkeypatch.setattr(requests, "post", refuse)

    with caplog.at_level(logging.WARNING, logger="airflow.task"):
        # Must not raise: the callback runs while a task is already failing.
        observability.on_failure_alert(failure_context())

    events = [json.loads(r.getMessage())["event"] for r in caplog.records]
    # Both are recorded, and the real failure comes first.
    assert events == ["task_failed", "alert_delivery_failed"]


def test_a_context_without_a_task_instance_still_logs(monkeypatch, caplog):
    # Airflow does not guarantee every key in every callback context.
    monkeypatch.delenv(observability.WEBHOOK_ENV, raising=False)

    with caplog.at_level(logging.ERROR, logger="airflow.task"):
        observability.on_failure_alert({})

    logged = json.loads(caplog.records[0].getMessage())
    assert logged["dag_id"] is None
    assert logged["event"] == "task_failed"
