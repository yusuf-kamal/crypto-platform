"""Structured logging, a run-metrics table, and failure alerts.

Until now every job printed a JSON blob to its task log, which meant the only
way to see how a run went was to open that task in the UI. Three things change
that:

* `json_log()` writes one machine-readable line per event, always with the
  same envelope, so logs can be grepped or shipped somewhere.
* `write_run_metrics()` appends a row per processed entity to
  `ops.pipeline_runs`, which turns run history into something SQL can answer
  ("which symbol has the highest reject rate?").
* `on_failure_alert()` is an Airflow callback: it logs the failure as a
  structured event and, if ALERT_WEBHOOK_URL is set, posts a short message to
  it. Without the variable it stays a log-only alert, so the platform runs
  unchanged on a laptop with no webhook.
"""

import json
import logging
import os
from datetime import datetime, timezone

log = logging.getLogger("airflow.task")

METRICS_TABLE = "ops.pipeline_runs"
WEBHOOK_ENV = "ALERT_WEBHOOK_URL"


def json_log(event: str, level: int = logging.INFO, **fields) -> dict:
    """Emit one structured line and return the payload (handy for XCom)."""
    payload = {
        "ts": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
        "event": event,
        **fields,
    }
    log.log(level, json.dumps(payload, default=str))
    return payload


def _jdbc_url() -> str:
    return (
        f"jdbc:sqlserver://{os.environ['MSSQL_HOST']}:{os.environ['MSSQL_PORT']};"
        f"databaseName={os.environ['MSSQL_DB']};encrypt=true;trustServerCertificate=true"
    )


def write_run_metrics(spark, layer: str, rows: list[dict]) -> int:
    """Append run metrics to ops.pipeline_runs. Never fails the caller.

    A metrics write is observability, not data: if the warehouse is briefly
    unreachable the pipeline must still count as successful, so the exception
    is logged and swallowed.
    """
    if not rows:
        return 0

    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType([
        StructField("layer", StringType(), False),
        StructField("entity", StringType(), False),
        StructField("status", StringType(), False),
        StructField("rows_in", LongType(), True),
        StructField("rows_valid", LongType(), True),
        StructField("rows_rejected", LongType(), True),
        StructField("seconds", DoubleType(), True),
        StructField("details", StringType(), True),
    ])

    prepared = [
        {
            "layer": layer,
            "entity": str(row.get("entity", "")),
            "status": str(row.get("status", "processed")),
            "rows_in": row.get("rows_in"),
            "rows_valid": row.get("rows_valid"),
            "rows_rejected": row.get("rows_rejected"),
            "seconds": row.get("seconds"),
            "details": json.dumps(row.get("details") or {}, default=str),
        }
        for row in rows
    ]

    try:
        (
            spark.createDataFrame(prepared, schema)
            .coalesce(1)
            .write.format("jdbc")
            .option("url", _jdbc_url())
            .option("dbtable", METRICS_TABLE)
            .option("user", os.environ["MSSQL_USER"])
            .option("password", os.environ["MSSQL_PASSWORD"])
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .mode("append")
            .save()
        )
        return len(prepared)
    except Exception as exc:                                  # noqa: BLE001
        json_log("metrics_write_failed", level=logging.WARNING,
                 layer=layer, error=str(exc))
        return 0


def on_failure_alert(context) -> None:
    """Airflow on_failure_callback: structured log + optional webhook."""
    task_instance = context.get("task_instance")
    payload = {
        "dag_id": getattr(task_instance, "dag_id", None),
        "task_id": getattr(task_instance, "task_id", None),
        "try_number": getattr(task_instance, "try_number", None),
        "log_url": getattr(task_instance, "log_url", None),
        "error": str(context.get("exception"))[:500],
    }
    json_log("task_failed", level=logging.ERROR, **payload)

    url = os.environ.get(WEBHOOK_ENV)
    if not url:
        return

    try:
        import requests

        requests.post(
            url,
            json={"text": f"Pipeline failure: {payload['dag_id']}."
                          f"{payload['task_id']} - {payload['error']}"},
            timeout=10,
        )
    except Exception as exc:                                  # noqa: BLE001
        # An alert that cannot be delivered must not mask the real failure.
        json_log("alert_delivery_failed", level=logging.WARNING,
                 error=str(exc))
