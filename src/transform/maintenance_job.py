"""Delta housekeeping: compact small files, then reclaim old versions.

Delta keeps superseded files so that time travel works, and nothing reclaims
them on its own -- every re-run of a Silver file leaves its predecessor
behind. Two statements per table fix that:

* `OPTIMIZE` compacts the small files a partitioned write leaves behind.
  Section 10 of the README shows what tiny files cost on object storage.
* `VACUUM` deletes files no version inside the retention window references.
  It runs *after* OPTIMIZE, so the files OPTIMIZE just replaced are the ones
  it has to clean up -- the other order would leave them for a week.

Nothing here touches data: both statements only rewrite or delete files that
the transaction log has already superseded.
"""

import json
import sys
import time

from src.common.lake import (
    LAKE_FORMAT,
    SILVER_TABLES,
    VACUUM_RETAIN_HOURS,
    optimize,
    table_detail,
    table_exists,
    vacuum,
)
from src.common.observability import json_log, write_run_metrics
from src.common.spark import get_spark


def run(retain_hours: int = VACUUM_RETAIN_HOURS) -> dict:
    if LAKE_FORMAT != "delta":
        metrics = {"status": "skipped",
                   "reason": f"LAKE_FORMAT is {LAKE_FORMAT}, not delta"}
        json_log("lake_maintenance_skipped", **metrics)
        return metrics

    started = time.time()
    spark = get_spark("lake_maintenance")

    results = []
    for path in SILVER_TABLES:
        if not table_exists(spark, path):
            # A quarantine prefix with nothing in it, or a streaming table
            # from a path that has never been started, is not a failure.
            results.append({"path": path, "status": "absent"})
            json_log("lake_maintenance_table_absent", path=path)
            continue

        table_started = time.time()
        before = table_detail(spark, path)
        optimize(spark, path)
        vacuum(spark, path, retain_hours)
        after = table_detail(spark, path)

        result = {
            "path": path,
            "status": "maintained",
            "files_before": before["num_files"],
            "files_after": after["num_files"],
            "bytes_before": before["size_bytes"],
            "bytes_after": after["size_bytes"],
            "version": after["version"],
            "seconds": round(time.time() - table_started, 1),
        }
        results.append(result)
        json_log("lake_maintenance_table_done", **result)

    maintained = [r for r in results if r["status"] == "maintained"]
    metrics = {
        "status": "processed",
        "tables_maintained": len(maintained),
        "tables_absent": len(results) - len(maintained),
        "files_reclaimed": sum(r["files_before"] - r["files_after"]
                               for r in maintained),
        "bytes_reclaimed": sum(r["bytes_before"] - r["bytes_after"]
                               for r in maintained),
        "retain_hours": retain_hours,
        "seconds": round(time.time() - started, 1),
    }
    json_log("lake_maintenance_done", **metrics)

    write_run_metrics(spark, "maintenance", [
        {"entity": r["path"], "status": "processed",
         "seconds": r["seconds"], "details": r}
        for r in maintained
    ])

    print(json.dumps(metrics))
    return metrics


if __name__ == "__main__":
    hours = int(sys.argv[1]) if len(sys.argv) > 1 else VACUUM_RETAIN_HOURS
    run(hours)
