"""Every DAG file must import cleanly.

A DAG that raises at parse time does not disappear loudly -- it shows up as
an import error on a page nobody opens, and its schedule simply never fires.
This test turns that into a failing build.

It needs Airflow itself, which the PySpark test job does not install, so it
skips there and runs in the dedicated CI job.
"""

import pytest

pytest.importorskip("airflow", reason="only runs in the Airflow CI job")

import inspect  # noqa: E402
from pathlib import Path  # noqa: E402

from airflow.models import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"

EXPECTED_DAG_IDS = {
    "binance_batch_ingestion",
    "binance_silver",
    "binance_gold",
    "api_daily_ingestion",
    "api_daily_warehouse",
    "dbt_build",
    "lake_maintenance",
}


@pytest.fixture(scope="module")
def dag_bag():
    # DagBag's signature changed across Airflow versions -- `include_examples`
    # is gone in Airflow 3.3 -- and this test is meant to survive an upgrade
    # rather than become the thing that breaks on one. Examples are off via
    # AIRFLOW__CORE__LOAD_EXAMPLES anyway; the keyword is only passed when the
    # installed version still accepts it.
    kwargs = {"dag_folder": str(DAGS_DIR)}
    if "include_examples" in inspect.signature(DagBag.__init__).parameters:
        kwargs["include_examples"] = False
    return DagBag(**kwargs)


def test_no_import_errors(dag_bag):
    assert dag_bag.import_errors == {}, dag_bag.import_errors


def test_every_expected_dag_is_present(dag_bag):
    assert EXPECTED_DAG_IDS <= set(dag_bag.dag_ids)


def test_every_dag_file_defines_at_least_one_dag(dag_bag):
    dag_files = {p.name for p in DAGS_DIR.glob("*.py")}
    found = {Path(dag.fileloc).name for dag in dag_bag.dags.values()}
    assert dag_files <= found, dag_files - found


def test_every_task_has_a_failure_callback(dag_bag):
    # Alerting is only worth anything if no DAG is missing it.
    missing = [
        f"{dag_id}.{task.task_id}"
        for dag_id, dag in dag_bag.dags.items()
        for task in dag.tasks
        if not task.on_failure_callback
    ]
    assert not missing, missing
