"""Runs dbt whenever a staging table has been reloaded.

dbt lives in its own image (Python 3.11, pinned adapter) while Airflow runs
on Python 3.13, so it cannot simply be imported here. The DAG therefore
starts the dbt container through the host's Docker socket, which keeps the
version split from section 7 of the README intact and still puts dbt under
orchestration instead of a manual compose command.
"""

import logging
import os
from datetime import datetime, timedelta

from airflow.sdk import dag

from src.common.observability import on_failure_alert
from src.common.assets import WAREHOUSE_STAGING

log = logging.getLogger("airflow.task")

DBT_IMAGE = os.environ.get("DBT_IMAGE", "crypto-dbt:1.0")
# Fixed by `name: crypto-platform` in docker-compose.yml, so the dbt
# container joins the same network as the warehouse.
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "crypto-platform_default")
DBT_TARGET_VOLUME = os.environ.get("DBT_TARGET_VOLUME",
                                   "crypto-platform_dbt_target")

DBT_ENV = {
    key: os.environ.get(key, "")
    for key in ("MSSQL_HOST", "MSSQL_PORT", "MSSQL_DB",
                "MSSQL_USER", "MSSQL_PASSWORD")
}


def dbt_task(task_id: str, command: str):
    from airflow.providers.docker.operators.docker import DockerOperator
    from docker.types import Mount

    return DockerOperator(
        task_id=task_id,
        image=DBT_IMAGE,
        entrypoint="dbt",
        command=command,
        working_dir="/app/dbt",
        environment={**DBT_ENV, "DBT_PROFILES_DIR": "/app/dbt"},
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        auto_remove="success",
        # dbt writes manifest.json, run_results.json and the catalog into
        # target/. The container is removed after each task, so that folder
        # is a named volume -- otherwise `docs generate` would produce
        # artefacts that nothing can read afterwards.
        mounts=[Mount(source=DBT_TARGET_VOLUME, target="/app/dbt/target",
                      type="volume")],
        mount_tmp_dir=False,
        retrieve_output=False,
    )


@dag(
    dag_id="dbt_build",
    start_date=datetime(2026, 1, 1),
    schedule=[WAREHOUSE_STAGING],
    catchup=False,
    max_active_runs=1,
    tags=["gold", "dbt"],
    default_args={"on_failure_callback": on_failure_alert,
                  "retries": 1, "retry_delay": timedelta(minutes=2)},
)
def dbt_build():
    # `dbt build` would interleave run and test per model, but keeping them
    # apart makes a failing test visible as its own red task in the UI.
    # `dbt docs generate` is the lineage/catalog artefact: it needs a parsed
    # project plus the warehouse catalog, so it runs after the tests pass.
    (
        dbt_task("dbt_run", "run")
        >> dbt_task("dbt_test", "test")
        >> dbt_task("dbt_docs_generate", "docs generate")
    )


dbt_build()
