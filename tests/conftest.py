import pytest


@pytest.fixture(scope="session")
def spark():
    # Imported inside the fixture, not at module level: the Airflow DAG-import
    # CI job installs Airflow but not PySpark, and a top-level import here
    # would make pytest fail while merely *collecting* tests it is going to
    # skip anyway.
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("unit-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()
