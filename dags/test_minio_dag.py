from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="test_minio_connection",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["test"],
)
def test_minio_connection():
    @task
    def write_test_file():
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id="minio_s3")
        hook.load_string(
            string_data="hello from airflow",
            key="test/hello.txt",
            bucket_name="bronze",
            replace=True,
        )

    write_test_file()


test_minio_connection()