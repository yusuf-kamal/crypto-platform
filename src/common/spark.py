import os

from pyspark.sql import SparkSession

from src.common.lake import LAKE_FORMAT

JARS = (
    "/opt/spark-jars/hadoop-aws-3.3.4.jar,"
    "/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar,"
    "/opt/spark-jars/mssql-jdbc-13.4.0.jre11.jar,"
    "/opt/spark-jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,"
    "/opt/spark-jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,"
    "/opt/spark-jars/kafka-clients-3.4.1.jar,"
    "/opt/spark-jars/commons-pool2-2.11.1.jar,"
    "/opt/spark-jars/delta-spark_2.12-3.2.0.jar,"
    "/opt/spark-jars/delta-storage-3.2.0.jar"
)

# Delta ships as jars only here. The `delta-spark` pip package pins its own
# PySpark version, which would fight the 3.5.1 the rest of the image is built
# around, and nothing in this project needs the Python DeltaTable API.
DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def get_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[4]")
        .config("spark.driver.memory", "6g")
        .config("spark.jars", JARS)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["S3_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Only relevant on the parquet fallback: Delta commits through its own
        # transaction log and does not use the output committer at all, which
        # is what makes its writes atomic.
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    )

    if LAKE_FORMAT == "delta":
        for key, value in DELTA_CONF.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()
