import os

from pyspark.sql import SparkSession

JARS = (
    "/opt/spark-jars/hadoop-aws-3.3.4.jar,"
    "/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar,"
    "/opt/spark-jars/mssql-jdbc-13.4.0.jre11.jar,"
    "/opt/spark-jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,"
    "/opt/spark-jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,"
    "/opt/spark-jars/kafka-clients-3.4.1.jar,"
    "/opt/spark-jars/commons-pool2-2.11.1.jar"
)


def get_spark(app_name: str) -> SparkSession:
    return (
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
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )
