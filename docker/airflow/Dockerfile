FROM apache/airflow:3.3.2

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless curl \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/opt/java

# Spark jars: S3A (object store), SQL Server JDBC, the Kafka source for
# Structured Streaming, and Delta Lake for the Silver layer. Versions match
# PySpark 3.5.1 / Scala 2.12 (delta-spark 3.2.0 is the Spark 3.5 line).
#
# kafka-clients stays at 3.4.1 on purpose, even though the broker in
# docker-compose.yml is Kafka 3.9.0. 3.4.1 is the version
# spark-sql-kafka-0-10_2.12:3.5.1 declares as its own dependency, so it is
# what Spark was built against -- not a pin someone forgot to bump. Kafka has
# had bidirectional client/broker compatibility since 0.10.2 and an older
# client against a newer broker is the supported direction; "fixing" this to
# 3.9.0 would introduce a mismatch rather than remove one.
#
# The retry flags are not optional. This layer pulls about 280 MB, 268 MB of
# which is aws-java-sdk-bundle alone, and a plain `curl -fsSLO` does not retry
# a partial transfer: one dropped connection anywhere in those 280 MB fails
# with exit 18 (CURLE_PARTIAL_FILE) and discards the whole layer. Because this
# is the last RUN of the build, that costs the entire image -- measured at ten
# and a half minutes on one machine. --retry-all-errors is what makes curl
# retry a truncated transfer at all, and --continue-at - resumes instead of
# restarting 268 MB. The URLs themselves are verified: all nine resolve.
ARG CURL_RETRY="--retry 5 --retry-delay 3 --retry-all-errors --continue-at -"
RUN mkdir -p /opt/spark-jars && cd /opt/spark-jars \
    && for url in \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/13.4.0.jre11/mssql-jdbc-13.4.0.jre11.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
    ; do \
        echo "fetching $(basename "$url")" \
        && curl -fsSLO $CURL_RETRY "$url" ; \
    done
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
