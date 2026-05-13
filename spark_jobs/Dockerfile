FROM bitnamilegacy/spark:3.5.0

USER root

ENV PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell"

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER 1001

