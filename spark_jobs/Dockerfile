FROM bitnamilegacy/spark:3.5.0

USER root

ENV PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell"

WORKDIR /app

# Bu Dockerfile'ın build context'i projenin kök dizini (`.`) olmalıdır.
COPY spark_jobs/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Gerekli tüm Python script'lerini imajın içine kopyala
COPY spark_jobs/ ./spark_jobs/
COPY ml_pipeline/ ./ml_pipeline/

USER 1001
