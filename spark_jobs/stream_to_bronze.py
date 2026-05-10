import os

from pyspark.sql.functions import col, current_timestamp, length, sha2

from common import create_spark


def main():
    spark = create_spark("yellow-taxi-stream-to-bronze", include_kafka=True)

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "yellow_taxi_trips")
    starting_offsets = os.getenv("BRONZE_STARTING_OFFSETS", "earliest")
    max_offsets = os.getenv("BRONZE_MAX_OFFSETS_PER_TRIGGER")
    bronze_path = os.getenv("DELTA_BRONZE_PATH", "/app/data/delta/bronze/yellow_taxi_trips")
    checkpoint_path = os.getenv(
        "BRONZE_CHECKPOINT_PATH",
        "/app/data/delta/checkpoints/bronze_yellow_taxi_trips",
    )

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
    )

    if max_offsets:
        reader = reader.option("maxOffsetsPerTrigger", max_offsets)

    stream_df = reader.load()

    bronze_df = stream_df.select(
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("raw_json"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at"),
    ).filter(
        col("raw_json").isNotNull() & (length(col("raw_json")) > 0)
    ).withColumn(
        "raw_json_sha256",
        sha2(col("raw_json"), 256),
    )

    print(
        "Starting Bronze streaming job "
        f"topic={topic} bootstrap={kafka_bootstrap} output={bronze_path} checkpoint={checkpoint_path}"
    )

    query = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", bronze_path)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
