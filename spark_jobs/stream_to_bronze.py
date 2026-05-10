import os

from pyspark.sql.functions import col, current_timestamp

from common import create_spark


def main():
    spark = create_spark("yellow-taxi-stream-to-bronze")

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "yellow_taxi_trips")
    bronze_path = os.getenv("DELTA_BRONZE_PATH", "/app/data/delta/bronze/yellow_taxi_trips")
    checkpoint_path = os.getenv(
        "BRONZE_CHECKPOINT_PATH",
        "/app/data/delta/checkpoints/bronze_yellow_taxi_trips",
    )

    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    bronze_df = stream_df.select(
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("raw_json"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at"),
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

