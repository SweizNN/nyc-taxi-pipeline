import os

from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    dayofweek,
    from_json,
    hour,
    lit,
    month,
    round as spark_round,
    to_date,
    to_timestamp,
    trim,
    unix_timestamp,
    year,
)

from common import create_spark, get_bool_env
from schemas import YELLOW_TAXI_SCHEMA


def parse_bronze_records(bronze_df):
    parsed_df = bronze_df.select(
        from_json(col("raw_json"), YELLOW_TAXI_SCHEMA).alias("trip"),
        col("topic").alias("source_topic"),
        col("partition").alias("source_partition"),
        col("offset").alias("source_offset"),
        col("kafka_timestamp"),
        col("ingested_at"),
        col("raw_json_sha256"),
    ).filter(col("trip").isNotNull())

    return parsed_df.select(
        "trip.*",
        "source_topic",
        "source_partition",
        "source_offset",
        "kafka_timestamp",
        "ingested_at",
        "raw_json_sha256",
    )


def clean_trips(parsed_df):
    pickup_ts = coalesce(
        to_timestamp("tpep_pickup_datetime"),
        to_timestamp("tpep_pickup_datetime", "yyyy-MM-dd'T'HH:mm:ss"),
    )
    dropoff_ts = coalesce(
        to_timestamp("tpep_dropoff_datetime"),
        to_timestamp("tpep_dropoff_datetime", "yyyy-MM-dd'T'HH:mm:ss"),
    )

    enriched_df = (
        parsed_df.withColumn("pickup_datetime", pickup_ts)
        .withColumn("dropoff_datetime", dropoff_ts)
        .withColumn(
            "trip_duration_minutes",
            spark_round(
                (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60.0,
                2,
            ),
        )
        .withColumn("pickup_date", to_date("pickup_datetime"))
        .withColumn("pickup_hour", hour("pickup_datetime"))
        .withColumn("pickup_day_of_week", dayofweek("pickup_datetime"))
        .withColumn("pickup_month", month("pickup_datetime"))
        .withColumn("pickup_year", year("pickup_datetime"))
    )

    return enriched_df.filter(
        (col("pickup_datetime").isNotNull())
        & (col("dropoff_datetime").isNotNull())
        & (col("dropoff_datetime") > col("pickup_datetime"))
        & (col("trip_distance") > 0)
        & (col("trip_distance") <= 100)
        & (col("fare_amount") > 0)
        & (col("fare_amount") <= 500)
        & (col("trip_duration_minutes") > 0)
        & (col("trip_duration_minutes") <= 240)
        & (col("PULocationID").isNotNull())
        & (col("DOLocationID").isNotNull())
        & ((col("passenger_count").isNull()) | ((col("passenger_count") >= 0) & (col("passenger_count") <= 8)))
    )


def load_zone_lookup(spark, lookup_path):
    zones_df = spark.read.option("header", True).csv(lookup_path)

    return zones_df.select(
        col("LocationID").cast("int").alias("LocationID"),
        trim(col("Borough")).alias("borough"),
        trim(col("Zone")).alias("zone"),
        trim(col("service_zone")).alias("service_zone"),
    )


def enrich_with_zones(clean_df, zones_df):
    pickup_zones = zones_df.select(
        col("LocationID").alias("PULocationID"),
        col("borough").alias("pickup_borough"),
        col("zone").alias("pickup_zone"),
        col("service_zone").alias("pickup_service_zone"),
    )

    dropoff_zones = zones_df.select(
        col("LocationID").alias("DOLocationID"),
        col("borough").alias("dropoff_borough"),
        col("zone").alias("dropoff_zone"),
        col("service_zone").alias("dropoff_service_zone"),
    )

    return (
        clean_df.join(pickup_zones, on="PULocationID", how="left")
        .join(dropoff_zones, on="DOLocationID", how="left")
        .withColumn("pickup_borough", coalesce(col("pickup_borough"), lit("Unknown")))
        .withColumn("pickup_zone", coalesce(col("pickup_zone"), lit("Unknown")))
        .withColumn("pickup_service_zone", coalesce(col("pickup_service_zone"), lit("Unknown")))
        .withColumn("dropoff_borough", coalesce(col("dropoff_borough"), lit("Unknown")))
        .withColumn("dropoff_zone", coalesce(col("dropoff_zone"), lit("Unknown")))
        .withColumn("dropoff_service_zone", coalesce(col("dropoff_service_zone"), lit("Unknown")))
        .withColumn("silver_processed_at", current_timestamp())
    )


def main():
    spark = create_spark("yellow-taxi-bronze-to-silver")

    bronze_path = os.getenv("DELTA_BRONZE_PATH", "/app/data/delta/bronze/yellow_taxi_trips")
    silver_path = os.getenv("DELTA_SILVER_PATH", "/app/data/delta/silver/yellow_taxi_trips")
    lookup_path = os.getenv("TAXI_ZONE_LOOKUP", "/app/data/lookup/taxi_zone_lookup.csv")
    log_counts = get_bool_env("SILVER_LOG_COUNTS", True)

    print(f"Reading Bronze Delta from {bronze_path}")
    bronze_df = spark.read.format("delta").load(bronze_path)

    parsed_df = parse_bronze_records(bronze_df)
    clean_df = clean_trips(parsed_df)
    zones_df = load_zone_lookup(spark, lookup_path)
    silver_df = enrich_with_zones(clean_df, zones_df)

    if log_counts:
        bronze_count = bronze_df.count()
        parsed_count = parsed_df.count()
        silver_count = silver_df.count()
        print(f"Bronze rows: {bronze_count}")
        print(f"Parsed rows: {parsed_count}")
        print(f"Silver rows after cleaning/enrichment: {silver_count}")

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("pickup_year", "pickup_month")
        .save(silver_path)
    )

    print(f"Wrote Silver Delta to {silver_path}")


if __name__ == "__main__":
    main()

