import os

from pyspark.sql.functions import (
    col,
    dayofweek,
    from_json,
    hour,
    round as spark_round,
    to_timestamp,
    unix_timestamp,
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from common import create_spark


TAXI_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", StringType()),
        StructField("tpep_dropoff_datetime", StringType()),
        StructField("passenger_count", DoubleType()),
        StructField("trip_distance", DoubleType()),
        StructField("RatecodeID", DoubleType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("payment_type", IntegerType()),
        StructField("fare_amount", DoubleType()),
        StructField("extra", DoubleType()),
        StructField("mta_tax", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()),
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("congestion_surcharge", DoubleType()),
        StructField("airport_fee", DoubleType()),
    ]
)


def main():
    spark = create_spark("yellow-taxi-bronze-to-silver")

    bronze_path = os.getenv("DELTA_BRONZE_PATH", "/app/data/delta/bronze/yellow_taxi_trips")
    silver_path = os.getenv("DELTA_SILVER_PATH", "/app/data/delta/silver/yellow_taxi_trips")
    lookup_path = os.getenv("TAXI_ZONE_LOOKUP", "/app/data/lookup/taxi_zone_lookup.csv")

    bronze_df = spark.read.format("delta").load(bronze_path)
    parsed_df = bronze_df.select(from_json(col("raw_json"), TAXI_SCHEMA).alias("trip")).select("trip.*")

    trips_df = (
        parsed_df.withColumn("pickup_datetime", to_timestamp("tpep_pickup_datetime"))
        .withColumn("dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
        .withColumn(
            "trip_duration_minutes",
            spark_round(
                (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60.0,
                2,
            ),
        )
        .withColumn("pickup_hour", hour("pickup_datetime"))
        .withColumn("pickup_day_of_week", dayofweek("pickup_datetime"))
    )

    clean_df = trips_df.filter(
        (col("pickup_datetime").isNotNull())
        & (col("dropoff_datetime").isNotNull())
        & (col("trip_distance") > 0)
        & (col("trip_distance") <= 100)
        & (col("fare_amount") > 0)
        & (col("fare_amount") <= 500)
        & (col("trip_duration_minutes") > 0)
        & (col("trip_duration_minutes") <= 240)
        & (col("PULocationID").isNotNull())
        & (col("DOLocationID").isNotNull())
    )

    zones_df = spark.read.option("header", True).csv(lookup_path)
    pickup_zones = zones_df.select(
        col("LocationID").cast("int").alias("PULocationID"),
        col("Borough").alias("pickup_borough"),
        col("Zone").alias("pickup_zone"),
        col("service_zone").alias("pickup_service_zone"),
    )
    dropoff_zones = zones_df.select(
        col("LocationID").cast("int").alias("DOLocationID"),
        col("Borough").alias("dropoff_borough"),
        col("Zone").alias("dropoff_zone"),
        col("service_zone").alias("dropoff_service_zone"),
    )

    silver_df = (
        clean_df.join(pickup_zones, on="PULocationID", how="left")
        .join(dropoff_zones, on="DOLocationID", how="left")
    )

    silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)


if __name__ == "__main__":
    main()

