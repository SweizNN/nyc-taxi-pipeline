import os

from pyspark.sql.functions import col, lower, when

from common import create_spark


def main():
    spark = create_spark("yellow-taxi-silver-to-gold")

    silver_path = os.getenv("DELTA_SILVER_PATH", "/app/data/delta/silver/yellow_taxi_trips")
    gold_path = os.getenv("DELTA_GOLD_PATH", "/app/data/delta/gold/fare_features")

    silver_df = spark.read.format("delta").load(silver_path)

    gold_df = silver_df.select(
        col("fare_amount").alias("label"),
        "trip_distance",
        "passenger_count",
        "pickup_hour",
        "pickup_day_of_week",
        "PULocationID",
        "DOLocationID",
        "RatecodeID",
        "payment_type",
        "pickup_borough",
        "dropoff_borough",
        "pickup_zone",
        "dropoff_zone",
    ).withColumn(
        "is_airport_trip",
        when(
            lower(col("pickup_zone")).contains("airport")
            | lower(col("dropoff_zone")).contains("airport"),
            1,
        ).otherwise(0),
    )

    gold_df = gold_df.na.fill(
        {
            "passenger_count": 1.0,
            "RatecodeID": 1.0,
            "payment_type": 1,
            "pickup_borough": "Unknown",
            "dropoff_borough": "Unknown",
            "pickup_zone": "Unknown",
            "dropoff_zone": "Unknown",
        }
    )

    gold_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path)


if __name__ == "__main__":
    main()
