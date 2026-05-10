from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


YELLOW_TAXI_SCHEMA = StructType(
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
        StructField("cbd_congestion_fee", DoubleType()),
    ]
)
