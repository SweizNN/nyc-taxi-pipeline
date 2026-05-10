import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_spark(app_name: str, include_kafka: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )

    if include_kafka:
        kafka_package = os.getenv(
            "SPARK_KAFKA_PACKAGE",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        builder = builder.config("spark.jars.packages", kafka_package)

    return configure_spark_with_delta_pip(builder).getOrCreate()


def get_bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}
