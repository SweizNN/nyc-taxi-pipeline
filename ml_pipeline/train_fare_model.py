import os
import sys
from pathlib import Path

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor, LinearRegression

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_jobs.common import create_spark


def train_and_log(model_name, estimator, train_df, test_df, feature_pipeline):
    pipeline = Pipeline(stages=feature_pipeline + [estimator])

    with mlflow.start_run(run_name=model_name):
        model = pipeline.fit(train_df)
        predictions = model.transform(test_df)

        evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
        evaluator_mae = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae")
        evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")

        rmse = evaluator_rmse.evaluate(predictions)
        mae = evaluator_mae.evaluate(predictions)
        r2 = evaluator_r2.evaluate(predictions)

        mlflow.log_param("model_type", model_name)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2", r2)
        mlflow.spark.log_model(model, artifact_path="model")

        print(f"{model_name}: RMSE={rmse:.3f}, MAE={mae:.3f}, R2={r2:.3f}")


def main():
    spark = create_spark("yellow-taxi-fare-model-training")

    gold_path = os.getenv("DELTA_GOLD_PATH", "/app/data/delta/gold/fare_features")
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    experiment_name = os.getenv("MLFLOW_EXPERIMENT_NAME", "nyc-yellow-taxi-fare-prediction")

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    df = spark.read.format("delta").load(gold_path).dropna(subset=["label", "trip_distance"])
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    categorical_cols = ["pickup_borough", "dropoff_borough"]
    indexers = [
        StringIndexer(inputCol=col_name, outputCol=f"{col_name}_idx", handleInvalid="keep")
        for col_name in categorical_cols
    ]
    encoders = [
        OneHotEncoder(inputCol=f"{col_name}_idx", outputCol=f"{col_name}_ohe")
        for col_name in categorical_cols
    ]

    numeric_cols = [
        "trip_distance",
        "passenger_count",
        "pickup_hour",
        "pickup_day_of_week",
        "PULocationID",
        "DOLocationID",
        "RatecodeID",
        "payment_type",
        "is_airport_trip",
    ]

    assembler = VectorAssembler(
        inputCols=numeric_cols + [f"{col_name}_ohe" for col_name in categorical_cols],
        outputCol="features",
        handleInvalid="keep",
    )

    feature_pipeline = indexers + encoders + [assembler]

    train_and_log("linear_regression_baseline", LinearRegression(featuresCol="features", labelCol="label"), train_df, test_df, feature_pipeline)
    train_and_log("decision_tree_baseline", DecisionTreeRegressor(featuresCol="features", labelCol="label", maxDepth=6), train_df, test_df, feature_pipeline)


if __name__ == "__main__":
    main()
