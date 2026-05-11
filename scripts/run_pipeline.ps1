$ErrorActionPreference = "Stop"

Write-Host "Starting infrastructure..."
docker compose up -d zookeeper kafka mlflow spark

Write-Host "Publishing sample taxi records to Kafka..."
docker compose --profile pipeline run --rm producer

Write-Host "Writing Kafka messages to Bronze Delta..."
docker compose exec spark spark-submit spark_jobs/stream_to_bronze.py

Write-Host "Building Silver Delta layer..."
docker compose exec spark spark-submit spark_jobs/bronze_to_silver.py

Write-Host "Building Gold Delta feature layer..."
docker compose exec spark spark-submit spark_jobs/silver_to_gold.py

Write-Host "Training fare prediction models and logging to MLflow..."
docker compose exec spark spark-submit ml_pipeline/model_training.py

Write-Host "Done. Open MLflow at http://localhost:5000"

