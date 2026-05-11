# NYC Yellow Taxi Big Data Pipeline

End-to-end big data project for NYC Yellow Taxi trip records. The pipeline replays historical TLC Parquet data through Kafka as a pseudo-stream, processes it with Spark, stores curated layers in Delta Lake, and tracks fare prediction experiments with MLflow.

## Project Scope

- Dataset: NYC TLC Yellow Taxi Trip Records
- Initial data target: `yellow_tripdata_2023-01.parquet`
- Lookup data: `taxi_zone_lookup.csv`
- Main prediction target: `fare_amount`
- Optional second target: `trip_duration_minutes`

## Architecture

```text
NYC Yellow Taxi Parquet
        |
        v
Kafka Producer
        |
        v
Kafka topic: yellow_taxi_trips
        |
        v
Spark Structured Streaming
        |
        v
Delta Bronze
        |
        v
Spark batch cleaning + zone lookup join
        |
        v
Delta Silver
        |
        v
Spark batch feature engineering
        |
        v
Delta Gold
        |
        v
ML training + MLflow tracking
```

## Repository Layout

```text
producer/       Kafka producer that replays Parquet rows as JSON messages
spark_jobs/     Spark jobs for Bronze, Silver, and Gold Delta layers
ml_pipeline/    Fare prediction training and MLflow logging
scripts/        Helper scripts for local pipeline execution
docs/           Architecture and presentation notes
data/           Local-only raw, lookup, and Delta data folders
```

## Data Files

Do not commit large data files to GitHub. Download these files locally:

- `data/raw/yellow_tripdata_2023-01.parquet`
- `data/lookup/taxi_zone_lookup.csv`

The `.gitignore` keeps large raw and Delta files out of version control.

## Quick Start

Start infrastructure:

```powershell
docker compose up -d kafka zookeeper mlflow spark
```

### Producer Dry Run

Use dry-run mode before starting Kafka publishing. It reads the Parquet file, normalizes rows to JSON-compatible records, and validates batching without connecting to Kafka.

Docker one-shot:

```powershell
docker compose --profile pipeline build producer
docker compose --profile pipeline run --rm --no-deps -e PRODUCER_DRY_RUN=true -e PRODUCER_MAX_ROWS=1000 -e PRODUCER_BATCH_SIZE=250 producer
```

Use `-e LOG_LEVEL=DEBUG` to print a short sample JSON payload in dry-run logs.

### Publish To Kafka

After dry-run succeeds, publish records to Kafka:

```powershell
docker compose --profile pipeline run --rm producer
```

Producer settings are controlled with environment variables:

```text
PRODUCER_MAX_ROWS=10000       # use 0 to replay all rows
PRODUCER_BATCH_SIZE=500       # Kafka publish/flush batch size
PRODUCER_SLEEP_SECONDS=0.2    # delay between batches for pseudo-streaming
PRODUCER_KEY_FIELD=PULocationID
PRODUCER_DRY_RUN=false        # true validates Parquet/JSON without Kafka publish
```

### Build Bronze Layer

After the producer publishes messages to Kafka, run the streaming job. It reads JSON messages from Kafka and writes raw records with Kafka metadata to Delta Bronze.

```powershell
docker compose exec spark spark-submit spark_jobs/stream_to_bronze.py
```

Useful Bronze environment variables:

```text
BRONZE_STARTING_OFFSETS=earliest
BRONZE_MAX_OFFSETS_PER_TRIGGER=     # optional throttle
BRONZE_CHECKPOINT_PATH=/app/data/delta/checkpoints/bronze_yellow_taxi_trips
```

### Build Silver Layer

The Silver job parses Bronze JSON, filters invalid trips, calculates time features, and enriches pickup/dropoff IDs with `taxi_zone_lookup.csv`.

```powershell
docker compose exec spark spark-submit spark_jobs/bronze_to_silver.py
```

Inspect Silver output:

```powershell
docker compose exec spark spark-sql -e 'SELECT pickup_borough, dropoff_borough, COUNT(*) AS trips FROM delta.`/app/data/delta/silver/yellow_taxi_trips` GROUP BY pickup_borough, dropoff_borough LIMIT 10'
```

### Build Gold Layer

```powershell
docker compose exec spark spark-submit spark_jobs/silver_to_gold.py
```

Run model training:

```powershell
docker compose exec spark spark-submit ml_pipeline/model_training.py
```

MLflow UI:

```text
http://localhost:5000
```

## Team Workflow

Suggested feature branches:

- `feature/kafka-producer`
- `feature/spark-bronze-silver`
- `feature/mlflow-training`
- `feature/docker-docs`

Each member should make real, explainable commits in their own branch.
