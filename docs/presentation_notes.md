# Presentation Notes

## One-Sentence Summary

We built a Dockerized big data pipeline that replays NYC Yellow Taxi trips through Kafka, processes them with Spark into Delta Lake Bronze/Silver/Gold layers, and trains fare prediction models tracked in MLflow.

## Demo Checklist

- Docker containers are running
- Kafka topic exists
- Producer publishes taxi trip messages in configurable batches
- Bronze Delta table is created
- Silver Delta table contains cleaned and enriched trips
- Gold Delta table contains ML-ready features
- MLflow shows at least one experiment run with metrics

## Key Defense Points

- Historical data is replayed as a Kafka pseudo-stream.
- Bronze/Silver/Gold layers make the pipeline easier to debug and explain.
- Fare prediction avoids leakage by removing payment and surcharge columns from features.
- The first version uses one month of data to prove the full pipeline, then can scale to more months.
