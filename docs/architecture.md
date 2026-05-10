# Architecture

This project uses a medallion architecture to separate raw ingestion, cleaned records, and machine-learning-ready features.

## Bronze Layer

The Bronze layer stores Kafka messages with minimal transformation. It preserves the original event payload, Kafka metadata, and ingestion timestamp.

## Silver Layer

The Silver layer parses raw JSON records into typed columns, filters invalid taxi trips, calculates trip duration, and enriches pickup/dropoff locations using the NYC taxi zone lookup table.

## Gold Layer

The Gold layer contains leakage-safe features for fare prediction. It excludes fare-related columns that would reveal the prediction target.

## Pseudo-Streaming

NYC TLC data is historical, not a live API stream. To satisfy the streaming requirement, historical Parquet rows are replayed through Kafka with configurable batch size and delay. This models a real ingestion stream while keeping the project reproducible.

## Data Leakage Rules

For `fare_amount` prediction, the model must not use columns such as:

- `fare_amount`
- `total_amount`
- `tip_amount`
- `tolls_amount`
- `extra`
- `mta_tax`
- `improvement_surcharge`
- `congestion_surcharge`
- `airport_fee`
- `cbd_congestion_fee`
- `trip_duration_minutes` if the business question is pre-trip fare prediction

For duration prediction, `tpep_dropoff_datetime` is used only to compute the label and must not be used as a model feature.
