import json
import logging
import math
import os
import signal
import time
from datetime import datetime, date
from decimal import Decimal
from typing import Any

import numpy as np
from kafka import KafkaProducer
from kafka.errors import KafkaError
import pyarrow.parquet as pq


STOP_REQUESTED = False


def configure_logging():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def request_stop(_signum, _frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True


def json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        if np.isnan(value):
            return None
        return float(value)
    if isinstance(value, float) and math.isnan(value):
        return None
    return str(value)


def get_int_env(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def get_float_env(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def get_bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        if np.isnan(value):
            return None
        return float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    return {key: normalize_value(value) for key, value in record.items()}


def iter_parquet_records(parquet_path: str, max_rows: int, batch_size: int):
    parquet_file = pq.ParquetFile(parquet_path)
    rows_remaining = max_rows if max_rows > 0 else None

    for record_batch in parquet_file.iter_batches(batch_size=batch_size):
        for record in record_batch.to_pylist():
            if STOP_REQUESTED:
                return
            if rows_remaining == 0:
                return

            yield normalize_record(record)

            if rows_remaining is not None:
                rows_remaining -= 1


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value, default=json_default).encode("utf-8"),
        key_serializer=lambda value: str(value).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
        compression_type="gzip",
    )


def message_key(record: dict[str, Any], key_field: str) -> str:
    value = record.get(key_field)
    return "unknown" if value is None else str(value)


def publish_batch(producer: KafkaProducer, topic: str, records: list[dict[str, Any]], key_field: str) -> int:
    futures = [
        producer.send(topic, key=message_key(record, key_field), value=record)
        for record in records
    ]

    published = 0
    for future in futures:
        try:
            future.get(timeout=30)
            published += 1
        except KafkaError as exc:
            logging.exception("Kafka publish failed: %s", exc)
            raise

    producer.flush()
    return published


def dry_run_batch(topic: str, records: list[dict[str, Any]], key_field: str, rows_seen: int) -> int:
    for record in records:
        json.dumps(record, default=json_default)

    sample = records[0] if records else {}
    sample_payload = json.dumps(sample, default=json_default)[:500]
    logging.info(
        "Dry-run batch: topic=%s records=%s first_key=%s total_rows=%s",
        topic,
        len(records),
        message_key(sample, key_field),
        rows_seen + len(records),
    )
    logging.debug("Dry-run first payload sample: %s", sample_payload)
    return len(records)


def validate_settings(max_rows: int, batch_size: int, sleep_seconds: float):
    if batch_size <= 0:
        raise ValueError("PRODUCER_BATCH_SIZE must be greater than 0")
    if max_rows < 0:
        raise ValueError("PRODUCER_MAX_ROWS must be 0 or greater")
    if sleep_seconds < 0:
        raise ValueError("PRODUCER_SLEEP_SECONDS must be 0 or greater")


def main():
    configure_logging()
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic = os.getenv("KAFKA_TOPIC", "yellow_taxi_trips")
    parquet_path = os.getenv("RAW_TAXI_PARQUET", "data/raw/yellow_tripdata_2023-01.parquet")
    max_rows = get_int_env("PRODUCER_MAX_ROWS", 10000)
    batch_size = get_int_env("PRODUCER_BATCH_SIZE", 500)
    sleep_seconds = get_float_env("PRODUCER_SLEEP_SECONDS", 0.2)
    key_field = os.getenv("PRODUCER_KEY_FIELD", "PULocationID")
    dry_run = get_bool_env("PRODUCER_DRY_RUN", False)

    validate_settings(max_rows=max_rows, batch_size=batch_size, sleep_seconds=sleep_seconds)

    if not os.path.exists(parquet_path):
        raise FileNotFoundError(
            f"Missing input file: {parquet_path}. "
            "Download yellow_tripdata_2023-01.parquet into data/raw first."
        )

    logging.info(
        "Starting Parquet replay: file=%s topic=%s bootstrap=%s max_rows=%s batch_size=%s delay=%s dry_run=%s",
        parquet_path,
        topic,
        bootstrap_servers,
        "all" if max_rows <= 0 else max_rows,
        batch_size,
        sleep_seconds,
        dry_run,
    )

    producer = None if dry_run else build_producer(bootstrap_servers)
    pending_records = []
    rows_sent = 0

    try:
        for record in iter_parquet_records(parquet_path, max_rows=max_rows, batch_size=batch_size):
            pending_records.append(record)

            if len(pending_records) >= batch_size:
                if dry_run:
                    rows_sent += dry_run_batch(topic, pending_records, key_field, rows_sent)
                else:
                    rows_sent += publish_batch(producer, topic, pending_records, key_field)
                    logging.info("Published %s rows to topic '%s'", rows_sent, topic)
                pending_records = []
                time.sleep(sleep_seconds)

        if pending_records and not STOP_REQUESTED:
            if dry_run:
                rows_sent += dry_run_batch(topic, pending_records, key_field, rows_sent)
            else:
                rows_sent += publish_batch(producer, topic, pending_records, key_field)
                logging.info("Published %s rows to topic '%s'", rows_sent, topic)

    finally:
        if producer is not None:
            producer.flush()
            producer.close()

    action = "validated" if dry_run else "published"
    logging.info("Finished: %s %s rows for topic '%s'", action, rows_sent, topic)


if __name__ == "__main__":
    main()
