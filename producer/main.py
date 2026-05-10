import json
import os
import time
from datetime import datetime, date
from decimal import Decimal

import numpy as np
import pandas as pd
from kafka import KafkaProducer


def json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if pd.isna(value):
        return None
    return str(value)


def row_to_message(row):
    return {
        key: (None if pd.isna(value) else value)
        for key, value in row.items()
    }


def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic = os.getenv("KAFKA_TOPIC", "yellow_taxi_trips")
    parquet_path = os.getenv("RAW_TAXI_PARQUET", "data/raw/yellow_tripdata_2023-01.parquet")
    max_rows = int(os.getenv("PRODUCER_MAX_ROWS", "10000"))
    batch_size = int(os.getenv("PRODUCER_BATCH_SIZE", "500"))
    sleep_seconds = float(os.getenv("PRODUCER_SLEEP_SECONDS", "0.2"))

    if not os.path.exists(parquet_path):
        raise FileNotFoundError(
            f"Missing input file: {parquet_path}. "
            "Download yellow_tripdata_2023-01.parquet into data/raw first."
        )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value, default=json_default).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )

    rows_sent = 0
    for chunk in pd.read_parquet(parquet_path, engine="pyarrow").head(max_rows).to_dict(orient="records"):
        message = row_to_message(chunk)
        key = str(message.get("VendorID", "unknown"))
        producer.send(topic, key=key, value=message)
        rows_sent += 1

        if rows_sent % batch_size == 0:
            producer.flush()
            print(f"Published {rows_sent} rows to topic '{topic}'")
            time.sleep(sleep_seconds)

    producer.flush()
    producer.close()
    print(f"Finished publishing {rows_sent} rows to topic '{topic}'")


if __name__ == "__main__":
    main()
