import json
import os
from datetime import datetime

import pandas as pd
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "duckdb-consumer",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["orders"])

SAVE_DIR = "data/orders"

os.makedirs(SAVE_DIR, exist_ok=True)

BATCH_SIZE = 100

records = []

print("Listening for Kafka messages...")

try:

    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(msg.error())
            continue

        order = json.loads(msg.value().decode("utf-8"))

        records.append(order)

        if len(records) >= BATCH_SIZE:

            df = pd.DataFrame(records)

            file_name = f"orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"

            file_path = os.path.join(SAVE_DIR, file_name)

            df.to_parquet(
                file_path,
                index=False
            )

            print(f"Saved {len(records)} records -> {file_path}")

            records.clear()

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:

    if records:

        df = pd.DataFrame(records)

        file_name = f"orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"

        df.to_parquet(
            os.path.join(SAVE_DIR, file_name),
            index=False
        )

        print(f"Saved remaining {len(records)} records.")

    consumer.close()