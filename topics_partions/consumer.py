import json
import random
import time
from uuid import uuid4
from datetime import datetime

import psycopg2
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9093",
    value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8")
)

connection = psycopg2.connect(
    host="localhost",
    port=5433,
    database="kafka_practice",
    user="postgres",
    password="admin123"
)

cursor = connection.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS transactions(
    transaction_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(50),
    account_number VARCHAR(30),
    amount DECIMAL(12,2),
    merchant VARCHAR(100),
    city VARCHAR(100),
    payment_type VARCHAR(50),
    transaction_time TIMESTAMP
)
""")

connection.commit()

TOPIC = "transactions"

cities = [
    "Chennai",
    "Bangalore",
    "Hyderabad",
    "Mumbai",
    "Delhi",
    "Coimbatore",
    "Madurai",
    "Pune",
    "Kolkata",
    "Ahmedabad"
]

merchants = [
    "Amazon",
    "Flipkart",
    "Swiggy",
    "Zomato",
    "Myntra",
    "Reliance",
    "BigBasket",
    "IRCTC",
    "DMart",
    "Ajio"
]

payment_types = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking"
]


def generate_transaction():

    return {
        "transaction_id": str(uuid4()),
        "customer_id": f"CUST{random.randint(1000,9999)}",
        "account_number": f"XXXX{random.randint(1000,9999)}",
        "amount": round(random.uniform(100,100000),2),
        "merchant": random.choice(merchants),
        "city": random.choice(cities),
        "payment_type": random.choice(payment_types),
        "transaction_time": datetime.now()
    }


print("Generator Started...")

while True:

    batch = []

    for _ in range(20):

        transaction = generate_transaction()

        producer.send(TOPIC, transaction)

        batch.append(transaction)

    producer.flush()

    rows = []

    for transaction in batch:

        rows.append(
            (
                transaction["transaction_id"],
                transaction["customer_id"],
                transaction["account_number"],
                transaction["amount"],
                transaction["merchant"],
                transaction["city"],
                transaction["payment_type"],
                transaction["transaction_time"]
            )
        )

    cursor.executemany(
        """
        INSERT INTO transactions
        (
            transaction_id,
            customer_id,
            account_number,
            amount,
            merchant,
            city,
            payment_type,
            transaction_time
        )
        VALUES
        (
            %s,%s,%s,%s,%s,%s,%s,%s
        )
        """,
        rows
    )

    connection.commit()

    print(f"Generated {len(rows)} transactions")

    time.sleep(1)