import json
import random
import time
import uuid
from datetime import datetime

from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

users = [
    "Alice", "Bob", "Charlie", "David", "Emma",
    "Frank", "Grace", "Henry", "Isabella", "Jack"
]

products = [
    ("Laptop", "Electronics", 65000),
    ("Mouse", "Electronics", 700),
    ("Keyboard", "Electronics", 1800),
    ("Headphones", "Electronics", 3500),
    ("Phone", "Electronics", 25000),
    ("Milk", "Groceries", 60),
    ("Bread", "Groceries", 40),
    ("Rice", "Groceries", 1200),
    ("Apple", "Groceries", 180),
    ("Coffee", "Groceries", 450),
    ("T-Shirt", "Fashion", 799),
    ("Jeans", "Fashion", 1899),
    ("Shoes", "Fashion", 2999),
    ("Watch", "Accessories", 4999),
    ("Backpack", "Accessories", 1599)
]

cities = [
    "Chennai",
    "Bangalore",
    "Hyderabad",
    "Mumbai",
    "Delhi",
    "Coimbatore",
    "Madurai",
    "Trichy"
]

payment_methods = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Cash",
    "Net Banking"
]

order_status = [
    "Placed",
    "Packed",
    "Shipped",
    "Delivered"
]


def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(
            f"Order sent -> Partition {msg.partition()} Offset {msg.offset()}"
        )


while True:

    product, category, price = random.choice(products)

    quantity = random.randint(1, 5)

    order = {
        "order_id": str(uuid.uuid4()),
        "user_id": random.randint(1000, 9999),
        "customer_name": random.choice(users),
        "city": random.choice(cities),
        "product": product,
        "category": category,
        "unit_price": price,
        "quantity": quantity,
        "total_amount": price * quantity,
        "payment_method": random.choice(payment_methods),
        "status": random.choice(order_status),
        "discount": random.choice([0, 5, 10, 15]),
        "order_time": datetime.now().isoformat()
    }

    producer.produce(
        topic="orders",
        value=json.dumps(order).encode("utf-8"),
        callback=delivery_report
    )

    producer.poll(0)
    time.sleep(1)