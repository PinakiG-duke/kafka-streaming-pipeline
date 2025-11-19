import time
import json
import uuid
import random
from datetime import datetime

from kafka import KafkaProducer
from faker import Faker

fake = Faker()


def generate_synthetic_order():
    """
    Generates synthetic ride-sharing trip data.

    NOTE: We keep the original field names to avoid changing the rest of the pipeline:
      - order_id  -> trip_id
      - status    -> trip_status  (Requested / Ongoing / Completed / Cancelled)
      - category  -> trip_type    (Economy / Premium / Shared / XL)
      - value     -> fare_usd
    """
    trip_types = ["Economy", "Premium", "Shared", "XL"]
    statuses = ["Requested", "Ongoing", "Completed", "Cancelled"]
    cities = [
        "New York",
        "San Francisco",
        "Chicago",
        "Los Angeles",
        "Seattle",
        "Boston",
        "Austin",
    ]
    payment_methods = ["Credit Card", "Cash", "Digital Wallet", "Corporate Account"]
    discounts = [0.0, 0.05, 0.10, 0.15]

    trip_type = random.choice(trip_types)
    status = random.choice(statuses)
    city = random.choice(cities)
    payment_method = random.choice(payment_methods)
    discount = random.choice(discounts)

    # Simple fare model: base + per-km
    distance_km = random.uniform(2, 25)
    base_fare = 3.0
    per_km = 1.25
    gross_fare = base_fare + distance_km * per_km
    net_fare = gross_fare * (1 - discount)

    return {
        "order_id": str(uuid.uuid4())[:8],  # trip_id
        "status": status,
        "category": trip_type,
        "value": round(net_fare, 2),
        "timestamp": datetime.now().isoformat(),
        "city": city,
        "payment_method": payment_method,
        "discount": round(discount, 2),
    }


def run_producer():
    """Kafka producer that sends synthetic ride-sharing trips to the 'orders' topic."""
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")

        count = 0
        while True:
            trip = generate_synthetic_order()
            print(f"[Producer] Sending trip #{count}: {trip}")

            future = producer.send("orders", value=trip)
            record_metadata = future.get(timeout=10)
            print(
                f"[Producer] ✓ Sent to partition {record_metadata.partition} "
                f"at offset {record_metadata.offset}"
            )

            producer.flush()
            count += 1

            sleep_time = random.uniform(0.5, 2.0)
            time.sleep(sleep_time)

    except Exception as e:
        print(f"[Producer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_producer()
