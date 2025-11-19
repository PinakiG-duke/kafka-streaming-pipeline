import json
import psycopg2
from kafka import KafkaConsumer


def run_consumer():
    """
    Consumes ride-sharing trip messages from Kafka and inserts them into PostgreSQL.

    NOTE: We still use the existing 'orders' table and column names to minimize
    code changes. Semantically:
      - order_id  -> trip_id
      - status    -> trip_status
      - category  -> trip_type
      - value     -> fare_usd
    """
    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "orders",  # topic name unchanged
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="orders-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")

        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        # Schema unchanged; we just reinterpret columns as trip data.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                order_id VARCHAR(50) PRIMARY KEY,
                status VARCHAR(50),
                category VARCHAR(50),
                value NUMERIC(10, 2),
                timestamp TIMESTAMP,
                city VARCHAR(100),
                payment_method VARCHAR(50),
                discount NUMERIC(4, 2)
            );
            """
        )
        print("[Consumer] ✓ Table 'orders' ready.")
        print("[Consumer] 🎧 Listening for trip messages...\n")

        message_count = 0
        for message in consumer:
            try:
                trip_data = message.value  # formerly order_data

                insert_query = """
                    INSERT INTO orders (order_id, status, category, value, timestamp, city, payment_method, discount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO NOTHING;
                """
                cur.execute(
                    insert_query,
                    (
                        trip_data["order_id"],
                        trip_data["status"],
                        trip_data["category"],
                        trip_data["value"],
                        trip_data["timestamp"],
                        trip_data.get("city", "N/A"),
                        trip_data["payment_method"],
                        trip_data["discount"],
                    ),
                )
                message_count += 1
                print(
                    f"[Consumer] ✓ #{message_count} Inserted trip {trip_data['order_id']} "
                    f"| type={trip_data['category']} | fare=${trip_data['value']} | city={trip_data['city']}"
                )

            except Exception as e:
                print(f"[Consumer ERROR] Failed to process message: {e}")
                continue

    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_consumer()
