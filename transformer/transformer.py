from kafka import KafkaConsumer
import json
import psycopg2
from collections import defaultdict

# Kafka consumer
consumer = KafkaConsumer(
    "raw-events",
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda v: json.loads(v.decode()),
    auto_offset_reset="earliest"
)

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="postgres",
    dbname="streaming_db",
    user="de_user",
    password="de_pass"
)
cur = conn.cursor()

# Create table for events
cur.execute("""
CREATE TABLE IF NOT EXISTS events (
    user_id INT,
    event TEXT,
    amount INT,
    amount_eur NUMERIC,
    is_purchase BOOLEAN,
    user_total_spend INT,
    timestamp TIMESTAMP
)
""")
conn.commit()

# Stateful aggregation
user_total_spend = defaultdict(int)

print("Transformer started... Writing to PostgreSQL")

for msg in consumer:
    event = msg.value

    # 1️⃣ Clean data
    if "user_id" not in event or "amount" not in event:
        continue

    # 2️⃣ Enrichment
    event["amount_eur"] = round(event["amount"] * 0.93, 2)
    event["is_purchase"] = event["event"] == "purchase"

    # 3️⃣ Stateful transformation
    user_total_spend[event["user_id"]] += event["amount"]
    event["user_total_spend"] = user_total_spend[event["user_id"]]

    # 4️⃣ Insert into PostgreSQL
    cur.execute("""
        INSERT INTO events (user_id, event, amount, amount_eur, is_purchase, user_total_spend, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        event["user_id"],
        event["event"],
        event["amount"],
        event["amount_eur"],
        event["is_purchase"],
        event["user_total_spend"],
        event["timestamp"]
    ))
    conn.commit()

    print("Processed & inserted:", event)
