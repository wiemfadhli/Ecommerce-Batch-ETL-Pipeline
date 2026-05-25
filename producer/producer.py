from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

events = ["login", "click", "purchase"]

while True:
    data = {
        "user_id": random.randint(1, 100),
        "event": random.choice(events),
        "amount": random.randint(10, 500),
        "timestamp": datetime.utcnow().isoformat()
    }

    producer.send("events", data)
    print("Sent:", data)
    time.sleep(2)



