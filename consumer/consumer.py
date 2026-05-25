import psycopg2

conn = psycopg2.connect(
    host="postgres",
    dbname="streaming_db",
    user="de_user",
    password="de_pass"
)
cur = conn.cursor()

print("Consumer started: querying PostgreSQL every 5 seconds")

import time
while True:
    cur.execute("""
        SELECT event, COUNT(*), SUM(amount)
        FROM events
        GROUP BY event
        ORDER BY event
    """)
    rows = cur.fetchall()
    print("Aggregated SQL results:")
    for row in rows:
        print(row)
    time.sleep(5)
