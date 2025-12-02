from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Starting ATM Simulation...")

while True:
    # 1. Create a random transaction
    tx = {
        "id": random.randint(1000, 9999),
        "amount": random.randint(10, 1000),  # Random amount $10 to $1000
        "user": f"user_{random.randint(1, 5)}"
    }
    
    # 2. Send to Kafka topic 'transactions'
    producer.send('transactions', tx)
    
    print(f"Sent: {tx}")
    time.sleep(1) # Send 1 per second
