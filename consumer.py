import json
import time
from kafka import KafkaConsumer
from pymongo import MongoClient
import os

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
TOPIC = 'sensor-data'

def main():
    print("Starting consumer...", flush=True)
    print(f"Connecting to Kafka at {KAFKA_BROKER}", flush=True)
    time.sleep(50)
    
    while True:
        try:
            print("Trying to connect to Redpanda...", flush=True)
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='sensor-group'
            )
            print("Connected to Redpanda!", flush=True)
            break
        except Exception as e:
            print(f"Connection failed: {e}", flush=True)
            time.sleep(10)

    print("Connecting to MongoDB...", flush=True)
    client = MongoClient(MONGO_URI)
    db = client['sensordata']
    collection = db['measurements']
    print("Connected to MongoDB!", flush=True)
    
    for message in consumer:
        try:
            data = message.value
            collection.insert_one(data)
            print(f"Saved to MongoDB: {data}", flush=True)
        except Exception as e:
            print(f"Error: {e}", flush=True)

if __name__ == "__main__":
    main()