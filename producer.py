import requests
import json
import time
from kafka import KafkaProducer
import os

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
TOPIC = 'sensor-data'

def get_sensor_data():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.52,
        "longitude": 13.41,
        "current": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"],
        "timezone": "Europe/Berlin"
    }
    response = requests.get(url, params=params)
    data = response.json()
    current = data["current"]
    return {
        "timestamp": current["time"],
        "temperature": current["temperature_2m"],
        "humidity": current["relative_humidity_2m"],
        "wind_speed": current["wind_speed_10m"],
        "location": "Berlin"
    }

def main():
    print("Starting producer...", flush=True)
    print(f"Connecting to Kafka at {KAFKA_BROKER}", flush=True)
    time.sleep(40)
    
    while True:
        try:
            print("Trying to connect to Redpanda...", flush=True)
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Redpanda!", flush=True)
            break
        except Exception as e:
            print(f"Connection failed: {e}", flush=True)
            time.sleep(10)
    
    while True:
        try:
            data = get_sensor_data()
            producer.send(TOPIC, value=data)
            print(f"Sent: {data}", flush=True)
            time.sleep(10)
        except Exception as e:
            print(f"Error: {e}", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    main()