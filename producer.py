import requests
import json
import time
from kafka import KafkaProducer
import os
from datetime import datetime

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
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    current = data["current"]
    return {
        "timestamp": current["time"],
        "temperature": current["temperature_2m"],
        "humidity": current["relative_humidity_2m"],
        "wind_speed": current["wind_speed_10m"],
        "location": "Berlin",
        "status": "ok",
        "fetched_at": datetime.utcnow().isoformat()
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
            print(f"[HEALTH OK] Data sent at {data['fetched_at']}: {data}", flush=True)
            time.sleep(10)
        except requests.exceptions.Timeout:
            error = {
                "status": "error",
                "error_type": "timeout",
                "message": "API request timed out",
                "fetched_at": datetime.utcnow().isoformat()
            }
            producer.send(TOPIC, value=error)
            print(f"[HEALTH ERROR] API timeout at {error['fetched_at']}", flush=True)
            time.sleep(10)
        except requests.exceptions.ConnectionError:
            error = {
                "status": "error",
                "error_type": "connection_error",
                "message": "API is unavailable",
                "fetched_at": datetime.utcnow().isoformat()
            }
            producer.send(TOPIC, value=error)
            print(f"[HEALTH ERROR] API unavailable at {error['fetched_at']}", flush=True)
            time.sleep(10)
        except Exception as e:
            print(f"[HEALTH ERROR] Unexpected error: {e}", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    main()