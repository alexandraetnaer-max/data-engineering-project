# Producer: fetches real-time weather data from Open-Meteo API
# and publishes it to Redpanda (Kafka-compatible broker).
# Includes retry logic for broker connection and health-event reporting.

import requests
import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Read from environment variables (configured in docker-compose.yml)
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
TOPIC = 'sensor-data'
MAX_RETRIES = 10
RETRY_DELAY = 5


def wait_for_kafka():
    """Retry connecting to Redpanda until successful or MAX_RETRIES exhausted."""
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            attempt += 1
            logger.info(f"[KAFKA] Connection attempt {attempt}/{MAX_RETRIES}...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=30000
            )
            logger.info("[KAFKA] Successfully connected to Redpanda!")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"[KAFKA] No brokers available. Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
        except Exception as e:
            logger.error(f"[KAFKA] Connection failed: {e}. Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

    logger.critical(f"[KAFKA] Could not connect after {MAX_RETRIES} attempts. Exiting.")
    raise RuntimeError(f"Could not connect to Redpanda after {MAX_RETRIES} attempts")


def get_sensor_data():
    """Fetch current weather measurements from Open-Meteo API for Berlin."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.52,   # Berlin
        "longitude": 13.41,
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "wind_speed_10m"
        ],
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
    logger.info("Starting producer...")
    logger.info(f"Kafka broker: {KAFKA_BROKER}")

    producer = wait_for_kafka()

    while True:
        try:
            data = get_sensor_data()
            future = producer.send(TOPIC, value=data)
            # Wait for delivery confirmation (raises on failure)
            record_metadata = future.get(timeout=10)
            logger.info(f"[HEALTH OK] Sent: temp={data['temperature']}°C, "
                       f"humidity={data['humidity']}%, "
                       f"wind={data['wind_speed']}km/h, "
                       f"at={data['fetched_at']} "
                       f"(topic={record_metadata.topic}, "
                       f"partition={record_metadata.partition}, "
                       f"offset={record_metadata.offset})")
            time.sleep(10)

        except requests.exceptions.Timeout:
            error = {
                "status": "error",
                "error_type": "timeout",
                "message": "API request timed out",
                "fetched_at": datetime.utcnow().isoformat()
            }
            future = producer.send(TOPIC, value=error)
            future.get(timeout=10)
            logger.warning(f"[HEALTH WARNING] API timeout at {error['fetched_at']}")
            time.sleep(10)

        except requests.exceptions.ConnectionError:
            error = {
                "status": "error",
                "error_type": "connection_error",
                "message": "API is unavailable",
                "fetched_at": datetime.utcnow().isoformat()
            }
            producer.send(TOPIC, value=error)
            logger.error(f"[HEALTH ERROR] API unavailable at {error['fetched_at']}")
            time.sleep(10)

        except Exception as e:
            logger.error(f"[HEALTH ERROR] Unexpected error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()