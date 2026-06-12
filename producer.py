# =============================================================================
# PRODUCER SCRIPT
# Purpose: Fetches real-time environmental data from Open-Meteo API
#          and sends it to Redpanda (Kafka-compatible message broker)
# Uses retry loops with healthchecks instead of fixed sleep delays
# Uses Python logging module for professional log output
# =============================================================================

import requests
import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import os
from datetime import datetime

# =============================================================================
# LOGGING CONFIGURATION
# Format: timestamp - level - message
# Levels: DEBUG < INFO < WARNING < ERROR < CRITICAL
# =============================================================================
logging.basicConfig(
    level=logging.INFO,  # Show INFO and above (INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    datefmt='%Y-%m-%d %H:%M:%S'  # Timestamp format
)
logger = logging.getLogger(__name__)  # Create logger for this module

# =============================================================================
# CONFIGURATION
# Read from environment variables defined in docker-compose.yml
# =============================================================================
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
TOPIC = 'sensor-data'
MAX_RETRIES = 10
RETRY_DELAY = 5  # Seconds between retries
# =============================================================================
# FUNCTION: wait_for_kafka
# Retries connection to Redpanda until successful or max retries reached
# Returns KafkaProducer instance when connected
# =============================================================================
def wait_for_kafka():
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            attempt += 1
            logger.info(f"[KAFKA] Connection attempt {attempt}/{MAX_RETRIES}...")
            
            # Try to create Kafka producer - will fail if Redpanda not ready
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                # Serialize Python dict to JSON bytes before sending
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Timeout for connection attempt in milliseconds
                request_timeout_ms=30000
            )
            logger.info("[KAFKA] Successfully connected to Redpanda!")
            return producer
            
        except NoBrokersAvailable:
            # Redpanda not ready yet - wait and retry
            logger.warning(f"[KAFKA] No brokers available. Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
            
        except Exception as e:
            # Other connection error - wait and retry
            logger.error(f"[KAFKA] Connection failed: {e}. Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
    
    # All retries exhausted
    logger.critical(f"[KAFKA] Could not connect after {MAX_RETRIES} attempts. Exiting.")
    raise RuntimeError(f"Could not connect to Redpanda after {MAX_RETRIES} attempts")

# =============================================================================
# FUNCTION: get_sensor_data
# Connects to Open-Meteo API and fetches current weather measurements
# Returns a dictionary with sensor readings and health status
# =============================================================================
def get_sensor_data():
    # Open-Meteo API endpoint for weather forecast
    url = "https://api.open-meteo.com/v1/forecast"
    
    # Parameters: location (Berlin), measurements to fetch, timezone
    params = {
        "latitude": 52.52,
        "longitude": 13.41,
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "wind_speed_10m"
        ],
        "timezone": "Europe/Berlin"
    }
    
    # Make HTTP request with 10 second timeout (health monitoring)
    response = requests.get(url, params=params, timeout=10)
    
    # Raise exception if HTTP error occurred (e.g. 404, 500)
    response.raise_for_status()
    
    # Parse JSON response
    data = response.json()
    current = data["current"]
    
    # Return structured sensor reading with health status
    return {
        "timestamp": current["time"],
        "temperature": current["temperature_2m"],   # Celsius
        "humidity": current["relative_humidity_2m"], # Percent
        "wind_speed": current["wind_speed_10m"],     # km/h
        "location": "Berlin",
        "status": "ok",
        "fetched_at": datetime.utcnow().isoformat()  # UTC timestamp for health monitoring
    }

# =============================================================================
# MAIN FUNCTION
# Sets up Kafka producer and continuously sends sensor data to Redpanda
# =============================================================================
def main():
    logger.info("Starting producer...")
    logger.info(f"Kafka broker: {KAFKA_BROKER}")

    # Wait for Redpanda using healthcheck retry loop
    producer = wait_for_kafka()

    # Main data collection loop: runs continuously
    while True:
        try:
            # Fetch fresh sensor data from API
            data = get_sensor_data()
            
            # Send data to Redpanda topic and wait for confirmation
            future = producer.send(TOPIC, value=data)
            
            # Wait up to 10 seconds for delivery confirmation
            # Raises exception if message was not delivered
            record_metadata = future.get(timeout=10)
            
            # Log successful health status with delivery confirmation
            logger.info(f"[HEALTH OK] Sent: temp={data['temperature']}°C, "
                       f"humidity={data['humidity']}%, "
                       f"wind={data['wind_speed']}km/h, "
                       f"at={data['fetched_at']} "
                       f"(topic={record_metadata.topic}, "
                       f"partition={record_metadata.partition}, "
                       f"offset={record_metadata.offset})")
            
            # Wait 10 seconds before next measurement
            time.sleep(10)
            
        except requests.exceptions.Timeout:
            # API did not respond within 10 seconds - health alert
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
            # API is completely unreachable - health alert
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
            # Unexpected error - log and continue
            logger.error(f"[HEALTH ERROR] Unexpected error: {e}")
            time.sleep(5)

# Entry point: run main function when script is executed
if __name__ == "__main__":
    main()