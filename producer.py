# =============================================================================
# PRODUCER SCRIPT
# Purpose: Fetches real-time environmental data from Open-Meteo API
#          and sends it to Redpanda (Kafka-compatible message broker)
# =============================================================================

import requests      # For making HTTP requests to Open-Meteo API
import json          # For converting data to JSON format
import time          # For adding delays between requests
from kafka import KafkaProducer  # For sending messages to Redpanda
import os            # For reading environment variables
from datetime import datetime    # For timestamps in health monitoring

# =============================================================================
# CONFIGURATION
# These values are read from environment variables (set in docker-compose.yml)
# =============================================================================
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')  # Redpanda address
TOPIC = 'sensor-data'  # Name of the Kafka topic to send data to

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
        "latitude": 52.52,           # Berlin latitude
        "longitude": 13.41,          # Berlin longitude
        "current": [
            "temperature_2m",        # Temperature at 2 meters height
            "relative_humidity_2m",  # Relative humidity at 2 meters
            "wind_speed_10m"         # Wind speed at 10 meters height
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
        "timestamp": current["time"],                    # Time of measurement
        "temperature": current["temperature_2m"],        # Temperature in Celsius
        "humidity": current["relative_humidity_2m"],     # Humidity in percent
        "wind_speed": current["wind_speed_10m"],         # Wind speed in km/h
        "location": "Berlin",                            # Sensor location
        "status": "ok",                                  # Health status: ok
        "fetched_at": datetime.utcnow().isoformat()      # Exact fetch timestamp
    }

# =============================================================================
# MAIN FUNCTION
# Sets up Kafka producer and continuously sends sensor data to Redpanda
# =============================================================================
def main():
    print("Starting producer...", flush=True)
    print(f"Connecting to Kafka at {KAFKA_BROKER}", flush=True)
    
    # Wait for Redpanda to fully start before connecting
    time.sleep(40)

    # Retry loop: keep trying to connect until successful
    while True:
        try:
            print("Trying to connect to Redpanda...", flush=True)
            
            # Create Kafka producer with JSON serialization
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                # Serialize Python dict to JSON bytes before sending
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Redpanda!", flush=True)
            break  # Exit retry loop on successful connection
            
        except Exception as e:
            # Connection failed - log error and retry after 10 seconds
            print(f"Connection failed: {e}", flush=True)
            time.sleep(10)

    # Main data collection loop: runs continuously
    while True:
        try:
            # Fetch fresh sensor data from API
            data = get_sensor_data()
            
            # Send data to Redpanda topic
            producer.send(TOPIC, value=data)
            
            # Log successful health status with timestamp
            print(f"[HEALTH OK] Data sent at {data['fetched_at']}: {data}", flush=True)
            
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
            # Send error status to Redpanda so consumers know data is missing
            producer.send(TOPIC, value=error)
            print(f"[HEALTH ERROR] API timeout at {error['fetched_at']}", flush=True)
            time.sleep(10)
            
        except requests.exceptions.ConnectionError:
            # API is completely unreachable - health alert
            error = {
                "status": "error",
                "error_type": "connection_error",
                "message": "API is unavailable",
                "fetched_at": datetime.utcnow().isoformat()
            }
            # Send error status to Redpanda
            producer.send(TOPIC, value=error)
            print(f"[HEALTH ERROR] API unavailable at {error['fetched_at']}", flush=True)
            time.sleep(10)
            
        except Exception as e:
            # Unexpected error - log and continue
            print(f"[HEALTH ERROR] Unexpected error: {e}", flush=True)
            time.sleep(5)

# Entry point: run main function when script is executed
if __name__ == "__main__":
    main()