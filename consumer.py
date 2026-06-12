# =============================================================================
# CONSUMER SCRIPT
# Purpose: Reads sensor data messages from Redpanda (Kafka-compatible broker)
#          and stores them in MongoDB database
# Uses retry loops with healthchecks instead of fixed sleep delays
# Uses Python logging module for professional log output
# =============================================================================

import json
import time
import logging
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os

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
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
TOPIC = 'sensor-data'
MAX_RETRIES = 10
RETRY_DELAY = 5  # Seconds between retries

# =============================================================================
# FUNCTION: wait_for_kafka
# Retries connection to Redpanda until successful or max retries reached
# Returns KafkaConsumer instance when connected
# =============================================================================
def wait_for_kafka():
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            attempt += 1
            logger.info(f"[KAFKA] Connection attempt {attempt}/{MAX_RETRIES}...")
            
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',  # Start from beginning if no offset exists
                group_id='sensor-group',       # Consumer group for offset tracking
                request_timeout_ms=30000
            )
            logger.info("[KAFKA] Successfully connected to Redpanda!")
            return consumer
            
        except NoBrokersAvailable:
            logger.warning(f"[KAFKA] No brokers available. Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
            
        except Exception as e:
            logger.error(f"[KAFKA] Connection failed: {e}. Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
    
    logger.critical(f"[KAFKA] Could not connect after {MAX_RETRIES} attempts. Exiting.")
    raise RuntimeError(f"Could not connect to Redpanda after {MAX_RETRIES} attempts")
    
# =============================================================================
# FUNCTION: wait_for_mongodb
# Retries connection to MongoDB until successful or max retries reached
# Returns MongoDB collection instance when connected
# =============================================================================
def wait_for_mongodb():
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            attempt += 1
            logger.info(f"[MONGODB] Connection attempt {attempt}/{MAX_RETRIES}...")
            
            # Create MongoDB client with timeout
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            
            # Force connection check - will raise error if MongoDB not ready
            client.admin.command('ping')
            
            # Select database and collection
            db = client['sensordata']           
            collection = db['measurements']     
            
            logger.info("[MONGODB] Successfully connected to MongoDB!")
            return collection
            
        except ConnectionFailure:
            logger.warning(f"[MONGODB] Not available. Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
            
        except Exception as e:
            logger.error(f"[MONGODB] Connection failed: {e}. Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
    
    # All retries exhausted
    logger.critical(f"[MONGODB] Could not connect after {MAX_RETRIES} attempts. Exiting.")
    raise RuntimeError(f"Could not connect to MongoDB after {MAX_RETRIES} attempts")

# =============================================================================
# MAIN FUNCTION
# Sets up connections and continuously reads and stores incoming sensor data
# =============================================================================
def main():
    logger.info("Starting consumer...")
    logger.info(f"Kafka broker: {KAFKA_BROKER}")
    logger.info(f"MongoDB URI: {MONGO_URI}")

    # Wait for Redpanda using healthcheck retry loop
    consumer = wait_for_kafka()

    # Wait for MongoDB using healthcheck retry loop
    collection = wait_for_mongodb()

    logger.info("All connections established! Waiting for messages...")

    # Main message processing loop: runs continuously
    for message in consumer:
        try:
            # Extract data from Kafka message
            data = message.value

            # Store measurement document in MongoDB
            collection.insert_one(data)

            
            if data.get('status') == 'ok':
                
                logger.info(f"[OK] Saved: temp={data.get('temperature')}°C, "
                           f"humidity={data.get('humidity')}%, "
                           f"wind={data.get('wind_speed')}km/h, "
                           f"at={data.get('fetched_at')}")
            else:
                
                logger.warning(f"[HEALTH WARNING] Error received: "
                              f"type={data.get('error_type')}, "
                              f"message={data.get('message')}, "
                              f"at={data.get('fetched_at')}")

        except Exception as e:
            # Unexpected error - log and continue processing
            logger.error(f"[ERROR] Failed to save to MongoDB: {e}")


if __name__ == "__main__":
    main()