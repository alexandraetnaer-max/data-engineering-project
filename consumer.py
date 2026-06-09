# =============================================================================
# CONSUMER SCRIPT
# Purpose: Reads sensor data messages from Redpanda (Kafka-compatible broker)
#          and stores them in MongoDB database
# Uses retry loops with healthchecks instead of fixed sleep delays
# =============================================================================

import json          # For deserializing JSON messages from Redpanda
import time          # For adding delays between retries
from kafka import KafkaConsumer  # For reading messages from Redpanda
from kafka.errors import NoBrokersAvailable  # Specific Kafka connection error
from pymongo import MongoClient  # For connecting to MongoDB database
from pymongo.errors import ConnectionFailure  # Specific MongoDB connection error
import os            # For reading environment variables

# =============================================================================
# CONFIGURATION
# These values are read from environment variables (set in docker-compose.yml)
# =============================================================================
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')  # Redpanda address
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')  # MongoDB address
TOPIC = 'sensor-data'   # Name of the Kafka topic to read from
MAX_RETRIES = 10         # Maximum number of connection attempts
RETRY_DELAY = 5          # Seconds to wait between retries

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
            print(f"[KAFKA] Connection attempt {attempt}/{MAX_RETRIES}...", flush=True)
            
            # Try to create Kafka consumer - will fail if Redpanda not ready
            consumer = KafkaConsumer(
                TOPIC,                          # Topic to read from
                bootstrap_servers=KAFKA_BROKER, # Redpanda address
                # Deserialize JSON bytes to Python dict
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                # Start from beginning if no offset exists
                auto_offset_reset='earliest',
                # Consumer group ID for offset tracking
                group_id='sensor-group',
                # Timeout for connection attempt in milliseconds
                request_timeout_ms=5000
            )
            print("[KAFKA] Successfully connected to Redpanda!", flush=True)
            return consumer
            
        except NoBrokersAvailable:
            # Redpanda not ready yet - wait and retry
            print(f"[KAFKA] No brokers available. Retrying in {RETRY_DELAY}s...", flush=True)
            time.sleep(RETRY_DELAY)
            
        except Exception as e:
            # Other connection error - wait and retry
            print(f"[KAFKA] Connection failed: {e}. Retrying in {RETRY_DELAY}s...", flush=True)
            time.sleep(RETRY_DELAY)
    
    # All retries exhausted
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
            print(f"[MONGODB] Connection attempt {attempt}/{MAX_RETRIES}...", flush=True)
            
            # Create MongoDB client
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            
            # Force connection check - will raise error if MongoDB not ready
            client.admin.command('ping')
            
            # Select database and collection
            db = client['sensordata']           # Database name
            collection = db['measurements']     # Collection (table) name
            
            print("[MONGODB] Successfully connected to MongoDB!", flush=True)
            return collection
            
        except ConnectionFailure:
            # MongoDB not ready yet - wait and retry
            print(f"[MONGODB] Not available. Retrying in {RETRY_DELAY}s...", flush=True)
            time.sleep(RETRY_DELAY)
            
        except Exception as e:
            # Other connection error - wait and retry
            print(f"[MONGODB] Connection failed: {e}. Retrying in {RETRY_DELAY}s...", flush=True)
            time.sleep(RETRY_DELAY)
    
    # All retries exhausted
    raise RuntimeError(f"Could not connect to MongoDB after {MAX_RETRIES} attempts")

# =============================================================================
# MAIN FUNCTION
# Sets up connections and continuously reads and stores incoming sensor data
# =============================================================================
def main():
    print("Starting consumer...", flush=True)
    print(f"Kafka broker: {KAFKA_BROKER}", flush=True)
    print(f"MongoDB URI: {MONGO_URI}", flush=True)

    # Wait for Redpanda using healthcheck retry loop
    consumer = wait_for_kafka()
    
    # Wait for MongoDB using healthcheck retry loop
    collection = wait_for_mongodb()

    print("All connections established! Waiting for messages...", flush=True)

    # Main message processing loop: runs continuously
    # Waits for new messages from Redpanda and stores them
    for message in consumer:
        try:
            # Extract data from Kafka message
            data = message.value
            
            # Store measurement document in MongoDB
            # MongoDB automatically adds '_id' field
            collection.insert_one(data)
            
            # Log stored measurement with health status
            if data.get('status') == 'ok':
                # Normal sensor reading - log success
                print(f"[OK] Saved to MongoDB: {data}", flush=True)
            else:
                # Health error from producer - log warning
                print(f"[WARNING] Health error received: {data}", flush=True)
                
        except Exception as e:
            # Unexpected error - log and continue processing
            print(f"[ERROR] Failed to save to MongoDB: {e}", flush=True)

# Entry point: run main function when script is executed
if __name__ == "__main__":
    main()