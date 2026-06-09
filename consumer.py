# =============================================================================
# CONSUMER SCRIPT
# Purpose: Reads sensor data messages from Redpanda (Kafka-compatible broker)
#          and stores them in MongoDB database
# =============================================================================

import json          # For deserializing JSON messages from Redpanda
import time          # For adding delays before connecting
from kafka import KafkaConsumer  # For reading messages from Redpanda
from pymongo import MongoClient  # For connecting to MongoDB database
import os            # For reading environment variables

# =============================================================================
# CONFIGURATION
# These values are read from environment variables (set in docker-compose.yml)
# =============================================================================
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')  # Redpanda address
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')  # MongoDB address
TOPIC = 'sensor-data'  # Name of the Kafka topic to read from

# =============================================================================
# MAIN FUNCTION
# Sets up Kafka consumer and MongoDB connection,
# then continuously reads and stores incoming sensor data
# =============================================================================
def main():
    print("Starting consumer...", flush=True)
    print(f"Connecting to Kafka at {KAFKA_BROKER}", flush=True)
    
    # Wait for Redpanda and producer to fully start before connecting
    time.sleep(50)

    # Retry loop: keep trying to connect to Redpanda until successful
    while True:
        try:
            print("Trying to connect to Redpanda...", flush=True)
            
            # Create Kafka consumer
            consumer = KafkaConsumer(
                TOPIC,                          # Topic to read from
                bootstrap_servers=KAFKA_BROKER, # Redpanda address
                # Deserialize JSON bytes to Python dict
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                # Start from beginning if no offset exists
                auto_offset_reset='earliest',
                # Consumer group ID for offset tracking
                group_id='sensor-group'
            )
            print("Connected to Redpanda!", flush=True)
            break  # Exit retry loop on successful connection
            
        except Exception as e:
            # Connection failed - log error and retry after 10 seconds
            print(f"Connection failed: {e}", flush=True)
            time.sleep(10)

    # Connect to MongoDB database
    print("Connecting to MongoDB...", flush=True)
    
    # Create MongoDB client
    client = MongoClient(MONGO_URI)
    
    # Select database named 'sensordata'
    db = client['sensordata']
    
    # Select collection (table) named 'measurements'
    collection = db['measurements']
    
    print("Connected to MongoDB!", flush=True)

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
                print(f"[OK] Saved to MongoDB: {data}", flush=True)
            else:
                # Health error from producer - log warning
                print(f"[WARNING] Health error received: {data}", flush=True)
                
        except Exception as e:
            # Unexpected error - log and continue processing
            print(f"Error saving to MongoDB: {e}", flush=True)

# Entry point: run main function when script is executed
if __name__ == "__main__":
    main()