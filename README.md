\# Data Engineering Project

\## Stream Processing Pipeline with Redpanda and MongoDB



\### What does this project do?

This project implements a real-time stream processing pipeline for environmental sensor data.

It collects weather measurements from the Open-Meteo API and streams them through Redpanda

(a Kafka-compatible message broker) into a MongoDB database.



\*\*Data flow:\*\*

Open-Meteo API → Producer → Redpanda → Consumer → MongoDB



\*\*The system supports three user stories:\*\*

\- 🏙️ City planners monitor temperature and humidity trends to manage public buildings

\- 🚨 Citizen warning system receives alerts when values exceed safe thresholds

\- 🔧 System administrators monitor pipeline health to detect API failures



\---



\### System Architecture

| Component | Technology | Purpose |

|-----------|-----------|---------|

| Message Broker | Redpanda v23.3.11 | Streams sensor data |

| Database | MongoDB 6.0.14 | Stores measurements |

| Producer | Python 3.11 | Fetches data from API |

| Consumer | Python 3.11 | Saves data to MongoDB |

| Containerization | Docker | Portable deployment |



\---



\### Requirements

\- \[Docker Desktop](https://www.docker.com/products/docker-desktop/)

\- Git



No other dependencies needed — everything runs inside Docker containers.



\---



\### How to run



\*\*1. Clone the repository:\*\*

```bash

git clone https://github.com/alexandraetnaer-max/data-engineering-project.git

cd data-engineering-project

```



\*\*2. Start all containers:\*\*

```bash

docker-compose up --build

```



\*\*3. Stop the system:\*\*

```bash

docker-compose down

```



\---



\### How to verify data in MongoDB



\*\*1. Open a new terminal while the system is running\*\*



\*\*2. Connect to MongoDB container:\*\*

```bash

docker exec -it mongodb mongosh

```



\*\*3. Run these commands inside MongoDB shell:\*\*

```javascript

// Select database

use sensordata



// Count stored measurements

db.measurements.countDocuments()



// Show last 5 measurements

db.measurements.find().sort({fetched\_at: -1}).limit(5)



// Show only healthy measurements

db.measurements.find({status: "ok"}).limit(5)



// Show only error measurements (health monitoring)

db.measurements.find({status: "error"}).limit(5)

```



\*\*4. Exit MongoDB shell:\*\*

```javascript

exit

```



\---



\### Health Monitoring

The producer script monitors the health of the Open-Meteo API:

\- ✅ \*\*\[HEALTH OK]\*\* — data fetched and sent successfully

\- ⚠️ \*\*\[HEALTH WARNING]\*\* — API timeout (no response in 10 seconds)

\- ❌ \*\*\[HEALTH ERROR]\*\* — API unavailable (connection failed)



Check producer logs:

```bash

docker logs producer

```



Check consumer logs:

```bash

docker logs consumer

```



\---



\### Project Structure

data-engineering-project/

├── docker-compose.yml    # Orchestrates all containers

├── producer.py           # Fetches API data, sends to Redpanda

├── consumer.py           # Reads from Redpanda, stores in MongoDB

├── Dockerfile.producer   # Docker image for producer

├── Dockerfile.consumer   # Docker image for consumer

├── requirements.txt      # Python dependencies with fixed versions

└── README.md             # This file



\---



\### Author

Alexandra Etnaer — IU Internationale Hochschule

Course: Project: Data Engineering (DLBDSEDE02)

