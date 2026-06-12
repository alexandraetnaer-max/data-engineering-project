# Data Engineering Project

## Stream Processing Pipeline with Redpanda and MongoDB

### What does this project do?

This project implements a real-time stream processing pipeline for environmental sensor data.

It collects weather measurements from the Open-Meteo API and streams them through Redpanda (a Kafka-compatible message broker) into a MongoDB database.

**Data flow:**

```text
┌─────────────────────┐     every 10s      ┌──────────────┐
│   Open-Meteo API    │ ─────────────────► │   Producer   │
│  (Weather Data)     │                    │   (Python)   │
│  temperature        │                    └──────┬───────┘
│  humidity           │                           │
│  wind speed         │                    publish│message
└─────────────────────┘                           │
                                                  │
                                           ┌──────▼───────┐
                                           │   Redpanda   │
                                           │   (Kafka)    │
                                           │ sensor-data  │
                                           └──────┬───────┘
                                                  │
                                           consume│message
                                                  │
                                           ┌──────▼───────┐     ┌─────────────┐
                                           │   Consumer   │────►│   MongoDB   │
                                           │   (Python)   │     │ sensordata  │
                                           └──────────────┘     │measurements │
                                                                └─────────────┘
```

**Health monitoring:**

```text
┌─────────────────────────────────────────────────────────┐
│ Producer checks API status on every request:            │
│ ✅ [HEALTH OK]       data sent successfully             │
│ ⚠️ [HEALTH WARNING]  API timeout (>10s)                 │
│ ❌ [HEALTH ERROR]    API connection failed              │
└─────────────────────────────────────────────────────────┘
```

**The system supports three user stories:**
- 🏙️ City planners monitor temperature and humidity trends to manage public buildings
- 🚨 Citizen warning system receives alerts when values exceed safe thresholds
- 🔧 System administrators monitor pipeline health to detect API failures

---

### System Architecture

| Component | Technology | Purpose |
|-----------|------------|---------|
| Message Broker | Redpanda v23.3.11 | Streams sensor data |
| Database | MongoDB 6.0.14 | Stores measurements |
| Producer | Python 3.11 | Fetches data from API |
| Consumer | Python 3.11 | Saves data to MongoDB |
| Containerization | Docker | Portable deployment |

---

### Requirements

- Docker Desktop
- Git

No other dependencies are needed — everything runs inside Docker containers.

---

### How to run

**1. Clone the repository:**

```bash
git clone https://github.com/alexandraetnaer-max/data-engineering-project.git
cd data-engineering-project
```

**2. Start all containers:**

```bash
docker-compose up --build
```

**3. Stop the system:**

```bash
docker-compose down
```

---

### How to verify data in MongoDB

**1. Open a new terminal while the system is running**

**2. Connect to the MongoDB container:**

```bash
docker exec -it mongodb mongosh
```

**3. Run these commands inside the MongoDB shell:**

```javascript
use sensordata

db.measurements.countDocuments()

db.measurements.find().sort({ fetched_at: -1 }).limit(5)

db.measurements.find({ status: "ok" }).limit(5)

db.measurements.find({ status: "error" }).limit(5)
```

**4. Exit MongoDB shell:**

```javascript
exit
```

---

### Health Monitoring

The producer script monitors the health of the Open-Meteo API:

- ✅ **[HEALTH OK]** — data fetched and sent successfully
- ⚠️ **[HEALTH WARNING]** — API timeout (no response in 10 seconds)
- ❌ **[HEALTH ERROR]** — API unavailable (connection failed)

Check producer logs:

```bash
docker logs producer
```

Check consumer logs:

```bash
docker logs consumer
```

---

### Project Structure

```text
data-engineering-project/
├── docker-compose.yml
├── producer.py
├── consumer.py
├── Dockerfile.producer
├── Dockerfile.consumer
├── requirements.txt
└── README.md
```

---

### Author

Alexandra Etnaer — IU Internationale Hochschule  
Martikelnummer: UPS10750192
Course: Project: Data Engineering (DLBDSEDE02)