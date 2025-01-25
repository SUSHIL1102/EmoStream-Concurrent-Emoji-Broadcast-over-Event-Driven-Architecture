# RR-Team-64-emostream-concurrent-emoji-broadcast-over-event-driven-architecture




# Emoji Event Processing System

A distributed system for processing emoji events using Apache Kafka and WebSocket servers.

## Prerequisites

- Python 3.x
- Apache Kafka
- Apache Zookeeper
- pip (Python package manager)

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd <project-directory>
```

2. Create and activate a virtual environment:

```bash
python -m venv venv

Activate virtual environment:

On Windows:
venv\Scripts\activate

On Unix or MacOS:
source venv/bin/activate
```

3. Install required packages:

```bash
pip install -r requirements.txt

```

## Running the System

### 1. Start Kafka Infrastructure
1)Make sure to cd into your kafka folder

2)Open separate terminals for each of these commands:
```bash
Terminal 1 - Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

Terminal 2 - Start Kafka
bin/kafka-server-start.sh config/server.properties

Terminal 3 - Create the topics
bin/kafka-topics.sh --create --topic emoji-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic aggregated-emoji-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic cluster1-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic cluster2-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic cluster3-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic cluster1-topic-subscribers --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic cluster2-topic-subscribers --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic cluster3-topic-subscribers --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

```
### 2. Start System Components

Run the following components in order, each in a separate terminal:

1. Registration API
 ```bash
   python registration_api2.py
   ```
 

2. WebSocket Server
 ```bash
   python ws_server5.py
   ```
```bash
   python main3.py
   ```
3. Emoji Processing
 ```bash
   python emoji12.py
   ```
4. Publishers
```bash
   python main_publisher.py
   python cluster1_publisher.py
   python cluster2_publisher.py
   python cluster3_publisher.py
   ```
5. Start Subscribers
   ```bash
   chmod +x start_subscribers.sh
   ./start_subscribers
   ```

6. Start client simulator(different terminal for each client, with different client id)
```bash
   python client_simulator8.py client_1
   ```

### 3. Monitor Events (Optional)

To monitor events in real-time, open additional terminals:

a) Monitor input events
```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic emoji-events --from-beginning
```
b) Monitor processed events
```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic aggregated-emoji-topic --from-beginning
```
c) To list all Kafka topics:
```bash
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```


## System Architecture

System Architecture:
The emoji event processing system follows a distributed architecture with multiple components working together. Here's a detailed breakdown:

High-Level Architecture:

    A[Client Simulator] -->|Emoji Events| B[Kafka Topic: emoji-events]
    B --> C[Spark Streaming Job]
    C -->|Aggregated Data| D[Kafka Topic: aggregated-emoji-topic]
    D --> E[Main Publisher]
    E -->|Distributes Data| F[Cluster Topics]
    F -->|cluster1-topic| G[Cluster1 Publisher]
    F -->|cluster2-topic| H[Cluster2 Publisher]
    F -->|cluster3-topic| I[Cluster3 Publisher]
    G --> J[Cluster1 Subscribers]
    H --> K[Cluster2 Subscribers]
    I --> L[Cluster3 Subscribers]
    J --> M[WebSocket Server Port 5002]
    K --> N[WebSocket Server Port 5003]
    L --> O[WebSocket Server Port 5004]

Component Details:

1. Data Ingestion Layer
   - Client simulators generate emoji events
   - Events are published to `emoji-events` Kafka topic
   - Format: `{user_id, emoji_type, timestamp}`

2. Processing Layer

	  
       A[Spark Streaming Job] -->|Reads| B[Raw Events]
       A -->|Processes| C[Window Operations]
       C -->|2-second windows| D[Aggregations]
       D -->|Outputs| E[Normalized Counts]

- Performs real-time aggregation using Spark Structured Streaming
- 2-second windowing operations
- Calculates emoji counts and unique users
- Normalizes counts based on threshold (≥20)

3. Distribution Layer
  
       A[Main Publisher] -->|Reads| B[aggregated-emoji-topic]
       A -->|Distributes to| C[Cluster Topics]
       C --> D[cluster1-topic]
       C --> E[cluster2-topic]
       C --> F[cluster3-topic]
       D --> G[Cluster1 Publisher]
       E --> H[Cluster2 Publisher]
       F --> I[Cluster3 Publisher]

- Main publisher distributes data to cluster-specific topics
- Each cluster has dedicated publisher and subscribers
- Ensures scalable data distribution

4. WebSocket Layer

	  
       A[Cluster Publishers] -->|Forwards to| B[Subscriber Topics]
       B -->|Consumed by| C[WebSocket Clients]
       C -->|Port 5002| D[Cluster1 Clients]
       C -->|Port 5003| E[Cluster2 Clients]
       C -->|Port 5004| F[Cluster3 Clients]

- Three WebSocket servers on different ports
- Each cluster serves multiple subscribers
- Real-time data delivery to end clients
 
### Data Flow
- Emoji events are generated and sent to the initial Kafka topic
- Spark job processes and aggregates data in 2-second windows
- Aggregated data flows through the distribution system
- Cluster publishers forward data to respective subscriber topics
- WebSocket subscribers receive and forward data to connected clients

### Scalability Features
- Distributed processing using Spark Streaming
- Multiple clusters for load distribution
- Independent subscriber groups for parallel processing
- Separate WebSocket servers to handle different client groups

### Fault Tolerance
- Kafka provides message persistence and replication
- Multiple subscribers per cluster for redundancy
- Automatic reconnection logic in WebSocket clients
- Error handling and logging throughout the system

### This architecture ensures scalable, real-time processing of emoji events with fault tolerance and efficient distribution to end clients.
