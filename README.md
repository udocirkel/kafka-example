# Kafka Geo Example

This repository demonstrates **geo-redundant Kafka clusters** across **two data centers (DC1 and DC2)** with **active-standby replication** for failover.
Messages can be produced via a REST endpoint to a Kafka topic and consumed by a **Spring Kafka** consumer.
The demo shows how data is replicated between clusters and how applications behave when brokers or entire clusters are temporarily unavailable.

---

## Features Demonstrated

- Sending messages to a Kafka topic via **REST endpoint** and **Spring Kafka producer**
- Consuming messages using **Spring Kafka consumer**
- Configuring **topic replication** and **min in-sync replicas** for fault tolerance and durability
- Using **transactional producers** with **idempotence** to prevent duplicates
- Handling **temporary broker failures**, ensuring that producers and consumers continue to operate
- Handling **failover scenarios** with Active-Standby replication across data centers

---

## Architecture

```
+---------------------+       +---------------------+
|  Kafka Cluster DC1  |       |  Kafka Cluster DC2  |
|                     |       |                     |
|     Datacenter 1    |       |     Datacenter 2    |
|       (Active)      |       |      (Standby)      |
|                     |       |                     |
|     ZooKeeper 1     |       |     ZooKeeper 2     |
|      Broker 1-3     |       |      Broker 4-6     |
|                     |       |     MirrorMaker     |
+---------------------+       +---------------------+
           |                             ^
           |                             |
           +-------- Replication --------+
```

- DC1 is the primary cluster where producers write messages.
- DC2 is the standby cluster that receives replicated data.
- In case of failure in DC1, consumers can switch to DC2 to continue processing messages.

---

## 📦 Project Structure

```
├── demo/                   # Demo service using Kafka
│
├── pom.xml                 # Maven build for demo service and container image
│
├── docker-compose.yml      # Full geo-redundant environment (Kafka clusters DC1/DC2 + Mirror Maker + Demo service)
├── docker-compose-dev.yml  # Single-node Kafka environment (Single Kafka broker + Demo service)
├── start.sh                # Start full geo-redundant environment
├── stop.sh                 # Stop full geo-redundant environment
├── startdev.sh             # Start single-node environment
└── stopdev.sh              # Stop single-node environment
```

---

## 🚀 Getting Started

### **Prerequisites / Required Software on Your Machine**

To run this project locally, you need the following installed on your **client machine**:

| Software                  | Purpose                                                            |
|---------------------------|--------------------------------------------------------------------|
| **Java JDK 21+**          | Build the demo REST APIs, Keycloak extensions and Gravitee plugins |
| **Maven 3.8+**            | Build the example Java project and Docker images                   |
| **Docker/Docker Compose** | Run containers for Keycloak, Gravitee, and backend services        |

> Any terminal or shell to run bash scripts (`start.sh`, `stop.sh`, etc.)

### **Get project from SCM**

```bash
git clone https://github.com/udocirkel/kafka-geo-example.git
cd kafka-geo-example
```

### **Build project**

```bash
mvn clean verify
```

### **Start full environment**

```bash
./start.sh
```

### **Stop environment**

```bash
./stop.sh
```

---

## 🌐 Access the Services

| Service                  | URL                   |
|--------------------------|-----------------------|
| **Kafka Console (AKHQ)** | http://localhost:8081 |
| **Demo Service**         | http://localhost:8087 |

---

## 🧪 Test the Integration

The project includes a ready-to-use HTTP test script located at:

```bash
http-test/kafka-check.http
```

This script can be executed with an HTTP client that supports `.http` files, such as the built-in HTTP client in
IntelliJ IDEA or VS Code (REST Client extension).
Open the file in your editor and execute the requests.

The test script shows the Kafka producer & consumer demo in action.

View the Kafka configuration and topic data via the [Kafka Console (AKHQ)](http://localhost:8081).

---

## 🤝 Contributing

Contributions and improvements are welcome.

---

## 📄 License

This project is licensed under the **Apache License 2.0**.
