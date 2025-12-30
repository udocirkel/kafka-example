# Kafka Example

This repository contains a simple example project demonstrating **Spring Boot** with **Apache Kafka**.
It shows how messages can be sent from a REST endpoint to a Kafka topic and consumed by a Kafka consumer.

---

## 📦 Project Structure

```
├── demo/               # Example service using Kafka
│
├── pom.xml             # Maven build for demo service and container image
│
├── docker-compose.yml  # Full local environment (Kafka + service)
├── start.sh            # Start full local environment
└── stop.sh             # Stop full local environment
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
git clone https://github.com/udocirkel/kafka-example.git
cd kafka-example
```

### **Build project**

```bash
mvn clean verify
```

### **Start full environment**

```bash
./start.sh
```

### **Play demo**

Send messages to `demo-topic`
```bash
curl -X POST 'http://localhost:8087/api/messages' -H 'Content-Type: text/plain' -d 'Hallo Kafka!'
```

Read messages from `demo-topic`  
* Query parameter `offset` (required): start reading from this message offset
* Query parameter `limit` (optional): maximum number of messages to read
```bash
curl 'http://localhost:8087/api/messages?offset=5&limit=3'
```

View Kafka configuration and topic data via the [Kafka Console (AKHQ)](http://localhost:8081)

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

## 🤝 Contributing

Contributions and improvements are welcome.

---

## 📄 License

This project is licensed under the **Apache License 2.0**.
