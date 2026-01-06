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
