# MyOwnKafka

A Kafka-style messaging broker implemented in C, designed to run in a containerized Docker environment, featuring support for partitions, binary indexing, replication, and custom clients (`publisher` and `consumer`).

## 🛠 Project Architecture

The project consists of the following main components written in C and managed via Docker Compose:

* **`broker` (`kafka.c`)**: The central broker that handles client requests (`PRODUCE` and `FETCH`), organizes data by topics and partitions, manages locks with multi-threading[cite: 3], and handles replication to secondary nodes.
* **`publisher` (`publisher.c`)**: The producer client that connects to the broker to send messages to a specific topic and partition.
* **`consumer` (`subscribe.c`)**: The consumer client that streams messages from a specific partition starting from a desired offset.

## 📂 File Structure

* **`compose.yaml`**: Configures the container infrastructure (`broker-leader`, `broker-replica`, `producer`, `consumer`).
* **`Dockerfile`**: Automatically compiles the binaries for the broker (`broker`), publisher (`publisher`), and consumer (`consumer`).
* **`kafka.c`**: Broker logic, log file system management, and binary search indices for messages.
* **`publisher.c`**: Source code for the write client
* **`subscribe.c`**: Source code for the read client
