# Real-Time Fraud Detection System

A real-time fraud detection pipeline built using Apache Kafka, Apache Spark Streaming, and Docker.

## Overview

This project simulates financial transactions, streams them through Apache Kafka, and processes them using Apache Spark Streaming to identify potentially fraudulent activities in real time.

## Architecture

Producer → Kafka Topic → Spark Streaming → Fraud Detection Logic

## Technologies

- Apache Kafka
- Apache Spark Streaming
- Python
- Docker & Docker Compose

## Project Structure

```text
real-time-fraud-detection/
├── docker-compose.yml
├── producer/
│   ├── Dockerfile
│   └── producer.py
├── spark/
│   └── spark_app.py
├── docs/
│   └── project-report.pdf
└── README.md
```

## Features

- Real-time transaction streaming
- Event-driven architecture
- Fraud detection logic
- Distributed data processing
- Dockerized deployment

## Getting Started

### Prerequisites

- Docker
- Docker Compose

### Run the Project

```bash
docker-compose up --build
```

### Start Streaming

The producer generates transaction events and publishes them to Kafka.

Spark Streaming consumes the events and applies fraud detection rules in real time.

## Example Use Cases

- Financial transaction monitoring
- Payment fraud detection
- Real-time event analytics
- Streaming data processing

## Learning Outcomes

- Building streaming data pipelines
- Working with Apache Kafka
- Processing data using Spark Streaming
- Containerized deployment with Docker
- Real-time analytics and fraud detection
