# Real-Time Social Media Activity Tracker with Kafka

## Overview

This project demonstrates a real-time social media activity tracking system using Apache Kafka. It is designed to be a self-contained, easy-to-run example that showcases the core concepts of Kafka's producer-consumer architecture, along with stream processing and real-time dashboarding.

The system consists of four main components:
- **Producer (producer.py):** Simulates user activity (posts, likes, comments, shares) and sends these events to a Kafka topic.
- **Consumer (consumer.py):** A basic consumer that reads events from the Kafka topic and prints them to the console.
- **Analytics Processor (nalytics_processor.py):** An intermediate service that consumes raw events, performs a simple aggregation (counting likes and posts), and sends the results to a new topic.
- **Dashboard (dashboard.py):** A real-time web dashboard built with Streamlit that visualizes the aggregated data from the analytics topic.

The entire environment is containerized using Docker, making it easy to set up and run with minimal configuration.

## Prerequisites

- **Docker:** To run the Kafka and Zookeeper services.
- **Python 3.8+:** To run the producer, consumer, and dashboard applications.

## Quick Start Guide

This guide will help you get the basic producer and consumer running.

1.  **Start the Kafka Environment:**
    Open your terminal, navigate to the project's root directory, and run the following command to start Kafka and Zookeeper in Docker:
    docker-compose up -d

2.  **Install Python Dependencies:**
    It is recommended to use a virtual environment. In the same directory, install the required Python libraries:
    pip install -r requirements.txt

3.  **Run the Producer and Consumer:**
    You will need two separate terminal windows for this step.

    - In your **first terminal**, run the producer to start generating and sending events:
      python producer.py
      You should see a confirmation that a message has been sent every second.

    - In your **second terminal**, run the consumer to receive and display the events:
      python consumer.py
      You will see the events being printed to the console as they are received.

## Advanced Features

This section guides you through running the stream processor and the real-time dashboard. Make sure you have completed the "Quick Start Guide" steps first. The producer (producer.py) must be running to generate data.

1.  **Run the Analytics Processor:**
    This service will consume raw events and produce aggregated data.

    - In a **third terminal**, run the analytics processor:
      python analytics_processor.py
      This will start counting "like" and "post" events and sending the aggregated counts to a new topic.

2.  **Run the Real-Time Dashboard:**
    This will launch a web application to visualize the aggregated data.

    - In a **fourth terminal**, run the Streamlit dashboard:
      streamlit run dashboard.py
      A new tab should open in your web browser with the real-time dashboard, displaying the counts of likes and posts as they are updated.

To shut down the Kafka environment, simply run:
docker-compose down
"@;

Set-Content -Path 'docker-compose.yml' -Value @"
version: '3'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
