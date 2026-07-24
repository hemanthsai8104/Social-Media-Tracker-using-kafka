# Real-Time Social Media Activity Tracker with Kafka

## Overview

This project demonstrates a real-time social media activity tracking system using Apache Kafka. It's a self-contained, easy-to-run example showcasing Kafka's producer-consumer architecture, stream processing, and real-time dashboarding.

The system consists of four main components:

- **Producer (`producer.py`):** Simulates user activity (posts, likes, comments, shares) and sends these events to a Kafka topic.
- **Consumer (`consumer.py`):** A basic consumer that reads events from the Kafka topic and prints them to the console.
- **Analytics Processor (`analytics_processor.py`):** An intermediate service that consumes raw events, performs aggregation (counting likes and posts), and sends the results to a new topic.
- **Dashboard (`dashboard.py`):** A real-time web dashboard built with Streamlit that visualizes the aggregated data from the analytics topic.

The entire environment is containerized using Docker, making it easy to set up and run with minimal configuration.

## Prerequisites

- **Docker:** To run the Kafka and Zookeeper services.
- **Python 3.8+:** To run the producer, consumer, and dashboard applications.

## Quick Start Guide

1. **Start the Kafka environment:**
```bash
   docker-compose up -d
```

2. **Install Python dependencies** (recommended inside a virtual environment):
```bash
   pip install -r requirements.txt
```

3. **Run the producer and consumer** (two separate terminal windows):

   Terminal 1 — start the producer:
```bash
   python producer.py
```
   You should see a confirmation that a message is sent every second.

   Terminal 2 — start the consumer:
```bash
   python consumer.py
```
   You'll see events printed to the console as they're received.

## Advanced Features

Make sure the Quick Start steps above are running first — `producer.py` needs to be active to generate data.

1. **Run the analytics processor** (third terminal):
```bash
   python analytics_processor.py
```
   This counts "like" and "post" events and sends aggregated counts to a new topic.

2. **Run the real-time dashboard** (fourth terminal):
```bash
   streamlit run dashboard.py
```
   A browser tab opens showing live counts of likes and posts as they update.

To shut down the Kafka environment:
```bash
docker-compose down
```

## Tech Stack

- **Apache Kafka** — event streaming (producer-consumer architecture)
- **Docker / Docker Compose** — containerized Kafka + Zookeeper setup
- **Python** — producer, consumer, and analytics services
- **Streamlit** — real-time dashboard visualization
