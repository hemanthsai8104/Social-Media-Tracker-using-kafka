import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaConsumer # --- CORRECTED: Import from the aiokafka library --- # --- NEW: Using the Asyncio Kafka Consumer ---
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- FastAPI App Initialization ---
app = FastAPI()

# --- Kafka Configuration ---
KAFKA_TOPIC = "analytics_dashboard_data"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_CONSUMER_GROUP = "fastapi-consumer-group"

# A list to keep track of active WebSocket connections
active_connections: list[WebSocket] = []

async def consume_kafka_messages():
    """
    Consumes messages from the Kafka topic and broadcasts them to all
    active WebSocket clients.
    """
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset='latest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    await consumer.start()
    logger.info("Kafka consumer started.")
    try:
        # Continuously listen for messages
        async for msg in consumer:
            logger.info(f"Received from Kafka: {msg.value}")
            # Broadcast the message to all connected clients
            for connection in active_connections:
                await connection.send_json(msg.value)
    finally:
        await consumer.stop()
        logger.info("Kafka consumer stopped.")

@app.on_event("startup")
async def startup_event():
    """
    This function is called when the FastAPI application starts up.
    It creates a background task to run the Kafka consumer.
    """
    logger.info("Application startup...")
    # The `create_task` function runs our consumer in the background
    asyncio.create_task(consume_kafka_messages())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """

    This is the main WebSocket endpoint. It accepts a new connection,
    adds it to our list of active connections, and then waits for the client
    to disconnect.
    """
    await websocket.accept()
    active_connections.append(websocket)
    logger.info(f"New client connected: {websocket.client}. Total clients: {len(active_connections)}")
    try:
        # This loop keeps the connection alive.
        # It's waiting for a message from the client, but we don't expect any.
        # Its main purpose is to detect when the client disconnects.
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        # When the client disconnects, we remove them from our active list.
        active_connections.remove(websocket)
        logger.info(f"Client disconnected: {websocket.client}. Total clients: {len(active_connections)}")