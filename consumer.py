import json
from kafka import KafkaConsumer

# --- Configuration ---
KAFKA_TOPIC = "social_media_events"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
CONSUMER_GROUP_ID = "basic-consumer-group"

def create_kafka_consumer(topic, bootstrap_servers, group_id):
    """
    Creates a Kafka consumer instance.
    """
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',  # Start reading at the latest message
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

if __name__ == "__main__":
    consumer = create_kafka_consumer(
        KAFKA_TOPIC, 
        KAFKA_BOOTSTRAP_SERVERS, 
        CONSUMER_GROUP_ID
    )
    
    print(f"Subscribed to topic: {KAFKA_TOPIC}")
    print("Waiting for messages...")

    try:
        for message in consumer:
            event_data = message.value
            print(f"Received event: User ID = {event_data.get('user_id')}, Event Type = {event_data.get('event_type')}")
            
    except KeyboardInterrupt:
        print("\nStopping consumer.")
    finally:
        consumer.close()
