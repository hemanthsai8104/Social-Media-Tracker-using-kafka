import json
import time
import threading
from collections import deque
from kafka import KafkaConsumer, KafkaProducer

# --- Kafka Configuration ---
SOURCE_TOPIC = "social_media_events"
DESTINATION_TOPIC = "analytics_dashboard_data"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
CONSUMER_GROUP_ID = "analytics-processor-group"

# --- In-memory State ---
# Counters for total events
like_count = 0
post_count = 0
comment_count = 0
share_count = 0
hashtag_counts = {}

# --- NEW: State for Time-Windowed Analytics ---
# A deque is a memory-efficient list that can have a max length
# We'll store the timestamps of the last 2000 events
recent_event_timestamps = deque(maxlen=2000) 
events_per_minute = 0
state_lock = threading.Lock() # A lock to safely update shared state between threads

def calculate_events_per_minute():
    """
    This function runs on a separate thread every 5 seconds.
    It calculates the number of events that have occurred in the last 60 seconds.
    """
    global events_per_minute
    
    # Schedule this function to run again in 5 seconds
    threading.Timer(5.0, calculate_events_per_minute).start()
    
    # Get the current time
    now = time.time()
    one_minute_ago = now - 60
    
    # Filter the deque to count only timestamps within the last minute
    # A deque is thread-safe for appends and pops, but iterating needs care.
    # We copy it to a list to be safe during iteration.
    recent_events_copy = list(recent_event_timestamps)
    count = sum(1 for ts in recent_events_copy if ts > one_minute_ago)

    # Use the lock to update the shared variable safely
    with state_lock:
        events_per_minute = count
    
def create_kafka_consumer_producer():
    consumer = KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return consumer, producer

if __name__ == "__main__":
    consumer, producer = create_kafka_consumer_producer()
    
    # --- NEW: Start the background timer thread for EPM calculation ---
    calculate_events_per_minute()
    
    print("Starting time-windowed analytics processor...")
    event_batch_counter = 0
    BATCH_SIZE = 10
    
    try:
        for message in consumer:
            # --- NEW: Add the current timestamp to our deque ---
            recent_event_timestamps.append(time.time())
            
            event = message.value
            event_type = event.get("event_type")

            # Increment total counters
            if event_type == "like": like_count += 1
            elif event_type == "post": post_count += 1
            elif event_type == "comment": comment_count += 1
            elif event_type == "share": share_count += 1
            
            hashtag = event.get("hashtag")
            if hashtag:
                hashtag_counts[hashtag] = hashtag_counts.get(hashtag, 0) + 1

            event_batch_counter += 1

            if event_batch_counter >= BATCH_SIZE:
                top_hashtags = sorted(hashtag_counts.items(), key=lambda item: item[1], reverse=True)[:5]
                
                # --- MODIFIED: Add the new EPM metric to the payload ---
                with state_lock:
                    current_epm = events_per_minute

                analytics_data = {
                    "like_count": like_count,
                    "post_count": post_count,
                    "comment_count": comment_count,
                    "share_count": share_count,
                    "trending_hashtags": top_hashtags,
                    "events_per_minute": current_epm # Add the new metric
                }
                
                producer.send(DESTINATION_TOPIC, value=analytics_data)
                producer.flush()
                print(f"Sent analytics update: {analytics_data}")
                
                event_batch_counter = 0

    except KeyboardInterrupt:
        print("\nStopping analytics processor.")
    finally:
        consumer.close()
        producer.close()