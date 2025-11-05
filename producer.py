import praw
import json
import time
import random
import threading
from kafka import KafkaProducer
from collections import deque

# --- CONFIGURATION ---
KAFKA_TOPIC = "social_media_events"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# --- YOUR REDDIT API CREDENTIALS (INCLUDED) ---
REDDIT_CONFIG = {
    "client_id": "_aEnYOY-4HTqzhLEDjJldg",
    "client_secret": "CY_eNRYXPegCiMIsO0wIc0IIjZpxFw",
    "user_agent": "MyKafkaTracker v0.1 by Critical-Mall-2618", 
    "username": "Critical-Mall-2618",
    "password": "Hemanth@8104"
}

# --- List of high-traffic subreddits to stream ---
SUBREDDITS_TO_STREAM = ["AskReddit", "funny", "gaming", "worldnews", "memes"]

# --- A thread-safe list to share recent, real post IDs ---
# The like simulator will pick IDs from this list.
active_post_ids = deque(maxlen=200)
# A lock to ensure only one thread modifies the list at a time
post_ids_lock = threading.Lock()

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def create_reddit_instance():
    return praw.Reddit(**REDDIT_CONFIG)

# --- Thread 1: Streams real posts and shares ---
def stream_posts(producer, reddit_instance):
    """Listens for new posts (submissions) and identifies posts vs. shares."""
    subreddit_string = "+".join(SUBREDDITS_TO_STREAM)
    subreddit = reddit_instance.subreddit(subreddit_string)
    print("[Thread-Posts] Starting to stream REAL posts and shares...")
    try:
        for post in subreddit.stream.submissions(skip_existing=True):
            event_type = "share" if post.is_crosspostable else "post"
            
            message = {
                "event_type": event_type,
                "post_id": post.id,
                "user_id": str(post.author),
                "hashtag": f"r/{str(post.subreddit)}",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(post.created_utc))
            }
            producer.send(KAFKA_TOPIC, value=message)
            print(f"Sent REAL {event_type.upper()} from r/{post.subreddit}: post '{post.id}'")
            
            with post_ids_lock:
                active_post_ids.append(post.id)

    except Exception as e:
        print(f"[Thread-Posts] Error: {e}")

# --- Thread 2: Streams real comments ---
def stream_comments(producer, reddit_instance):
    """Listens for new comments."""
    subreddit_string = "+".join(SUBREDDITS_TO_STREAM)
    subreddit = reddit_instance.subreddit(subreddit_string)
    print("[Thread-Comments] Starting to stream REAL comments...")
    try:
        for comment in subreddit.stream.comments(skip_existing=True):
            message = {
                "event_type": "comment",
                "post_id": comment.submission.id,
                "user_id": str(comment.author),
                "hashtag": f"r/{str(comment.subreddit)}",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(comment.created_utc))
            }
            producer.send(KAFKA_TOPIC, value=message)
            print(f"Sent REAL COMMENT from r/{comment.subreddit}: user '{comment.author}'")

    except Exception as e:
        print(f"[Thread-Comments] Error: {e}")

# --- Thread 3: Simulates realistic likes ---
def simulate_likes(producer):
    """Periodically generates 'like' events for real, recently seen posts."""
    print("[Thread-Likes] Starting realistic 'like' simulator...")
    while True:
        try:
            with post_ids_lock:
                if not active_post_ids:
                    time.sleep(5)
                    continue
                post_to_like = random.choice(active_post_ids)
            
            message = {
                "event_type": "like",
                "post_id": post_to_like,
                "user_id": f"user_{random.randint(9000, 9999)}",
                "hashtag": None,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            producer.send(KAFKA_TOPIC, value=message)
            print(f"--> Sent SIMULATED LIKE for REAL post: {post_to_like}")
            time.sleep(2)
            
        except Exception as e:
            print(f"[Thread-Likes] Error: {e}")

if __name__ == "__main__":
    kafka_producer = create_kafka_producer()
    reddit = create_reddit_instance()

    # Create the three threads
    post_thread = threading.Thread(target=stream_posts, args=(kafka_producer, reddit), daemon=True)
    comment_thread = threading.Thread(target=stream_comments, args=(kafka_producer, reddit), daemon=True)
    like_thread = threading.Thread(target=simulate_likes, args=(kafka_producer,), daemon=True)

    # Start all three threads
    post_thread.start()
    comment_thread.start()
    like_thread.start()
    print("\nAll producer threads started. Streaming real-time data...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        kafka_producer.flush()
        kafka_producer.close()