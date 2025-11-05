import json
import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime

# --- Page Configuration ---
# Using "wide" layout is better for dashboards
st.set_page_config(
    page_title="Real-Time Social Media Dashboard",
    page_icon="📊",
    layout="wide"
)

# --- Kafka Configuration ---
KAFKA_TOPIC = "analytics_dashboard_data"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
CONSUMER_GROUP_ID = "dashboard-consumer-group-v2" # Using a new group id is good practice

def create_kafka_consumer(topic, bootstrap_servers, group_id):
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

def main():
    st.title("📊 Real-Time Social Media Analytics Dashboard")

    # --- NEW: Add a sidebar for information ---
    st.sidebar.title("About")
    st.sidebar.info(
        "This dashboard visualizes real-time social media activity "
        "processed through an Apache Kafka pipeline."
    )
    st.sidebar.header("Data Flow")
    st.sidebar.markdown(
        "1. **Producer**: Simulates user events.\n"
        "2. **Kafka**: Streams the events.\n"
        "3. **Processor**: Aggregates data in real-time.\n"
        "4. **Dashboard**: Visualizes the insights."
    )

    # --- NEW: Use tabs to organize content ---
    tab1, tab2 = st.tabs(["📈 Live Overview", "🔍 Hashtag Trends"])

    # --- Content for the first tab ---
    with tab1:
        st.header("Live Activity Metrics")
        
        # Layout for total count metrics
        col1, col2, col3, col4 = st.columns(4)
        like_placeholder = col1.empty()
        post_placeholder = col2.empty()
        comment_placeholder = col3.empty()
        share_placeholder = col4.empty()

        # Layout for the live chart
        st.header("Live Activity (Events Per Minute)")
        chart_placeholder = st.empty()

    # --- Content for the second tab ---
    with tab2:
        st.header("Top 5 Trending Hashtags")
        hashtags_placeholder = st.empty()

    consumer = create_kafka_consumer(
        KAFKA_TOPIC, 
        KAFKA_BOOTSTRAP_SERVERS, 
        CONSUMER_GROUP_ID
    )

    print("Dashboard consumer started with new layout. Waiting for messages...")
    
    # Initialize an empty DataFrame for the chart
    chart_data = pd.DataFrame(columns=['Time', 'Events Per Minute'])

    # Initialize metric display
    like_placeholder.metric(label="Likes 👍", value=0)
    post_placeholder.metric(label="Posts 📝", value=0)
    comment_placeholder.metric(label="Comments 💬", value=0)
    share_placeholder.metric(label="Shares 🔁", value=0)
    
    try:
        for message in consumer:
            data = message.value
            
            # --- Update content in Tab 1 ---
            with tab1:
                # Update metrics
                like_placeholder.metric(label="Likes 👍", value=data.get("like_count", 0))
                post_placeholder.metric(label="Posts 📝", value=data.get("post_count", 0))
                comment_placeholder.metric(label="Comments 💬", value=data.get("comment_count", 0))
                share_placeholder.metric(label="Shares 🔁", value=data.get("share_count", 0))
                
                # Update the line chart
                epm = data.get("events_per_minute", 0)
                new_row = pd.DataFrame([{'Time': datetime.now(), 'Events Per Minute': epm}])
                
                chart_data = pd.concat([chart_data, new_row], ignore_index=True)
                if len(chart_data) > 50:
                    chart_data = chart_data.tail(50)
                
                chart_placeholder.line_chart(chart_data.set_index('Time'))

            # --- Update content in Tab 2 ---
            with tab2:
                # Update hashtags table
                hashtags = data.get("trending_hashtags", [])
                if hashtags:
                    df_hashtags = pd.DataFrame(hashtags, columns=['Hashtag', 'Count'])
                    hashtags_placeholder.dataframe(df_hashtags, use_container_width=True)
                else:
                    # Show a message if there are no hashtags yet
                    hashtags_placeholder.info("Waiting for events with hashtags...")
            
    except KeyboardInterrupt:
        print("Stopping dashboard.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()