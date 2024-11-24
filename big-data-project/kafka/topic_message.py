import json
from kafka import KafkaConsumer, KafkaAdminClient

def consume_all_messages_excluding(excluded_topics):
    # Create a Kafka Admin client to list topics
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    
    # Get the list of all topics
    all_topics = admin_client.list_topics()
    
    # Filter out the excluded topics
    topics_to_consume = [topic for topic in all_topics if topic not in excluded_topics]

    # Create a Kafka consumer for the filtered topics
    consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='my-group',
        value_deserializer=lambda x: deserialize_message(x)
    )

    # Subscribe to the filtered list of topics
    consumer.subscribe(topics=topics_to_consume)

    print("Consuming messages from topics excluding:", excluded_topics)
    
    try:
        for message in consumer:
            print(f"Received message from topic '{message.topic}': {message.value}")
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

def deserialize_message(message_bytes):
    try:
        return json.loads(message_bytes.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        print(f"Failed to decode message: {e}")
        return None  # or handle it some other way

if __name__ == "__main__":
    excluded_topics = [
        "local-connect-config",
        "local-connect-offsets",
        "local-connect-status",
        "__consumer_offsets"
    ]
    consume_all_messages_excluding(excluded_topics)
    