import json
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer

# Connect to Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id="test"
)

# # Create a new topic
# topic_name = "data_testing_2"
# topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
# admin_client.create_topics(new_topics=topic_list, validate_only=False)

def delete_topic(topic_name):
    # Create an admin client
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092",
        client_id="test"
    )
    
    # Delete the specified topic
    try:
        admin_client.delete_topics([topic_name])
        print(f"Topic '{topic_name}' deleted successfully.")
    except Exception as e:
        print(f"Failed to delete topic '{topic_name}': {e}")

# List all topics
consumer = KafkaConsumer(bootstrap_servers="localhost:9092")
topics = consumer.topics()
topics = sorted(list(topics))
print("Available topics:", json.dumps(topics, indent=4))
#delete_topic("data_testing")