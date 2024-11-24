from kafka import KafkaAdminClient

def delete_topics(topic_names, bootstrap_servers='localhost:9092'):
    """
    Delete specified Kafka topics.

    :param topic_names: List of topic names to delete.
    :param bootstrap_servers: Kafka bootstrap servers.
    """
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    try:
        admin_client.delete_topics(topic_names)
        print(f"Successfully deleted topics: {topic_names}")
    except Exception as e:
        print(f"Failed to delete topics: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    topics_to_delete = ["bucket_dont_exist"]  # Replace with your topic names
    delete_topics(topics_to_delete)