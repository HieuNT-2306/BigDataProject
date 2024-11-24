from kafka import KafkaAdminClient
from kafka.errors import InvalidGroupIdError

def find_consumers_subscribed_to_topic(topic_name):
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")

    # Get all consumer groups
    consumer_groups = admin_client.list_consumer_groups()

    consumers = []

    for group_id, _ in consumer_groups:
        try:
            # Describe each group
            group_description = admin_client.describe_consumer_groups([group_id])
            
            for group in group_description:
                if group.error_code is None:
                    for member in group.members:
                        # Check if the topic is in the list of topics the member is subscribed to
                        if topic_name in member.assignment.topic_partitions:
                            consumers.append({
                                "group_id": group_id,
                                "member_id": member.id,
                                "client_id": member.client_id,
                                "client_host": member.client_host,
                            })
                else:
                    print(f"Error in group {group_id}: {group.error_message}")
        except InvalidGroupIdError:
            print(f"Unknown consumer group ID: {group_id}")
        except Exception as e:
            print(f"An error occurred while describing group {group_id}: {e}")

    return consumers

if __name__ == "__main__":
    topic_name = "bucket_dont_exist"  # Replace with your topic name
    consumers = find_consumers_subscribed_to_topic(topic_name)
    
    if consumers:
        print(f"Consumers subscribed to topic '{topic_name}':")
        for consumer in consumers:
            print(f"Group ID: {consumer['group_id']}, Member ID: {consumer['member_id']}, Client ID: {consumer['client_id']}, Client Host: {consumer['client_host']}")
    else:
        print(f"No consumers found for topic '{topic_name}'.")