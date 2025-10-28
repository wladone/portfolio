import json
import time
from datetime import datetime

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

# First, create the topic if it doesn't exist
admin_client = KafkaAdminClient(bootstrap_servers=["localhost:9092"])

# Create topic configuration
topic_list = [NewTopic(name="orders", num_partitions=1, replication_factor=1)]

# Try to create the topic
try:
    admin_client.create_topics(topic_list)
    print("Topic 'orders' created successfully")
except Exception as e:
    print(f"Topic creation error (might already exist): {e}")

time.sleep(1)  # Give the broker a moment to set up the topic

# Create producer instance
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Create test order message
test_order = {
    "order_id": "test-001",
    "customer_id": "cust-001",
    "order_date": datetime.now().isoformat(),
    "items": [{"product_id": "prod-001", "quantity": 2, "price": 19.99}],
    "total_amount": 39.98,
}

# Send message
future = producer.send("orders", test_order)
result = future.get(timeout=60)
print(
    f"Message sent successfully to partition {result.partition} at offset {result.offset}"
)

# Close producer
# Close connections
producer.close()
admin_client.close()
