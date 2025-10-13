from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(10):
    data = {"sensor_id": f"sensor_{i}", "value": str(i * 10)}
    producer.send('sensor_data', value=data)
    print(f"Sent: {data}")
    time.sleep(1)
