from kafka import KafkaConsumer
from json import loads


consumer = KafkaConsumer(
    'testtopic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

print("Listening for messages on topic 'testtopic'...")


for message in consumer:
    print(f"Received message: {message.value}")
