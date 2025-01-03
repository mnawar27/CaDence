from kafka import KafkaConsumer
from json import loads

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'testing12',  # Topic name
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    key_deserializer=lambda x: x.decode('utf-8')  # Deserialize key
)

print("Listening for messages on topic 'testing'...")

# Consume messages and process them
for message in consumer:
    try:
        record = message.value
        session_id = message.key
        print(f"Received message from sessionId={session_id}: {record}")
    except Exception as e:
        print(f"Error processing message: {message}. Error: {e}")
