###  Old code  ###

# from kafka import KafkaConsumer
# from json import loads


# consumer = KafkaConsumer(
#     'testtopic',
#     bootstrap_servers=['localhost:9092'],
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     group_id='my-group',
#     value_deserializer=lambda x: loads(x.decode('utf-8'))
# )

# print("Listening for messages on topic 'testtopic'...")


# for message in consumer:
#     print(f"Received message: {message.value}")


###  New code  ###
from kafka import KafkaConsumer
from json import loads

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'testtopic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

print("Listening for messages on topic 'testtopic'...")

# Consume messages and process them
for message in consumer:
    try:
        record = message.value
        print(f"Received message: {record}")
    except Exception as e:
        print(f"Error processing message: {message}. Error: {e}")