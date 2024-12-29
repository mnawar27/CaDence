###  Old code  ###

## from time import sleep
# from json import dumps
# from kafka import KafkaProducer


# producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))


# for e in range(10000):
#     data = {'number': e}  
#     producer.send('testtopic', value=data)  
#     sleep(2)


# producer.flush()
# producer.close()

###  New code  ###
from time import sleep
from json import dumps, loads
from kafka import KafkaProducer

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

# Path to the JSON file
file_path = 'data/listen_events.json'

# Read the JSON file line by line and send to Kafka
with open(file_path, 'r') as file:
    for line in file:
        try:
            record = loads(line.strip())  # Parse JSON line
            producer.send('testtopic', value=record)  # Send record to Kafka
            print(f"Sent message: {record}")
            sleep(1)  # Simulate a streaming delay
        except Exception as e:
            print(f"Error processing line: {line}. Error: {e}")

producer.flush()
producer.close()