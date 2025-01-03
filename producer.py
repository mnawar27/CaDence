import os
from time import sleep
from json import dumps, loads
from kafka import KafkaProducer

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'),
    key_serializer=lambda x: str(x).encode('utf-8')  # Serialize key as string
)

# Path to the folder containing session files
folder_path = '/Users/mnawar/Desktop/passion/CaDence/scripts/sessions'

# Get a list of all files in the folder, filtering only JSON files
json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]

# Sort the files by the numeric part in the filename (session_x.json -> x)
json_files = sorted(json_files, key=lambda x: int(x.split('_')[1].split('.')[0]))

# Iterate over the sorted list of JSON files and send them to Kafka
for filename in json_files:
    file_path = os.path.join(folder_path, filename)
    
    # Read each JSON file line by line
    with open(file_path, 'r') as file:
        for line in file:
            try:
                record = loads(line.strip())  # Load each line as a separate JSON object
                session_id = record.get("sessionId", 0)  # Use sessionId as key
                producer.send('testing12', key=str(session_id), value=record)  # Send with key
                print(f"Sent message: {record}")
                sleep(1)  # Simulate streaming delay (adjust as needed)
            except Exception as e:
                print(f"Error processing record: {line}. Error: {e}")

# Flush and close producer to ensure all messages are sent
producer.flush()
producer.close()
