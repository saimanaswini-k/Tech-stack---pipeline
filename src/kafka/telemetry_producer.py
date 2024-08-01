import json
import time
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

KAFKA_BROKER = 'localhost:9092'  
TOPIC = 'local.telemetry'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)

def create_topic():
    try:
        existing_topics = admin_client.list_topics()
        if TOPIC not in existing_topics:
            topic = NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)
            admin_client.create_topics([topic])
            print(f"Topic '{TOPIC}' created.")
        else:
            print(f"Topic '{TOPIC}' already exists.")
    except Exception as e:
        print(f"Error creating topic: {e}")
    finally:
        admin_client.close()

def read_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def epoch(data):
    epoch = int(time.time())
    for item in data:
        item['timestamp'] = epoch
    return data

def write_to_kafka(data):
    for record in data:
        producer.send(TOPIC, value=record)
    producer.flush()
    print(f"Data written to topic '{TOPIC}'.")

def main(file_path):
    create_topic()
    data = read_file(file_path)
    if data:
        data_with_timestamp = epoch(data)
        write_to_kafka(data_with_timestamp)
    else:
        print("No valid JSON data.")

if __name__ == "__main__":
    file_path = '../../data/telemetry.json'  
    main(file_path)