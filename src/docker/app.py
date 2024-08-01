from flask import Flask, jsonify
import json
import time
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

app = Flask(__name__)

KAFKA_BROKER = 'host.docker.internal:9092'
TOPIC = 'local.telemetry'
FILE_PATH = '../../data/telemetry.json'  

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
            return f"Topic '{TOPIC}' created."
        else:
            return f"Topic '{TOPIC}' already exists."
    except Exception as e:
        return f"Error creating topic: {e}"
    finally:
        admin_client.close()

def read_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except Exception as e:
        return f"Error reading file: {e}"

def epoch(data):
    epoch_time = int(time.time())
    for item in data:
        item['timestamp'] = epoch_time
    return data

def write_to_kafka(data):
    for record in data:
        producer.send(TOPIC, value=record)
    producer.flush()
    return f"Data written to topic '{TOPIC}'."

def api_create_topic():
    result = create_topic()
    return jsonify({'message': result})

@app.route('/send_data', methods=['POST'])
def api_send_data():
    data = read_file(FILE_PATH)
    if isinstance(data, str): 
        return jsonify({'error': data}), 400

    data_with_timestamp = epoch(data)
    result = write_to_kafka(data_with_timestamp)
    
    return jsonify({'message': result})

if __name__ == '__main__':
    app.run(debug=True)
