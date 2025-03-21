from kafka import KafkaProducer
from kafka import KafkaConsumer
import time

# Kafka configuration
consumer = KafkaConsumer('lakehouse_topic',bootstrap_servers=['kafka:29092'])
print(consumer.config)
print(consumer.bootstrap_connected())

bootstrap_servers = 'kafka:29092'
topic = 'lakehouse_topic'

# Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: v.encode('utf-8')
)

file_path = '/home/docker/sampledata/Worker_Coops.csv'

print("Data successfully fetched!")

def send_file_to_kafka(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            producer.send(topic, value=line.strip())
            print(f"Sent: {line.strip()}")
            time.sleep(1)

    producer.flush()


send_file_to_kafka(file_path)
producer.close()
