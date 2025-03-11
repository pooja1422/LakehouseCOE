from kafka import KafkaProducer
import time

# Kafka configuration
bootstrap_servers = 'localhost:29092'
topic = 'lake_house_topic'  # The Kafka topic you want to send messages to

# Create a Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: v.encode('utf-8')  # Serialize the message to UTF-8
)

def send_file_to_kafka(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            producer.send(topic, value=line.strip())
            print(f"Sent: {line.strip()}")
            time.sleep(1)

    producer.flush()


file_path = 'hdfs://namenode:9000/stock_details_5_years.csv'

send_file_to_kafka(file_path)

# Close the producer connection
producer.close()