import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from config import parseArguments

if __name__ == '__main__':
    args = parseArguments()
    # Connect to Kafka
    kafka_connected = False
    while not kafka_connected:
        try:
            print("Connecting to Kafka...")
            consumer = KafkaConsumer(
                bootstrap_servers=[args.kafka],
                auto_offset_reset='earliest',
            )
            kafka_connected = True
        except NoBrokersAvailable:
            print("Could not connect to Kafka, trying again in 10...")
            time.sleep(10)

    # Subscribe to the metrics topic.
    consumer.subscribe(topics=[args.topic])

    print("Start reading input.")
    for item in consumer:
        print("Received item: ", item)
        if item.offset == int(args.end):
            break
