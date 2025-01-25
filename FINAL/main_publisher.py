#!/usr/bin/env python3

# main_publisher.py

from kafka import KafkaConsumer, KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main_publisher():
    consumer = KafkaConsumer(
        'aggregated-emoji-topic',# Reads form where the kafka consumer running the spark job writes to
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit=True
    )
    #Create the 3 topics, one for each cluster
    producers = {
        'cluster1-topic': KafkaProducer(bootstrap_servers='localhost:9092'),
        'cluster2-topic': KafkaProducer(bootstrap_servers='localhost:9092'),
        'cluster3-topic': KafkaProducer(bootstrap_servers='localhost:9092')
    }

    for message in consumer:
        for topic, producer in producers.items():
            producer.send(topic, message.value)
            producer.flush()
            logger.info(f"Published message to {topic}")

if __name__ == '__main__':
    main_publisher()