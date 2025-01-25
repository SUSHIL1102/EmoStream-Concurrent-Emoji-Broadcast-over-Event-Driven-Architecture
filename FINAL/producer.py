#!/usr/bin/env python3
# producer.py
from kafka import KafkaProducer
import json
import logging
import time

#This is where kafka is running and the topic
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'emoji-events'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=3,
    batch_size=16384,
    linger_ms=500,  
    buffer_memory=33554432 
)

def send_to_kafka(data):
    """Asynchronously send data to Kafka topic."""
    try:
        print(data)
        producer.send(KAFKA_TOPIC, value=data).add_callback(on_send_success).add_errback(on_send_error)
        return True
    except Exception as e:
        logger.error(f"Failed to send message: {e}")
        return False

def on_send_success(record_metadata):
    logger.info(f"Message sent to {record_metadata.topic} at partition {record_metadata.partition}")

def on_send_error(excp):
    logger.error(f"Failed to send message: {excp}")

def start_periodic_flush():
    while True:
        producer.flush()
        time.sleep(0.5)

