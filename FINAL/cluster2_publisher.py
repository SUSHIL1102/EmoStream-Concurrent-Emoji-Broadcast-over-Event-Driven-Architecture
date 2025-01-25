#!/usr/bin/env python3
from kafka import KafkaConsumer, KafkaProducer
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def cluster2_publisher():
    # Consumer reading from cluster2-topic
    consumer = KafkaConsumer(
        'cluster2-topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='cluster2-group'
    )

    # Producer writing to cluster2-subscribers-topic
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict) else v
    )

    logger.info("Cluster 2 publisher started - reading from cluster2-topic and writing to cluster2-topic-subscribers")

    try:
        for message in consumer:
            try:
                # Send to subscriber topic
                producer.send('cluster2-topic-subscribers', message.value)
                producer.flush()
                logger.info(f"Published message to cluster2-topic-sub")
            except Exception as e:
                logger.error(f"Error publishing message: {e}")

    except KeyboardInterrupt:
        logger.info("Shutting down Cluster 2 publisher")
    finally:
        consumer.close()
        producer.close()

if __name__ == '__main__':
    cluster2_publisher() 
