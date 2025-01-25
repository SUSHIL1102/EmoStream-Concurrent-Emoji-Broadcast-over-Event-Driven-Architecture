#!/usr/bin/env python3
from kafka import KafkaConsumer
import logging
import json
from websocket import create_connection, WebSocketConnectionClosedException
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def subscriber():
    # Specific endpoint for this subscriber
    websocket_url = 'ws://localhost:5002/sub1'
    
    consumer = KafkaConsumer(
        'cluster1-topic-subscribers',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='cluster1-sub1-group'  # Unique group ID for this subscriber
    )

    ws = None
    while ws is None:
        try:
            ws = create_connection(websocket_url)
            logger.info(f"Connected to WebSocket at {websocket_url}")
        except Exception as e:
            logger.error(f"Failed to connect to WebSocket {websocket_url}: {e}")
            time.sleep(1)

    try:
        for message in consumer:
            try:
                data = json.loads(message.value.decode('utf-8'))
                ws.send(json.dumps(data))
                logger.info(f"Sent data to WebSocket {websocket_url}: {data}")
            except WebSocketConnectionClosedException:
                logger.error(f"WebSocket connection closed for {websocket_url}")
                break
            except Exception as e:
                logger.error(f"Failed to send data to WebSocket {websocket_url}: {e}")
                break
    finally:
        ws.close()
        consumer.close()
        logger.info("Subscriber shutdown complete")

if __name__ == '__main__':
    try:
        subscriber()
    except KeyboardInterrupt:
        logger.info("Shutting down subscriber...")
