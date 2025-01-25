#!/usr/bin/env python3
import requests
import time
import random
import logging
from datetime import datetime
import sys
from websocket import WebSocketApp
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmojiClient:
    def __init__(self, client_id):
        self.client_id = client_id
        self.emojis = ['😀', '😍', '🎉', '👏', '❤️', '🏏', '🎯', '🔥']
        self.ws = None
        self.subscriber_id = None

    def start(self):
        self.register_client()
        if self.subscriber_id:
            logger.info(f"Client {self.client_id} will connect to WebSocket at {self.subscriber_id}")
            ws_thread = threading.Thread(target=self.start_listening)
            ws_thread.daemon = True
            ws_thread.start()

            send_thread = threading.Thread(target=self.send_emojis)
            send_thread.daemon = True
            send_thread.start()

            while True:
                time.sleep(1)

    def send_emojis(self):
        try:
            while True:
                emoji_data = {
                    'user_id': f"user_{self.client_id}",
                    'emoji_type': random.choice(self.emojis),
                    'timestamp': datetime.now().isoformat()
                }
                response = requests.post(
                    'http://localhost:5000/emoji',
                    json=emoji_data,
                    headers={'Content-Type': 'application/json'}
                )
                #if response.status_code == 200:
                    #logger.info(f"Client {self.client_id} sent emoji successfully")
                #else:
                    #logger.error(f"Client {self.client_id} error: {response.text}")

                time.sleep(0.0002)  # Adjusted to 1 second for better readability

        except requests.exceptions.RequestException as e:
            logger.error(f"Client {self.client_id} request error: {e}")
            self.deregister_client()

    def register_client(self):
        try:
            response = requests.post(
                'http://localhost:5001/register',
                json={'client_id': self.client_id},
                headers={'Content-Type': 'application/json'}
            )
            if response.status_code == 200:
                self.subscriber_id = response.json().get('subscriber_endpoint')
                logger.info(f"Client {self.client_id} registered with subscriber {self.subscriber_id}")
            else:
                logger.error(f"Client {self.client_id} registration error: {response.text}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Client {self.client_id} registration request error: {e}")

    def deregister_client(self):
        try:
            response = requests.post(
                'http://localhost:5001/deregister',
                json={'client_id': self.client_id},
                headers={'Content-Type': 'application/json'}
            )
            if response.status_code == 200:
                logger.info(f"Client {self.client_id} deregistered successfully")
            else:
                logger.error(f"Client {self.client_id} deregistration error: {response.text}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Client {self.client_id} deregistration request error: {e}")

    def start_listening(self):
        logger.info(f"Attempting to connect to WebSocket at {self.subscriber_id}")
        self.ws = WebSocketApp(self.subscriber_id,
                               on_open=self.on_open,
                               on_message=self.on_message,
                               on_error=self.on_error,
                               on_close=self.on_close)
        self.ws.run_forever()

    def on_open(self, ws):
        logger.info(f"Client {self.client_id} WebSocket connection opened")

    def on_message(self, ws, message):
        logger.info(f"Client {self.client_id} received message: {message}")

    def on_error(self, ws, error):
        logger.error(f"Client {self.client_id} encountered error: {error}")

    def on_close(self, ws):
        logger.info(f"Client {self.client_id} WebSocket closed")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python client_simulator7.py <client_id>")
        sys.exit(1)

    client_id = sys.argv[1]
    client = EmojiClient(client_id)
    client.start()
