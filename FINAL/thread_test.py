import threading
import time
import logging
from websocket import WebSocketApp
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(message)s'  # Added threadName to see which thread is running
)
logger = logging.getLogger(__name__)

class ThreadTester:
    def __init__(self):
        self.ws = None
        self.running = True
        self.message_received = False
        
    def start(self):
        # Start WebSocket listener thread
        logger.info("Starting WebSocket listener thread")
        ws_thread = threading.Thread(target=self.websocket_listener, name="WebSocketThread")
        ws_thread.daemon = True
        ws_thread.start()

        # Start message sender thread
        logger.info("Starting message sender thread")
        sender_thread = threading.Thread(target=self.message_sender, name="SenderThread")
        sender_thread.daemon = True
        sender_thread.start()

        # Main thread monitoring
        try:
            while self.running:
                logger.info("Main thread is alive")
                logger.info(f"WebSocket connected: {self.ws is not None}")
                logger.info(f"Message received: {self.message_received}")
                time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            self.running = False

    def websocket_listener(self):
        # Test with a public WebSocket echo server
        websocket_url = "ws://localhost:5002/sub1"  # Using your WebSocket server
        
        while self.running:
            try:
                logger.info("Attempting to connect to WebSocket")
                self.ws = WebSocketApp(
                    websocket_url,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                self.ws.run_forever()
                time.sleep(5)  # Wait before reconnecting
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                time.sleep(5)

    def message_sender(self):
        counter = 0
        while self.running:
            logger.info(f"Sender thread iteration {counter}")
            counter += 1
            time.sleep(2)

    def on_open(self, ws):
        logger.info("WebSocket connection opened")

    def on_message(self, ws, message):
        self.message_received = True
        logger.info(f"Received message: {message}")

    def on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code=None, close_msg=None):
        logger.info("WebSocket connection closed")
        self.ws = None

if __name__ == "__main__":
    tester = ThreadTester()
    tester.start() 