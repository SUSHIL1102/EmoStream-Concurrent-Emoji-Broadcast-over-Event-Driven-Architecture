#!/usr/bin/env python3
from threading import Thread
import signal
import time
import logging
from api2 import app # this is api2.py
from producer import start_periodic_flush #this is producer.py

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def start_flask():
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

def graceful_shutdown(signum, frame):
    logger.info("Shutting down gracefully...")
    exit(0)

if __name__ == '__main__':
    # Starting the flask servewr
    flask_thread = Thread(target=start_flask)
    flask_thread.daemon = True
    flask_thread.start()

    # Starting the kafka thread
    kafka_flush_thread = Thread(target=start_periodic_flush)
    kafka_flush_thread.daemon = True
    kafka_flush_thread.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        graceful_shutdown(None, None)