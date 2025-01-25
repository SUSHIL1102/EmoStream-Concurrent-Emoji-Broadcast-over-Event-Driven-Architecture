#!/usr/bin/env python3
# Registers clients as they start sending data and sends to kafka

from flask import Flask, request, jsonify
from producer import send_to_kafka
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/emoji', methods=['POST'])
def receive_emoji():
    data = request.json
    required_fields = ['user_id', 'emoji_type', 'timestamp']

    if not all(field in data for field in required_fields):
        return jsonify({'error': 'Missing required fields'}), 400

    success = send_to_kafka(data)
    if success:
        return jsonify({'status': 'success'}), 200
    else:
        return jsonify({'status': 'error'}), 500

@app.route('/register', methods=['POST'])
def register_client():
    client_id = request.json.get('client_id')
    subscriber_id = request.json.get('subscriber_id')
    logger.info(f"Registered client {client_id} with subscriber {subscriber_id}")
    return jsonify({"message": "Client registered successfully"}), 200

@app.route('/deregister', methods=['POST'])
def deregister_client():
    client_id = request.json.get('client_id')
    logger.info(f"Deregistered client {client_id}")
    return jsonify({"message": "Client deregistered successfully"}), 200