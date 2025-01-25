#!/usr/bin/env python3
from flask import Flask, request, jsonify
import logging
import random

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# The WebSocket endpoints

subscribers = {
    'cluster1': ['ws://localhost:5002/sub1', 'ws://localhost:5002/sub2'],
    'cluster2': ['ws://localhost:5003/sub4', 'ws://localhost:5003/sub5'],
    'cluster3': ['ws://localhost:5004/sub7', 'ws://localhost:5004/sub8']
}
'''
subscribers = {
    'cluster1': ['ws://localhost:5002/sub1'],
    'cluster2': ['ws://localhost:5003/sub4'],
    'cluster3': ['ws://localhost:5004/sub7']
}
'''
clients = {}

@app.route('/register', methods=['POST'])
def register_client():
    data = request.json
    client_id = data.get('client_id')
    
    if client_id:
        # This will randomly choose a cluster(to implement dynmaic allocation rather than hardcoding)
        cluster_id = random.choice(list(subscribers.keys()))
        subscriber_endpoint = random.choice(subscribers[cluster_id])
        
        clients[client_id] = subscriber_endpoint
        logger.info(f"Registered client {client_id} with subscriber {subscriber_endpoint} in cluster {cluster_id}")
        return jsonify({'status': 'success', 'subscriber_endpoint': subscriber_endpoint, 'cluster_id': cluster_id}), 200
    else:
        return jsonify({'status': 'error', 'message': 'Invalid data'}), 400

@app.route('/deregister', methods=['POST'])
def deregister_client():
    data = request.json
    client_id = data.get('client_id')
    if client_id in clients:
        del clients[client_id]
        logger.info(f"Deregistered client {client_id}")
        return jsonify({'status': 'success'}), 200
    else:
        return jsonify({'status': 'error', 'message': 'Client not found'}), 404

if __name__ == '__main__':
    app.run(port=5001)
