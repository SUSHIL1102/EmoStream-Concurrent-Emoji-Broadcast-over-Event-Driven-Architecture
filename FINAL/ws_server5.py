#!/usr/bin/env python3
from websocket_server import WebsocketServer
import logging
import threading

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

clients = {}

def new_client(client, server):
    logger.info(f"New client connected: {client['id']} on port {server.port}")
    clients[client['id']] = client
    logger.debug(f"Current clients on port {server.port}: {list(clients.keys())}")

def client_left(client, server):
    logger.info(f"Client disconnected: {client['id']} from port {server.port}")
    if client['id'] in clients:
        del clients[client['id']]
    logger.debug(f"Current clients on port {server.port}: {list(clients.keys())}")

#def message_received(client, server, message):
#    logger.info(f"Client {client['id']} on port {server.port} sent message: {message}")

def message_received(client, server, message):
    logger.info(f"Server received message from client {client['id']}: {message}")
    # Echo the message back to the client
    server.send_message(client, message)


def start_websocket_server(port):
    server = WebsocketServer(host='0.0.0.0', port=port)
    server.set_fn_new_client(new_client)
    server.set_fn_client_left(client_left)
    server.set_fn_message_received(message_received)
    logger.info(f"WebSocket server is running on port {port}")
    server.run_forever()

if __name__ == '__main__':
    # These are all the ports used
    cluster_ports = [5002, 5003, 5004]

    #This cerates a different thread for each cluster
    threads = []
    for port in cluster_ports:
        thread = threading.Thread(target=start_websocket_server, args=(port,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
