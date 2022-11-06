import fxp_bytes_subscriber
import socket
import sys
from datetime import datetime
import bellman_ford

BLOCKING_TIME = 0.2
BUFFER_SIZE = 1600

class Lab3(object):
    
    def start_listener_server(self):
        listener_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listener_socket.bind(('localhost', 0))
        return listener_socket

    def subscribe_to_forex_provider(self, forex_address):
        message = fxp_bytes_subscriber.serialize_message_for_subscription(self.listener_address)

        try:
            sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            print(forex_address, type(forex_address))
            sender_socket.sendto(message, forex_address)
        except Exception as e:
            print("Error subscribing to forex -> " + str(e))
            raise e


    def __init__(self):
        self.listener = self.start_listener_server()
        self.listener_address = self.listener.getsockname()
        self.exchange_graph = {}

    def parse_message_and_run(self, data):
        exchange = fxp_bytes_subscriber.unmarshall_exchange_data(data)
        self.add_edge_and_run_arbitrage(exchange)

    def add_edge_and_run_arbitrage(self, edges):
        for edge in edges:
            bellman_ford.run(edge)

    def run(self, host, port):
        self.subscribe_to_forex_provider((host, int(port)))
        self.listener.settimeout(600.0)

        while True:    
            data = self.listener.recv(BUFFER_SIZE)
            self.parse_message_and_run(data)
            
if __name__  == '__main__':

    if len(sys.argv) != 3:
        print("Please pass Forex Host and Por as input")
        sys.exit(1)
    
    host, port = sys.argv[1], sys.argv[2]

    lab3 = Lab3()
    lab3.run(host, port)