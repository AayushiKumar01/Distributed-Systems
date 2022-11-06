"""
CPSC 5520 01
This is a simple client that connects to GCD and to a list of potential
group members based on the response received from the GCD.
:Authors: Aayushi Kumar
"""

from logging import exception
from socket import *
import sys
import pickle

BUFFER_SIZE = 1024

class Lab1():
    def __init__(self,host,port) -> None:
        self.gcd_host = host
        self.gcd_port = int(port)

    
    def connect_to_gcd(self):
        """
        connects to GCD and return the response for 'JOIN' msg
        """
        client_socket = socket(AF_INET, SOCK_STREAM)
        client_socket.connect((self.gcd_host,self.gcd_port))
        msg = pickle.dumps('JOIN')
        print('JOIN ' +'('+ self.gcd_host + ',' + str(self.gcd_port) + ')')
        client_socket.send(msg)
        recieved_msg = client_socket.recv(BUFFER_SIZE)
        return pickle.loads(recieved_msg)
            
    def connect_to_members(self, members):
        """
        connects to other potential memebers and prints received message, or, 
        an error message in case of a failed connection.
        """
        for member in members:
            try:
                client_socket = socket(AF_INET, SOCK_STREAM)
                msg = pickle.dumps('HELLO')
                print('HELLO TO ' + str(member))
                client_socket.connect((member['host'],member['port']))
                client_socket.settimeout(1.5)
                client_socket.send(msg)
                recieved_msg = client_socket.recv(BUFFER_SIZE)
                print(pickle.loads(recieved_msg))

            except Exception as e:
                print('failed to connect: ' + str(e))
        
        

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print ('Please pass the host name and port number as command line arguments')
        sys.exit(1)
    host,port = sys.argv[1],sys.argv[2]
    lab1 = Lab1(host,port)
    members = lab1.connect_to_gcd()
    lab1.connect_to_members(members)

