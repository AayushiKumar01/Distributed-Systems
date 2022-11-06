"""
CPSC 5520 01
This is a simple node that join the group
by talking to the GCD and participate in elections.
:Authors: Aayushi Kumar
"""
from datetime import datetime
from operator import contains
from pydoc import cli
import selectors
from socket import *
import sys
import pickle
from enum import Enum

BUFFER_SIZE = 1024
BLOCKING_TIME = 1
PEER_DIGITS = 100

class state(Enum):
    """
    Enumeration for the states of my peer.
    This will decide what message I want to send to my peers
    """
    QUISCENT = "QUISCENT"
    SEND_ELECTION = "ELECTION"
    SEND_OK = "OK"
    SEND_VICTORY = "COORDINATE"
    SELF = "SELF"

    WAIT_FOR_OK = "WAIT_OK"
    WAIT_FOR_MESSAGE = "WAITING"

class Lab2(object):

    group_members = {}
    peer_state = {}
    
    def __init__(self, birthDay, suId, host, port) -> None:
        print(birthDay, suId, host, port)
        self.gcd_address = (host, int(port))
        days_to_next_bday = (birthDay - datetime.now()).days
        self.process_id = (days_to_next_bday, int(suId))
        self.listener = self.listener_server()
        self.host, self.port = self.listener.getsockname()
        self.selector = selectors.DefaultSelector()
        self.register_socket_with_selector(self.listener, selectors.EVENT_READ)
        self.bully = None
        self.elecion_in_progress = False

    def join_group(self):
        """
        connects to GCD and return the response for 'JOIN' msg
        """
        print("-=-=-=-=-=-=-= Joining GCD -=-=-=-=-=-=-=-=")
        try:
            client_socket = socket(AF_INET, SOCK_STREAM)
            print("GCD address - {}".format(self.gcd_address))
            client_socket.connect(self.gcd_address)
            msg = pickle.dumps(('JOIN', (self.process_id, (self.host, self.port))))
            client_socket.send(msg)
            recieved_msg = client_socket.recv(BUFFER_SIZE)
            self.group_members = pickle.loads(recieved_msg)
            print("current members of GCD: {} ".format(self.group_members))
        except Exception as e:
            print(str(e))

    def listener_server(self):
        listener_socket = socket(AF_INET, SOCK_STREAM)
        listener_socket.bind(('localhost', 0))
        listener_socket.setblocking(False)
        listener_socket.listen(100)
        return listener_socket       

    def set_state(self, peer_socket, state):
        self.peer_state[peer_socket] = state

    def get_state(self, peer_socket):
        if peer_socket in self.peer_state:
            return self.peer_state[peer_socket]
        return None

    def compare_process_id(self, process1, process2):
        if process1[0] > process2[0]:
            return True
        elif process1[0] < process2[0]:
            return False
        else:
            return process1[1] > process2[1]

    def send(self, peer_socket, message_name, message_data):
        msg = pickle.dumps((message_name, message_data))
        print("sending message {} from {}".format(message_name, self.pr_sock(peer_socket)))
        peer_socket.send(msg)

    def unregister_socket(self, client_sock):
        try:
            self.selector.unregister(client_sock)
        except KeyError:
            print("{} socket already registered".format(self.pr_sock(client_sock)))
        except ValueError:
            print("Value error thrown for i/p {}".format(self.pr_sock(client_sock)))

    def declare_victory(self):
        for member in self.group_members:
            if member != self.process_id:
                self.set_state(self.group_members[member], state.SEND_VICTORY)
                peer_sock = self.create_and_register_peer_socket(member)
                self.set_state(peer_sock, state.SEND_VICTORY)

        self.bully = self.process_id
        self.elecion_in_progress = False

    def start_election(self):
        print("-=-=-=-=-=-=-=- Starting Election -=-=-=-=-=-=-=-=-")
        self.elecion_in_progress = True
        highest_pid = True
        for member in self.group_members:
            if self.compare_process_id(member, self.process_id):
                peer_sock = self.create_and_register_peer_socket(member)
                self.set_state(peer_sock, state.SEND_ELECTION)
                highest_pid = False
            else:
                self.set_state(self.group_members[member], state.QUISCENT)

        if highest_pid:
            print("{} is the leader".format(self.process_id))
            self.declare_victory()

    @staticmethod
    def states_require_outgoing_socket(peer_state):
        outgoing_states = {state.SEND_ELECTION, state.SEND_OK, state.SEND_VICTORY}
        return peer_state in outgoing_states

    def create_and_register_peer_socket(self, member):
        try: 
            peer_socket = socket(AF_INET, SOCK_STREAM)
            peer_socket.connect(self.group_members[member])
            peer_socket.setblocking(False)
            self.register_socket_with_selector(peer_socket, selectors.EVENT_WRITE)
            return peer_socket
        except Exception as e:
            print("Failed to connect to {}".format(member))
            print(str(e))

    def accept_peer(self):
        client_sock, addr = self.listener.accept()
        print("accepting connection from {} with addr {}".format(self.pr_sock(client_sock), addr))
        client_sock.setblocking(False)
        self.register_socket_with_selector(client_sock, selectors.EVENT_READ)
        client_state = self.get_state(client_sock)
        
        if client_state == state.SEND_ELECTION:
            self.set_state(client_sock, state.WAIT_FOR_OK)
        else:
            self.set_state(client_sock, state.WAIT_FOR_MESSAGE)

    def handle_member_update(self, data, cordinate=False):
        highest_pid = self.process_id
        for member in data:
            self.group_members[member]
        
        if cordinate:
            for member in self.group_members:
                if self.compare_process_id(member, highest_pid):
                    highest_pid = member
            print("Setting leader to {}".format(highest_pid))
            self.bully = highest_pid

    def parse_message(self, msg, client_state, client_sock):
        print("msg {} received from sock {}".format(msg, self.pr_sock(client_sock)))
        message_name = msg[0]
        data = msg[1] if len(msg) >= 2 else None

        if message_name == "OK":
           print("received OK from {} ".format(self.pr_sock(client_sock)))
           self.unregister_socket(client_sock)
           self.set_state(client_sock, state.QUISCENT)
           self.elecion_in_progress = False
        elif message_name == "ELECTION":
            self.group_members = data
            self.handle_member_update(data)
            self.set_state(client_sock, state.SEND_OK)
            self.modify_socket_with_selector(client_sock, selectors.EVENT_WRITE)
            if not self.elecion_in_progress:
                self.start_election()
        elif message_name == "COORDINATE":
            self.handle_member_update(data, True)
        else:
            print("Unknown message received {}".format(msg))

    def receive_message(self, client_sock):
        client_state = self.get_state(client_sock)
        received_msg = client_sock.recv(BUFFER_SIZE)
        msg = pickle.loads(received_msg)
        if client_state == state.WAIT_FOR_OK:
            self.parse_message(msg, client_state, client_sock)
        elif client_state == state.WAIT_FOR_MESSAGE:
            self.parse_message(msg, client_state, client_sock)
        elif client_state == state.SEND_ELECTION:
            self.parse_message(msg, client_state, client_sock)
        else:    
            print("Unknown state transition in receiving message - {}".format(client_state))

    def send_message(self, peer_socket):
        peer_state = self.get_state(peer_socket)
        if peer_state == state.SEND_ELECTION:
            self.send(peer_socket, "ELECTION", self.group_members)
            self.modify_socket_with_selector(peer_socket, selectors.EVENT_READ)
        elif peer_state == state.SEND_OK:
            self.send(peer_socket, "OK", None)
            self.unregister_socket(peer_socket)
            self.set_state(peer_socket, state.QUISCENT)
        elif peer_state == state.SEND_VICTORY:
            self.send(peer_socket, "COORDINATE", self.group_members)
            self.unregister_socket(peer_socket)
            self.set_state(peer_socket, state.QUISCENT)
        else:
            print("{} unknown peer state transition in send_message".format(peer_state))
            print("{} current peer states ".format(self.peer_state))
            raise Exception("Something went wrong!!")

    def check_timeout(self):
        pass

    def run(self):
        
        self.join_group()
        self.start_election()

        while True:
            events = self.selector.select(BLOCKING_TIME)
            for key, mask in events:
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask & selectors.EVENT_READ:
                    self.receive_message(key.fileobj)
                else:
                    self.send_message(key.fileobj)
            self.check_timeout()

    def modify_socket_with_selector(self, sock, event):
        try:
            self.selector.modify(sock, event)
        except KeyError as e:
            print("Unable to modify socket KeyError {} -> {}".format(self.pr_sock(sock), str(e)))
        except ValueError as e:
            print("Unable to modify socket ValueError {} -> {}".format(self.pr_sock(sock, event), str(e)))

    def register_socket_with_selector(self, sock, event):
        try:
            self.selector.register(sock, event)
        except KeyError:
            print("{} socket already registered".format(self.pr_sock(sock)))
        except ValueError:
            print("Value error thrown for i/p {} and {}".format(self.pr_sock(sock, event)))

    @staticmethod
    def pr_now():
        """Printing helper for current timestamp."""
        return datetime.now().strftime('%H:%M:%S.%f')

    def pr_sock(self,sock):
        """Printing helper for given socket."""
        if sock is None or sock == self or sock == self.listener:
            return 'self'
        return self.cpr_sock(sock)

    @staticmethod
    def cpr_sock(sock):
        """"Static version of helper for printing given socket."""
        l_port = sock.getsockname()[1] % PEER_DIGITS
        try:
            r_port = sock.getpeername()[1] % PEER_DIGITS
        except OSError:
            r_port = '???'
        return '{}->{} ({})'.format(l_port, r_port, id(sock))

    def pr_leader(self):
        """Printing helper for current leader's name."""
        return 'unknown' if self.bully is None else ('self' if self.bully == self.process_id else self.bully)



if __name__ == '__main__':
    
    if len(sys.argv) != 5:
        print("Please pass Birthday, SuId, GCD Host and Por as input")
        sys.exit(1)
    
    birthDay, suId, host, port = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] 
    birthdayDate = datetime.strptime(birthDay, "%m/%d/%y")
    lab2 = Lab2(birthdayDate, suId, host, port)
    lab2.run()

