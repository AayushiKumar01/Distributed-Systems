import struct
from datetime import datetime
import socket

data_tracker = {}
def serialize_message_for_subscription(address):
    host = address[0]
    port = address[1]
    print("Connecting to publisher with my address {}".format((host, port)))
    message = socket.inet_aton(host) + port.to_bytes(2, 'big')
    return message

def unmarshall_exchange_data(data):
    size = len(list(data))
    i = 0
    exchange_data = []
    while i < size:
        valid_data = get_exchange_rate(data[i:i+32])
        if valid_data != None:
            exchange_data.append(valid_data)
        i += 32
    return exchange_data

def get_exchange_rate(data):
    timestamp = int.from_bytes(data[0:8], "big")
    curr = data[8:14]
    curr = curr.decode()
    price = struct.unpack('<d', data[14:22])
    price = price[0]
    #construct graph here if needed
    print(datetime.fromtimestamp(timestamp/1000000.0), curr[0:3], curr[3:6], price)
    if validate_data(curr, timestamp):
        return (timestamp/1000000.0, curr[0:3], curr[3:6], price)
    else:
        return None

def validate_data(curr, timestamp):
    if curr in data_tracker:
        if data_tracker[curr] > timestamp:
            print("ignoring out of sequence data")
            return False
        else:
            print("removing stale data")
            data_tracker[curr] = timestamp
            return True
    else:
        data_tracker[curr] = timestamp
        return True