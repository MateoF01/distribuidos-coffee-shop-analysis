import socket
import struct
import time

HOST = 'gateway'  # Docker Compose service name for gateway
PORT = 5000       # Server port

def send_message(sock, msg_type, data_type, payload):
    payload_bytes = payload.encode('utf-8')
    header = struct.pack('>BBI', msg_type, data_type, len(payload_bytes))
    sock.sendall(header + payload_bytes)

def send_file(sock, data_type, rows):
    payload = '\n'.join(['|'.join(row) for row in rows])
    send_message(sock, 1, data_type, payload)
    send_message(sock, 2, data_type, '')

if __name__ == '__main__':
    files = {
        1: [['tx1', '100', '2025-09-18'], ['tx2', '200', '2025-09-17']],
        2: [['tx1', 'item1', '2'], ['tx2', 'item2', '1']],
        3: [['item1', 'Coffee', '5.00'], ['item2', 'Tea', '3.00']],
        4: [['user1', 'Alice'], ['user2', 'Bob']],
        5: [['store1', 'Main St'], ['store2', '2nd Ave']]
    }
    max_retries = 10
    for attempt in range(max_retries):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((HOST, PORT))
                for data_type in range(1, 6):
                    send_file(sock, data_type, files[data_type])
                send_message(sock, 2, 6, '')
                print('All files sent.')
            break
        except ConnectionRefusedError:
            print(f"Gateway not ready, retrying ({attempt+1}/{max_retries})...")
            time.sleep(3)
    else:
        print("Failed to connect to gateway after retries.")