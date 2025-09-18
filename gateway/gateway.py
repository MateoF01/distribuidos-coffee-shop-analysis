import socket
import struct

HOST = '0.0.0.0'  # Listen on all interfaces
PORT = 5000       # Change as needed

data_type_names = {
    1: 'transactions',
    2: 'transaction_items',
    3: 'menu_items',
    4: 'users',
    5: 'stores',
    6: 'end'
}

def recv_exact(sock, n):
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            raise ConnectionError('Socket closed prematurely')
        data += packet
    return data

def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen(1)
        print(f'Server listening on {HOST}:{PORT}')
        conn, addr = s.accept()
        with conn:
            print(f'Connected by {addr}')
            while True:
                header = recv_exact(conn, 6)
                msg_type, data_type, payload_len = struct.unpack('>BBI', header)
                if payload_len > 0:
                    payload = recv_exact(conn, payload_len).decode('utf-8')
                else:
                    payload = ''
                if msg_type == 1:
                    print(f'Received data for {data_type_names.get(data_type, data_type)}:')
                    for row in payload.split('\n'):
                        print('  ', row)
                elif msg_type == 2:
                    if data_type == 6:
                        print('All files received. Closing connection.')
                        break
                    else:
                        print(f'Finished receiving {data_type_names.get(data_type, data_type)}.')
                else:
                    print(f'Unknown message type: {msg_type}')

if __name__ == '__main__':
    main()
