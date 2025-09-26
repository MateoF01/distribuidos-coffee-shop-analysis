
import socket
import struct
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue
import os

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

queue_names = {
    1: 'transactions_queue',
    2: 'transaction_items_queue',
    3: 'menu_items_queue',
    4: 'users_queue',
    5: 'stores_queue'
}

class Server:
    def __init__(self, host, port, listen_backlog=1):
        self.host = host
        self.port = port
        self.listen_backlog = listen_backlog
        self._setup_rabbitmq()
        self._setup_socket()

    def _setup_rabbitmq(self):
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        self.queues = {}
        for q in queue_names.values():
            self.queues[q] = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=q)

    def _setup_socket(self):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(self.listen_backlog)

    def run(self):
        print(f'Server listening on {self.host}:{self.port}')
        while True:
            conn, addr = self._accept_new_connection()
            self._handle_client_connection(conn, addr)

    def _accept_new_connection(self):
        conn, addr = self._server_socket.accept()
        print(f'Connected by {addr}')
        return conn, addr

    def _handle_client_connection(self, conn, addr):
        try:
            while True:
                header = self._recv_exact(conn, 6)
                msg_type, data_type, payload_len = struct.unpack('>BBI', header)
                if payload_len > 0:
                    payload = self._recv_exact(conn, payload_len)
                else:
                    payload = b''
                message = header + payload
                if msg_type == 1:
                    print(f'Received data for {data_type_names.get(data_type, data_type)}:')
                    queue_name = queue_names.get(data_type)
                    if queue_name:
                        self.queues[queue_name].send(message)
                elif msg_type == 2:
                    if data_type == 6:
                        print('All files received. Closing connection.')
                        break
                    else:
                        queue_name = queue_names.get(data_type)
                        if queue_name:
                            self.queues[queue_name].send(message)
                        print(f'Finished receiving {data_type_names.get(data_type, data_type)}.')
                else:
                    print(f'Unknown message type: {msg_type}')
        finally:
            conn.close()

    def _recv_exact(self, sock, n):
        data = b''
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                raise ConnectionError('Socket closed prematurely')
            data += packet
        return data

    def close(self):
        for q in self.queues.values():
            q.close()
        self._server_socket.close()

if __name__ == '__main__':
    server = Server(HOST, PORT)
    try:
        server.run()
    except KeyboardInterrupt:
        print('Shutting down server.')
    finally:
        server.close()
