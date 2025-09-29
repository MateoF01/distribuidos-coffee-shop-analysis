import socket
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue
import os
from shared import protocol

HOST = os.environ.get('GATEWAY_HOST', '0.0.0.0')
PORT = int(os.environ.get('GATEWAY_PORT', 5000))

data_type_names = {
    protocol.DATA_TRANSACTIONS: 'transactions',
    protocol.DATA_TRANSACTION_ITEMS: 'transaction_items',
    protocol.DATA_MENU_ITEMS: 'menu_items',
    protocol.DATA_USERS: 'users',
    protocol.DATA_STORES: 'stores',
    protocol.DATA_END: 'end'
}

queue_names = {
    protocol.DATA_TRANSACTIONS: 'transactions_queue',
    protocol.DATA_TRANSACTION_ITEMS: 'transaction_items_queue',
    protocol.DATA_MENU_ITEMS: 'menu_items_queue',
    protocol.DATA_USERS: 'users_queue',
    protocol.DATA_STORES: 'stores_queue'
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
                msg_type, data_type, payload = protocol.receive_message(conn)
                message = protocol.pack_message(msg_type, data_type, payload)
                if msg_type == protocol.MSG_TYPE_DATA:
                    print(f'Received data for {data_type_names.get(data_type, data_type)}:')
                    queue_name = queue_names.get(data_type)
                    if queue_name:
                        self.queues[queue_name].send(message)
                elif msg_type == protocol.MSG_TYPE_END:
                    if data_type == protocol.DATA_END:
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
