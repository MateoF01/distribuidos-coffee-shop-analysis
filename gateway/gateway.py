import socket
import struct
import pika
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

def recv_exact(sock, n):
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            raise ConnectionError('Socket closed prematurely')
        data += packet
    return data

def main():
    # Setup RabbitMQ connection using environment variables
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    rabbitmq_user = os.environ.get('RABBITMQ_USER', 'admin')
    rabbitmq_pass = os.environ.get('RABBITMQ_PASS', 'secretpassword')
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
    import time
    max_retries = 10
    for attempt in range(max_retries):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host, credentials=credentials))
            break
        except pika.exceptions.AMQPConnectionError:
            print(f"RabbitMQ not ready, retrying ({attempt+1}/{max_retries})...")
            time.sleep(3)
    else:
        print("Failed to connect to RabbitMQ after retries.")
        return
    channel = connection.channel()
    # Declare all queues
    for q in queue_names.values():
        channel.queue_declare(queue=q)

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
                    queue_name = queue_names.get(data_type)
                    for row in payload.split('\n'):
                        print('  ', row)
                        if queue_name:
                            # Send to RabbitMQ
                            channel.basic_publish(
                                exchange='',
                                routing_key=queue_name,
                                body=row
                            )
                            print(f"Sent to queue '{queue_name}': {row}")
                elif msg_type == 2:
                    if data_type == 6:
                        print('All files received. Closing connection.')
                        break
                    else:
                        print(f'Finished receiving {data_type_names.get(data_type, data_type)}.')
                else:
                    print(f'Unknown message type: {msg_type}')
    connection.close()

if __name__ == '__main__':
    main()
