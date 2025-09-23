import pika
import os
import time
from middleware.coffeeMiddleware import (
  CoffeeMessageMiddlewareQueue
)

class Cleaner:
    def __init__(self, queue_in, queue_out, columns_have, columns_want, rabbitmq_host, rabbitmq_user, rabbitmq_pass):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.columns_have = columns_have
        self.columns_want = columns_want
        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_user = rabbitmq_user
        self.rabbitmq_pass = rabbitmq_pass
        self.keep_indices = [self.columns_have.index(col) for col in self.columns_want]
        self._setup_rabbitmq()

    def _setup_rabbitmq(self):
        credentials = pika.PlainCredentials(self.rabbitmq_user, self.rabbitmq_pass)
        max_retries = 10
        for attempt in range(max_retries):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(self.rabbitmq_host, credentials=credentials)
                )
                break
            except pika.exceptions.AMQPConnectionError:
                print(f"RabbitMQ not ready, retrying ({attempt+1}/{max_retries})...")
                time.sleep(3)
        else:
            print("Failed to connect to RabbitMQ after retries.")
            raise RuntimeError("RabbitMQ connection failed")
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_in)
        self.channel.queue_declare(queue=self.queue_out)

    def _filter_row(self, row):
        items = row.split('|')
        # Drop rows with nulls in any wanted column
        selected = [items[i] for i in self.keep_indices]
        if any(x == '' for x in selected):
            return None
        return '|'.join(selected)

    def run(self):
        def callback(ch, method, properties, body):
            row = body.decode()
            filtered = self._filter_row(row)
            if filtered:
                self.channel.basic_publish(exchange='', routing_key=self.queue_out, body=filtered)
                print(f"Filtered: {filtered}")
            else:
                print(f"Dropped row: {row}")
        self.channel.basic_consume(queue=self.queue_in, on_message_callback=callback, auto_ack=True)
        print(f"Cleaner listening on {self.queue_in}, outputting to {self.queue_out}")
        self.channel.start_consuming()

    def close(self):
        self.connection.close()

if __name__ == '__main__':
    # Example: configure via environment variables
    queue_in = os.environ.get('QUEUE_IN')
    queue_out = os.environ.get('QUEUE_OUT')
    data_type = os.environ.get('DATA_TYPE')
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    rabbitmq_user = os.environ.get('RABBITMQ_USER', 'admin')
    rabbitmq_pass = os.environ.get('RABBITMQ_PASS', 'secretpassword')

    # Define columns for each data type
    configs = {
        'transactions': {
            'have': ['transaction_id','store_id','payment_method_id','voucher_id','user_id','original_amount','discount_applied','final_amount','created_at'],
            'want': ['transaction_id','final_amount','created_at','store_id','user_id']
        },
        'transaction_items': {
            'have': ['transaction_id','item_id','quantity','unit_price','subtotal','created_at'],
            'want': ['item_id','quantity','subtotal','created_at']
        },
        'users': {
            'have': ['user_id','gender','birthdate','registered_at'],
            'want': ['user_id','birthdate']
        },
        'stores': {
            'have': ['store_id','store_name','street','postal_code','city','state','latitude','longitude'],
            'want': ['store_id','store_name']
        },
        'menu_items': {
            'have': ['item_id','item_name','category','price','is_seasonal','available_from','available_to'],
            'want': ['item_id','item_name']
        }
    }
    if data_type not in configs:
        raise ValueError(f"Unknown data type: {data_type}")
    columns_have = configs[data_type]['have']
    columns_want = configs[data_type]['want']

    cleaner = Cleaner(queue_in, queue_out, columns_have, columns_want, rabbitmq_host, rabbitmq_user, rabbitmq_pass)
    try:
        cleaner.run()
    except KeyboardInterrupt:
        print('Shutting down cleaner.')
    finally:
        cleaner.close()
