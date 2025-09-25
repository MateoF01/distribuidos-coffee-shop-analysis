import os
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue

class Cleaner:
    def __init__(self, queue_in, queue_out, columns_have, columns_want, rabbitmq_host):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.columns_have = columns_have
        self.columns_want = columns_want
        self.keep_indices = [self.columns_have.index(col) for col in self.columns_want]
        # Use CoffeeMessageMiddlewareQueue for both input and output queues
        self.in_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_in)
        self.out_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_out)

    def _filter_row(self, row):
        items = row.split('|')
        # Drop rows with nulls in any wanted column
        selected = [items[i] for i in self.keep_indices]
        if any(x == '' for x in selected):
            return None
        return '|'.join(selected)

    def run(self):
        def on_message(row):
            # row is already decoded (middleware sends as string)
            filtered = self._filter_row(row)
            if filtered:
                self.out_queue.send(filtered)
                print(f"Filtered: {filtered}")
            else:
                print(f"Dropped row: {row}")
        print(f"Cleaner listening on {self.queue_in}, outputting to {self.queue_out}")
        self.in_queue.start_consuming(on_message)

    def close(self):
        self.in_queue.close()
        self.out_queue.close()

if __name__ == '__main__':
    # Example: configure via environment variables
    queue_in = os.environ.get('QUEUE_IN')
    queue_out = os.environ.get('QUEUE_OUT')
    data_type = os.environ.get('DATA_TYPE')
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

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

    cleaner = Cleaner(queue_in, queue_out, columns_have, columns_want, rabbitmq_host)
    try:
        cleaner.run()
    except KeyboardInterrupt:
        print('Shutting down cleaner.')
    finally:
        cleaner.close()
