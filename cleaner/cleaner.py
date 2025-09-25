import os
import signal
import sys
import threading
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue

class Cleaner:
    def __init__(self, queue_in, queue_out, columns_have, columns_want, rabbitmq_host):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.columns_have = columns_have
        self.columns_want = columns_want
        self.keep_indices = [self.columns_have.index(col) for col in self.columns_want]
        self._running = False
        self._shutdown_event = threading.Event()
        self.in_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_in)
        self.out_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_out)

    def _filter_row(self, row):
        items = row.split('|')
        
        if len(items) < max(self.keep_indices) + 1:
            print(f"Row has insufficient columns: {row} (expected {max(self.keep_indices) + 1}, got {len(items)})")
            return None
            
        try:
            selected = [items[i] for i in self.keep_indices]
        except IndexError as e:
            print(f"Index error processing row: {row} - {e}")
            return None
            
        if all(x == '' for x in selected):
            return None
            
        return '|'.join(selected)

    def run(self):
        self._running = True
        
        def on_message(message):
            if not self._running:
                return
                
            try:
                # Handle raw bytes message from middleware
                if isinstance(message, bytes):
                    row = message.decode('utf-8')  # Convert bytes to string
                else:
                    row = str(message)  # Convert anything else to string
                
                row = row.strip()  # Remove any trailing whitespace/newlines
                if not row:  # Skip empty messages
                    return
                
                filtered = self._filter_row(row)
                if filtered:
                    self.out_queue.send(filtered)  # Send as raw string/bytes
                    print(f"Filtered: {filtered}")
                else:
                    print(f"Dropped row: {row}")
                    
            except Exception as e:
                print(f"Error processing message: {e} - Message: {message}")
        
        print(f"Cleaner listening on {self.queue_in}, outputting to {self.queue_out}")
        self.in_queue.start_consuming(on_message)
        
        # Keep the main thread alive - wait indefinitely until shutdown is signaled
        try:
            self._shutdown_event.wait()
        except KeyboardInterrupt:
            print("Keyboard interrupt received, shutting down...")
            self.stop()

    def stop(self):
        self._running = False
        self._shutdown_event.set()
        try:
            self.in_queue.stop_consuming()
        except Exception as e:
            print(f"Error stopping consumer: {e}")
        self.close()

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
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print(f'Received signal {signum}, shutting down cleaner gracefully...')
        cleaner.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        print(f"Starting cleaner for {data_type} data...")
        cleaner.run()
    except KeyboardInterrupt:
        print('Keyboard interrupt received, shutting down cleaner.')
        cleaner.stop()
    except Exception as e:
        print(f'Error in cleaner: {e}')
        cleaner.stop()
    finally:
        print('Cleaner shutdown complete.')
