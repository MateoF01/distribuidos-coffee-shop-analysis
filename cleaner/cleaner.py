import os
import signal
import sys
import threading
import struct
import configparser
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue

class Cleaner:
    def __init__(self, queue_in, queue_out, columns_have, columns_want, rabbitmq_host, keep_when_empty=None):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.columns_have = columns_have
        self.columns_want = columns_want
        self.keep_indices = [self.columns_have.index(col) for col in self.columns_want]
        self.keep_when_empty = [self.columns_want.index(col) for col in keep_when_empty] if keep_when_empty else []
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

        if any(selected[i] == '' and i not in self.keep_when_empty for i in range(len(selected))):
            return None
            
        return '|'.join(selected)

    def run(self):
        self._running = True

        def on_message(message):
            if not self._running:
                return
            try:
                # Expect message as bytes: header (6 bytes) + payload
                if not isinstance(message, bytes) or len(message) < 6:
                    print(f"Invalid message format or too short: {message}")
                    return
                header = message[:6]
                msg_type, data_type, payload_len = struct.unpack('>BBI', header)
                payload = message[6:]

                if msg_type == 2 and data_type == 6:
                    print('End-of-data signal received. Closing cleaner for this queue.')
                    self.stop()
                    return

                # Decode payload, split into rows, filter, repack
                try:
                    payload_str = payload.decode('utf-8')
                except Exception as e:
                    print(f"Failed to decode payload: {e}")
                    return
                rows = payload_str.split('\n')
                filtered_rows = [self._filter_row(row) for row in rows if row.strip()]
                filtered_rows = [row for row in filtered_rows if row]
                if not filtered_rows:
                    print("No rows after filtering, nothing sent.")
                    return
                new_payload_str = '\n'.join(filtered_rows)
                new_payload = new_payload_str.encode('utf-8')
                new_payload_len = len(new_payload)
                new_header = struct.pack('>BBI', msg_type, data_type, new_payload_len)
                new_message = new_header + new_payload
                self.out_queue.send(new_message)
                print(f"Sent filtered message to {self.queue_out} (rows: {len(filtered_rows)})")
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

    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_path)

    if data_type not in config:
        raise ValueError(f"Unknown data type: {data_type}")
    columns_have = [col.strip() for col in config[data_type]['have'].split(',')]
    columns_want = [col.strip() for col in config[data_type]['want'].split(',')]
    keep_when_empty_str = config[data_type].get('keep_when_empty', '').strip()
    keep_when_empty = [col.strip() for col in keep_when_empty_str.split(',')] if keep_when_empty_str else None

    cleaner = Cleaner(queue_in, queue_out, columns_have, columns_want, rabbitmq_host, keep_when_empty)

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
