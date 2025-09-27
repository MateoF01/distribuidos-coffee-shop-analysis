import os
import signal
import sys
import threading
import struct
import configparser
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue

class Filter:
    def __init__(self, queue_in, queue_out, rabbitmq_host,):
        self.queue_in = queue_in

        if isinstance(queue_out, str):
            queue_out = [q.strip() for q in queue_out.split(',')]
        self.queue_out = queue_out #list of queues

        self._running = False
        self._shutdown_event = threading.Event()
        self.in_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_in)
        self.out_queues = [CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=q) for q in self.queue_out]

    def _filter_row(row):
        print(f"ROW A FILTRAR: {row}")
        return row

    def run(self):

        print("CORRIENDO EL FILTER LOCO")
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

                print(f"PAYLOAD FILTER:  {payload}")

                if msg_type == 2:
                    for q in self.out_queues:
                        q.send(message)

                    if data_type == 6:
                        print('End-of-data signal received. Closing Filter for this queue.')
                        self.stop()
                        return
                    else:
                        print(f"Sent end-of-data msg_type:{msg_type} signal to {self.queue_out}")
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
                for q in self.out_queues:
                    q.send(new_message)
                print(f"Sent filtered message to {self.queue_out}")
            except Exception as e:
                print(f"Error processing message: {e} - Message: {message}")

        print(f"Filter listening on {self.queue_in}, outputting to {self.queue_out}")
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


    filter = Filter(queue_in, queue_out, rabbitmq_host)

    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print(f'Received signal {signum}, shutting down Filter gracefully...')
        filter.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        print(f"Starting filter for {data_type} data...")
        filter.run()
    except KeyboardInterrupt:
        print('Keyboard interrupt received, shutting down filter.')
        filter.stop()
    except Exception as e:
        print(f'Error in filter: {e}')
        filter.stop()
    finally:
        print('filter shutdown complete.')
