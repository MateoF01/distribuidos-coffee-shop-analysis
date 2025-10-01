import os
import signal
import sys
import threading
import struct
import configparser
import csv
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue

class Joiner:
    def __init__(self, queue_in, queue_out, output_file, columns_want, rabbitmq_host):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.output_file = output_file
        self.columns_want = columns_want
        self._running = False
        self._shutdown_event = threading.Event()
        self.in_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_in)
        self.out_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_out) if queue_out else None
        self._lock = threading.Lock()
        self._csv_initialized = False
        self._rows_written = 0

    def _process_row(self, row):
        """Process a row and extract the required columns"""
        items = row.split('|')
        
        # For Q1, we expect: transaction_id,final_amount,created_at,store_id,user_id
        # But we only want: transaction_id,final_amount
        if len(items) < 2:
            print(f"Row has insufficient columns: {row}")
            return None
            
        try:
            # Extract only the columns we want (transaction_id, final_amount)
            transaction_id = items[0]
            final_amount = items[1]
            return [transaction_id, final_amount]
        except IndexError as e:
            print(f"Index error processing row: {row} - {e}")
            return None

    def _initialize_csv(self):
        """Initialize CSV file with headers"""
        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(self.columns_want)
            self._csv_initialized = True
            print(f"Initialized CSV file {self.output_file} with headers: {self.columns_want}")
        except Exception as e:
            print(f"Error initializing CSV file {self.output_file}: {e}")

    def _write_rows_to_csv(self, rows_data):
        """Append rows to CSV file"""
        try:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(rows_data)
            self._rows_written += len(rows_data)
            print(f"Wrote {len(rows_data)} rows to {self.output_file}. Total rows written: {self._rows_written}")
        except Exception as e:
            print(f"Error writing rows to CSV file {self.output_file}: {e}")

    def _send_sort_request(self):
        """Send a sort request to the sorter"""
        try:
            if self.out_queue:
                # Create sort signal message (msg_type=3, data_type=0)
                header = struct.pack('>BBI', 3, 0, 0)  # Sort signal
                self.out_queue.send(header)
                print(f"Sent sort request for {self.output_file}")
            else:
                print("No output queue configured for sort requests")
        except Exception as e:
            print(f"Error sending sort request: {e}")

    def run(self):
        self._running = True

        def on_message(message):
            if not self._running:
                return
            try:
                # Initialize CSV file on first message
                if not self._csv_initialized:
                    self._initialize_csv()

                # Expect message as bytes: header (6 bytes) + payload
                if not isinstance(message, bytes) or len(message) < 6:
                    print(f"Invalid message format or too short: {message}")
                    return
                
                header = message[:6]
                msg_type, data_type, payload_len = struct.unpack('>BBI', header)
                payload = message[6:]
                print(f"Received message: msg_type={msg_type}, data_type={data_type}, payload_len={payload_len}")
                # Check for end-of-data signal
                if msg_type == 2 and data_type == 1:
                    print(f'End-of-data signal received. Sending sort request...')
                    self._send_sort_request()
                    print(f'CSV data collection complete with {self._rows_written} total rows. Sort request sent.')
                    # self.stop()
                    return

                # Process regular data message
                try:
                    payload_str = payload.decode('utf-8')
                except Exception as e:
                    print(f"Failed to decode payload: {e}")
                    return

                # Process rows and write directly to CSV
                rows = payload_str.split('\n')
                processed_rows = []
                
                for row in rows:
                    if row.strip():
                        processed_row = self._process_row(row.strip())
                        if processed_row:
                            processed_rows.append(processed_row)
                
                # Write processed rows to CSV immediately
                if processed_rows:
                    with self._lock:
                        self._write_rows_to_csv(processed_rows)
                
            except Exception as e:
                print(f"Error processing message: {e} - Message: {message}")

        print(f"Joiner listening on {self.queue_in}, will output to {self.output_file}")
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
        if self.out_queue:
            self.out_queue.close()

if __name__ == '__main__':
    # Configure via environment variables
    queue_in = os.environ.get('QUEUE_IN')
    queue_out = os.environ.get('QUEUE_OUT')
    output_file = os.environ.get('OUTPUT_FILE')
    query_type = os.environ.get('QUERY_TYPE')
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_path)

    if query_type not in config:
        raise ValueError(f"Unknown query type: {query_type}")
    
    columns_want = [col.strip() for col in config[query_type]['columns'].split(',')]

    joiner = Joiner(queue_in, queue_out, output_file, columns_want, rabbitmq_host)
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print(f'Received signal {signum}, shutting down joiner gracefully...')
        joiner.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        print(f"Starting joiner for {query_type} query...")
        joiner.run()
    except KeyboardInterrupt:
        print('Keyboard interrupt received, shutting down joiner.')
        joiner.stop()
    except Exception as e:
        print(f'Error in joiner: {e}')
        joiner.stop()
    finally:
        print('Joiner shutdown complete.')
