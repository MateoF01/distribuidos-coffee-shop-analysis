import os
import signal
import sys
import threading
import struct
import csv
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue

class Joiner:
    def __init__(self, queue_in, output_file, columns, rabbitmq_host):
        self.queue_in = queue_in
        self.output_file = output_file
        self.columns = columns
        self._running = False
        self._shutdown_event = threading.Event()
        self.in_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_in)
        self.data_rows = []
        self.output_dir = '/app/output'
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)

    def _process_row(self, row):
        """Process a single row and extract the required columns"""
        items = row.split('|')
        
        if len(items) < len(self.columns):
            print(f"Row has insufficient columns: {row} (expected {len(self.columns)}, got {len(items)})")
            return None
            
        # For Q1, we expect columns: transaction_id, final_amount
        # Based on the cleaner config, the filtered transactions have: transaction_id,final_amount,created_at,store_id,user_id
        # So we need indices 0 and 1 for transaction_id and final_amount
        try:
            if len(self.columns) == 2 and self.columns == ['transaction_id', 'final_amount']:
                # Extract transaction_id (index 0) and final_amount (index 1)
                return [items[0], items[1]]
            else:
                # Generic extraction based on column positions
                return items[:len(self.columns)]
        except IndexError as e:
            print(f"Index error processing row: {row} - {e}")
            return None

    def _write_to_csv(self):
        """Write collected data to CSV file"""
        output_path = os.path.join(self.output_dir, f"{self.output_file}.csv")
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                # Write header
                writer.writerow(self.columns)
                # Write data rows
                writer.writerows(self.data_rows)
            print(f"Successfully wrote {len(self.data_rows)} rows to {output_path}")
        except Exception as e:
            print(f"Error writing to CSV file {output_path}: {e}")

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

                # Check for end-of-data signal
                if msg_type == 2 and data_type == 6:
                    print('End-of-data signal received. Writing data to CSV and shutting down.')
                    self._write_to_csv()
                    self.stop()
                    return

                # Process data message
                if msg_type == 1:  # Data message
                    try:
                        payload_str = payload.decode('utf-8')
                    except Exception as e:
                        print(f"Failed to decode payload: {e}")
                        return
                        
                    rows = payload_str.split('\n')
                    for row in rows:
                        if row.strip():
                            processed_row = self._process_row(row.strip())
                            if processed_row:
                                self.data_rows.append(processed_row)
                    
                    print(f"Processed batch with {len([r for r in rows if r.strip()])} rows. Total rows collected: {len(self.data_rows)}")
                    
            except Exception as e:
                print(f"Error processing message: {e} - Message: {message}")

        print(f"Joiner listening on {self.queue_in}, will output to {self.output_file}.csv")
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

if __name__ == '__main__':
    # Configure via environment variables
    queue_in = os.environ.get('QUEUE_IN', 'Q1_transactions_filtered')
    output_file = os.environ.get('OUTPUT_FILE', 'q1')
    columns_str = os.environ.get('COLUMNS', 'transaction_id,final_amount')
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    
    # Parse columns
    columns = [col.strip() for col in columns_str.split(',')]
    
    joiner = Joiner(queue_in, output_file, columns, rabbitmq_host)
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print(f'Received signal {signum}, shutting down joiner gracefully...')
        joiner.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        print(f"Starting joiner for queue {queue_in}...")
        joiner.run()
    except KeyboardInterrupt:
        print('Keyboard interrupt received, shutting down joiner.')
        joiner.stop()
    except Exception as e:
        print(f'Error in joiner: {e}')
        joiner.stop()
    finally:
        print('Joiner shutdown complete.')
