import os
import signal
import sys
import threading
import struct
import configparser
import csv
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue

class Joiner:
    def __init__(self, queue_in, queue_out, output_file, columns_want, rabbitmq_host, multiple_queues=None):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.output_file = output_file
        self.columns_want = columns_want
        self.rabbitmq_host = rabbitmq_host
        self._running = False
        self._shutdown_event = threading.Event()
        self._lock = threading.Lock()
        self._csv_initialized = False
        self._rows_written = 0
        
        # Multiple queues support
        self.multiple_queues = multiple_queues or [queue_in]  # Default to single queue for backward compatibility
        self.in_queues = {}
        self.out_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_out) if queue_out else None
        
        # Track end messages from each queue
        self.end_received = {queue: False for queue in self.multiple_queues}
        
        # Temp directory for storing data from each queue
        self.temp_dir = os.path.join(os.path.dirname(output_file), 'temp')
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Initialize queues
        for queue_name in self.multiple_queues:
            self.in_queues[queue_name] = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_name)

    def _process_row(self, row, queue_name):
        """Process a row and extract the required columns based on queue"""
        items = row.split('|')
        
        # For single queue mode (Q1), we expect: transaction_id,final_amount,created_at,store_id,user_id
        # But we only want: transaction_id,final_amount
        if not self.multiple_queues or len(self.multiple_queues) == 1:
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
        
        # For multiple queue mode, return raw data to be processed later
        return items

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

    def _save_to_temp_file(self, queue_name, rows_data):
        """Save rows to temporary file for a specific queue"""
        temp_file = os.path.join(self.temp_dir, f"{queue_name}.csv")
        
        try:
            # Check if file exists to determine if we need headers
            file_exists = os.path.exists(temp_file)
            
            with open(temp_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write headers only for the first write
                if not file_exists:
                    if queue_name == 'stores_cleaned_q4':
                        writer.writerow(['store_id', 'store_name'])
                    elif queue_name == 'users_cleaned':
                        writer.writerow(['user_id', 'birthdate'])
                    elif queue_name == 'resultados_groupby_q4':
                        writer.writerow(['store_id', 'user_id', 'purchase_qty'])
                
                writer.writerows(rows_data)
            
            print(f"Saved {len(rows_data)} rows to temp file: {temp_file}")
        except Exception as e:
            print(f"Error saving to temp file {temp_file}: {e}")

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

    def _send_notification_signal(self):
        """Send a notification signal to indicate completion"""
        try:
            if self.out_queue:
                # Create notification signal message (msg_type=3, data_type=0) - MSG_TYPE_NOTI
                header = struct.pack('>BBI', 3, 0, 0)  # Notification signal
                self.out_queue.send(header)
                print(f"Sent notification signal through {self.out_queue.queue_name}")
            else:
                print("No output queue configured for notification signals")
        except Exception as e:
            print(f"Error sending notification signal: {e}")

    def run(self):
        self._running = True

        def create_message_handler(queue_name):
            def on_message(message):
                if not self._running:
                    return
                try:
                    # For single queue mode, initialize CSV file on first message
                    if len(self.multiple_queues) == 1 and not self._csv_initialized:
                        self._initialize_csv()

                    # All messages have protocol headers
                    if not isinstance(message, bytes) or len(message) < 6:
                        print(f"Invalid message format from {queue_name}: {message}")
                        return
                    
                    header = message[:6]
                    msg_type, data_type, payload_len = struct.unpack('>BBI', header)
                    payload = message[6:]
                    print(f"Received message from {queue_name}: msg_type={msg_type}, data_type={data_type}, payload_len={payload_len}")
                    
                    # Check for end-of-data signal
                    if msg_type == 2:  # MSG_TYPE_END - accept any data_type
                        print(f'End-of-data signal received from {queue_name} (data_type={data_type})')
                        
                        # Only process the first END signal from each queue
                        with self._lock:
                            if not self.end_received[queue_name]:
                                self.end_received[queue_name] = True
                                print(f'Marked {queue_name} as completed')
                                
                                # Check if all queues have sent end signals
                                if all(self.end_received.values()):
                                    print("All queues have sent end signals. Processing joined data...")
                                    if len(self.multiple_queues) > 1:
                                        self._process_joined_data()
                                    else:
                                        self._send_sort_request()
                                        print(f'CSV data collection complete with {self._rows_written} total rows. Sort request sent.')
                            else:
                                print(f'Already received END signal from {queue_name}, ignoring duplicate')
                        return

                    # Process regular data message
                    try:
                        payload_str = payload.decode('utf-8')
                    except Exception as e:
                        print(f"Failed to decode payload from {queue_name}: {e}")
                        return

                    # Process rows based on queue type
                    rows = payload_str.split('\n')
                    processed_rows = []
                    
                    for row in rows:
                        if row.strip():
                            if queue_name == 'resultados_groupby_q4':
                                # Sender data (separated by ,)
                                items = row.strip().split(',')
                                if len(items) >= 3:  # store_id, user_id, purchase_qty
                                    processed_rows.append(items)
                            else:
                                # All other data including Q1 (separated by |)
                                items = row.strip().split('|')
                                if len(items) >= 2:
                                    processed_rows.append(items)
                    
                    # Handle rows based on mode
                    if processed_rows:
                        with self._lock:
                            if len(self.multiple_queues) > 1:
                                # Save to temp files for joining later
                                self._save_to_temp_file(queue_name, processed_rows)
                            else:
                                # Single queue mode - extract required columns and write to CSV
                                final_rows = []
                                for row_items in processed_rows:
                                    # For Q1: extract transaction_id, final_amount from items
                                    if len(row_items) >= 2:
                                        transaction_id = row_items[0]
                                        final_amount = row_items[1]
                                        final_rows.append([transaction_id, final_amount])
                                
                                if final_rows:
                                    self._write_rows_to_csv(final_rows)
                    
                except Exception as e:
                    print(f"Error processing message from {queue_name}: {e} - Message: {message}")
            
            return on_message

        # Start consuming from all queues
        for queue_name in self.multiple_queues:
            handler = create_message_handler(queue_name)
            print(f"Joiner listening on {queue_name}")
            self.in_queues[queue_name].start_consuming(handler)

        print(f"Joiner will output to {self.output_file}")

        # Keep the main thread alive - wait indefinitely until shutdown is signaled
        try:
            self._shutdown_event.wait()
        except KeyboardInterrupt:
            print("Keyboard interrupt received, shutting down...")
            self.stop()

    def _process_joined_data(self):
        """Process and join data from multiple temp files"""
        try:
            # Load lookup tables from temp files
            stores_lookup = {}
            users_lookup = {}
            
            # Load stores data (store_id -> store_name)
            stores_file = os.path.join(self.temp_dir, 'stores_cleaned_q4.csv')
            if os.path.exists(stores_file):
                with open(stores_file, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader, None)  # Skip header
                    for row in reader:
                        if len(row) >= 2:
                            stores_lookup[row[0]] = row[1]  # store_id -> store_name
                print(f"Loaded {len(stores_lookup)} store mappings")
            
            # Load users data (user_id -> birthdate)
            users_file = os.path.join(self.temp_dir, 'users_cleaned.csv')
            if os.path.exists(users_file):
                with open(users_file, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader, None)  # Skip header
                    for row in reader:
                        if len(row) >= 2:
                            users_lookup[row[0]] = row[1]  # user_id -> birthdate
                print(f"Loaded {len(users_lookup)} user mappings")
            
            # Process main data from resultados_groupby_q4
            main_file = os.path.join(self.temp_dir, 'resultados_groupby_q4.csv')
            if not os.path.exists(main_file):
                print("Error: Main data file (resultados_groupby_q4.csv) not found")
                return
            
            # Initialize output CSV
            self._initialize_csv()
            
            processed_rows = []
            with open(main_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)  # Skip header
                
                for row in reader:
                    # Expected format: store_id, user_id, purchase_qty
                    if len(row) >= 3:
                        store_id = row[0]
                        user_id = row[1]
                        purchase_qty = row[2]
                        
                        # Replace store_id with store_name
                        store_name = stores_lookup.get(store_id, store_id)  # Fallback to store_id if not found
                        
                        # Replace user_id with birthdate
                        birthdate = users_lookup.get(user_id, user_id)  # Fallback to user_id if not found
                        
                        # Create joined row: store_name, birthdate (no purchase_qty)
                        processed_row = [store_name, birthdate]
                        processed_rows.append(processed_row)
            
            # Write all processed rows
            if processed_rows:
                self._write_rows_to_csv(processed_rows)
                print(f"Successfully joined and wrote {len(processed_rows)} rows")
            
            # Send sort request signal 
            self._send_sort_request()
            print(f'Joined data processing complete. Sort request sent.')
            
        except Exception as e:
            print(f"Error processing joined data: {e}")

    def stop(self):
        self._running = False
        self._shutdown_event.set()
        try:
            # Stop all queue consumers
            for queue_name, queue in self.in_queues.items():
                try:
                    queue.stop_consuming()
                except Exception as e:
                    print(f"Error stopping consumer for {queue_name}: {e}")
        except Exception as e:
            print(f"Error stopping consumers: {e}")
        self.close()

    def close(self):
        # Close all queue connections
        for queue_name, queue in self.in_queues.items():
            try:
                queue.close()
            except Exception as e:
                print(f"Error closing queue {queue_name}: {e}")
        
        if self.out_queue:
            self.out_queue.close()

if __name__ == '__main__':
    # Configure via environment variables
    queue_in = os.environ.get('QUEUE_IN')
    queue_out = os.environ.get('QUEUE_OUT')
    output_file = os.environ.get('OUTPUT_FILE')
    query_type = os.environ.get('QUERY_TYPE')
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    
    # Support for multiple input queues (comma-separated)
    multiple_queues_str = os.environ.get('MULTIPLE_QUEUES')
    multiple_queues = None
    if multiple_queues_str:
        multiple_queues = [q.strip() for q in multiple_queues_str.split(',')]
    elif queue_in:
        multiple_queues = [queue_in]

    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_path)

    if query_type not in config:
        raise ValueError(f"Unknown query type: {query_type}")
    
    columns_want = [col.strip() for col in config[query_type]['columns'].split(',')]

    joiner = Joiner(queue_in, queue_out, output_file, columns_want, rabbitmq_host, multiple_queues)
    
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
