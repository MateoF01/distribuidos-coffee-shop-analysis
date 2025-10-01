import os
import sys
import signal
import threading
import configparser
import csv
import heapq
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue
from shared import protocol

# Load configuration
config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
config = configparser.ConfigParser()
config.read(config_path)

# Allow environment variable override for BASE_TEMP_DIR
BASE_TEMP_DIR = os.environ.get('BASE_TEMP_DIR', config['topper']['base_temp_dir'])
os.makedirs(BASE_TEMP_DIR, exist_ok=True)

class Topper:
    def __init__(self, queue_in, input_dir, output_file, rabbitmq_host, top_n=3, completion_queue=None):
        self.queue_in = queue_in
        self.input_dir = input_dir
        self.rabbitmq_host = rabbitmq_host
        self.top_n = top_n
        self.completion_queue = completion_queue
        self.query_id = queue_in
        self.output_dir = os.path.join(BASE_TEMP_DIR, self.query_id)
        os.makedirs(self.output_dir, exist_ok=True)
        output_filename = os.path.basename(output_file)
        self.output_file = os.path.join(self.output_dir, output_filename)
        self._running = False
        self._shutdown_event = threading.Event()
        self.in_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_in)
        self.out_queue = None
        if self.completion_queue:
            self.out_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=completion_queue)

    def run(self):
        self._running = True
        
        def on_message(message):
            if not self._running:
                return
            try:
                print(f"[Topper] Received signal from queue: {self.queue_in}")
                if not isinstance(message, bytes) or len(message) < 6:
                    print(f"Invalid message format or too short: {message}")
                    return
                
                msg_type, data_type, payload = protocol._unpack_message(message)
                
                if msg_type == protocol.MSG_TYPE_NOTI:
                    print('[Topper] Received completion signal, starting CSV processing...')
                    self.process_csv_files()
                    return
                else:
                    print(f"[Topper] Received unknown message type: {msg_type}")
                    
            except Exception as e:
                print(f"Error processing message: {e} - Message: {message}")

        print(f"Topper listening on {self.queue_in}")
        self.in_queue.start_consuming(on_message)
        
        try:
            self._shutdown_event.wait()
        except KeyboardInterrupt:
            print("Keyboard interrupt received, shutting down...")
            self.stop()

    def process_csv_files(self):
        """Process all CSV files in the input directory and create combined output"""
        print(f"[Topper] Processing CSV files from directory: {self.input_dir}")
        
        if not os.path.exists(self.input_dir):
            print(f"[Topper] Input directory does not exist: {self.input_dir}")
            return
        
        try:
            all_files = os.listdir(self.input_dir)
            csv_files = [f for f in all_files if f.lower().endswith('.csv')]
        except OSError as e:
            print(f"[Topper] Error reading directory {self.input_dir}: {e}")
            return
        
        if not csv_files:
            print(f"[Topper] No CSV files found in {self.input_dir}")
            return
        
        print(f"[Topper] Found {len(csv_files)} CSV files to process")
        
        all_rows = []
        
        # Sort CSV files numerically by filename (assuming filenames are numbers)
        def numeric_sort_key(filename):
            try:
                return int(os.path.splitext(filename)[0])
            except ValueError:
                return float('inf'), filename
        
        csv_files.sort(key=numeric_sort_key)
        
        for csv_filename in csv_files:
            filename_without_ext = os.path.splitext(csv_filename)[0]
            
            csv_file_path = os.path.join(self.input_dir, csv_filename)
            
            try:
                print(f"[Topper] Processing file: {csv_filename}")
                
                with open(csv_file_path, 'r', newline='', encoding='utf-8') as f:
                    csv_reader = csv.reader(f)
                    
                    min_heap = []
                    
                    for row in csv_reader:
                        # Skip rows that don't have at least 2 columns
                        if len(row) < 2:
                            continue
                        
                        # Try to parse the second column as a number for comparison
                        try:
                            current_value = float(row[1])
                        except (ValueError, IndexError):
                            continue
                        
                        if len(min_heap) < self.top_n:
                            heapq.heappush(min_heap, (current_value, row))
                        else:
                            if current_value > min_heap[0][0]:
                                heapq.heapreplace(min_heap, (current_value, row))
                    
                    if not min_heap:
                        print(f"[Topper] Skipping file with no valid numeric rows: {csv_filename}")
                        continue
                    
                    top_rows = [item[1] for item in sorted(min_heap, reverse=True)]
                    
                    for row in top_rows:
                        new_row = [filename_without_ext] + row
                        all_rows.append(new_row)
                    
                    print(f"[Topper] Added {len(top_rows)} rows from {csv_filename}")
                
            except Exception as e:
                print(f"[Topper] Error processing file {csv_filename}: {e}")
                continue
        
        if all_rows:
            # Create output directory if it doesn't exist
            output_dir = os.path.dirname(self.output_file)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            
            # Write all rows to output file
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                csv_writer = csv.writer(f)
                # Write header
                csv_writer.writerow(['store_id', 'user_id', 'purchases_qty'])
                # Write data rows
                csv_writer.writerows(all_rows)
            
            print(f"[Topper] Successfully created output file: {self.output_file}")
            print(f"[Topper] Total rows in output: {len(all_rows)}")
            
            # Send completion signal if completion queue is configured
            self._send_completion_signal()
        else:
            print("[Topper] No data to process")

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

    def _send_completion_signal(self):
        """Send completion signal to the next stage if completion queue is configured"""
        if self.out_queue:
            try:
                print(f"[Topper:{self.query_id}] Sending completion signal to {self.completion_queue}")
                completion_message = protocol.pack_message(protocol.MSG_TYPE_NOTI, protocol.DATA_END, b"")
                self.out_queue.send(completion_message)
                print(f"[Topper:{self.query_id}] Completion signal sent successfully")
            except Exception as e:
                print(f"[Topper:{self.query_id}] Error sending completion signal: {e}")

if __name__ == '__main__':
    # Configure via environment variables
    queue_in = os.environ.get('QUEUE_IN', 'topper_q4_signal')
    input_dir = os.environ.get('INPUT_DIR', '/app/temp/transactions_filtered_Q4')
    output_file = os.environ.get('OUTPUT_FILE', '/app/output/q4_top3.csv')
    top_n = int(os.environ.get('TOP_N', '3'))
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    completion_queue = os.environ.get('COMPLETION_QUEUE')

    topper = Topper(queue_in, input_dir, output_file, rabbitmq_host, top_n, completion_queue)
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print(f'Received signal {signum}, shutting down topper gracefully...')
        topper.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        print(f"Starting topper...")
        topper.run()
    except KeyboardInterrupt:
        print('Keyboard interrupt received, shutting down topper.')
        topper.stop()
    except Exception as e:
        print(f'Error in topper: {e}')
        topper.stop()
    finally:
        print('Topper shutdown complete.')
