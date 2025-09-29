import os
import signal
import sys
import threading
import struct
import configparser
import csv
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue

class SenderConfig:
    def __init__(self, config_parser):
        """Initialize configuration from ConfigParser object"""
        # Sender configuration
        self.batch_size = int(config_parser['sender']['batch_size'])
        self.encoding = config_parser['sender']['encoding']
        
        # Message configuration
        self.header_size = int(config_parser['messages']['header_size'])
        self.completion_signal_type = int(config_parser['messages']['completion_signal_type'])
        self.completion_signal_data_type = int(config_parser['messages']['completion_signal_data_type'])
        
        # File configuration
        self.file_encoding = config_parser['files']['encoding']
        self.newline_mode = config_parser['files']['newline_mode'] or None
        
        # Validate configuration
        self._validate()
    
    def _validate(self):
        """Validate configuration parameters"""
        if self.batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {self.batch_size}")
        
        if self.header_size <= 0:
            raise ValueError(f"header_size must be positive, got {self.header_size}")

class Sender:
    def __init__(self, queue_in, queue_out, input_file, rabbitmq_host, config):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.input_file = input_file
        self.config = config
        self._running = False
        self._shutdown_event = threading.Event()
        self.in_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_in)
        self.out_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_out)

    def _send_csv_file(self):
        """Read CSV file and send it in batches to output queue"""
        if not os.path.exists(self.input_file):
            print(f"[ERROR] Input file {self.input_file} does not exist")
            return
        
        try:
            rows_sent = 0
            batch = []
            
            print(f"[INFO] Starting to send file {self.input_file}")
            
            with open(self.input_file, 'r', encoding=self.config.file_encoding, newline=self.config.newline_mode) as csvfile:
                reader = csv.reader(csvfile)
                
                # Skip header if it exists
                header = next(reader, None)
                if header:
                    print(f"[INFO] Header: {header}")
                
                for row in reader:
                    if not self._running:
                        print("[INFO] Sender stopped, aborting file transmission")
                        return
                    
                    # Convert row to CSV string format
                    row_str = ','.join(row)
                    batch.append(row_str)
                    
                    # Send batch when it reaches configured size
                    if len(batch) >= self.config.batch_size:
                        print(f"[INFO] Sending batch of size: {len(batch)}, batch: {batch}")
                        self._send_batch(batch)
                        rows_sent += len(batch)
                        batch = []
                        print(f"[INFO] Sent batch, total rows sent: {rows_sent}")
                
                # Send remaining rows in batch
                if batch:
                    print(f"[INFO] Sending batch of size: {len(batch)}, batch: {batch}")
                    self._send_batch(batch)
                    rows_sent += len(batch)
                    print(f"[INFO] Sent final batch, total rows sent: {rows_sent}")
            
            # Send END signal
            self._send_end_signal()
            print(f"[INFO] Sent END signal. Total rows transmitted: {rows_sent}")
            
        except Exception as e:
            print(f"[ERROR] Error sending CSV file: {e}")

    def _send_batch(self, batch):
        """Send a batch of rows to output queue"""
        try:
            payload = "\n".join(batch).encode(self.config.encoding)
            # Create message with header: msg_type=1 (DATA), data_type=1 (TRANSACTIONS)
            header = struct.pack('>BBI', 1, 1, len(payload))
            message = header + payload
            self.out_queue.send(message)
        except Exception as e:
            print(f"[ERROR] Error sending batch: {e}")
            raise

    def _send_end_signal(self):
        """Send END signal to output queue"""
        try:
            # Create END message: msg_type=2 (END), data_type=6 (DATA_END)
            header = struct.pack('>BBI', 2, 6, 0)
            self.out_queue.send(header)
        except Exception as e:
            print(f"[ERROR] Error sending END signal: {e}")

    def run(self):
        """Main run loop - listen for completion signals"""
        self._running = True

        def on_message(message):
            if not self._running:
                return
            try:
                # Expect message as bytes: header (configured size) + payload
                if not isinstance(message, bytes) or len(message) < self.config.header_size:
                    print(f"[WARN] Invalid message format or too short: {message}")
                    return
                
                header = message[:self.config.header_size]
                msg_type, data_type, payload_len = struct.unpack('>BBI', header)
                payload = message[self.config.header_size:] if payload_len > 0 else b""

                print(f"[INFO] Received message: msg_type={msg_type}, data_type={data_type}")

                # Check for completion signal from sorter
                if msg_type == self.config.completion_signal_type and data_type == self.config.completion_signal_data_type:
                    print(f'[INFO] Completion signal received. Starting to send file: {self.input_file}')
                    self._send_csv_file()
                    print(f'[INFO] File transmission complete')
                    # self.stop()
                    return
                else:
                    print(f"[WARN] Received unexpected message type: {msg_type}/{data_type}")
                
            except Exception as e:
                print(f"[ERROR] Error processing message: {e} - Message: {message}")

        print(f"[INFO] Sender listening on {self.queue_in} for completion signals")
        self.in_queue.start_consuming(on_message)

        # Keep the main thread alive
        try:
            self._shutdown_event.wait()
        except KeyboardInterrupt:
            print("[INFO] Keyboard interrupt received, shutting down...")
            self.stop()

    def stop(self):
        """Stop the sender"""
        print("[INFO] Stopping sender...")
        self._running = False
        self._shutdown_event.set()
        try:
            self.in_queue.stop_consuming()
        except Exception as e:
            print(f"[ERROR] Error stopping consumer: {e}")
        self.close()

    def close(self):
        """Close all connections"""
        try:
            self.in_queue.close()
            self.out_queue.close()
        except Exception as e:
            print(f"[ERROR] Error closing queues: {e}")

if __name__ == '__main__':
    # Configure via environment variables
    queue_in = os.environ.get('QUEUE_IN')
    queue_out = os.environ.get('QUEUE_OUT')
    input_file = os.environ.get('INPUT_FILE')
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

    if not all([queue_in, queue_out, input_file]):
        raise ValueError("Missing required environment variables: QUEUE_IN, QUEUE_OUT, INPUT_FILE")

    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    config_parser = configparser.ConfigParser()
    config_parser.read(config_path)
    
    # Create configuration object
    config = SenderConfig(config_parser)
    print(f"[INFO] Loaded sender configuration")
    
    sender = Sender(queue_in, queue_out, input_file, rabbitmq_host, config)
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print(f'[INFO] Received signal {signum}, shutting down sender gracefully...')
        sender.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        print(f"[INFO] Starting sender...")
        sender.run()
    except KeyboardInterrupt:
        print('[INFO] Keyboard interrupt received, shutting down sender.')
        sender.stop()
    except Exception as e:
        print(f'[ERROR] Error in sender: {e}')
        sender.stop()
    finally:
        print('[INFO] Sender shutdown complete.')
