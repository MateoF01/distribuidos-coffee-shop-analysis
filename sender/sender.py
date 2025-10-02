import os
import signal
import sys
import threading
import csv
import logging
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue
from shared import protocol
from shared.logging_config import initialize_log

class Sender:
    def __init__(self, queue_in, queue_out, input_file, rabbitmq_host, batch_size=5000, query_type='q1', include_headers=False):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.input_file = input_file
        self.batch_size = batch_size
        self.query_type = query_type
        self.rabbitmq_host = rabbitmq_host
        self.include_headers = include_headers
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
            
            with open(self.input_file, 'r', encoding='utf-8', newline='') as csvfile:
                reader = csv.reader(csvfile)
                
                # Handle header
                header = next(reader, None)
                if header:
                    print(f"[INFO] Header: {header}")
                    if self.include_headers:
                        # Include header as first row if flag is set
                        header_str = ','.join(header)
                        batch.append(header_str)
                        print(f"[INFO] Including header in output")
                
                for row in reader:
                    if not self._running:
                        print("[INFO] Sender stopped, aborting file transmission")
                        return
                    
                    # Convert row to CSV string format
                    row_str = ','.join(row)
                    batch.append(row_str)
                    
                    # Send batch when it reaches configured size
                    if len(batch) >= self.batch_size:
                        #print(f"[INFO] Sending batch of size: {len(batch)}, batch: {batch}")
                        self._send_batch(batch)
                        rows_sent += len(batch)
                        batch = []
                        #print(f"[INFO] Sent batch, total rows sent: {rows_sent}")
                
                # Send remaining rows in batch
                if batch:
                    #print(f"[INFO] Sending batch of size: {len(batch)}, batch: {batch}")
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
            payload = "\n".join(batch).encode('utf-8')
            # Map query type to protocol data type
            data_type_map = {
                'q1': protocol.Q1_RESULT,
                'q2_a': protocol.Q2_RESULT_a,
                'q2_b': protocol.Q2_RESULT_b,
                'q3': protocol.Q3_RESULT,
                'q4': protocol.Q4_RESULT
            }
            data_type = data_type_map.get(self.query_type, protocol.Q1_RESULT)
            message = protocol.pack_message(protocol.MSG_TYPE_DATA, data_type, payload)
            self.out_queue.send(message)
        except Exception as e:
            print(f"[ERROR] Error sending batch: {e}")
            raise

    def _send_end_signal(self):
        """Send END signals to output queue"""
        try:
            # Send first END signal with query-specific result type
            data_type_map = {
                'q1': protocol.Q1_RESULT,
                'q2_a': protocol.Q2_RESULT_a,
                'q2_b': protocol.Q2_RESULT_b,
                'q3': protocol.Q3_RESULT,
                'q4': protocol.Q4_RESULT
            }
            query_result_type = data_type_map.get(self.query_type, protocol.Q1_RESULT)
            message1 = protocol.pack_message(protocol.MSG_TYPE_END, query_result_type, b"")
            self.out_queue.send(message1)
            print(f"[INFO] Sent MSG_TYPE_END with {self.query_type.upper()}_RESULT ({query_result_type})")
            
            # Send second END signal with DATA_END
            message2 = protocol.pack_message(protocol.MSG_TYPE_END, protocol.DATA_END, b"")
            self.out_queue.send(message2)
            print(f"[INFO] Sent MSG_TYPE_END with DATA_END")
            
        except Exception as e:
            print(f"[ERROR] Error sending END signals: {e}")

    def run(self):
        """Main run loop - listen for completion signals"""
        self._running = True

        def on_message(message):
            if not self._running:
                return
            try:
                if not isinstance(message, bytes) or len(message) < 6:
                    print(f"[WARN] Invalid message format or too short: {message}")
                    return
                
                msg_type, data_type, payload = protocol._unpack_message(message)

                print(f"[INFO] Received message: msg_type={msg_type}, data_type={data_type}")

                # Check for completion signal (like grouper uses MSG_TYPE_NOTI)
                if msg_type == protocol.MSG_TYPE_NOTI:
                    print(f'[INFO] Completion signal received. Starting to send file: {self.input_file}')
                    self._send_csv_file()
                    print(f'[INFO] File transmission complete')
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
    batch_size = int(os.environ.get('BATCH_SIZE', '5000'))
    query_type = os.environ.get('QUERY_TYPE', 'q1')
    include_headers = os.environ.get('INCLUDE_HEADERS', 'false').lower() == 'true'

    if not all([queue_in, queue_out, input_file]):
        raise ValueError("Missing required environment variables: QUEUE_IN, QUEUE_OUT, INPUT_FILE")

    print(f"[INFO] Using batch_size: {batch_size}, query_type: {query_type}, include_headers: {include_headers}")
    
    sender = Sender(queue_in, queue_out, input_file, rabbitmq_host, batch_size, query_type, include_headers)
    
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
