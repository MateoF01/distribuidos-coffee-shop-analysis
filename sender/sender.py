import os
import signal
import sys
import threading
import csv
import logging
import time
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue
from shared import protocol
from shared.logging_config import initialize_log
from shared.worker import FileProcessingWorker

class Sender(FileProcessingWorker):
    def __init__(self, queue_in, queue_out, input_file, rabbitmq_host, batch_size=5000, query_type='q1', include_headers=False):
        super().__init__(queue_in, queue_out, rabbitmq_host, input_file=input_file)
        self.batch_size = batch_size
        self.query_type = query_type
        self.include_headers = include_headers
        
        # Store base path for request_id-based subdirectory creation
        self.base_input_file = input_file
        self.request_id_initialized = False

    def _initialize_request_paths(self, request_id):
        """Initialize input path with request_id subdirectory"""
        if self.request_id_initialized and self.current_request_id == request_id:
            return
        
        # Create request_id-based path for input file
        dir_path = os.path.dirname(self.base_input_file)
        filename = os.path.basename(self.base_input_file)
        new_path = os.path.join(dir_path, str(request_id), filename)
        
        # Update input file path
        self.input_file = new_path
        
        self.request_id_initialized = True
        print(f"[INFO] Initialized sender paths for request_id {request_id}")
        print(f"[INFO]   Input file: {self.input_file}")

    def _process_message(self, message, msg_type, data_type, request_id, timestamp, payload, queue_name=None):
        """Process message - handles completion signals and initializes request_id paths."""
        if msg_type == protocol.MSG_TYPE_NOTI:
            # Store request_id from notification for use in outgoing messages
            self.current_request_id = request_id
            
            # Initialize request-specific paths
            self._initialize_request_paths(request_id)
            
            print(f'[INFO] Completion signal received with request_id {request_id}. Starting file processing: {self.input_file}')
            self._process_file()
            print('[INFO] File processing complete')
        else:
            print(f"[WARNING] Received unexpected message type: {msg_type}/{data_type}")

    def _process_file(self):
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
            message = protocol.create_data_message(data_type, payload, self.current_request_id)
            self.out_queues[0].send(message)
            print(f"[INFO] Sent batch to results queue: query_type={self.query_type}, request_id={self.current_request_id}, rows={len(batch)}")
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
            message1 = protocol.create_end_message(query_result_type, self.current_request_id)
            self.out_queues[0].send(message1)
            print(f"[INFO] Sent MSG_TYPE_END with {self.query_type.upper()}_RESULT ({query_result_type}) to results queue: request_id={self.current_request_id}")
            
            # Send second END signal with DATA_END
            message2 = protocol.create_end_message(protocol.DATA_END, self.current_request_id)
            self.out_queues[0].send(message2)
            print(f"[INFO] Sent MSG_TYPE_END with DATA_END to results queue: request_id={self.current_request_id}")
            
        except Exception as e:
            print(f"[ERROR] Error sending END signals: {e}")



if __name__ == '__main__':
    def create_sender():
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
        
        return Sender(queue_in, queue_out, input_file, rabbitmq_host, batch_size, query_type, include_headers)
    
    # Use the Worker base class main entry point
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Sender.run_worker_main(create_sender, config_path)
