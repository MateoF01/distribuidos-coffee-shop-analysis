import os
import csv
from shared import protocol
from shared.worker import FileProcessingWorker

class Sender(FileProcessingWorker):
    """
    File-to-queue sender for final query results.
    
    Reads processed CSV files and transmits them in batches to result queues.
    Triggered by notification signals, supports per-request file isolation.
    
    Architecture:
        - Input: Notification signals with request_id
        - Reads: CSV files from request-specific directories
        - Batches: Configurable batch size for efficient transmission
        - Output: Data messages + END signals to result queue
    
    Attributes:
        batch_size (int): Number of rows per batch.
        query_type (str): Query identifier ('q1', 'q2_a', 'q2_b', 'q3', 'q4').
        include_headers (bool): Whether to include CSV header.
        base_input_file (str): Template path for input files.
        _initialized_requests (set): Set of initialized request IDs.
    
    Example:
        >>> sender = Sender(
        ...     queue_in='sender_notifications_q2_a',
        ...     queue_out='results_q2_a',
        ...     input_file='/app/output/q2_a/sorted.csv',
        ...     rabbitmq_host='rabbitmq',
        ...     batch_size=5000,
        ...     query_type='q2_a'
        ... )
        
        On notification with request_id=req-123:
        Reads: /app/output/q2_a/req-123/sorted.csv
        Sends batches with Q2_RESULT_a data type
    """
    def __init__(self, queue_in, queue_out, input_file, rabbitmq_host, batch_size=5000, query_type='q1', include_headers=False):
        """
        Initialize sender with file path and transmission settings.
        
        Args:
            queue_in (str): Input queue for notification signals.
            queue_out (str): Output queue for result data.
            input_file (str): Base path template for CSV files.
            rabbitmq_host (str): RabbitMQ server hostname.
            batch_size (int, optional): Rows per batch. Defaults to 5000.
            query_type (str, optional): Query identifier. Defaults to 'q1'.
            include_headers (bool, optional): Include CSV header. Defaults to False.
        """
        super().__init__(queue_in, queue_out, rabbitmq_host, input_file=input_file)
        self.batch_size = batch_size
        self.query_type = query_type
        self.include_headers = include_headers
        
        self.base_input_file = input_file
        self.request_id_initialized = False
        self._initialized_requests = set()


    def _initialize_request_paths(self, request_id):
        """
        Initialize per-request input file path.
        
        Args:
            request_id (str): Request identifier.
        
        Example:
            Base: /app/output/q2_a/sorted.csv
            Request: req-123
            Result: /app/output/q2_a/req-123/sorted.csv
        """
        if hasattr(self, "_initialized_requests") and request_id in self._initialized_requests:
            return
        if not hasattr(self, "_initialized_requests"):
            self._initialized_requests = set()

        self._initialized_requests.add(request_id)

        dir_path = os.path.dirname(self.base_input_file)
        filename = os.path.basename(self.base_input_file)

        new_path = os.path.join(dir_path, str(request_id), filename)
        os.makedirs(os.path.dirname(new_path), exist_ok=True)

        self.input_file = new_path
        self.current_request_id = request_id

        print(f"[INFO] Initialized sender paths for request_id {request_id}")
        print(f"[INFO]   Input file set to: {self.input_file}")


    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
        """
        Process notification signal to trigger file transmission.
        
        Args:
            message (bytes): Raw message.
            msg_type (int): Message type.
            data_type (int): Data type.
            request_id (str): Request identifier.
            position (int): Position number.
            payload (bytes): Message payload.
            queue_name (str, optional): Source queue name.
        """
        if msg_type == protocol.MSG_TYPE_NOTI:
            if data_type == protocol.DATA_END:
                print(f"[INFO] DATA_END received for request_id {request_id}, cleaning up files and forwarding")
                message = protocol.create_end_message(protocol.DATA_END, request_id)
                self.out_queues[0].send(message)
                print(f"[INFO] Forwarded DATA_END for request_id {request_id}")
                return
            
            self.current_request_id = request_id
            
            self._initialize_request_paths(request_id)
            
            print(f'[INFO] Completion signal received with request_id {request_id}. Starting file processing: {self.input_file}')
            self._process_file()
            print('[INFO] File processing complete')
        else:
            print(f"[WARNING] Received unexpected message type: {msg_type}/{data_type}")

    def _process_file(self):
        """
        Read CSV file and transmit in batches.
        
        Reads CSV, batches rows, sends with position tracking, then sends END signals.
        
        Example:
            File: 11234 rows, batch_size: 5000
            
            Sends:
            - position=1: 5000 rows
            - position=2: 5000 rows
            - position=3: 1234 rows
            - END (position=4, Q2_RESULT_a)
            - END (DATA_END)
        """
        if not os.path.exists(self.input_file):
            print(f"[ERROR] Input file {self.input_file} does not exist")
            return
        
        try:
            rows_sent = 0
            batch = []
            position_counter = 1
            
            print(f"[INFO] Starting to send file {self.input_file}")
            
            with open(self.input_file, 'r', encoding='utf-8', newline='') as csvfile:
                reader = csv.reader(csvfile)
                
                header = next(reader, None)
                if header:
                    print(f"[INFO] Header: {header}")
                    if self.include_headers:
                        header_str = ','.join(header)
                        batch.append(header_str)
                        print(f"[INFO] Including header in output")
                
                for row in reader:
                    if not self._running:
                        print("[INFO] Sender stopped, aborting file transmission")
                        return
                    
                    row_str = ','.join(row)
                    batch.append(row_str)
                    
                    if len(batch) >= self.batch_size:
                        self._send_batch(batch, position_counter)
                        rows_sent += len(batch)
                        batch = []
                        position_counter += 1
                
                if batch:
                    self._send_batch(batch, position_counter)
                    position_counter += 1
                    rows_sent += len(batch)
                    print(f"[INFO] Sent final batch, total rows sent: {rows_sent}")
            
            self._send_end_signal(position_counter)
            print(f"[INFO] Sent END signal. Total rows transmitted: {rows_sent}")
            
        except Exception as e:
            print(f"[ERROR] Error sending CSV file: {e}")

    def _send_batch(self, batch, position_counter):
        """
        Send batch of rows to output queue.
        
        Args:
            batch (list): List of CSV row strings.
            position_counter (int): Position number for this batch.
        """
        try:
            payload = "\n".join(batch).encode('utf-8')
            data_type_map = {
                'q1': protocol.Q1_RESULT,
                'q2': protocol.Q2_RESULT_a,
                'q2_a': protocol.Q2_RESULT_a,
                'q2_b': protocol.Q2_RESULT_b,
                'q3': protocol.Q3_RESULT,
                'q4': protocol.Q4_RESULT
            }
            data_type = data_type_map.get(self.query_type, protocol.Q1_RESULT)
            message = protocol.create_data_message(data_type, payload, self.current_request_id, position_counter)
            self.out_queues[0].send(message)
            print(f"[INFO] Sent batch to results queue: query_type={self.query_type}, request_id={self.current_request_id}, rows={len(batch)}, position={position_counter}")
        except Exception as e:
            print(f"[ERROR] Error sending batch: {e}")
            raise

    def _send_end_signal(self, position_counter):
        """
        Send END signals to mark transmission completion.
        
        Sends two END signals:
        1. Query-specific result type with position
        2. DATA_END signal
        
        Args:
            position_counter (int): Next position after last batch.
        """
        try:
            data_type_map = {
                'q1': protocol.Q1_RESULT,
                'q2': protocol.Q2_RESULT_a,
                'q2_a': protocol.Q2_RESULT_a,
                'q2_b': protocol.Q2_RESULT_b,
                'q3': protocol.Q3_RESULT,
                'q4': protocol.Q4_RESULT
            }
            query_result_type = data_type_map.get(self.query_type, protocol.Q1_RESULT)
            message1 = protocol.create_end_message(query_result_type, self.current_request_id, position_counter)
            self.out_queues[0].send(message1)
            print(f"[INFO] Sent MSG_TYPE_END with {self.query_type.upper()}_RESULT ({query_result_type}) and position {position_counter} to results queue: request_id={self.current_request_id}")
            
            message2 = protocol.create_end_message(protocol.DATA_END, self.current_request_id, position_counter+1)
            self.out_queues[0].send(message2)
            print(f"[INFO] Sent MSG_TYPE_END with DATA_END to results queue: request_id={self.current_request_id}")
            
        except Exception as e:
            print(f"[ERROR] Error sending END signals: {e}")



if __name__ == '__main__':
    def create_sender():
        """
        Factory function to create Sender from environment configuration.
        
        Environment Variables:
            QUEUE_IN: Input queue for notifications (required).
            QUEUE_OUT: Output queue for result data (required).
            INPUT_FILE: Base path for CSV files (required).
            RABBITMQ_HOST: RabbitMQ hostname (default: rabbitmq).
            BATCH_SIZE: Rows per batch (default: 5000).
            QUERY_TYPE: Query ID ('q1', 'q2_a', 'q2_b', 'q3', 'q4', default: 'q1').
            INCLUDE_HEADERS: Include CSV header ('true'/'false', default: 'false').
        
        Returns:
            Sender: Configured sender instance.
        """
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
    
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Sender.run_worker_main(create_sender, config_path)
