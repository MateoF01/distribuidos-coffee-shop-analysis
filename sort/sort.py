import os
import configparser
import csv
import heapq
import time
from shared.worker import FileProcessingWorker
from shared import protocol

from WSM.wsm_client import WSMClient
from wsm_config import WSM_NODES
import socket
import logging

class SorterConfig:
    """
    Configuration container for Sorter.
    
    Loads and validates sorting parameters from config file.
    
    Attributes:
        chunk_size (int): Rows per sorting chunk.
        temp_file_suffix (str): Suffix for temporary files.
        temp_file_prefix (str): Prefix for temporary files.
        header_size (int): Message header size.
        sort_signal_type (int): Message type for sort signals.
        sort_signal_data_type (int): Data type for sort signals.
        encoding (str): File encoding.
        newline_mode (str): Newline mode for CSV files.
        invalid_number_fallback (float): Fallback for invalid numbers.
        sort_column_index (int): Primary sort column index.
        sort_columns (list): List of column indices for multi-column sorting.
    
    Example:
        >>> config = SorterConfig(config_parser, 'q2_a')
        >>> config.chunk_size
        10000
        >>> config.sort_columns
        [0, 1]
    """
    def __init__(self, config_parser, data_type):
        """
        Initialize configuration from ConfigParser.
        
        Args:
            config_parser (ConfigParser): Configuration parser.
            data_type (str): Data type identifier (e.g., 'q2_a', 'q3').
        
        Raises:
            ValueError: If data_type not in config or validation fails.
        """
        self.chunk_size = int(config_parser['sort']['chunk_size'])
        self.temp_file_suffix = config_parser['sort']['temp_file_suffix']
        self.temp_file_prefix = config_parser['sort']['temp_file_prefix']
        
        self.header_size = int(config_parser['messages']['header_size'])
        self.sort_signal_type = int(config_parser['messages']['sort_signal_type'])
        self.sort_signal_data_type = int(config_parser['messages']['sort_signal_data_type'])
        
        self.encoding = config_parser['files']['encoding']
        self.newline_mode = config_parser['files']['newline_mode'] or None
        
        self.invalid_number_fallback = float(config_parser['sorting']['invalid_number_fallback'])
        
        if data_type not in config_parser:
            raise ValueError(f"Unknown data type: {data_type}")
        self.sort_column_index = int(config_parser[data_type]['sort_column_index'])
        
        if 'sort_columns' in config_parser[data_type]:
            sort_columns_str = config_parser[data_type]['sort_columns']
            self.sort_columns = [int(col.strip()) for col in sort_columns_str.split(',')]
        else:
            self.sort_columns = [self.sort_column_index]
        
        self._validate()
    
    def _validate(self):
        """
        Validate configuration parameters.
        
        Raises:
            ValueError: If any parameter is invalid.
        """
        if self.chunk_size <= 0:
            raise ValueError(f"chunk_size must be positive, got {self.chunk_size}")
        
        if self.header_size <= 0:
            raise ValueError(f"header_size must be positive, got {self.header_size}")
        
        if not self.temp_file_suffix:
            raise ValueError("temp_file_suffix cannot be empty")
        
        if not self.temp_file_prefix:
            raise ValueError("temp_file_prefix cannot be empty")
        
        if not self.encoding:
            raise ValueError("encoding cannot be empty")
        
        if self.sort_column_index < 0:
            raise ValueError(f"sort_column_index must be non-negative, got {self.sort_column_index}")
    
    def __str__(self):
        """
        String representation for debugging.
        
        Returns:
            str: Configuration summary.
        """
        return (f"SorterConfig(chunk_size={self.chunk_size}, "
                f"temp_file_suffix='{self.temp_file_suffix}', "
                f"temp_file_prefix='{self.temp_file_prefix}', "
                f"header_size={self.header_size}, "
                f"sort_signal_type={self.sort_signal_type}, "
                f"encoding='{self.encoding}', "
                f"invalid_number_fallback={self.invalid_number_fallback}, "
                f"sort_column_index={self.sort_column_index})")

class Sorter(FileProcessingWorker):
    """
    External merge sort worker for large CSV files.
    
    Uses chunk-based sorting with temporary files and heap-based merging.
    Supports single or multi-column lexicographic sorting. Triggered by
    sort signals, supports per-request file isolation.
    
    Architecture:
        - Input: Sort signal with request_id
        - Reads: CSV file from request-specific directory
        - Chunks: Sorts chunks in memory, writes to temp files
        - Merges: k-way merge using min-heap
        - Output: Sorted CSV + completion notification
    
    Attributes:
        sort_column_index (int): Primary sort column.
        config (SorterConfig): Configuration object.
        completion_queue_name (str): Completion queue name.
        base_input_file (str): Template path for input files.
        base_output_file (str): Template path for output files.
        _temp_files_by_request (dict): Temporary files per request.
        _processed_requests (set): Processed request IDs.
    
    Example:
        >>> sorter = Sorter(
        ...     queue_in='sorter_q2_a',
        ...     input_file='/app/output/q2_a/joined.csv',
        ...     output_file='/app/output/q2_a/sorted.csv',
        ...     rabbitmq_host='rabbitmq',
        ...     config=config,
        ...     completion_queue='sender_notifications_q2_a'
        ... )
        
        On sort signal with request_id=req-123:
        Reads: /app/output/q2_a/req-123/joined.csv
        Writes: /app/output/q2_a/req-123/sorted.csv
        Sends: Completion notification
    """
    def __init__(self, queue_in, input_file, output_file, rabbitmq_host, config: SorterConfig, completion_queue=None):
        """
        Initialize sorter with file paths and configuration.
        
        Args:
            queue_in (str): Input queue for sort signals.
            input_file (str): Base path template for input CSV files.
            output_file (str): Base path template for output CSV files.
            rabbitmq_host (str): RabbitMQ server hostname.
            config (SorterConfig): Configuration object.
            completion_queue (str, optional): Queue for completion notifications.
        """
        queue_out = completion_queue if completion_queue else None
        super().__init__(queue_in, queue_out, rabbitmq_host, input_file=input_file, output_file=output_file)
        
        # --- WSM Heartbeat Integration ----
        self.replica_id = socket.gethostname()

        worker_type_key = "coordinator"

        # Read WSM host/port (OPTIONAL for single-node; required if you specify wsm host in compose)
        wsm_host = os.environ.get("WSM_HOST", None)
        wsm_port = int(os.environ.get("WSM_PORT", "0")) if os.environ.get("WSM_PORT") else None

        # Load multi-node config if exists
        wsm_nodes = WSM_NODES.get(worker_type_key)

        # Create client in heartbeat-only mode
        self.wsm_client = WSMClient(
            worker_type=worker_type_key,
            replica_id=self.replica_id,
            host=wsm_host,
            port=wsm_port,
            nodes=wsm_nodes
        )

        logging.info(f"[Coordinator] Heartbeat WSM client ready for {worker_type_key}, replica={self.replica_id}")

        
        
        
        self.sort_column_index = config.sort_column_index
        self.config = config
        self.completion_queue_name = completion_queue
        self._temp_files = []
        self._temp_files_by_request = {}
        self._processed_requests = set()

        self.base_input_file = input_file
        self.base_output_file = output_file
        self.request_id_initialized = False
        self.current_request_id = 0

    def _initialize_request_paths(self, request_id):
        """
        Initialize per-request input/output file paths.
        
        Args:
            request_id (str): Request identifier.
        
        Example:
            Base input: /app/output/q2_a/joined.csv
            Base output: /app/output/q2_a/sorted.csv
            Request: req-123
            
            Result input: /app/output/q2_a/req-123/joined.csv
            Result output: /app/output/q2_a/req-123/sorted.csv
        """
        if hasattr(self, "_initialized_requests") and request_id in self._initialized_requests:
            return
        if not hasattr(self, "_initialized_requests"):
            self._initialized_requests = set()
        self._initialized_requests.add(request_id)

        dir_in = os.path.dirname(self.base_input_file)
        file_in = os.path.basename(self.base_input_file)
        new_input = os.path.join(dir_in, str(request_id), file_in)

        dir_out = os.path.dirname(self.base_output_file)
        file_out = os.path.basename(self.base_output_file)
        new_output = os.path.join(dir_out, str(request_id), file_out)

        os.makedirs(os.path.dirname(new_input), exist_ok=True)
        os.makedirs(os.path.dirname(new_output), exist_ok=True)

        self.input_file = new_input
        self.output_file = new_output
        self.current_request_id = request_id

        self._temp_files_by_request[request_id] = []

        print(f"[Sorter] Initialized paths for request_id={request_id}")
        print(f"[Sorter]   Input file: {self.input_file}")
        print(f"[Sorter]   Output file: {self.output_file}")


    def _write_sorted_chunk(self, chunk_data):
        """
        Sort chunk in memory and write to temporary file.
        
        Args:
            chunk_data (list): List of CSV rows to sort.
        
        Example:
            >>> sorter._write_sorted_chunk([['b', '2'], ['a', '1'], ['c', '3']])
            Writes sorted chunk to temp file: /app/output/q2_a/req-123/temp_1732800000_0.csv
        """
        try:
            if len(self.config.sort_columns) > 1:
                chunk_data.sort(key=lambda row: tuple(row[col] for col in self.config.sort_columns))
            else:
                chunk_data.sort(key=lambda row: row[self.sort_column_index])
            
            timestamp = str(int(time.time() * 1000000))
            chunk_id = len(self._temp_files_by_request[self.current_request_id])
            temp_filename = f"{self.config.temp_file_prefix}{timestamp}_{chunk_id}{self.config.temp_file_suffix}"
            
            output_dir = os.path.dirname(self.output_file)
            temp_path = os.path.join(output_dir, temp_filename)
            
            if self.current_request_id not in self._temp_files_by_request:
                self._temp_files_by_request[self.current_request_id] = []
            self._temp_files_by_request[self.current_request_id].append(temp_path)
            
            def write_func(path):
                with open(path, 'w', newline=self.config.newline_mode, encoding=self.config.encoding) as temp_file:
                    writer = csv.writer(temp_file)
                    writer.writerows(chunk_data)
            
            FileProcessingWorker.atomic_write(temp_path, write_func)
            
            print(f"Wrote sorted chunk with {len(chunk_data)} rows to {temp_path}")
            
        except Exception as e:
            print(f"Error writing sorted chunk: {e}")

    def _merge_sorted_files(self, header, request_id):
        """
        Merge sorted temporary files using k-way heap merge.
        
        Args:
            header (list): CSV header row.
            request_id (str): Request identifier.
        
        Example:
            Input: 3 sorted temp files
            - temp1.csv: a,1\nc,3
            - temp2.csv: b,2
            - temp3.csv: d,4\ne,5
            
            Output: sorted.csv (header + merged rows)
            header\na,1\nb,2\nc,3\nd,4\ne,5
        """
        temp_files = self._temp_files_by_request.get(request_id, [])
        if not temp_files:
            return

        try:
            file_readers = []
            
            for temp_file in temp_files:
                f = open(temp_file, 'r', newline=self.config.newline_mode, encoding=self.config.encoding)
                reader = csv.reader(f)
                file_readers.append((reader, f))
            
            heap = []
            
            for i, (reader, f) in enumerate(file_readers):
                try:
                    row = next(reader)
                    if len(self.config.sort_columns) > 1:
                        sort_value = tuple(row[col] for col in self.config.sort_columns)
                    else:
                        sort_value = row[self.sort_column_index]
                    heapq.heappush(heap, (sort_value, i, row))
                except StopIteration:
                    f.close()
            
            def merge_write_func(output_path):
                with open(output_path, 'w', newline=self.config.newline_mode, encoding=self.config.encoding) as output_file:
                    writer = csv.writer(output_file)
                    writer.writerow(header)
                    
                    while heap:
                        sort_value, file_idx, row = heapq.heappop(heap)
                        writer.writerow(row)
                        
                        reader, f = file_readers[file_idx]
                        try:
                            next_row = next(reader)
                            if len(self.config.sort_columns) > 1:
                                next_sort_value = tuple(next_row[col] for col in self.config.sort_columns)
                            else:
                                next_sort_value = next_row[self.sort_column_index]
                            heapq.heappush(heap, (next_sort_value, file_idx, next_row))
                        except StopIteration:
                            f.close()
            
            FileProcessingWorker.atomic_write(self.output_file, merge_write_func)
            
            for reader, f in file_readers:
                if not f.closed:
                    f.close()
                    
            for temp_file in temp_files:
                try:
                    os.unlink(temp_file)
                except OSError:
                    pass

            print(f"Successfully merged {len(temp_files)} sorted chunks into {self.output_file}")
            
        except Exception as e:
            print(f"Error merging sorted files: {e}")

    def _process_file(self):
        """
        Sort input file using external merge sort.
        
        Steps:
        1. Read CSV in chunks
        2. Sort each chunk in memory
        3. Write sorted chunks to temp files
        4. K-way merge all temp files
        5. Send completion notification
        
        Example:
            Input: 25000 rows, chunk_size: 10000
            Creates: 3 temp files (10000, 10000, 5000 rows)
            Merges: Into single sorted output
        """
        try:
            if not os.path.exists(self.input_file):
                print(f"Input file {self.input_file} does not exist (empty stream). Sending completion signal.")
                self._send_completion_signal()
                return
            
            current_chunk = []
            header = None
            
            with open(self.input_file, 'r', newline=self.config.newline_mode, encoding=self.config.encoding) as csvfile:
                reader = csv.reader(csvfile)
                header = next(reader)
                
                for row in reader:
                    current_chunk.append(row)
                    
                    if len(current_chunk) >= self.config.chunk_size:
                        self._write_sorted_chunk(current_chunk)
                        current_chunk = []
                
                if current_chunk:
                    self._write_sorted_chunk(current_chunk)
            
            self._merge_sorted_files(header, self.current_request_id)
            
            if len(self.config.sort_columns) > 1:
                print(f"Successfully sorted {self.input_file} by columns {self.config.sort_columns} and saved to {self.output_file}")
            else:
                print(f"Successfully sorted {self.input_file} by column {self.sort_column_index} and saved to {self.output_file}")
            
            self._send_completion_signal()
            
        except Exception as e:
            print(f"Error sorting file: {e}")

    def _send_completion_signal(self):
        """
        Send completion notification to downstream workers.
        """
        try:
            if self.out_queues:
                message = protocol.create_notification_message(0, b'', self.current_request_id)
                self.out_queues[0].send(message)
                print(f"Sent completion signal for {self.output_file} with request_id={self.current_request_id}")
            else:
                print("No completion queue configured")
        except Exception as e:
            print(f"Error sending completion signal: {e}")

    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
        """
        Process sort signal to trigger file sorting.
        Handles DATA_END for cleanup during abnormal termination.
        
        Args:
            message (bytes): Raw message.
            msg_type (int): Message type.
            data_type (int): Data type.
            request_id (str): Request identifier.
            position (int): Position number.
            payload (bytes): Message payload.
            queue_name (str, optional): Source queue name.
        """
        if msg_type == self.config.sort_signal_type and data_type == protocol.DATA_END:
            print(f"[Sorter] Received DATA_END for request_id={request_id}")
            self._cleanup_request_files(request_id)
            cleanup_message = protocol.create_notification_message(protocol.DATA_END, b"", request_id)
            for q in self.out_queues:
                q.send(cleanup_message)
            print(f"[Sorter] Forwarded DATA_END notification for request_id={request_id}")
            return
        
        if msg_type == self.config.sort_signal_type:
            if request_id in self._processed_requests:
                print(f'Sort signal for request_id={request_id} already processed, ignoring duplicate')
                return
            
            self._processed_requests.add(request_id)
            
            self.current_request_id = request_id
            
            self._initialize_request_paths(request_id)
            
            print(f'Sort signal received for file: {self.input_file} with request_id={request_id}')
            self._process_file()
            print(f'Sort complete. Output saved to: {self.output_file}')
        else:
            print(f"Received unexpected message type: {msg_type}/{data_type}")
    
    def _cleanup_request_files(self, request_id):
        """
        Clean up tracking data structures for a request during abnormal termination.
        
        Note: File cleanup is handled by the upstream joiner, which shares the same
        output directory (/app/output/<request_id>/) and removes it when receiving DATA_END.
        This prevents redundant cleanup and race conditions.
        
        Args:
            request_id (str): Request identifier to clean up.
        """
        # Clean up tracking data
        if request_id in self._temp_files_by_request:
            del self._temp_files_by_request[request_id]
        
        if request_id in self._processed_requests:
            self._processed_requests.discard(request_id)
        
        if hasattr(self, "_initialized_requests") and request_id in self._initialized_requests:
            self._initialized_requests.discard(request_id)
        
        print(f"[Sorter] Cleared tracking data for request_id={request_id}")

    def _validate_message(self, message):
        """
        Validate message format and size.
        
        Args:
            message (bytes): Raw message.
        
        Returns:
            bool: True if valid, False otherwise.
        """
        if not isinstance(message, bytes) or len(message) < self.config.header_size:
            print(f"Invalid message format or too short: {message}")
            return False
        return True

if __name__ == '__main__':
    def create_sorter():
        """
        Factory function to create Sorter from environment configuration.
        
        Environment Variables:
            QUEUE_IN: Input queue for sort signals (required).
            INPUT_FILE: Base path for input CSV files (required).
            OUTPUT_FILE: Base path for output CSV files (required).
            DATA_TYPE: Data type identifier (required, e.g., 'q2_a', 'q3').
            COMPLETION_QUEUE: Queue for completion notifications (optional).
            RABBITMQ_HOST: RabbitMQ hostname (default: rabbitmq).
        
        Returns:
            Sorter: Configured sorter instance.
        """
        queue_in = os.environ.get('QUEUE_IN')
        input_file = os.environ.get('INPUT_FILE')
        output_file = os.environ.get('OUTPUT_FILE')
        data_type = os.environ.get('DATA_TYPE')
        completion_queue = os.environ.get('COMPLETION_QUEUE')
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

        if not all([queue_in, input_file, output_file, data_type]):
            raise ValueError("Missing required environment variables: QUEUE_IN, INPUT_FILE, OUTPUT_FILE, DATA_TYPE")

        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config_parser = configparser.ConfigParser()
        config_parser.read(config_path)
        
        config = SorterConfig(config_parser, data_type)
        print(f"Loaded configuration: {config}")
        
        return Sorter(queue_in, input_file, output_file, rabbitmq_host, config, completion_queue)
    
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Sorter.run_worker_main(create_sorter, config_path)
