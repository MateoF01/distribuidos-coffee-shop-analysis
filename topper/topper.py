import os
import configparser
import csv
import heapq
import logging
from shared import protocol
from shared.worker import Worker
from WSM.wsm_client import WSMClient
from wsm_config import WSM_NODES
import socket

config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
config = configparser.ConfigParser()
config.read(config_path)

BASE_TEMP_DIR = os.environ.get('BASE_TEMP_DIR', config['topper']['base_temp_dir'])
os.makedirs(BASE_TEMP_DIR, exist_ok=True)

class Topper(Worker):
    """
    Top-N selection worker for processing aggregated CSV files.
    
    Processes sorted CSV files from reducers to extract top N entries by value.
    Supports multiple query modes (Q2: top items by quantity/subtotal, Q3: top stores by TPV,
    Q4: top users by purchases). Uses per-request directories for isolation and atomic writes
    for crash consistency.
    
    Architecture:
        1. Receives completion notification from reducer
        2. Reads all CSV files from request-specific input directory
        3. Uses min-heaps to efficiently track top N entries per metric
        4. Writes consolidated top N results atomically
        5. Sends completion notification to next stage
    
    Attributes:
        queue_in (str): Input notification queue name.
        input_dir (str): Base input directory for CSV files.
        output_file (str): Base output file path.
        top_n (int): Number of top entries to select.
        topper_mode (str): Query mode ('Q2', 'Q3', 'Q4').
        completion_queue_name (str): Output notification queue.
        base_output_dir (str): Base directory for per-request outputs.
        output_dir (str): Request-specific output directory (set on message).
        current_request_id (int): Active request identifier.
    
    Example:
        ```python
        # Environment setup
        os.environ['QUEUE_IN'] = 'topper_q2_signal'
        os.environ['INPUT_DIR'] = '/app/temp/reducer_q2'
        os.environ['OUTPUT_FILE'] = '/app/output/q2_top3.csv'
        os.environ['TOP_N'] = '3'
        os.environ['TOPPER_MODE'] = 'Q2'
        os.environ['COMPLETION_QUEUE'] = 'joiner_signal'
        
        # Create topper
        topper = Topper(
            'topper_q2_signal',
            '/app/temp/reducer_q2',
            '/app/output/q2_top3.csv',
            'rabbitmq',
            3,
            'Q2',
            'joiner_signal'
        )
        
        # Start processing
        topper.start()
        
        # Input: /app/temp/reducer_q2/req-123/202401.csv
        #   item_id,quantity,subtotal
        #   100,1500,45000.00
        #   101,1200,38000.00
        #   ...
        
        # Output: /app/temp/topper_q2/req-123/q2_top.csv
        #   month_year,quantity_or_subtotal,item_id,quantity,subtotal
        #   202401,quantity,100,1500,45000.00
        #   202401,quantity,101,1200,38000.00
        #   202401,quantity,98,1100,35000.00
        #   202401,subtotal,100,1500,45000.00
        #   202401,subtotal,102,900,42000.00
        #   202401,subtotal,101,1200,38000.00
        ```
    """

    def __init__(self, queue_in, input_dir, output_file, rabbitmq_host, top_n, topper_mode, completion_queue):
        """
        Initialize Topper with query configuration.
        
        Args:
            queue_in (str): Input notification queue name.
            input_dir (str): Base input directory for CSV files.
            output_file (str): Base output file path.
            rabbitmq_host (str): RabbitMQ hostname.
            top_n (int): Number of top entries to select.
            topper_mode (str): Query mode ('Q2', 'Q3', 'Q4').
            completion_queue (str): Output notification queue.
        
        Example:
            ```python
            topper = Topper(
                'topper_q2_signal',
                '/app/temp/reducer_q2',
                '/app/output/q2_top3.csv',
                'rabbitmq',
                3,
                'Q2',
                'joiner_signal'
            )
            ```
        """
        service_name = f"topper_{topper_mode.lower()}"
        super().__init__(queue_in, completion_queue, rabbitmq_host, service_name=service_name)
        self.input_dir = input_dir
        self.top_n = top_n
        self.completion_queue_name = completion_queue
        self.query_id = queue_in
        self.base_output_dir = os.path.join(BASE_TEMP_DIR, self.query_id)
        self.base_output_file = output_file
        self.output_filename = os.path.basename(output_file)
        self.output_dir = None
        self.output_file = None
        self.topper_mode = topper_mode
        self.current_request_id = 0
        self.request_id_initialized = False

        self.replica_id = socket.gethostname()
        wsm_host = os.environ.get("WSM_HOST", "wsm")
        wsm_port = int(os.environ.get("WSM_PORT", "9000"))
        
        # Initialize WSMClient to start sending heartbeats automatically
        worker_type_key = f"topper_{topper_mode.lower()}"
        wsm_nodes = WSM_NODES.get(worker_type_key)
        
        self.wsm_client = WSMClient(
            worker_type=worker_type_key,
            replica_id=self.replica_id,
            host=wsm_host,
            port=wsm_port,
            nodes=wsm_nodes
        )
        logging.info(f"[Topper:{self.topper_mode}] Initialized WSMClient for heartbeats with replica_id={self.replica_id}, nodes={wsm_nodes}")

    def _initialize_request_paths(self, request_id):
        """
        Initialize per-request directory structure.
        
        Creates request-specific output directory for result isolation.
        Ensures each request has independent output paths.
        
        Args:
            request_id (int): Request identifier.
        
        Example:
            ```python
            topper._initialize_request_paths(123)
            # Creates: /app/temp/topper_q2/123/
            # Sets: topper.output_dir = '/app/temp/topper_q2/123'
            ```
        """
        if not hasattr(self, "_initialized_requests"):
            self._initialized_requests = set()

        if request_id in self._initialized_requests:
            logging.info(f"[Topper] Request {request_id} already initialized, skipping reinit.")
            return

        self._initialized_requests.add(request_id)

        request_input_dir = os.path.join(self.input_dir, str(request_id))
        if not os.path.exists(request_input_dir):
            logging.warning(f"[Topper] Input directory for request_id={request_id} not found: {request_input_dir}")
        else:
            logging.info(f"[Topper] Using input directory for request_id={request_id}: {request_input_dir}")

        self.output_dir = os.path.join(self.base_output_dir, str(request_id))
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.request_id_initialized = True
        print(f"[Topper] Initialized paths for request_id {request_id}")
        print(f"[Topper]   Output directory: {self.output_dir}")
        print(f"[Topper]   Output file: {self.output_file}")

    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
        """
        Process completion notifications to start top-N selection.
        
        Routes to query-specific processing based on topper_mode.
        Initializes request paths and dispatches to Q2/Q3/Q4 handlers.
        
        Args:
            message (bytes): Raw message.
            msg_type (str): Message type (expects MSG_TYPE_NOTI).
            data_type (str): Data type (expects DATA_END).
            request_id (int): Request identifier.
            position (int): Message position.
            payload (bytes): Message payload.
            queue_name (str, optional): Source queue.
        
        Example:
            ```python
            # Reducer sends: NOTI|DATA_END|req_id=123|pos=0|payload=b''
            topper._process_message(
                message=b'...',
                msg_type='NOTI',
                data_type='DATA_END',
                request_id=123,
                position=0,
                payload=b''
            )
            # Triggers: process_csv_files_Q2('/app/temp/reducer_q2/123')
            ```
        """
        logging.info("HOLA ESTOY EN PROCESS MESSAGE Y VOY A LLAMAR A SIMULAR CRASH")
        self.simulate_crash(queue_name, request_id)

        self.current_request_id = request_id

        self._initialize_request_paths(request_id)

        request_input_dir = os.path.join(self.input_dir, str(request_id))

        if msg_type == protocol.MSG_TYPE_NOTI:
            logging.info(f"[Topper] Received completion signal for request_id={request_id}, starting CSV processing...")
            if self.topper_mode == 'Q2':
                self.process_csv_files_Q2(request_input_dir)
            elif self.topper_mode == 'Q3':
                self.process_csv_files_Q3(request_input_dir)
            elif self.topper_mode == 'Q4':
                self.process_csv_files_Q4(request_input_dir)
        else:
            logging.warning(f"[Topper] Received unknown message type: {msg_type}")


    def _get_csv_files(self, directory):
        """
        Get sorted list of CSV files from directory.
        
        Lists all .csv files and sorts them numerically by filename stem.
        Handles missing directories and invalid filenames gracefully.
        
        Args:
            directory (str): Directory path to scan.
        
        Returns:
            list[str]: Sorted list of CSV filenames.
        
        Example:
            ```python
            # Directory: /app/temp/reducer_q2/123/
            #   202401.csv
            #   202402.csv
            #   202501.csv
            
            csv_files = topper._get_csv_files('/app/temp/reducer_q2/123')
            # Returns: ['202401.csv', '202402.csv', '202501.csv']
            ```
        """
        if not os.path.exists(directory):
            print(f"[Topper] Input directory does not exist: {directory}")
            return []

        try:
            all_files = os.listdir(directory)
            csv_files = [f for f in all_files if f.lower().endswith('.csv')]
        except OSError as e:
            print(f"[Topper] Error reading directory {directory}: {e}")
            return []

        if not csv_files:
            print(f"[Topper] No CSV files found in {directory}")
            return []

        def numeric_sort_key(filename):
            try:
                return int(os.path.splitext(filename)[0])
            except ValueError:
                return float('inf'), filename

        csv_files.sort(key=numeric_sort_key)
        print(f"[Topper] Found {len(csv_files)} CSV files to process")
        return csv_files

    def _process_file(self, directory, csv_filename, columns):
        """
        Extract top N entries per column using min-heaps.
        
        Processes CSV file to find top N entries by value for specified columns.
        Uses min-heap for efficient top-N tracking (O(N log k) space).
        
        Args:
            directory (str): Directory containing CSV file.
            csv_filename (str): CSV filename.
            columns (list[tuple]): List of (column_index, label) tuples.
        
        Returns:
            list[list]: Rows with [filename_stem, label, ...original_row].
        
        Example:
            ```python
            # Input: 202401.csv
            #   item_id,quantity,subtotal
            #   100,1500,45000.00
            #   101,1200,38000.00
            #   102,900,42000.00
            
            results = topper._process_file(
                '/app/temp/reducer_q2/123',
                '202401.csv',
                [(1, 'quantity'), (2, 'subtotal')]
            )
            
            # Returns (top_n=3):
            # [
            #   ['202401', 'quantity', '100', '1500', '45000.00'],
            #   ['202401', 'quantity', '101', '1200', '38000.00'],
            #   ['202401', 'quantity', '102', '900', '42000.00'],
            #   ['202401', 'subtotal', '100', '1500', '45000.00'],
            #   ['202401', 'subtotal', '102', '900', '42000.00'],
            #   ['202401', 'subtotal', '101', '1200', '38000.00']
            # ]
            ```
        """
        filename_without_ext = os.path.splitext(csv_filename)[0]
        csv_file_path = os.path.join(directory, csv_filename)
        results = []

        try:
            with open(csv_file_path, 'r', newline='', encoding='utf-8') as f:
                csv_reader = csv.reader(f)
                heaps = {col: [] for col, _ in columns}

                for row in csv_reader:
                    for col, _ in columns:
                        if len(row) <= col:
                            continue
                        try:
                            val = float(row[col])
                            heap = heaps[col]
                            if len(heap) < self.top_n:
                                heapq.heappush(heap, (val, row))
                            else:
                                if val > heap[0][0]:
                                    heapq.heapreplace(heap, (val, row))
                        except ValueError:
                            continue

                # convertir heaps en listas de top rows
                for col, tag in columns:
                    top_rows = [item[1] for item in sorted(heaps[col], reverse=True)]
                    for row in top_rows:
                        new_row = [filename_without_ext, tag] + row
                        results.append(new_row)

        except Exception as e:
            print(f"[Topper] Error processing file {csv_filename}: {e}")

        return results

    def _write_output(self, all_rows, header=None, output_path=None):
        """
        Write top-N results atomically and send completion notification.
        
        Uses atomic_write for crash-consistent output. Creates output directory if needed.
        Sends completion notification to next stage after successful write.
        
        Args:
            all_rows (list[list]): Rows to write.
            header (list[str], optional): CSV header row.
            output_path (str, optional): Override output path.
        
        Example:
            ```python
            rows = [
                ['202401', 'quantity', '100', '1500', '45000.00'],
                ['202401', 'quantity', '101', '1200', '38000.00'],
                ['202401', 'quantity', '102', '900', '42000.00']
            ]
            
            topper._write_output(
                rows,
                ['month_year', 'quantity_or_subtotal', 'item_id', 'quantity', 'subtotal'],
                '/app/temp/topper_q2/123/q2_top.csv'
            )
            
            # Writes atomically to temp file, then renames
            # Sends completion notification to next stage
            ```
        """
        if not all_rows:
            print("[Topper] No data to process")
            return

        output_file = output_path or self.output_file
        output_dir = os.path.dirname(output_file)
        os.makedirs(output_dir, exist_ok=True)

        def write_func(temp_path):
            with open(temp_path, 'w', newline='', encoding='utf-8') as f:
                csv_writer = csv.writer(f)
                if header:
                    csv_writer.writerow(header)
                csv_writer.writerows(all_rows)
        
        Worker.atomic_write(output_file, write_func)

        print(f"[Topper] Successfully created output file: {output_file}")
        print(f"[Topper] Total rows in output: {len(all_rows)}")

        self._send_completion_signal()



    def process_csv_files_Q2(self, request_input_dir):
        """
        Process Q2: Top N items by quantity and subtotal per month.
        
        Extracts top N entries from each month's aggregated data.
        Processes columns 1 (quantity) and 2 (subtotal) separately.
        
        Args:
            request_input_dir (str): Request-specific input directory.
        
        Example:
            ```python
            # Input: /app/temp/reducer_q2/123/202401.csv
            #   item_id,quantity,subtotal
            #   100,1500,45000.00
            #   101,1200,38000.00
            #   102,900,42000.00
            
            topper.process_csv_files_Q2('/app/temp/reducer_q2/123')
            
            # Output: /app/temp/topper_q2/123/q2_top.csv
            #   month_year,quantity_or_subtotal,item_id,quantity,subtotal
            #   202401,quantity,100,1500,45000.00
            #   202401,quantity,101,1200,38000.00
            #   202401,quantity,102,900,42000.00
            #   202401,subtotal,100,1500,45000.00
            #   202401,subtotal,102,900,42000.00
            #   202401,subtotal,101,1200,38000.00
            ```
        """
        csv_files = self._get_csv_files(request_input_dir)
        if not csv_files:
            logging.info(f"[Topper-Q2] No CSV files found for request {self.current_request_id} (empty stream). Sending completion signal.")
            self._send_completion_signal()
            return

        all_rows = []
        for csv_filename in csv_files:
            print(f"[Topper-Q2] Processing file: {csv_filename}")
            results = self._process_file(request_input_dir,csv_filename, [(1, "quantity"), (2, "subtotal")])
            all_rows.extend(results)

        output_path = os.path.join(self.output_dir, f"q2_top.csv")
        self._write_output(all_rows, ['month_year','quantity_or_subtotal','item_id','quantity','subtotal'], output_path)

    def process_csv_files_Q3(self, request_input_dir):
        """
        Process Q3: Collect store TPV per half-year.
        
        Reads single-value TPV files with naming format: year_half_store_id.csv.
        Parses filename to extract year_half and store_id, reads TPV value.
        
        Args:
            request_input_dir (str): Request-specific input directory.
        
        Example:
            ```python
            # Input files:
            #   /app/temp/reducer_q3/123/2024-H1_6.csv → "125000.50"
            #   /app/temp/reducer_q3/123/2024-H1_8.csv → "98000.75"
            #   /app/temp/reducer_q3/123/2024-H2_6.csv → "142000.25"
            
            topper.process_csv_files_Q3('/app/temp/reducer_q3/123')
            
            # Output: /app/temp/topper_q3/123/q3_top.csv
            #   year_half_created_at,store_id,tpv
            #   2024-H1,6,125000.50
            #   2024-H1,8,98000.75
            #   2024-H2,6,142000.25
            ```
        """
        csv_files = self._get_csv_files(request_input_dir)
        if not csv_files:
            logging.info(f"[Topper-Q3] No CSV files found for request {self.current_request_id} (empty stream). Sending completion signal.")
            self._send_completion_signal()
            return

        all_rows = []
        for csv_filename in csv_files:
            print(f"[Topper-Q3] Processing file: {csv_filename}")
            filename_without_ext = os.path.splitext(csv_filename)[0]
            
            parts = filename_without_ext.split('_')
            if len(parts) < 2:
                print(f"[Topper-Q3] Skipping file with invalid name format: {csv_filename}")
                continue
                
            year_half = parts[0]
            store_id = parts[1]
            
            csv_file_path = os.path.join(request_input_dir, csv_filename)
            try:
                with open(csv_file_path, 'r', newline='', encoding='utf-8') as f:
                    tpv_str = f.read().strip()
                    if tpv_str:
                        try:
                            tpv_float = float(tpv_str)
                            tpv = f"{tpv_float:.2f}"
                        except ValueError:
                            print(f"[Topper-Q3] Warning: Invalid TPV value '{tpv_str}' in file {csv_filename}, using as-is")
                            tpv = tpv_str
                        all_rows.append([year_half, store_id, tpv])
                        print(f"[Topper-Q3] Added row: {year_half}, {store_id}, {tpv}")
            except Exception as e:
                print(f"[Topper-Q3] Error reading file {csv_filename}: {e}")

        output_path = os.path.join(self.output_dir, f"q3_top.csv")
        self._write_output(all_rows, ['year_half_created_at', 'store_id', 'tpv'], output_path)

    def process_csv_files_Q4(self, request_input_dir):
        """
        Process Q4: Top N users by purchase quantity per store.
        
        Extracts top N users by purchases_qty from each store's aggregated data.
        Removes the 'quantity' label column from output.
        
        Args:
            request_input_dir (str): Request-specific input directory.
        
        Example:
            ```python
            # Input: /app/temp/reducer_q4/123/store_6.csv
            #   purchases_qty,user_id
            #   45,user_100
            #   38,user_101
            #   32,user_102
            
            topper.process_csv_files_Q4('/app/temp/reducer_q4/123')
            
            # Output: /app/temp/topper_q4/123/q4_top.csv
            #   store_id,purchases_qty,user_id
            #   store_6,45,user_100
            #   store_6,38,user_101
            #   store_6,32,user_102
            ```
        """
        csv_files = self._get_csv_files(request_input_dir)
        if not csv_files:
            logging.info(f"[Topper-Q4] No CSV files found for request {self.current_request_id} (empty stream). Sending completion signal.")
            self._send_completion_signal()
            return

        all_rows = []
        for csv_filename in csv_files:
            print(f"[Topper-Q4] Processing file: {csv_filename}")
            results = self._process_file(request_input_dir,csv_filename, [(1, "quantity")])
            all_rows.extend([[row[0]] + row[2:] for row in results])  

        output_path = os.path.join(self.output_dir, f"q4_top.csv")
        self._write_output(all_rows, ['store_id', 'purchases_qty', 'user_id'], output_path)

    def _send_completion_signal(self):
        """
        Send completion notification to next stage.
        
        Sends DATA_END notification with current request_id to completion queue.
        Signals downstream workers that top-N results are ready.
        
        Example:
            ```python
            topper._send_completion_signal()
            # Sends to joiner_signal: NOTI|DATA_END|req_id=123|pos=0|payload=b''
            ```
        """
        if self.out_queues:
            try:
                logging.info(f"[Topper:{self.query_id}] Sending completion signal to {self.completion_queue_name} with request_id={self.current_request_id}")
                completion_message = protocol.create_notification_message(protocol.DATA_END, b"", self.current_request_id)
                self.out_queues[0].send(completion_message)
                logging.info(f"[Topper:{self.query_id}] Completion signal sent successfully to {self.completion_queue_name} with request_id={self.current_request_id}")
            except Exception as e:
                logging.error(f"[Topper:{self.query_id}] Error sending completion signal: {e}")

if __name__ == '__main__':
    def create_topper():
        """
        Factory function for creating a Topper instance.
        
        Loads configuration from environment variables.
        Creates topper worker for top-N selection.
        
        Environment Variables:
            QUEUE_IN: Input notification queue (default: 'topper_q4_signal')
            INPUT_DIR: Input directory for CSV files (default: '/app/temp/transactions_filtered_Q4')
            OUTPUT_FILE: Base output file path (default: '/app/output/q4_top3.csv')
            TOP_N: Number of top entries (default: 3)
            TOPPER_MODE: Query mode 'Q2'/'Q3'/'Q4' (default: 'Q2')
            RABBITMQ_HOST: RabbitMQ hostname (default: 'rabbitmq')
            COMPLETION_QUEUE: Output notification queue
        
        Returns:
            Topper: Configured topper instance.
        
        Example:
            ```python
            # Environment setup
            os.environ['QUEUE_IN'] = 'topper_q2_signal'
            os.environ['INPUT_DIR'] = '/app/temp/reducer_q2'
            os.environ['OUTPUT_FILE'] = '/app/output/q2_top3.csv'
            os.environ['TOP_N'] = '3'
            os.environ['TOPPER_MODE'] = 'Q2'
            os.environ['COMPLETION_QUEUE'] = 'joiner_signal'
            
            # Create topper
            topper = create_topper()
            
            # Start processing (called by run_worker_main)
            topper.start()
            ```
        """
        queue_in = os.environ.get('QUEUE_IN', 'topper_q4_signal')
        input_dir = os.environ.get('INPUT_DIR', '/app/temp/transactions_filtered_Q4')
        output_file = os.environ.get('OUTPUT_FILE', '/app/output/q4_top3.csv')
        top_n = int(os.environ.get('TOP_N', '3'))
        topper_mode = os.environ.get('TOPPER_MODE', 'Q2')
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        completion_queue = os.environ.get('COMPLETION_QUEUE')

        return Topper(queue_in, input_dir, output_file, rabbitmq_host, top_n, topper_mode, completion_queue)
    
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Topper.run_worker_main(create_topper, config_path)
