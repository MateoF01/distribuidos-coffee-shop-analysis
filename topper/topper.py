import os
import sys
import signal
import threading
import configparser
import csv
import heapq
import logging
import time
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue
from shared import protocol
from shared.logging_config import initialize_log
from shared.worker import Worker

# Load configuration
config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
config = configparser.ConfigParser()
config.read(config_path)

# Allow environment variable override for BASE_TEMP_DIR
BASE_TEMP_DIR = os.environ.get('BASE_TEMP_DIR', config['topper']['base_temp_dir'])
os.makedirs(BASE_TEMP_DIR, exist_ok=True)

class Topper(Worker):

    def __init__(self, queue_in, input_dir, output_file, rabbitmq_host, top_n, topper_mode, completion_queue):
        super().__init__(queue_in, completion_queue, rabbitmq_host)
        self.input_dir = input_dir
        self.top_n = top_n
        self.completion_queue_name = completion_queue
        self.query_id = queue_in
        # Store base paths for request_id-based subdirectory creation
        self.base_output_dir = os.path.join(BASE_TEMP_DIR, self.query_id)
        self.base_output_file = output_file
        self.output_filename = os.path.basename(output_file)
        # These will be set when request_id is received
        self.output_dir = None
        self.output_file = None
        self.topper_mode = topper_mode
        self.current_request_id = 0
        self.request_id_initialized = False

    def _initialize_request_paths(self, request_id):
        """Initialize output paths with request_id subdirectory"""

        if not hasattr(self, "_initialized_requests"):
            self._initialized_requests = set()

        if request_id in self._initialized_requests:
            logging.info(f"[Topper] Request {request_id} already initialized, skipping reinit.")
            return

        self._initialized_requests.add(request_id)

        
        # Crear input específico del request
        request_input_dir = os.path.join(self.input_dir, str(request_id))
        if not os.path.exists(request_input_dir):
            logging.warning(f"[Topper] Input directory for request_id={request_id} not found: {request_input_dir}")
        else:
            logging.info(f"[Topper] Using input directory for request_id={request_id}: {request_input_dir}")


        # Create request_id-based paths
        self.output_dir = os.path.join(self.base_output_dir, str(request_id))
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.request_id_initialized = True
        print(f"[Topper] Initialized paths for request_id {request_id}")
        print(f"[Topper]   Output directory: {self.output_dir}")
        print(f"[Topper]   Output file: {self.output_file}")

    def _process_message(self, message, msg_type, data_type, request_id, timestamp, payload, queue_name=None):
        """Process completion signals to start CSV processing"""
        self.current_request_id = request_id

        # Initialize request-specific paths
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
        """Devuelve lista ordenada de archivos CSV en input_dir"""
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

        # Orden numérica de archivos
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
        Procesa un CSV y devuelve las filas top N para cada columna en `columns`.
        columns: lista de (index_col, etiqueta) → ej: [(1, "TOP_BY_COL1"), (2, "TOP_BY_COL2")]
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
        """Escribe las filas al archivo de salida y manda signal si corresponde"""
        if not all_rows:
            print("[Topper] No data to process")
            return

        output_file = output_path or self.output_file
        output_dir = os.path.dirname(output_file)
        os.makedirs(output_dir, exist_ok=True)

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            csv_writer = csv.writer(f)
            if header:
                csv_writer.writerow(header)
            csv_writer.writerows(all_rows)

        print(f"[Topper] Successfully created output file: {output_file}")
        print(f"[Topper] Total rows in output: {len(all_rows)}")

        self._send_completion_signal()



    def process_csv_files_Q2(self, request_input_dir):
        csv_files = self._get_csv_files(request_input_dir)
        if not csv_files:
            return

        all_rows = []
        for csv_filename in csv_files:
            print(f"[Topper-Q2] Processing file: {csv_filename}")
            results = self._process_file(request_input_dir,csv_filename, [(1, "quantity"), (2, "subtotal")])
            all_rows.extend(results)

        output_path = os.path.join(self.output_dir, f"q2_top.csv")
        self._write_output(all_rows, ['month_year','quantity_or_subtotal','item_id','quantity','subtotal'], output_path)

    def process_csv_files_Q3(self, request_input_dir):
        csv_files = self._get_csv_files(request_input_dir)
        if not csv_files:
            return

        all_rows = []
        for csv_filename in csv_files:
            print(f"[Topper-Q3] Processing file: {csv_filename}")
            # Parse filename: format is like "2024-H1_6.csv"
            filename_without_ext = os.path.splitext(csv_filename)[0]  # "2024-H1_6"
            
            # Split by underscore to get year_half and store_id
            parts = filename_without_ext.split('_')
            if len(parts) < 2:
                print(f"[Topper-Q3] Skipping file with invalid name format: {csv_filename}")
                continue
                
            year_half = parts[0]  # "2024-H1"
            store_id = parts[1]   # "6"
            
            # Read the TPV value from the file
            csv_file_path = os.path.join(request_input_dir, csv_filename)
            try:
                with open(csv_file_path, 'r', newline='', encoding='utf-8') as f:
                    tpv_str = f.read().strip()
                    if tpv_str:
                        # Parse TPV as float and format to 2 decimal places
                        try:
                            tpv_float = float(tpv_str)
                            tpv = f"{tpv_float:.2f}"
                        except ValueError:
                            print(f"[Topper-Q3] Warning: Invalid TPV value '{tpv_str}' in file {csv_filename}, using as-is")
                            tpv = tpv_str
                        # Create row: [year_half_created_at, store_id, tpv]
                        all_rows.append([year_half, store_id, tpv])
                        print(f"[Topper-Q3] Added row: {year_half}, {store_id}, {tpv}")
            except Exception as e:
                print(f"[Topper-Q3] Error reading file {csv_filename}: {e}")

        output_path = os.path.join(self.output_dir, f"q3_top.csv")
        self._write_output(all_rows, ['year_half_created_at', 'store_id', 'tpv'], output_path)

    def process_csv_files_Q4(self, request_input_dir):
        csv_files = self._get_csv_files(request_input_dir)
        if not csv_files:
            return

        all_rows = []
        for csv_filename in csv_files:
            print(f"[Topper-Q4] Processing file: {csv_filename}")
            results = self._process_file(request_input_dir,csv_filename, [(1, "quantity")])
            all_rows.extend([[row[0]] + row[2:] for row in results])  

        output_path = os.path.join(self.output_dir, f"q4_top.csv")
        self._write_output(all_rows, ['store_id', 'purchases_qty', 'user_id'], output_path)

    def _send_completion_signal(self):
        """Send completion signal to the next stage if completion queue is configured"""
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
        # Configure via environment variables
        queue_in = os.environ.get('QUEUE_IN', 'topper_q4_signal')
        input_dir = os.environ.get('INPUT_DIR', '/app/temp/transactions_filtered_Q4')
        output_file = os.environ.get('OUTPUT_FILE', '/app/output/q4_top3.csv')
        top_n = int(os.environ.get('TOP_N', '3'))
        topper_mode = os.environ.get('TOPPER_MODE', 'Q2')
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        completion_queue = os.environ.get('COMPLETION_QUEUE')

        return Topper(queue_in, input_dir, output_file, rabbitmq_host, top_n, topper_mode, completion_queue)
    
    # Use the Worker base class main entry point
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Topper.run_worker_main(create_topper, config_path)
