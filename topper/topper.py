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

    def __init__(self, queue_in, input_dir, output_file, rabbitmq_host, top_n, topper_mode, completion_queue):
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
        self.topper_mode = topper_mode

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
                    if(self.topper_mode == 'Q2'):
                        self.process_csv_files_Q2()
                    if(self.topper_mode == 'Q3'):
                        self.process_csv_files_Q3()
                    if(self.topper_mode == 'Q4'):
                        self.process_csv_files_Q4()
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

    def _get_csv_files(self):
        """Devuelve lista ordenada de archivos CSV en input_dir"""
        if not os.path.exists(self.input_dir):
            print(f"[Topper] Input directory does not exist: {self.input_dir}")
            return []

        try:
            all_files = os.listdir(self.input_dir)
            csv_files = [f for f in all_files if f.lower().endswith('.csv')]
        except OSError as e:
            print(f"[Topper] Error reading directory {self.input_dir}: {e}")
            return []

        if not csv_files:
            print(f"[Topper] No CSV files found in {self.input_dir}")
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

    def _process_file(self, csv_filename, columns):
        """
        Procesa un CSV y devuelve las filas top N para cada columna en `columns`.
        columns: lista de (index_col, etiqueta) → ej: [(1, "TOP_BY_COL1"), (2, "TOP_BY_COL2")]
        """
        filename_without_ext = os.path.splitext(csv_filename)[0]
        csv_file_path = os.path.join(self.input_dir, csv_filename)
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

    def _write_output(self, all_rows, header=None):
        """Escribe las filas al archivo de salida y manda signal si corresponde"""
        if not all_rows:
            print("[Topper] No data to process")
            return

        output_dir = os.path.dirname(self.output_file)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
            csv_writer = csv.writer(f)
            if header:
                csv_writer.writerow(header)
            csv_writer.writerows(all_rows)

        print(f"[Topper] Successfully created output file: {self.output_file}")
        print(f"[Topper] Total rows in output: {len(all_rows)}")

        # Si hay cola de completion configurada → manda signal
        self._send_completion_signal()


    def process_csv_files_Q2(self):
        print(f"[Topper-Q2] Processing CSV files from directory: {self.input_dir}")
        csv_files = self._get_csv_files()
        if not csv_files:
            return

        all_rows = []
        for csv_filename in csv_files:
            print(f"[Topper-Q2] Processing file: {csv_filename}")
            results = self._process_file(csv_filename, [(1, "quantity"), (2, "subtotal")])
            all_rows.extend(results)

        self._write_output(all_rows, ['month_year','quantity_or_subtotal','item_id','quantity','subtotal'])

    def process_csv_files_Q3(self):
        print(f"[Topper-Q3] Processing CSV files from directory: {self.input_dir}")
        csv_files = self._get_csv_files()
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
            csv_file_path = os.path.join(self.input_dir, csv_filename)
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

        self._write_output(all_rows, ['year_half_created_at', 'store_id', 'tpv'])

    def process_csv_files_Q4(self):
        print(f"[Topper-Q4] Processing CSV files from directory: {self.input_dir}")
        csv_files = self._get_csv_files()
        if not csv_files:
            return

        all_rows = []
        for csv_filename in csv_files:
            print(f"[Topper-Q4] Processing file: {csv_filename}")
            results = self._process_file(csv_filename, [(1, "quantity")])
            all_rows.extend([[row[0]] + row[2:] for row in results])  

        self._write_output(all_rows, ['store_id', 'purchases_qty', 'user_id'])

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
    topper_mode = os.environ.get('TOPPER_MODE', 'Q2')

    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    completion_queue = os.environ.get('COMPLETION_QUEUE')


    topper = Topper(queue_in, input_dir, output_file, rabbitmq_host, top_n, topper_mode, completion_queue)
    
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
