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
        self.query_type = query_type
        self.columns_want = columns_want
        self.rabbitmq_host = rabbitmq_host

        # --- OUTPUT QUEUES: múltiples colas separadas por coma ---
        if isinstance(queue_out, str):
            queue_out = [q.strip() for q in queue_out.split(',')]
        out_queues = [CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=q)
                      for q in (queue_out or [])]

        # --- OUTPUT FILES: múltiples archivos separados por coma ---
        if isinstance(output_file, str):
            output_file = [f.strip() for f in output_file.split(',')]
        output_files = output_file or []

        # Validación 1–a–1
        if len(out_queues) != len(output_files):
            raise ValueError(
                f"QUEUE_OUT ({len(out_queues)}) y OUTPUT_FILE ({len(output_files)}) deben tener la MISMA cantidad para mapeo 1–a–1."
            )

        # Pares (queue, file) en el mismo orden
        self.outputs = list(zip(out_queues, output_files))

        # Estado por archivo (usamos dict por file_path)
        self._csv_initialized = {f: False for _, f in self.outputs}
        self._rows_written = {f: 0 for _, f in self.outputs}

        self._running = False
        self._shutdown_event = threading.Event()
        self._lock = threading.Lock()

        self.multiple_queues = multiple_queues or [queue_in]
        self.in_queues = {}
        self.end_received = {queue: False for queue in self.multiple_queues}

        # Temp dir: usar la carpeta del primer output si existe, sino CWD
        base_for_temp = os.path.dirname(self.outputs[0][1]) if self.outputs else os.getcwd()
        self.temp_dir = os.path.join(base_for_temp, 'temp')
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Initialize queues
        for queue_name in self.multiple_queues:
            self.in_queues[queue_name] = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_name)

        # strategies
        self.strategies = {
            "q1": self._process_q1,
            "q4": self._process_q4,
            "q2": self._process_q2,
            "q3": self._process_q3
        }

        # Delimitadores por cola (fallback a '|')
        self.DELIMITERS = {
            "resultados_groupby_q4": ",",
            "resultados_groupby_q2": ",",
        }

    # =====================
    # Utils de CSV por salida
    # =====================

    def _initialize_csv_idx(self, out_idx: int):
        """Inicializa un archivo CSV (por índice de salida) con headers si aún no se hizo."""
        file_path = self.outputs[out_idx][1]
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(self.columns_want)
            self._csv_initialized[file_path] = True
            print(f"Initialized CSV file {file_path} with headers: {self.columns_want}")
        except Exception as e:
            print(f"Error initializing CSV file {file_path}: {e}")

    def _write_rows_to_csv_idx(self, out_idx: int, rows_data):
        """Escribe rows SOLO en el archivo del índice dado (inicializa si hace falta)."""
        file_path = self.outputs[out_idx][1]
        try:
            if not self._csv_initialized[file_path]:
                self._initialize_csv_idx(out_idx)
            with open(file_path, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(rows_data)
            self._rows_written[file_path] += len(rows_data)
            print(f"Wrote {len(rows_data)} rows to {file_path}. Total rows: {self._rows_written[file_path]}")
        except Exception as e:
            print(f"Error writing rows to CSV file {file_path}: {e}")

    def _write_rows_to_csv_all(self, rows_data):
        """Escribe rows en TODOS los archivos de salida."""
        for i in range(len(self.outputs)):
            self._write_rows_to_csv_idx(i, rows_data)

    def _save_to_temp_file(self, queue_name, rows_data):
        """Guarda rows crudas por cola en CSV temporales para joins posteriores."""
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
                    elif queue_name == 'menu_items_cleaned':
                        writer.writerow(['item_id', 'item_name'])
                    elif queue_name == 'resultados_groupby_q2':
                        writer.writerow(['month_year', 'metric', 'item_id', 'quantity', 'subtotal'])
                writer.writerows(rows_data)
            
            print(f"Saved {len(rows_data)} rows to temp file: {temp_file}")
        except Exception as e:
            print(f"Error saving to temp file {temp_file}: {e}")

    def _send_sort_request(self):
        """Envía señal de sort/notify a CADA cola de salida, indicando su archivo asociado."""
        try:
            header = struct.pack('>BBI', 3, 0, 0)
            for out_q, out_file in self.outputs:
                try:
                    out_q.send(header)
                    print(f"Sent sort request for {out_file} to {out_q.queue_name}")
                except Exception as inner:
                    print(f"Error sending sort to {out_q.queue_name} for {out_file}: {inner}")
        except Exception as e:
            print(f"Error sending sort request: {e}")

    def _split_row(self, queue_name, row):
        """Obtiene el delimitador según cola (o autodetecta) y hace split."""
        delim = self.DELIMITERS.get(queue_name)
        if not delim:
            # Autodetección simple: priorizar ',' si aparece, sino '|'
            delim = ',' if (',' in row and '|' not in row) else '|'
        return row.strip().split(delim)

    # =====================
    # Dispatcher
    # =====================

    def _process_joined_data(self):
        if self.query_type not in self.strategies:
            raise ValueError(f"Unsupported query_type: {self.query_type}")
        self.strategies[self.query_type]()  # Ejecuta la estrategia correspondiente

    # =====================
    # Strategies
    # =====================

    def _process_q1(self):
        """
        Q1: transaction_id y final_amount de una sola cola.
        En el flujo streaming ya escribimos; aquí sólo señalizamos (a todas las salidas).
        """
        print("Processing Q1 join...")
        self._send_sort_request()

    def _process_q4(self):
        """
        Q4: join entre resultados_groupby_q4, stores_cleaned_q4 y users_cleaned
        Salida esperada: [store_name, birthdate]
        Escribimos el MISMO dataset en todas las salidas (si hay más de una).
        """
        print("Processing Q4 join...")

        stores_lookup, users_lookup = {}, {}

        # Stores
        stores_file = os.path.join(self.temp_dir, 'stores_cleaned_q4.csv')
        if os.path.exists(stores_file):
            with open(stores_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f); next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        stores_lookup[row[0]] = row[1]
        print(f"Loaded {len(stores_lookup)} store mappings")

        # Users
        users_file = os.path.join(self.temp_dir, 'users_cleaned.csv')
        if os.path.exists(users_file):
            with open(users_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f); next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        users_lookup[row[0]] = row[1]
        print(f"Loaded {len(users_lookup)} user mappings")

        # Main
        main_file = os.path.join(self.temp_dir, 'resultados_groupby_q4.csv')
        if not os.path.exists(main_file):
            print("Main file for Q4 not found")
            return

        processed_rows = []
        with open(main_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f); next(reader, None)
            for row in reader:
                if len(row) >= 3:
                    store_id, user_id, _purchase_qty = row[0], row[1], row[2]
                    store_name = stores_lookup.get(store_id, store_id)
                    birthdate = users_lookup.get(user_id, user_id)
                    processed_rows.append([store_name, birthdate])

        if processed_rows:
            # mismo dataset a todas las salidas
            self._write_rows_to_csv_all(processed_rows)

        self._send_sort_request()
        print(f"Q4 join complete. {len(processed_rows)} rows written across {len(self.outputs)} output(s).")

    def _process_q2(self):
        """
        Q2: join con menú e items agrupados.
        Espera 'menu_items_cleaned.csv' y 'resultados_groupby_q2.csv' en temp/.
        Mapeo 1–a–1:
          - si hay 2 outputs: outputs[0] ← quantity, outputs[1] ← subtotal
          - si hay 1 output: todo va al mismo
          - si hay >2: quantity→0, subtotal→1 y el resto reciben el dataset completo (fallback)
        """
        print("Processing Q2 join...")

        items_lookup = {}

        # Menu Items
        menu_file = os.path.join(self.temp_dir, 'menu_items_cleaned.csv')
        if os.path.exists(menu_file):
            with open(menu_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f); next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        items_lookup[row[0]] = row[1]
        print(f"Loaded {len(items_lookup)} item mappings")

        # Main
        main_file = os.path.join(self.temp_dir, 'resultados_groupby_q2.csv')
        if not os.path.exists(main_file):
            print("Main file for Q2 not found")
            return

        rows_quantity = []
        rows_subtotal = []
        rows_all = []  # por si hay 1 output o fallback

        with open(main_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f); next(reader, None)
            for row in reader:
                # Esperado: month_year, metric('quantity'|'subtotal'), item_id, quantity, subtotal
                if len(row) >= 5:
                    month_year, metric, item_id, quantity, subtotal = row[0], row[1], row[2], row[3], row[4]
                    item_name = items_lookup.get(item_id, item_id)
                    if metric == 'quantity':
                        rows_quantity.append([month_year, item_name, quantity])
                        rows_all.append([month_year, item_name, quantity])
                    elif metric == 'subtotal':
                        rows_subtotal.append([month_year, item_name, subtotal])
                        rows_all.append([month_year, item_name, subtotal])

        out_count = len(self.outputs)
        if out_count == 0:
            print("No outputs configured; skipping CSV write.")
        elif out_count == 1:
            # todo junto al único output
            if rows_all:
                self._write_rows_to_csv_idx(0, rows_all)
        else:
            # out_count >= 2: mapeo explícito
            if rows_quantity:
                self._write_rows_to_csv_idx(0, rows_quantity)
            if rows_subtotal and out_count >= 2:
                self._write_rows_to_csv_idx(1, rows_subtotal)
            # si hay más de 2 salidas, opcionalmente replicamos todo en las restantes
            if out_count > 2 and rows_all:
                for i in range(2, out_count):
                    self._write_rows_to_csv_idx(i, rows_all)

        self._send_sort_request()
        print(f"Q2 join complete. "
              f"quantity rows: {len(rows_quantity)}, subtotal rows: {len(rows_subtotal)} "
              f"across {len(self.outputs)} output(s).")

    def _process_q3(self):
        """
        Q3: placeholder para lógica futura
        """
        print("Processing Q3 join... (TODO)")
        self._send_sort_request()

    # =====================
    # Principal Loop 
    # =====================

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
                            items = self._split_row(queue_name, row)
                            if len(items) >= 2:
                                processed_rows.append(items)

                    if processed_rows:
                        with self._lock:
                            if len(self.multiple_queues) > 1:
                                # modo "join later": guardamos crudo por cola
                                self._save_to_temp_file(queue_name, processed_rows)
                            else:
                                # modo "single queue": escribimos directamente
                                # por diseño, Q1 escribiría transaction_id, final_amount
                                final_rows = [[r[0], r[1]] for r in processed_rows if len(r) >= 2]
                                if final_rows:
                                    # como no sabemos a cuál salida corresponde,
                                    # en single-queue replicamos el dataset a TODAS
                                    for i in range(len(self.outputs)):
                                        self._write_rows_to_csv_idx(i, final_rows)
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
        for q in self.in_queues.values():
            try: q.close()
            except: pass
        for out_q, _ in self.outputs:
            try: out_q.close()
            except: pass

if __name__ == '__main__':
    # Configure via environment variables
    queue_in = os.environ.get('QUEUE_IN')
    queue_out_env = os.environ.get('QUEUE_OUT')           # puede tener múltiples, coma
    output_file_env = os.environ.get('OUTPUT_FILE')       # puede tener múltiples, coma
    query_type = os.environ.get('QUERY_TYPE')
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

    # múltiples input queues (para join)
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

    joiner = Joiner(
        queue_in=queue_in,
        queue_out=queue_out_env,
        output_file=output_file_env,
        columns_want=columns_want,
        rabbitmq_host=rabbitmq_host,
        query_type=query_type,
        multiple_queues=multiple_queues
    )

    def signal_handler(signum, frame):
        print(f"Received signal {signum}, shutting down joiner gracefully...")
        joiner.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        print(f"Starting joiner for {query_type} query...")
        joiner.run()
    except KeyboardInterrupt:
        print("Keyboard interrupt received.")
        joiner.stop()
    finally:
        print('Joiner shutdown complete.')
