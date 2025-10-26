import os
import socket
import time
import configparser
import csv
import logging
from shared.worker import Worker
from shared import protocol
from WSM.wsm_client import WSMClient
from SHM.hashmap_client import SharedHashmapClient


class Joiner_v2(Worker):
    def __init__(self, queue_out, output_file, columns_want, rabbitmq_host, query_type, shm_host, shm_port, multiple_queues=None):
        # Use multiple_input_queues parameter for the Worker superclass

        super().__init__(None, queue_out, rabbitmq_host, multiple_input_queues=multiple_queues)
        
        self.query_type = query_type
        self.columns_want = columns_want

        replica_id = socket.gethostname()
        # 🔗 Conexión con el Worker State Manager
        self.shm_client = SharedHashmapClient(
            replica_id=replica_id,
            host=shm_host,
            port=shm_port
        )

        # --- OUTPUT FILES: múltiples archivos separados por coma ---
        if isinstance(output_file, str):
            output_file = [f.strip() for f in output_file.split(',')]
        output_files = output_file or []

        # Validación 1–a–1
        if len(self.out_queues) != len(output_files):
            raise ValueError(
                f"QUEUE_OUT ({len(self.out_queues)}) y OUTPUT_FILE ({len(output_files)}) deben tener la MISMA cantidad para mapeo 1–a–1."
            )

        # Store base output paths (will be modified with request_id subdirectories)
        self.base_output_files = output_files
        # Pares (queue, file) en el mismo orden - initially set to base paths
        #self.outputs = list(zip(self.out_queues, output_files))
        self._outputs_by_request = {}         # { request_id: [(out_queue, out_file), ...] }
        self._temp_dir_by_request = {}        # { request_id: temp_dir }

        # Estado por archivo (usamos dict por file_path)
        # self._csv_initialized = {f: False for _, f in self.outputs}
        # self._rows_written = {f: 0 for _, f in self.outputs}
        # self.end_received = {queue: False for queue in self.multiple_input_queues}
        # self.current_request_id = 0  # Store current request_id

        # Temp dir will be set up after receiving request_id
        # self.temp_dir = None
        # self.request_id_initialized = False

        # strategies
        self.strategies = {
            "q4": self._process_q4,
            "q2": self._process_q2,
            "q3": self._process_q3
        }

        # Delimitadores por cola (fallback a '|')
        self.DELIMITERS = {
            "resultados_groupby_q4": ",",
            "resultados_groupby_q2": ",",
            "resultados_groupby_q3": ",",
        }

        self.end_received_by_request = {}
        self._processed_requests = set()  # Track completed requests to prevent reprocessing
        self._rows_received_per_request = {}  # Track rows read from queue per request_id
        # Per-request CSV state tracking - FIX for concurrent requests
        self._csv_initialized_per_request = {}  # {request_id: {file_path: bool}}
        self._rows_written_per_request = {}     # {request_id: {file_path: count}}

    def _initialize_request_paths(self, request_id):
        """
        Initialize per-request output and temp directories.
        Each request_id gets its own independent path config,
        and initialization happens only once per request_id.
        """
        with self._lock:
            # Skip if already initialized for this request
            if request_id in self._outputs_by_request:
                return

            # --- Build per-request output paths ---
            updated_output_files = []
            for base_path in self.base_output_files:
                dir_path = os.path.dirname(base_path)
                filename = os.path.basename(base_path)
                # output_dir/<request_id>/<filename>
                new_path = os.path.join(dir_path, str(request_id), filename)
                os.makedirs(os.path.dirname(new_path), exist_ok=True)
                updated_output_files.append(new_path)

            # Store mapping for this request_id (associate the same out_queues objects)
            self._outputs_by_request[request_id] = list(zip(self.out_queues, updated_output_files))

            # Initialize per-request CSV state
            self._csv_initialized_per_request[request_id] = {f: False for _, f in self._outputs_by_request[request_id]}
            self._rows_written_per_request[request_id] = {f: 0 for _, f in self._outputs_by_request[request_id]}

            # temp por request (queda en .../<request_id>/temp)
            base_for_temp = os.path.dirname(updated_output_files[0]) if updated_output_files else os.getcwd()
            temp_dir = os.path.join(base_for_temp, 'temp')
            os.makedirs(temp_dir, exist_ok=True)
            self._temp_dir_by_request[request_id] = temp_dir

            logging.info(f"[Joiner] Initialized request_id={request_id} → outputs={ [f for _, f in self._outputs_by_request[request_id]] } temp_dir={temp_dir}")

    
    def _process_message(self, message, msg_type, data_type, request_id, timestamp, payload, queue_name=None):
        """Process messages from input queues"""        
        # Initialize request-specific paths if needed
        self._initialize_request_paths(request_id)
                
        # Bind request-specific paths
        outputs = self._outputs_by_request[request_id]
        temp_dir = self._temp_dir_by_request[request_id]

        file_path = os.path.join(temp_dir, f"{queue_name}.csv")

        response = self.shm_client.register(file_path, "PROCESSING")
        if response == "ERROR":
            logging.error(f"[Joiner:{self.query_type}] ERROR registrando {file_path} en SHM.")
            return
        elif response == "ALREADY_REGISTERED":
            if self.shm_client.change_state(file_path, "PROCESSING") == "ERROR":
                logging.error(f"[Joiner:{self.query_type}] ERROR cambiando estado de {file_path} en SHM.")
                return

        try:
            payload_str = payload.decode('utf-8')
        except Exception as e:
            logging.error(f"Decode error: {e}")
            if self.shm_client.change_state(file_path, "WAITING") == "ERROR":
                logging.error(f"[Joiner:{self.query_type}] ERROR cambiando estado de {file_path} en SHM.")
            return

        rows = payload_str.split('\n')
        processed_rows = []
        for row in rows:
            if row.strip():
                items = self._split_row(queue_name, row)
                if len(items) >= 2:
                    processed_rows.append(items)

        if processed_rows:
            with self._lock:
                # Track rows received for this request_id
                if request_id not in self._rows_received_per_request:
                    self._rows_received_per_request[request_id] = 0
                self._rows_received_per_request[request_id] += len(processed_rows)

            while (response := self.shm_client.lock(file_path)) != "OK":
                if response == "ERROR":
                    logging.error(f"[Joiner:{self.query_type}] Error locking {file_path}")
                    if self.shm_client.change_state(file_path, "WAITING") == "ERROR":
                        logging.error(f"[Joiner:{self.query_type}] ERROR cambiando estado de {file_path} en SHM.")
                    return
                logging.info(f"[Joiner:{self.query_type}] Waiting for lock on {file_path}...")
                time.sleep(1)

            self._save_to_temp_file(queue_name, processed_rows, file_path)

            if self.shm_client.unlock(file_path) != "OK":
                logging.error(f"[Joiner:{self.query_type}] Error unlocking {file_path}")
            if self.shm_client.change_state(file_path, "WAITING") == "ERROR":
                logging.error(f"[Joiner:{self.query_type}] ERROR cambiando estado de {file_path} en SHM.")
    
    def _handle_end_signal(self, message, msg_type, data_type, request_id, queue_name=None):
        """
        Handle END signals per request_id and per input queue.
        Evita mezclar los END de distintos requests.
        """
        if data_type == protocol.DATA_END:
            logging.info(f"[Joiner:{self.query_type}] Handling DATA_END signal from {queue_name} for request_id={request_id}")
            return

        # 1️⃣ Asegurar paths del request actual
        self._initialize_request_paths(request_id)
        outputs = self._outputs_by_request[request_id]
        temp_dir = self._temp_dir_by_request[request_id]
        file_path = os.path.join(temp_dir, f"{queue_name}.csv")
        files_paths = list(map(lambda q: os.path.join(temp_dir, f"{q}.csv"), self.multiple_input_queues))

        if self.shm_client.register(file_path) == "ERROR":
            logging.error(f"[Joiner:{self.query_type}] ERROR registrando {file_path} en SHM.")
            return

        # 4️⃣ Procesar el END recibido
        all_ready = self.shm_client.last_map_is_not_ready(file_path,files_paths)
        if all_ready == "ALREADY_READY":
            logging.error(f"[Joiner:{self.query_type}] ERROR: Archivo {file_path} ya estaba marcado como listo en SHM.")
        elif all_ready == "MAPS_NOT_READY":
            while (response := self.shm_client.put_ready(file_path)) != "OK":
                if response == "ERROR":
                    logging.error(f"[Joiner:{self.query_type}] ERROR marcando {file_path} como listo en SHM.")
                    break
                logging.info(f"[Joiner:{self.query_type}] Marcando {file_path} como listo en SHM...")
                time.sleep(1)
            logging.info(f"[Joiner:{self.query_type}] Archivo {file_path} marcado como listo en SHM.")
        elif all_ready == "LAST_MAP_NOT_READY":
            while (response := self.shm_client.put_ready(file_path)) != "OK":
                if response == "ERROR":
                    logging.error(f"[Joiner:{self.query_type}] ERROR marcando {file_path} como listo en SHM.")
                    break
                logging.info(f"[Joiner:{self.query_type}] Marcando {file_path} como listo en SHM...")
                time.sleep(1)
            logging.info(f"[Joiner:{self.query_type}] Ultimo archivo {file_path} marcado como listo en SHM.")
            with self._lock:
                rows_received = self._rows_received_per_request.get(request_id, 0)
                logging.info(f"[Joiner:{self.query_type}] END recibido de {queue_name} (request_id={request_id}) - TOTAL ROWS FROM QUEUE: {rows_received}")
            logging.info(f"[Joiner:{self.query_type}] Todas las colas completaron (request_id={request_id}). Procesando join...")

            self._process_joined_data(request_id, temp_dir, outputs)

            # Mark request as fully processed and clean up state
            with self._lock:
                self._processed_requests.add(request_id)
                if request_id in self._rows_received_per_request:
                    del self._rows_received_per_request[request_id]

    # =====================
    # Utils de CSV por salida
    # =====================

    def _initialize_csv_idx(self, out_idx: int, request_id, outputs, custom_headers=None):
        """Inicializa un archivo CSV (por índice de salida) con headers si aún no se hizo."""
        file_path = outputs[out_idx][1]
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                headers = custom_headers if custom_headers else self.columns_want
                writer.writerow(headers)
            self._csv_initialized_per_request[request_id][file_path] = True
            logging.info(f"Initialized CSV file {file_path} (request_id={request_id}) with headers: {headers}")
        except Exception as e:
            logging.error(f"Error initializing CSV file {file_path}: {e}")

    def _write_rows_to_csv_idx(self, out_idx: int, rows_data, request_id, outputs, custom_headers=None):
        """Escribe rows SOLO en el archivo del índice dado (inicializa si hace falta)."""
        file_path = outputs[out_idx][1]

        try:
            with self._lock:
                if not self._csv_initialized_per_request[request_id].get(file_path, False):
                    self._initialize_csv_idx(out_idx, request_id, outputs, custom_headers)
            with open(file_path, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(rows_data)
            with self._lock:
                self._rows_written_per_request[request_id][file_path] += len(rows_data)
                logging.info(f"[Joiner:{self.query_type}] Appended {len(rows_data)} rows to {file_path} (request_id={request_id}). Total: {self._rows_written_per_request[request_id][file_path]}")
        except Exception as e:
            logging.error(f"Error writing rows to CSV file {file_path}: {e}")

    def _write_rows_to_csv_all(self, rows_data, request_id, outputs):
        """Escribe rows en TODOS los archivos de salida."""
        for i in range(len(outputs)):
            self._write_rows_to_csv_idx(i, rows_data, request_id, outputs)

    def _save_to_temp_file(self, queue_name, rows_data, temp_file):
        """Guarda rows crudas por cola en CSV temporales para joins posteriores."""

        try:
            file_exists = os.path.exists(temp_file)
            with open(temp_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
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
                        writer.writerow(['month_year', 'quantity_or_subtotal', 'item_id', 'quantity', 'subtotal'])
                    elif queue_name == 'stores_cleaned_q3':
                        writer.writerow(['store_id', 'store_name'])
                    elif queue_name == 'resultados_groupby_q3':
                        writer.writerow(['year_half_created_at', 'store_id', 'tpv'])
                writer.writerows(rows_data)
            logging.debug(f"Saved {len(rows_data)} rows to temp file: {temp_file}")
        except Exception as e:
            logging.error(f"Error saving to temp file {temp_file}: {e}")

    def _send_sort_request(self, request_id, outputs):
        """Envía señal de sort/notify a CADA cola de salida, indicando su archivo asociado."""
        try:
            sort_message = protocol.create_notification_message(0, b"", request_id)  # MSG_TYPE_NOTI with data_type 0
            logging.info(f"Sending sort request with request_id={request_id}")
            for out_q, out_file in outputs:
                try:
                    out_q.send(sort_message)
                    logging.info(f"Sent sort request for {out_file} to {out_q.queue_name} with request_id={request_id}")
                except Exception as inner:
                    logging.error(f"Error sending sort to {out_q.queue_name} for {out_file}: {inner}")
        except Exception as e:
            logging.error(f"Error sending sort request: {e}")

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

    def _process_joined_data(self, request_id, temp_dir, outputs):
        if self.query_type not in self.strategies:
            raise ValueError(f"Unsupported query_type: {self.query_type}")
        self.strategies[self.query_type](request_id, temp_dir, outputs)  # Ejecuta la estrategia correspondiente

    # =====================
    # Strategies
    # =====================

    def _process_q4(self, request_id, temp_dir, outputs):
        """
        Q4: join entre resultados_groupby_q4, stores_cleaned_q4 y users_cleaned
        Salida esperada: [store_name, birthdate]
        Escribimos el MISMO dataset en todas las salidas (si hay más de una).
        """
        logging.info(f"Processing Q4 join... (request_id={request_id})")

        stores_lookup, users_lookup = {}, {}

        # Stores
        stores_file = os.path.join(temp_dir, 'stores_cleaned_q4.csv')
        if os.path.exists(stores_file):
            with open(stores_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f); next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        stores_lookup[row[0]] = row[1]
        logging.debug(f"Loaded {len(stores_lookup)} store mappings")

        # Users
        users_file = os.path.join(temp_dir, 'users_cleaned.csv')
        if os.path.exists(users_file):
            with open(users_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f); next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        users_lookup[row[0]] = row[1]
        logging.debug(f"Loaded {len(users_lookup)} user mappings")

        # Main
        main_file = os.path.join(temp_dir, 'resultados_groupby_q4.csv')
        if not os.path.exists(main_file):
            logging.error(f"Main file for Q4 not found: {main_file}")
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
            self._write_rows_to_csv_all(processed_rows, request_id, outputs)

        self._send_sort_request(request_id, outputs)
        logging.info(f"Q4 join complete. {len(processed_rows)} rows written across {len(outputs)} output(s).")

    def _process_q2(self, request_id, temp_dir, outputs):
        """
        Q2: join con menú e items agrupados.
        Espera 'menu_items_cleaned.csv' y 'resultados_groupby_q2.csv' en temp/.
        Mapeo 1–a–1:
          - si hay 2 outputs: outputs[0] ← quantity (Q2_a), outputs[1] ← subtotal (Q2_b)
          - si hay 1 output: todo va al mismo
          - si hay >2: quantity→0, subtotal→1 y el resto reciben el dataset completo (fallback)
        """
        logging.info(f"Processing Q2 join... (request_id={request_id})")

        items_lookup = {}

        # Menu Items
        menu_file = os.path.join(temp_dir, 'menu_items_cleaned.csv')
        if os.path.exists(menu_file):
            with open(menu_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f); next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        items_lookup[row[0]] = row[1]
        logging.info(f"Loaded {len(items_lookup)} item mappings")

        # Main
        main_file = os.path.join(temp_dir, 'resultados_groupby_q2.csv')
        if not os.path.exists(main_file):
            logging.error(f"Main file for Q2 not found: {main_file}")
            return

        rows_quantity = []
        rows_subtotal = []
        rows_all = []  # por si hay 1 output o fallback

        with open(main_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f); next(reader, None)
            for row in reader:
                # Esperado: month_year, quantity_or_subtotal('quantity'|'subtotal'), item_id, quantity, subtotal
                if len(row) >= 5:
                    month_year, quantity_or_subtotal, item_id, quantity, subtotal = row[0], row[1], row[2], row[3], row[4]
                    item_name = items_lookup.get(item_id, item_id)
                    if quantity_or_subtotal == 'quantity':
                        rows_quantity.append([month_year, item_name, quantity])
                        rows_all.append([month_year, item_name, quantity])
                    elif quantity_or_subtotal == 'subtotal':
                        rows_subtotal.append([month_year, item_name, subtotal])
                        rows_all.append([month_year, item_name, subtotal])

        out_count = len(outputs)
        if out_count == 0:
            logging.error("No outputs configured; skipping CSV write.")
        elif out_count == 1:
            # todo junto al único output
            if rows_all:
                self._write_rows_to_csv_idx(0, rows_all, request_id, outputs)
        else:
            # out_count >= 2: mapeo explícito
            # Q2_a (quantity) goes to first output with specific headers
            if rows_quantity:
                q2a_headers = ['year_month_created_at', 'item_name', 'sellings_qty']
                self._write_rows_to_csv_idx(0, rows_quantity, request_id, outputs, q2a_headers)
            # Q2_b (subtotal) goes to second output with specific headers
            if rows_subtotal and out_count >= 2:
                q2b_headers = ['year_month_created_at', 'item_name', 'profit_sum']
                self._write_rows_to_csv_idx(1, rows_subtotal, request_id, outputs, q2b_headers)
            # si hay más de 2 salidas, opcionalmente replicamos todo en las restantes
            if out_count > 2 and rows_all:
                for i in range(2, out_count):
                    self._write_rows_to_csv_idx(i, rows_all, request_id, outputs)

        self._send_sort_request(request_id, outputs)
        logging.info(f"Q2 join complete. "
                     f"Q2_a (quantity) rows: {len(rows_quantity)} written to {outputs[0][1] if out_count > 0 else 'N/A'}"
                     f"{f', Q2_b (subtotal) rows: {len(rows_subtotal)} written to {outputs[1][1]}' if out_count >= 2 else ''} "
                     f"across {len(outputs)} output(s).")

    def _process_q3(self, request_id, temp_dir, outputs):
        """
        Q3: join con stores para obtener store names de los resultados agrupados.
        Espera 'stores_cleaned_q3.csv' y 'resultados_groupby_q3.csv' en temp/.
        Salida esperada: [year_half_created_at, store_name, tpv]
        """
        logging.info(f"Processing Q3 join... (request_id={request_id})")

        stores_lookup = {}

        # Stores
        stores_file = os.path.join(temp_dir, 'stores_cleaned_q3.csv')
        if os.path.exists(stores_file):
            with open(stores_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f); next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        stores_lookup[row[0]] = row[1]  # store_id -> store_name
        logging.info(f"Loaded {len(stores_lookup)} store mappings")

        # Main
        main_file = os.path.join(temp_dir, 'resultados_groupby_q3.csv')
        if not os.path.exists(main_file):
            logging.error(f"Main file for Q3 not found: {main_file}")
            return

        processed_rows = []
        with open(main_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f); next(reader, None)
            for row in reader:
                # Esperado: year_half_created_at, store_id, tpv
                if len(row) >= 3:
                    year_half, store_id, tpv = row[0], row[1], row[2]
                    store_name = stores_lookup.get(store_id, store_id)
                    processed_rows.append([year_half, store_name, tpv])

        if processed_rows:
            # escribir a todas las salidas (normalmente solo una para Q3)
            self._write_rows_to_csv_all(processed_rows, request_id, outputs)

        self._send_sort_request(request_id, outputs)
        logging.info(f"Q3 join complete. {len(processed_rows)} rows written across {len(outputs)} output(s).")

    # =====================
    # Principal Loop 
    # =====================

    def _log_startup_info(self):
        """Override to provide joiner-specific startup information"""
        output_info = f"output files: {', '.join(self.base_output_files)}"
        input_info = f"multiple input queues: {self.multiple_input_queues}"
        logging.info(f"Joiner started - {input_info}, {output_info}")


if __name__ == '__main__':
    def create_joiner():
        queue_out_env = os.environ.get('QUEUE_OUT')           # puede tener múltiples, coma
        output_file_env = os.environ.get('OUTPUT_FILE')       # puede tener múltiples, coma
        query_type = os.environ.get('QUERY_TYPE')
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

        shm_host = os.environ.get("SHM_HOST", "shm")
        shm_port = int(os.environ.get("SHM_PORT", "9100"))

        # múltiples input queues (para join)
        multiple_queues_str = os.environ.get('MULTIPLE_QUEUES')
        multiple_queues = [q.strip() for q in multiple_queues_str.split(',')] if multiple_queues_str else []

        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)

        if query_type not in config:
            raise ValueError(f"Unknown query type: {query_type}")

        columns_want = [col.strip() for col in config[query_type]['columns'].split(',')]

        return Joiner_v2(
            queue_out=queue_out_env,
            output_file=output_file_env,
            columns_want=columns_want,
            rabbitmq_host=rabbitmq_host,
            query_type=query_type,
            shm_host=shm_host,
            shm_port=shm_port,
            multiple_queues=multiple_queues
        )
    
    # Use the Worker base class main entry point
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Joiner_v2.run_worker_main(create_joiner, config_path)