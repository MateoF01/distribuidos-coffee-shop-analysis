import os
import signal
import sys
import threading
import struct
import configparser
import csv
import logging
import time
from shared.logging_config import initialize_log
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue
from shared.worker import Worker
from shared import protocol


class Joiner(Worker):
    def __init__(self, queue_in, queue_out, output_file, columns_want, rabbitmq_host, query_type, multiple_queues=None):
        # Use multiple_input_queues parameter for the Worker superclass
        input_queues = multiple_queues or [queue_in] if queue_in else multiple_queues
        super().__init__(queue_in, queue_out, rabbitmq_host, multiple_input_queues=input_queues)
        
        self.query_type = query_type
        self.columns_want = columns_want

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
        self.outputs = list(zip(self.out_queues, output_files))

        # Estado por archivo (usamos dict por file_path)
        self._csv_initialized = {f: False for _, f in self.outputs}
        self._rows_written = {f: 0 for _, f in self.outputs}
        self.end_received = {queue: False for queue in self.multiple_input_queues}
        self.current_request_id = 0  # Store current request_id

        # Temp dir will be set up after receiving request_id
        self.temp_dir = None
        self.request_id_initialized = False

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
        Inicializa rutas (outputs y temp) por request.
        Si cambia el request_id, reconfigura paths aunque ya se hayan inicializado antes.
        """
        # Si ya estamos en este mismo request, no hagas nada
        if self.request_id_initialized and self.current_request_id == request_id:
            return

        # --- construir paths con subcarpeta del request ---
        updated_output_files = []
        for base_path in self.base_output_files:
            dir_path = os.path.dirname(base_path)
            filename = os.path.basename(base_path)
            # output_dir/<request_id>/<filename>
            new_path = os.path.join(dir_path, str(request_id), filename)
            updated_output_files.append(new_path)
            os.makedirs(os.path.dirname(new_path), exist_ok=True)

        self.outputs = list(zip(self.out_queues, updated_output_files))

        # Initialize per-request CSV state if not already done
        if request_id not in self._csv_initialized_per_request:
            self._csv_initialized_per_request[request_id] = {f: False for _, f in self.outputs}
            self._rows_written_per_request[request_id] = {f: 0 for _, f in self.outputs}

        # temp por request (queda en .../<request_id>/temp)
        base_for_temp = os.path.dirname(updated_output_files[0]) if updated_output_files else os.getcwd()
        self.temp_dir = os.path.join(base_for_temp, 'temp')
        os.makedirs(self.temp_dir, exist_ok=True)

        # AHORA sí, actualizá el request activo
        self.current_request_id = request_id
        self.request_id_initialized = True

        logging.info(f"[Joiner] request_id={request_id} → outputs={ [f for _, f in self.outputs] } temp_dir={self.temp_dir}")

    
    def _process_message(self, message, msg_type, data_type, request_id, timestamp, payload, queue_name=None):
        """Process messages from input queues"""        
        # Initialize request-specific paths if needed
        self._initialize_request_paths(request_id)
        
        # Initialize CSV files on first message for single queue mode
        if len(self.multiple_input_queues) == 1:
            for i in range(len(self.outputs)):
                file_path = self.outputs[i][1]
                if not self._csv_initialized_per_request[request_id].get(file_path, False):
                    self._initialize_csv_idx(i, request_id)
        
        try:
            payload_str = payload.decode('utf-8')
        except Exception as e:
            logging.error(f"Decode error: {e}")
            return

        rows = payload_str.split('\n')
        processed_rows = []
        for row in rows:
            if row.strip():
                items = self._split_row(queue_name, row)
                if len(items) >= 2:
                    processed_rows.append(items)

        if processed_rows:
            # Track rows received for this request_id
            if request_id not in self._rows_received_per_request:
                self._rows_received_per_request[request_id] = 0
            self._rows_received_per_request[request_id] += len(processed_rows)
            
            with self._lock:
                if len(self.multiple_input_queues) > 1:
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
                            self._write_rows_to_csv_idx(i, final_rows, request_id)
    
    def _handle_end_signal(self, message, msg_type, data_type, request_id, queue_name=None):
        """
        Handle END signals per request_id and per input queue.
        Evita mezclar los END de distintos requests.
        """
        with self._lock:
            # 1️⃣ Asegurar paths del request actual
            self._initialize_request_paths(request_id)

            # 2️⃣ Check if this request has already been fully processed
            if request_id in self._processed_requests:
                logging.info(f"[Joiner:{self.query_type}] END ignorado de {queue_name} - request_id={request_id} ya fue procesado completamente")
                return

            # 3️⃣ Inicializar estado para este request si no existe
            if request_id not in self.end_received_by_request:
                self.end_received_by_request[request_id] = {q: False for q in self.multiple_input_queues}

            end_state = self.end_received_by_request[request_id]

            # 4️⃣ Procesar el END recibido
            if not end_state.get(queue_name, False):
                end_state[queue_name] = True
                rows_received = self._rows_received_per_request.get(request_id, 0)
                logging.info(f"[Joiner:{self.query_type}] END recibido de {queue_name} (request_id={request_id}) - TOTAL ROWS FROM QUEUE: {rows_received}")

                # 5️⃣ Si todas las colas de este request completaron, procesar join
                if all(end_state.values()):
                    logging.info(f"[Joiner:{self.query_type}] Todas las colas completaron (request_id={request_id}). Procesando join...")

                    self.current_request_id = request_id
                    if len(self.multiple_input_queues) > 1:
                        self._process_joined_data(request_id)
                    else:
                        self._send_sort_request()
                        total_rows_written = sum(self._rows_written_per_request[request_id].values())
                        rows_received = self._rows_received_per_request.get(request_id, 0)
                        logging.info(f"[Joiner:{self.query_type}] FINAL REQUEST_ID={request_id}: Received {rows_received} rows from queue → Wrote {total_rows_written} rows to CSV. Sort request sent.")

                    # Mark request as fully processed and clean up state
                    self._processed_requests.add(request_id)
                    del self.end_received_by_request[request_id]
                    if request_id in self._rows_received_per_request:
                        del self._rows_received_per_request[request_id]

            else:
                logging.warning(f"[Joiner:{self.query_type}] END duplicado ignorado de {queue_name} (request_id={request_id})")

    # =====================
    # Utils de CSV por salida
    # =====================

    def _initialize_csv_idx(self, out_idx: int, request_id, custom_headers=None):
        """Inicializa un archivo CSV (por índice de salida) con headers si aún no se hizo."""
        file_path = self.outputs[out_idx][1]
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                headers = custom_headers if custom_headers else self.columns_want
                writer.writerow(headers)
            self._csv_initialized_per_request[request_id][file_path] = True
            logging.info(f"[Joiner:{self.query_type}] Initialized CSV file {file_path} (request_id={request_id}) with headers: {headers}")
        except Exception as e:
            logging.error(f"Error initializing CSV file {file_path}: {e}")

    def _write_rows_to_csv_idx(self, out_idx: int, rows_data, request_id, custom_headers=None):
        """Escribe rows SOLO en el archivo del índice dado (inicializa si hace falta)."""
        file_path = self.outputs[out_idx][1]

        try:
            if not self._csv_initialized_per_request[request_id].get(file_path, False):
                self._initialize_csv_idx(out_idx, request_id, custom_headers)
            with open(file_path, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(rows_data)
            self._rows_written_per_request[request_id][file_path] += len(rows_data)
            logging.info(f"[Joiner:{self.query_type}] Appended {len(rows_data)} rows to {file_path} (request_id={request_id}). Total: {self._rows_written_per_request[request_id][file_path]}")
        except Exception as e:
            logging.error(f"Error writing rows to CSV file {file_path}: {e}")

    def _write_rows_to_csv_all(self, rows_data, request_id):
        """Escribe rows en TODOS los archivos de salida."""
        for i in range(len(self.outputs)):
            self._write_rows_to_csv_idx(i, rows_data, request_id)

    def _save_to_temp_file(self, queue_name, rows_data):
        """Guarda rows crudas por cola en CSV temporales para joins posteriores."""

        temp_file = os.path.join(self.temp_dir, f"{queue_name}.csv")
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

    def _send_sort_request(self):
        """Envía señal de sort/notify a CADA cola de salida, indicando su archivo asociado."""
        try:
            sort_message = protocol.create_notification_message(0, b"", self.current_request_id)  # MSG_TYPE_NOTI with data_type 0
            logging.info(f"Sending sort request with request_id={self.current_request_id}")
            for out_q, out_file in self.outputs:
                try:
                    out_q.send(sort_message)
                    logging.info(f"Sent sort request for {out_file} to {out_q.queue_name} with request_id={self.current_request_id}")
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

    def _process_joined_data(self, request_id):
        if self.query_type not in self.strategies:
            raise ValueError(f"Unsupported query_type: {self.query_type}")
        self.strategies[self.query_type](request_id)  # Ejecuta la estrategia correspondiente

    # =====================
    # Strategies
    # =====================

    def _process_q1(self, request_id):
        """
        Q1: transaction_id y final_amount de una sola cola.
        """
        logging.info(f"Processing Q1 join... (request_id={request_id})")
        self._send_sort_request()

    def _process_q4(self, request_id):
        """
        Q4: join entre resultados_groupby_q4, stores_cleaned_q4 y users_cleaned
        Salida esperada: [store_name, birthdate]
        Escribimos el MISMO dataset en todas las salidas (si hay más de una).
        """
        logging.info(f"Processing Q4 join... (request_id={request_id})")

        stores_lookup, users_lookup = {}, {}

        # Stores
        stores_file = os.path.join(self.temp_dir, 'stores_cleaned_q4.csv')
        if os.path.exists(stores_file):
            with open(stores_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f); next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        stores_lookup[row[0]] = row[1]
        logging.debug(f"Loaded {len(stores_lookup)} store mappings")

        # Users
        users_file = os.path.join(self.temp_dir, 'users_cleaned.csv')
        if os.path.exists(users_file):
            with open(users_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f); next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        users_lookup[row[0]] = row[1]
        logging.debug(f"Loaded {len(users_lookup)} user mappings")

        # Main
        main_file = os.path.join(self.temp_dir, 'resultados_groupby_q4.csv')
        if not os.path.exists(main_file):
            logging.warning("Main file for Q4 not found")
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
            self._write_rows_to_csv_all(processed_rows, request_id)

        self._send_sort_request()
        logging.info(f"Q4 join complete. {len(processed_rows)} rows written across {len(self.outputs)} output(s).")

    def _process_q2(self, request_id):
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

        out_count = len(self.outputs)
        if out_count == 0:
            print("No outputs configured; skipping CSV write.")
        elif out_count == 1:
            # todo junto al único output
            if rows_all:
                self._write_rows_to_csv_idx(0, rows_all, request_id)
        else:
            # out_count >= 2: mapeo explícito
            # Q2_a (quantity) goes to first output with specific headers
            if rows_quantity:
                q2a_headers = ['year_month_created_at', 'item_name', 'sellings_qty']
                self._write_rows_to_csv_idx(0, rows_quantity, request_id, q2a_headers)
            # Q2_b (subtotal) goes to second output with specific headers
            if rows_subtotal and out_count >= 2:
                q2b_headers = ['year_month_created_at', 'item_name', 'profit_sum']
                self._write_rows_to_csv_idx(1, rows_subtotal, request_id, q2b_headers)
            # si hay más de 2 salidas, opcionalmente replicamos todo en las restantes
            if out_count > 2 and rows_all:
                for i in range(2, out_count):
                    self._write_rows_to_csv_idx(i, rows_all, request_id)

        self._send_sort_request()
        print(f"Q2 join complete. "
              f"Q2_a (quantity) rows: {len(rows_quantity)} written to {self.outputs[0][1] if out_count > 0 else 'N/A'}"
              f"{f', Q2_b (subtotal) rows: {len(rows_subtotal)} written to {self.outputs[1][1]}' if out_count >= 2 else ''} "
              f"across {len(self.outputs)} output(s).")

    def _process_q3(self, request_id):
        """
        Q3: join con stores para obtener store names de los resultados agrupados.
        Espera 'stores_cleaned_q3.csv' y 'resultados_groupby_q3.csv' en temp/.
        Salida esperada: [year_half_created_at, store_name, tpv]
        """
        logging.info(f"Processing Q3 join... (request_id={request_id})")

        stores_lookup = {}

        # Stores
        stores_file = os.path.join(self.temp_dir, 'stores_cleaned_q3.csv')
        if os.path.exists(stores_file):
            with open(stores_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f); next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        stores_lookup[row[0]] = row[1]  # store_id -> store_name
        print(f"Loaded {len(stores_lookup)} store mappings")

        # Main
        main_file = os.path.join(self.temp_dir, 'resultados_groupby_q3.csv')
        if not os.path.exists(main_file):
            print("Main file for Q3 not found")
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
            self._write_rows_to_csv_all(processed_rows, request_id)

        self._send_sort_request()
        print(f"Q3 join complete. {len(processed_rows)} rows written across {len(self.outputs)} output(s).")

    # =====================
    # Principal Loop 
    # =====================

    def _log_startup_info(self):
        """Override to provide joiner-specific startup information"""
        output_files = [f for _, f in self.outputs]
        output_info = f"output files: {', '.join(output_files)}"
        input_info = f"multiple input queues: {self.multiple_input_queues}"
        logging.info(f"Joiner started - {input_info}, {output_info}")


if __name__ == '__main__':
    def create_joiner():
        queue_in = os.environ.get('QUEUE_IN')
        queue_out_env = os.environ.get('QUEUE_OUT')           # puede tener múltiples, coma
        output_file_env = os.environ.get('OUTPUT_FILE')       # puede tener múltiples, coma
        query_type = os.environ.get('QUERY_TYPE')
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

        # múltiples input queues (para join)
        multiple_queues_str = os.environ.get('MULTIPLE_QUEUES')
        multiple_queues = [q.strip() for q in multiple_queues_str.split(',')] if multiple_queues_str else [queue_in]

        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)

        if query_type not in config:
            raise ValueError(f"Unknown query type: {query_type}")

        columns_want = [col.strip() for col in config[query_type]['columns'].split(',')]

        return Joiner(
            queue_in=queue_in,
            queue_out=queue_out_env,
            output_file=output_file_env,
            columns_want=columns_want,
            rabbitmq_host=rabbitmq_host,
            query_type=query_type,
            multiple_queues=multiple_queues
        )
    
    # Use the Worker base class main entry point
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Joiner.run_worker_main(create_joiner, config_path)