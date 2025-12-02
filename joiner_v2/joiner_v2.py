import os
import signal
import socket
import time
import configparser
import csv
import logging
from shared.worker import Worker
from shared import protocol
from WSM.wsm_client import WSMClient
from pathlib import Path
from wsm_config import WSM_NODES


class Joiner_v2(Worker):
    """
    Distributed join worker that combines multiple input streams into enriched outputs.
    
    Joiner_v2 receives data from multiple input queues (grouper results, cleaned reference
    data), performs in-memory joins, and produces enriched CSV outputs. Uses WSM for
    coordination across replicas, duplicate detection, and synchronized END signal handling.
    
    Architecture position:
    GrouperV2/Cleaner (multiple replicas) -> Joiner_v2 -> Sorter -> Topper
    
    Key features:
    - Multi-queue input support (stores, users, menu items, aggregation results)
    - Per-request isolated temporary storage for join operations
    - WSM-coordinated duplicate detection via position tracking
    - Atomic CSV writes with crash recovery
    - Query-specific join strategies (Q2, Q3, Q4)
    - 1-to-1 mapping between output queues and output files
    
    Attributes:
        query_type (str): Join strategy ('q2', 'q3', or 'q4').
        columns_want (list): Expected output column names.
        backoff_start (float): Initial WSM polling delay.
        backoff_max (float): Maximum WSM polling delay.
        replica_id (str): Unique identifier (hostname).
        dict_wsm_clients (dict): WSM clients per input queue.
        base_output_files (list): Base output file paths.
        _outputs_by_request (dict): Per-request (queue, file) pairs.
        _temp_dir_by_request (dict): Per-request temp directories.
        strategies (dict): Query-type to join method mapping.
        DELIMITERS (dict): Queue-specific CSV delimiters.
    
    Example:
        Query 4 join (stores + users + visit frequency):
        >>> joiner = Joiner_v2(
        ...     queue_out='sorter_input_q4',
        ...     output_file='output/q4/results.csv',
        ...     columns_want=['store_name', 'birthdate'],
        ...     rabbitmq_host='rabbitmq',
        ...     query_type='q4',
        ...     wsm_host='wsm',
        ...     wsm_port=9000,
        ...     multiple_queues=['stores_cleaned_q4', 'users_cleaned', 'resultados_groupby_q4']
        ... )
        >>> joiner.run()
    """
    
    def __init__(self, queue_out, output_file, columns_want, rabbitmq_host, query_type, wsm_host, wsm_port, multiple_queues=None, backoff_start=0.1, backoff_max=3.0, wsm_nodes = None):
        """
        Initialize Joiner_v2 with multiple input queues and WSM coordination.
        
        Args:
            queue_out (str): Output queue name(s), comma-separated.
            output_file (str): Output file path(s), comma-separated, 1-to-1 with queue_out.
            columns_want (list): Expected column names for output CSV.
            rabbitmq_host (str): RabbitMQ server hostname.
            query_type (str): Join strategy ('q2', 'q3', or 'q4').
            wsm_host (str): Worker State Manager hostname.
            wsm_port (int): Worker State Manager port.
            multiple_queues (list, optional): Input queue names.
            backoff_start (float, optional): Initial WSM polling delay. Defaults to 0.1.
            backoff_max (float, optional): Maximum WSM polling delay. Defaults to 3.0.
            wsm_nodes (list, optional): WSM node addresses.
        
        Raises:
            ValueError: If output queues don't match output files count.
        
        Example:
            >>> joiner = Joiner_v2(
            ...     queue_out='sorter_q2a,sorter_q2b',
            ...     output_file='output/q2/q2a.csv,output/q2/q2b.csv',
            ...     columns_want=['year_month', 'item_name', 'value'],
            ...     rabbitmq_host='rabbitmq',
            ...     query_type='q2',
            ...     wsm_host='wsm',
            ...     wsm_port=9000,
            ...     multiple_queues=['menu_items_cleaned', 'resultados_groupby_q2']
            ... )
        """
        service_name = f"joiner_v2_{query_type}"
        super().__init__(None, queue_out, rabbitmq_host, multiple_input_queues=multiple_queues, service_name=service_name)
        
        self.query_type = query_type
        self.columns_want = columns_want
        self.backoff_start = backoff_start
        self.backoff_max = backoff_max

        self.replica_id = socket.gethostname()

        self.dict_wsm_clients = {}
        for q in self.multiple_input_queues:
            self.dict_wsm_clients[q] = WSMClient(
                worker_type=q,
                replica_id=self.replica_id,
                host=wsm_host,
                port=wsm_port,
                nodes=wsm_nodes
            )

        if isinstance(output_file, str):
            output_file = [f.strip() for f in output_file.split(',')]
        output_files = output_file or []

        if len(self.out_queues) != len(output_files):
            raise ValueError(
                f"QUEUE_OUT ({len(self.out_queues)}) y OUTPUT_FILE ({len(output_files)}) deben tener la MISMA cantidad para mapeo 1–a–1."
            )

        self.base_output_files = output_files
        self._outputs_by_request = {}
        self._temp_dir_by_request = {}

        self.strategies = {
            "q4": self._process_q4,
            "q2": self._process_q2,
            "q3": self._process_q3
        }

        self.DELIMITERS = {
            "resultados_groupby_q4": ",",
            "resultados_groupby_q2": ",",
            "resultados_groupby_q3": ",",
        }

        self.end_received_by_request = {}
        self._processed_requests = set()
        self._rows_received_per_request = {}
        self._csv_initialized_per_request = {}
        self._rows_written_per_request = {}



    def _initialize_request_paths(self, request_id):
        """
        Initialize per-request output and temp directories with isolated paths.
        
        Creates request-specific directory structure for output files and temporary
        join data. Only initializes once per request_id.
        
        Args:
            request_id (str): Request identifier.
        
        Example:
            >>> joiner._initialize_request_paths('req-123')
            # Creates:
            # output/q4/req-123/results.csv
            # output/q4/req-123/temp/stores_cleaned_q4/
            # output/q4/req-123/temp/users_cleaned/
            # output/q4/req-123/temp/resultados_groupby_q4/
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

            # input queues in temp
            for in_q in self.multiple_input_queues:
                in_temp_dir = os.path.join(temp_dir, in_q)
                os.makedirs(in_temp_dir, exist_ok=True)

            logging.info(f"[Joiner] Initialized request_id={request_id} → outputs={ [f for _, f in self._outputs_by_request[request_id]] } temp_dir={temp_dir}")


    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
        """
        Process incoming data messages with WSM-based duplicate detection.
        
        Saves incoming rows to per-queue temporary CSV files for later join processing.
        Uses WSM to prevent duplicate processing across replica failures.
        
        Args:
            message (bytes): Raw message bytes.
            msg_type (int): Protocol message type.
            data_type (int): Protocol data type.
            request_id (str): Request identifier.
            position (int): Message position for ordering.
            payload (bytes): Message payload containing CSV rows.
            queue_name (str, optional): Source queue name.
        
        Example:
            >>> joiner._process_message(
            ...     message=b'...',
            ...     msg_type=protocol.MSG_DATA,
            ...     data_type=protocol.DATA_STORE,
            ...     request_id='req-123',
            ...     position=10,
            ...     payload=b'STORE001,Coffee Shop A\\nSTORE002,Coffee Shop B',
            ...     queue_name='stores_cleaned_q4'
            ... )
            # Saves to temp/stores_cleaned_q4/replica-1.csv
        """

        self.simulate_crash(queue_name, request_id)

        self._initialize_request_paths(request_id)

        outputs = self._outputs_by_request[request_id]
        temp_dir = self._temp_dir_by_request[request_id]

        wsm_client = self.dict_wsm_clients[queue_name]

        if wsm_client.is_position_processed(request_id, position):
           logging.info(f"🔁 Mensaje duplicado detectado ({request_id}:{position}), descartando...")
           return

        wsm_client.update_state("PROCESSING", request_id, position)

        file_path = os.path.join(temp_dir, f"{queue_name}/" ,f"{self.replica_id}.csv")

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
            with self._lock:
                if request_id not in self._rows_received_per_request:
                    self._rows_received_per_request[request_id] = 0
                self._rows_received_per_request[request_id] += len(processed_rows)

            self._save_to_temp_file(queue_name, processed_rows, file_path, position)

            wsm_client.update_state("WAITING", request_id, position)
    
    def _handle_end_signal(self, message, msg_type, data_type, request_id, position, queue_name=None):
        """
        Handle END signals with WSM-coordinated synchronization and join processing.
        
        Coordinates END signals from multiple input queues using WSM. When the last queue
        sends END, triggers join processing. Supports crash recovery.
        
        Args:
            message (bytes): Raw END message.
            msg_type (int): Protocol message type.
            data_type (int): Protocol data type.
            request_id (str): Request identifier.
            position (int): Message position.
            queue_name (str, optional): Source queue name.
        
        Raises:
            TimeoutError: If WSM permission not granted within backoff_max.
        
        Example:
            >>> # Queue 1 sends END
            >>> joiner._handle_end_signal(..., queue_name='stores_cleaned_q4')
            # Returns without join (other queues not finished)
            
            >>> # Queue 3 sends END (last queue)
            >>> joiner._handle_end_signal(..., queue_name='resultados_groupby_q4')
            # Triggers join processing, writes output, sends sort notification
        """
        if data_type == protocol.DATA_END:
            logging.info(f"[Joiner:{self.query_type}] Handling DATA_END signal from {queue_name} for request_id={request_id}")
            return

        self._initialize_request_paths(request_id)
        outputs = self._outputs_by_request[request_id]
        temp_dir = self._temp_dir_by_request[request_id]

        wsm_client = self.dict_wsm_clients[queue_name]

        if wsm_client.is_position_processed(request_id, position):
           logging.info(f"🔁 Mensaje END duplicado detectado ({request_id}:{position}), descartando...")
           return

        all_outputs_exist = all(os.path.exists(file_path) for _, file_path in outputs)
        if all_outputs_exist:
            logging.info(f"[Joiner:{self.query_type}] ✅ Output files already exist for request_id={request_id}, skipping join processing")
            self._send_sort_request(request_id, outputs)
            wsm_client.update_state("WAITING", request_id, position)
            logging.info(f"[Joiner:{self.query_type}] Sent sort request and marked as WAITING for request_id={request_id}")
            return

        backoff = self.backoff_start
        total_wait = 0.0
        while not wsm_client.can_send_end(request_id, position):
            if total_wait >= self.backoff_max:
                error_msg = f"[Joiner:{self.query_type}] Timeout after {total_wait:.2f}s waiting for permission to send END from {queue_name} (request_id={request_id})"
                logging.error(error_msg)
                raise TimeoutError(error_msg)
            logging.info(f"[Joiner:{self.query_type}] Esperando permiso para verificacion de enviar END de {request_id} desde {queue_name} (backoff={backoff:.3f}s, total={total_wait:.2f}s)...")
            time.sleep(backoff)
            total_wait += backoff
            backoff = min(backoff * 2, self.backoff_max - total_wait)

        enviar_last_end = wsm_client.can_send_last_end(request_id)
        if not enviar_last_end:
            logging.info(f"[Joiner:{self.query_type}] ✅ Permiso otorgado para enviar END de {request_id} desde {queue_name}")
        else:
            logging.info(f"[Joiner:{self.query_type}] ✅ Permiso otorgado para enviar ÚLTIMO END de {request_id} desde {queue_name}")

            self._process_joined_data(request_id, temp_dir, outputs)

            with self._lock:
                self._processed_requests.add(request_id)
                if request_id in self._rows_received_per_request:
                    del self._rows_received_per_request[request_id]
            
        wsm_client.update_state("WAITING", request_id, position)


    def _write_rows_to_csv_idx(self, out_idx: int, rows_data, request_id, outputs, custom_headers=None):
        """
        Write rows to specific output file with atomic writes and crash recovery.
        
        Args:
            out_idx (int): Index into outputs list.
            rows_data (list): List of row lists to write.
            request_id (str): Request identifier.
            outputs (list): List of (queue, file_path) tuples.
            custom_headers (list, optional): Custom header row.
        
        Example:
            >>> joiner._write_rows_to_csv_idx(0, rows_data, 'req-123', outputs)
        """
        file_path = outputs[out_idx][1]

        try:
            file_exists = os.path.exists(file_path)
            
            # Skip if file already exists (join was already completed)
            # This handles recovery after crash during END signal processing
            if file_exists:
                with self._lock:
                    if not self._csv_initialized_per_request[request_id].get(file_path, False):
                        # First time seeing this file in this run, but it already exists
                        logging.info(f"[Joiner:{self.query_type}] ✓ Output file already exists, skipping write: {file_path}")
                        self._csv_initialized_per_request[request_id][file_path] = True
                        # Don't update row count as we didn't write anything
                return
            
            # File doesn't exist, proceed with write
            needs_header = False
            with self._lock:
                if not self._csv_initialized_per_request[request_id].get(file_path, False):
                    needs_header = True
                    self._csv_initialized_per_request[request_id][file_path] = True
            
            # Define write function for atomic operation (file is new)
            def write_func(path):
                with open(path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    
                    # Write header for new file
                    if needs_header:
                        headers = custom_headers if custom_headers else self.columns_want
                        writer.writerow(headers)
                    
                    # Write rows
                    writer.writerows(rows_data)
            
            # Perform atomic write
            Worker.atomic_write(file_path, write_func)
            
            with self._lock:
                self._rows_written_per_request[request_id][file_path] += len(rows_data)
                logging.info(f"[Joiner:{self.query_type}] Atomically appended {len(rows_data)} rows to {file_path} (request_id={request_id}). Total: {self._rows_written_per_request[request_id][file_path]}")
        except Exception as e:
            logging.error(f"Error writing rows to CSV file {file_path}: {e}")

    def _write_rows_to_csv_all(self, rows_data, request_id, outputs):
        """
        Write rows to all output files.
        
        Args:
            rows_data (list): List of row lists.
            request_id (str): Request identifier.
            outputs (list): List of (queue, file_path) tuples.
        """
        for i in range(len(outputs)):
            self._write_rows_to_csv_idx(i, rows_data, request_id, outputs)

    def _save_to_temp_file(self, queue_name, rows_data, temp_file, position):
        """
        Save rows to per-queue temporary CSV files with position tracking.
        
        Adds position as first column for deduplication. Uses optimized atomic writes.
        
        Args:
            queue_name (str): Source queue name.
            rows_data (list): List of row lists (without position).
            temp_file (str): Path to temporary CSV file.
            position (int): Position number to prepend.
        
        Example:
            >>> joiner._save_to_temp_file('stores_cleaned_q4', rows, temp_file, 10)
        """
        import shutil
        
        try:
            file_exists = os.path.exists(temp_file)
            
            # Add position as first column to each row
            rows_with_position = [[position] + list(row) for row in rows_data]
            
            # Define write function for atomic operation
            def write_func(path):
                if file_exists:
                    # Optimization: Copy existing file and append, instead of reading all rows
                    # This is much faster for large files (O(1) copy + O(n_new) vs O(n_total))
                    shutil.copy2(temp_file, path)
                    
                    # Append new rows to the copied file
                    with open(path, 'a', newline='', encoding='utf-8') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerows(rows_with_position)
                else:
                    # New file: write header and data
                    with open(path, 'w', newline='', encoding='utf-8') as csvfile:
                        writer = csv.writer(csvfile)
                        
                        # Write header based on queue name (position is always first column)
                        if queue_name == 'stores_cleaned_q4':
                            writer.writerow(['position', 'store_id', 'store_name'])
                        elif queue_name == 'users_cleaned':
                            writer.writerow(['position', 'user_id', 'birthdate'])
                        elif queue_name == 'resultados_groupby_q4':
                            writer.writerow(['position', 'store_id', 'user_id', 'purchase_qty'])
                        elif queue_name == 'menu_items_cleaned':
                            writer.writerow(['position', 'item_id', 'item_name'])
                        elif queue_name == 'resultados_groupby_q2':
                            writer.writerow(['position', 'month_year', 'quantity_or_subtotal', 'item_id', 'quantity', 'subtotal'])
                        elif queue_name == 'stores_cleaned_q3':
                            writer.writerow(['position', 'store_id', 'store_name'])
                        elif queue_name == 'resultados_groupby_q3':
                            writer.writerow(['position', 'year_half_created_at', 'store_id', 'tpv'])
                        
                        # Write new rows (with position already added)
                        writer.writerows(rows_with_position)
            
            # Perform atomic write
            Worker.atomic_write(temp_file, write_func)
            logging.debug(f"Atomically saved {len(rows_data)} rows to temp file: {temp_file}")
        except Exception as e:
            logging.error(f"Error saving to temp file {temp_file}: {e}")

    def _send_sort_request(self, request_id, outputs):
        """
        Send sort notification to downstream workers for each output queue.
        
        Args:
            request_id (str): Request identifier.
            outputs (list): List of (queue, file_path) tuples.
        """
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
        """
        Split CSV row using queue-specific delimiter with auto-detection.
        
        Args:
            queue_name (str): Source queue name.
            row (str): CSV row string.
        
        Returns:
            list: Split row items.
        """
        delim = self.DELIMITERS.get(queue_name)
        if not delim:
            # Autodetección simple: priorizar ',' si aparece, sino '|'
            delim = ',' if (',' in row and '|' not in row) else '|'
        return row.strip().split(delim)

    def _process_joined_data(self, request_id, temp_dir, outputs):
        """
        Dispatch to query-specific join strategy.
        
        Args:
            request_id (str): Request identifier.
            temp_dir (str): Directory with temporary CSV files.
            outputs (list): List of (queue, file_path) tuples.
        
        Raises:
            ValueError: If query_type not supported.
        """
        if self.query_type not in self.strategies:
            raise ValueError(f"Unsupported query_type: {self.query_type}")
        self.strategies[self.query_type](request_id, temp_dir, outputs)

    def _process_q4(self, request_id, temp_dir, outputs):
        """
        Q4 join strategy: stores + users + visit counts.
        
        Performs 3-way join on store_id and user_id. Enriches aggregation with store name
        and user email. Deduplicates by position column.
        
        Args:
            request_id (str): Request identifier.
            temp_dir (str): Directory with temporary CSV files.
            outputs (list): Single (queue, file_path) tuple.
        
        Example:
            Input temp files:
            - stores_cleaned_q4.csv: position,store_id,store_name,store_location
            - users_cleaned_q4.csv: position,user_id,user_email
            - visits_agg_q4.csv: position,store_id,user_id,visits
            
            Output: position,store_id,user_id,visits,store_name,user_email
        """
        logging.info(f"Processing Q4 join... (request_id={request_id})")

        stores_lookup, users_lookup = {}, {}

        # Stores
        stores_path = os.path.join(temp_dir, 'stores_cleaned_q4/')
        for file in (Path(stores_path).rglob("*.csv")):
            if os.path.exists(file):
                with open(file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f); next(reader, None)  # Skip header
                    for row in reader:
                        # Row format: [position, store_id, store_name]
                        if len(row) >= 3:
                            stores_lookup[row[1]] = row[2]
        logging.debug(f"Loaded {len(stores_lookup)} store mappings")

        # Users
        users_path = os.path.join(temp_dir, 'users_cleaned/')
        for file in (Path(users_path).rglob("*.csv")):
            if os.path.exists(file):
                with open(file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f); next(reader, None)  # Skip header
                    for row in reader:
                        # Row format: [position, user_id, birthdate]
                        if len(row) >= 3:
                            users_lookup[row[1]] = row[2]
        logging.debug(f"Loaded {len(users_lookup)} user mappings")

        processed_rows = []
        processed_positions = set()
        main_path = os.path.join(temp_dir, 'resultados_groupby_q4/')
        for file in (Path(main_path).rglob("*.csv")):
            if os.path.exists(file):
                file_position = None
                with open(file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f); next(reader, None)
                    first_row = next(reader, None)
                    if first_row and len(first_row) >= 4:
                        file_position = first_row[0]
                
                if file_position and file_position in processed_positions:
                    continue
                
                if file_position:
                    processed_positions.add(file_position)
                
                with open(file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f); next(reader, None)
                    for row in reader:
                        if len(row) >= 4:
                            store_id, user_id, _purchase_qty = row[1], row[2], row[3]
                            store_name = stores_lookup.get(store_id, store_id)
                            birthdate = users_lookup.get(user_id, user_id)
                            processed_rows.append([store_name, birthdate])

        if processed_rows:
            self._write_rows_to_csv_all(processed_rows, request_id, outputs)

        self._send_sort_request(request_id, outputs)
        logging.info(f"Q4 join complete. {len(processed_rows)} rows written across {len(outputs)} output(s).")

    def _process_q2(self, request_id, temp_dir, outputs):
        """
        Q2 join strategy: menu items + monthly aggregations (dual output).
        
        Joins on item_id. Produces two outputs:
        - outputs[0]: Quantity (quantity_sum) per item per month
        - outputs[1]: Revenue (subtotal_sum) per item per month
        
        Args:
            request_id (str): Request identifier.
            temp_dir (str): Directory with temporary CSV files.
            outputs (list): Two (queue, file_path) tuples.
        
        Example:
            Input temp files:
            - menu_items_cleaned_q2.csv: position,item_id,item_name
            - monthly_quantity_q2.csv: position,item_id,month,quantity_sum
            - monthly_revenue_q2.csv: position,item_id,month,subtotal_sum
            
            Outputs:
            - quantity: position,item_id,month,quantity_sum,item_name
            - revenue: position,item_id,month,subtotal_sum,item_name
        """
        logging.info(f"Processing Q2 join... (request_id={request_id})")

        items_lookup = {}

        # Menu Items
        menu_path = os.path.join(temp_dir, 'menu_items_cleaned/')
        for file in (Path(menu_path).rglob("*.csv")):
            if os.path.exists(file):
                with open(file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f); next(reader, None)  # Skip header
                    for row in reader:
                        # Row format: [position, item_id, item_name]
                        if len(row) >= 3:
                            items_lookup[row[1]] = row[2]
        logging.info(f"Loaded {len(items_lookup)} item mappings")

        rows_quantity = []
        rows_subtotal = []
        rows_all = []
        processed_positions = set()
        main_path = os.path.join(temp_dir, 'resultados_groupby_q2/')
        for file in (Path(main_path).rglob("*.csv")):
            if os.path.exists(file):
                file_position = None
                with open(file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f); next(reader, None)
                    first_row = next(reader, None)
                    if first_row and len(first_row) >= 6:
                        file_position = first_row[0]
                
                if file_position and file_position in processed_positions:
                    continue
                
                if file_position:
                    processed_positions.add(file_position)
                
                with open(file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f); next(reader, None)
                    for row in reader:
                        if len(row) >= 6:
                            month_year, quantity_or_subtotal, item_id, quantity, subtotal = row[1], row[2], row[3], row[4], row[5]
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
            if rows_all:
                self._write_rows_to_csv_idx(0, rows_all, request_id, outputs)
        else:
            if rows_quantity:
                q2a_headers = ['year_month_created_at', 'item_name', 'sellings_qty']
                self._write_rows_to_csv_idx(0, rows_quantity, request_id, outputs, q2a_headers)
            if rows_subtotal and out_count >= 2:
                q2b_headers = ['year_month_created_at', 'item_name', 'profit_sum']
                self._write_rows_to_csv_idx(1, rows_subtotal, request_id, outputs, q2b_headers)
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
        Q3 join strategy: stores + semester revenue aggregations.
        
        Joins on store_id. Enriches semester revenue with store name.
        Deduplicates by position column.
        
        Args:
            request_id (str): Request identifier.
            temp_dir (str): Directory with temporary CSV files.
            outputs (list): Single (queue, file_path) tuple.
        
        Example:
            Input temp files:
            - stores_cleaned_q3.csv: position,store_id,store_name,store_location
            - semester_revenue_q3.csv: position,store_id,semester,revenue
            
            Output: position,store_id,semester,revenue,store_name
        """
        logging.info(f"Processing Q3 join... (request_id={request_id})")

        stores_lookup = {}

        # Stores
        stores_path = os.path.join(temp_dir, 'stores_cleaned_q3/')
        for file in (Path(stores_path).rglob("*.csv")):
            if os.path.exists(file):
                with open(file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f); next(reader, None)  # Skip header
                    for row in reader:
                        # Row format: [position, store_id, store_name]
                        if len(row) >= 3:
                            stores_lookup[row[1]] = row[2]  # store_id -> store_name
        logging.info(f"Loaded {len(stores_lookup)} store mappings")

        processed_rows = []
        processed_positions = set()

        main_path = os.path.join(temp_dir, 'resultados_groupby_q3/')
        for file in (Path(main_path).rglob("*.csv")):
            if os.path.exists(file):
                file_position = None
                with open(file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f); next(reader, None)
                    first_row = next(reader, None)
                    if first_row and len(first_row) >= 4:
                        file_position = first_row[0]
                
                if file_position and file_position in processed_positions:
                    continue
                
                if file_position:
                    processed_positions.add(file_position)
                
                with open(file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f); next(reader, None)
                    for row in reader:
                        if len(row) >= 4:
                            year_half, store_id, tpv = row[1], row[2], row[3]
                            store_name = stores_lookup.get(store_id, store_id)
                            processed_rows.append([year_half, store_name, tpv])

        if processed_rows:
            self._write_rows_to_csv_all(processed_rows, request_id, outputs)

        self._send_sort_request(request_id, outputs)
        logging.info(f"Q3 join complete. {len(processed_rows)} rows written across {len(outputs)} output(s).")

    def _log_startup_info(self):
        """
        Log joiner startup information.
        """
        output_info = f"output files: {', '.join(self.base_output_files)}"
        input_info = f"multiple input queues: {self.multiple_input_queues}"
        logging.info(f"Joiner started - {input_info}, {output_info}")


if __name__ == '__main__':
    def create_joiner():
        """
        Factory function to create Joiner_v2 from environment configuration.
        
        Environment Variables:
            QUEUE_OUT: Comma-separated output queue names.
            OUTPUT_FILE: Comma-separated output file paths.
            QUERY_TYPE: Query type (q2, q3, q4).
            RABBITMQ_HOST: RabbitMQ server host (default: rabbitmq).
            WSM_HOST: WSM server host (default: wsm).
            WSM_PORT: WSM server port (default: 9100).
            MULTIPLE_QUEUES: Comma-separated input queue names.
            BACKOFF_START: Initial backoff seconds (default: 0.1).
            BACKOFF_MAX: Maximum backoff seconds (default: 3.0).
        
        Returns:
            Joiner_v2: Configured joiner instance.
        """
        queue_out_env = os.environ.get('QUEUE_OUT')
        output_file_env = os.environ.get('OUTPUT_FILE')
        query_type = os.environ.get('QUERY_TYPE')
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

        shm_host = os.environ.get("WSM_HOST", "wsm")
        shm_port = int(os.environ.get("WSM_PORT", "9100"))

        multiple_queues_str = os.environ.get('MULTIPLE_QUEUES')
        multiple_queues = [q.strip() for q in multiple_queues_str.split(',')] if multiple_queues_str else []

        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)

        if query_type not in config:
            raise ValueError(f"Unknown query type: {query_type}")

        columns_want = [col.strip() for col in config[query_type]['columns'].split(',')]

        backoff_start = float(config['DEFAULT'].get('BACKOFF_START', 0.1))
        backoff_max = float(config['DEFAULT'].get('BACKOFF_MAX', 3.0))

        wsm_nodes = WSM_NODES[query_type]
        print("WSM NODES: ", wsm_nodes)

        return Joiner_v2(
            queue_out=queue_out_env,
            output_file=output_file_env,
            columns_want=columns_want,
            rabbitmq_host=rabbitmq_host,
            query_type=query_type,
            wsm_host=shm_host,
            wsm_port=shm_port,
            multiple_queues=multiple_queues,
            backoff_start=backoff_start,
            backoff_max=backoff_max,
            wsm_nodes = wsm_nodes
        )
    
    # Use the Worker base class main entry point
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Joiner_v2.run_worker_main(create_joiner, config_path)