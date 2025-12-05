import os
import socket
import time
import logging
import configparser
import gc
from collections import defaultdict
from datetime import datetime
from shared import protocol
from shared.worker import StreamProcessingWorker
from WSM.wsm_client import WSMClient
from wsm_config import WSM_NODES


def get_month_str(dt_str):
    """
    Extract year-month string from datetime string.
    
    Args:
        dt_str (str): Datetime string in format 'YYYY-MM-DD...' or 'YYYY-MM...'.
    
    Returns:
        str: Month string in format 'YYYY-MM'.
    
    Example:
        >>> get_month_str('2024-03-15 10:30:00')
        '2024-03'
        >>> get_month_str('2024-12')
        '2024-12'
    """
    dt = datetime.strptime(dt_str[:7], '%Y-%m')
    return dt.strftime('%Y-%m')

def get_semester_str(dt_str):
    """
    Extract year-semester string from datetime string.
    
    Converts month to semester: H1 (Jan-Jun) or H2 (Jul-Dec).
    
    Args:
        dt_str (str): Datetime string in format 'YYYY-MM-DD...' or 'YYYY-MM...'.
    
    Returns:
        str: Semester string in format 'YYYY-H1' or 'YYYY-H2'.
    
    Example:
        >>> get_semester_str('2024-03-15 10:30:00')
        '2024-H1'
        >>> get_semester_str('2024-09-20')
        '2024-H2'
        >>> get_semester_str('2024-06')
        '2024-H1'
        >>> get_semester_str('2024-07')
        '2024-H2'
    """
    dt = datetime.strptime(dt_str[:7], '%Y-%m')
    year = dt.strftime('%Y')
    month = dt.month
    sem = 'H1' if 1 <= month <= 6 else 'H2'
    return f'{year}-{sem}'


class GrouperV2(StreamProcessingWorker):
    """
    Distributed aggregation worker that groups and accumulates transaction data.
    
    GrouperV2 is a replicated stream worker (multiple instances run in parallel) that
    receives filtered transaction data and performs mode-specific aggregations. It uses
    WSM (Worker State Manager) for coordination, duplicate detection, and synchronized
    END signal handling across replicas. Aggregation results are incrementally persisted
    to disk using atomic writes.
    
    The worker operates in three modes:
    - q2: Group by month and menu item, accumulate quantity and revenue
    - q3: Group by semester and store, accumulate total amount
    - q4: Group by store and user, count unique visit frequency
    
    Architecture position:
    Filter (multiple replicas)/ Cleaner (multiple replicas) -> Coordinator -> GrouperV2 (multiple replicas) -> Reducer (multiple replicas)
    
    Key features:
    - WSM-coordinated duplicate detection via position tracking
    - Atomic file writes for crash consistency
    - Exponential backoff for END signal synchronization
    - Per-request isolated temporary directories
    - Incremental aggregation to handle large datasets
    
    Attributes:
        grouper_mode (str): Aggregation mode ('q2', 'q3', or 'q4').
        replica_id (str): Unique identifier for this worker replica.
        backoff_start (float): Initial backoff delay for WSM polling.
        backoff_max (float): Maximum backoff delay for WSM polling.
        base_temp_root (str): Root directory for temporary aggregation files.
        temp_dir (str): Request-specific temporary directory.
        current_request_id (str): Currently processing request ID.
        request_id_initialized (bool): Whether temp_dir has been initialized.
        wsm_client (WSMClient): Client for Worker State Manager coordination.
    
    Example:
        Pipeline with 3 GrouperV2 replicas for query 2:
        >>> grouper1 = GrouperV2(
        ...     queue_in='filter_output_q2',
        ...     queue_out='coordinator_input_q2',
        ...     rabbitmq_host='rabbitmq',
        ...     grouper_mode='q2',
        ...     replica_id='grouper-q2-1',
        ...     wsm_nodes=['wsm-1:9000', 'wsm-2:9000']
        ... )
        >>> grouper1.run()
        
        Message flow with duplicate handling:
        >>> # Filter-1 sends: (req-123, pos=5, data)
        >>> # Filter-2 fails and retries: (req-123, pos=5, data)
        >>> # GrouperV2 processes first message, WSM marks pos=5 as processed
        >>> # GrouperV2 receives duplicate, WSM detects it, message discarded
        
        END signal synchronization:
        >>> # All 3 GrouperV2 replicas receive END
        >>> # Each notifies WSM and waits for permission
        >>> # WSM ensures all replicas finished processing
        >>> # WSM grants permission to first replica
        >>> # Only one END forwarded downstream to Coordinator
    """

    def __init__(self, queue_in, queue_out, rabbitmq_host, grouper_mode, replica_id, backoff_start=0.1, backoff_max=3.0, wsm_nodes = None):
        """
        Initialize GrouperV2 worker with WSM coordination.
        
        Sets up WSM client for distributed coordination, configures temporary storage
        directories, and initializes aggregation mode.
        
        Args:
            queue_in (str): Input RabbitMQ queue (receives from filter workers).
            queue_out (str|list): Output queue(s) (sends to coordinator).
            rabbitmq_host (str): RabbitMQ server hostname.
            grouper_mode (str): Aggregation mode ('q2', 'q3', or 'q4').
            replica_id (str): Unique identifier for this replica instance.
            backoff_start (float, optional): Initial WSM polling delay in seconds. Defaults to 0.1.
            backoff_max (float, optional): Maximum WSM polling delay in seconds. Defaults to 3.0.
            wsm_nodes (list, optional): List of WSM node addresses for coordination.
        
        Example:
            Mode-specific initialization:
            >>> grouper_q2 = GrouperV2(
            ...     queue_in='filter_output_q2',
            ...     queue_out='coordinator_input_q2',
            ...     rabbitmq_host='rabbitmq',
            ...     grouper_mode='q2',
            ...     replica_id='grouper-q2-1',
            ...     backoff_start=0.1,
            ...     backoff_max=5.0,
            ...     wsm_nodes=['wsm-1:9000', 'wsm-2:9000', 'wsm-3:9000']
            ... )
            
            With environment configuration:
            >>> os.environ['BASE_TEMP_DIR'] = '/tmp/grouper'
            >>> os.environ['WSM_HOST'] = 'wsm-cluster'
            >>> os.environ['WSM_PORT'] = '9500'
            >>> grouper_q4 = GrouperV2(
            ...     'filter_output_q4', 'coordinator_input_q4',
            ...     'rabbitmq', 'q4', 'grouper-q4-2'
            ... )
        """
        service_name = f"grouper_{grouper_mode}_v2"
        super().__init__(queue_in, queue_out, rabbitmq_host, service_name=service_name)
        self.grouper_mode = grouper_mode
        self.replica_id = replica_id
        self.backoff_start = backoff_start
        self.backoff_max = backoff_max
        self.base_temp_root = os.environ.get('BASE_TEMP_DIR', os.path.join(os.path.dirname(__file__), 'temp'))
        self.temp_dir = None
        self.current_request_id = None
        self.request_id_initialized = False

        wsm_host = os.environ.get("WSM_HOST", "wsm")
        wsm_port = int(os.environ.get("WSM_PORT", "9000"))
        self.wsm_client = WSMClient(
            worker_type="grouper",
            replica_id=replica_id,
            host=wsm_host,
            port=wsm_port,
            nodes=wsm_nodes
        )

        logging.info(f"[GrouperV2:{self.grouper_mode}:{replica_id}] Inicializado - input: {queue_in}, output: {queue_out}")

    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
        """
        Process incoming message with WSM-based duplicate detection.
        
        Coordinates with WSM to track processed positions and prevent duplicate processing
        across replica failures and retries. Updates WSM state through processing lifecycle:
        PROCESSING -> delegates to parent -> WAITING.
        
        Args:
            message (bytes): Raw message bytes.
            msg_type (int): Protocol message type.
            data_type (int): Protocol data type.
            request_id (str): Request identifier.
            position (int): Message position for ordering.
            payload (bytes): Message payload data.
            queue_name (str, optional): Source queue name.
        
        Returns:
            None: Silently discards duplicates.
        
        Example:
            Normal processing flow:
            >>> grouper._process_message(
            ...     message=b'...',
            ...     msg_type=protocol.MSG_DATA,
            ...     data_type=protocol.DATA_TRANSACTION_ITEM,
            ...     request_id='req-123',
            ...     position=42,
            ...     payload=b'item1|5|25.50|2024-03-15',
            ...     queue_name='filter_output_q2'
            ... )
            # WSM checks: position 42 not processed
            # WSM updates: state=PROCESSING
            # Calls parent._process_message -> _process_rows
            # Aggregates data to disk
            # WSM updates: state=WAITING
            
            Duplicate detection:
            >>> # First processing
            >>> grouper._process_message(..., position=42, ...)
            # Processed successfully, WSM records position=42
            
            >>> # Retry after failure
            >>> grouper._process_message(..., position=42, ...)
            # WSM detects position 42 already processed
            # Message discarded, no aggregation
        """
        # TEST-CASE: Crashear al Grouper luego de procesar varios mensajes
        self.simulate_crash(queue_name, request_id)

        self._initialize_request_paths(request_id)

        if self.wsm_client.is_position_processed(request_id, position):
            logging.info(f"🔁 Mensaje duplicado detectado ({request_id}:{position}), descartando...")
            return

        self.wsm_client.update_state("PROCESSING", request_id, position)

        super()._process_message(message, msg_type, data_type, request_id, position, payload, queue_name)

        self.wsm_client.update_state("WAITING", request_id, position)

    def _handle_end_signal(self, message, msg_type, data_type, request_id, position, queue_name=None):
        """
        Handle END signal with WSM-coordinated synchronization across replicas.
        
        Ensures only one replica forwards the END signal downstream by coordinating through
        WSM. Uses exponential backoff polling to wait for WSM permission. This prevents
        duplicate END signals when multiple replicas finish processing.
        
        Args:
            message (bytes): Raw END message.
            msg_type (int): Protocol message type.
            data_type (int): Protocol data type (END type).
            request_id (str): Request identifier.
            position (int): Message position.
            queue_name (str, optional): Source queue name.
        
        Raises:
            TimeoutError: If WSM permission not granted within backoff_max seconds.
        
        Example:
            Normal END coordination with 3 replicas:
            >>> # Replica 1 receives END at t=0
            >>> grouper1._handle_end_signal(
            ...     message=b'...',
            ...     msg_type=protocol.MSG_NOTIFICATION,
            ...     data_type=protocol.DATA_TRANSACTION_ITEM_END,
            ...     request_id='req-123',
            ...     position=100
            ... )
            # Updates WSM: state=END
            # Polls WSM: can_send_end? -> False (replicas 2,3 still processing)
            # Waits with backoff: 0.1s, 0.2s, 0.4s...
            # Eventually WSM grants permission
            # Forwards END to coordinator
            # Updates WSM: state=WAITING
            
            >>> # Replica 2 receives END at t=1
            >>> grouper2._handle_end_signal(..., request_id='req-123', position=100)
            # Updates WSM: state=END
            # Polls WSM: can_send_end? -> False (replica 1 already forwarded)
            # WSM never grants permission
            # No END forwarded (prevents duplicate)
            
            DATA_END handling:
            >>> grouper._handle_end_signal(
            ...     message=b'...',
            ...     data_type=protocol.DATA_END,
            ...     request_id='req-123',
            ...     position=50
            ... )
            # Logs receipt and returns immediately
            # No coordination needed for DATA_END
            
            Timeout scenario:
            >>> grouper.backoff_max = 2.0
            >>> grouper._handle_end_signal(..., request_id='req-456', position=200)
            # Polls WSM repeatedly
            # After 2.0s total wait, raises TimeoutError
        """
        if data_type == protocol.DATA_END:
            logging.info(f"[GrouperV2:{self.grouper_mode}] Manejo de DATA_END para request {request_id}")
            self.wsm_client.cleanup_request(request_id)
            self._cleanup_request_files(request_id)
            cleanup_message = protocol.create_notification_message(protocol.DATA_END, b"", request_id)
            for q in self.out_queues:
                q.send(cleanup_message)
            logging.info(f"[GrouperV2:{self.grouper_mode}] Sent cleanup notification for request {request_id}")
            return
        
        self.wsm_client.update_state("END", request_id, position)
        logging.info(f"[GrouperV2:{self.grouper_mode}] Recibido END para request {request_id}. Esperando permiso del WSM...")

        backoff = self.backoff_start
        total_wait = 0.0
        
        while not self.wsm_client.can_send_end(request_id, position):
            if total_wait >= self.backoff_max:
                error_msg = f"[GrouperV2:{self.grouper_mode}] Timeout esperando permiso WSM para END de {request_id} después de {total_wait:.2f}s"
                logging.error(error_msg)
                raise TimeoutError(error_msg)
            
            logging.info(f"[GrouperV2:{self.grouper_mode}] Esperando permiso... (backoff={backoff:.3f}s, total={total_wait:.2f}s)")
            time.sleep(backoff)
            total_wait += backoff
            backoff = min(backoff * 2, self.backoff_max - total_wait) if total_wait < self.backoff_max else 0

        logging.info(f"[GrouperV2:{self.grouper_mode}] ✅ Permiso otorgado por el WSM para reenviar END de {request_id}")

        noti_payload = f"completed_by={self.replica_id}".encode("utf-8")
        noti_message = protocol.create_notification_message(data_type, noti_payload, request_id)
        for q in self.out_queues:
            q.send(noti_message)

        self.wsm_client.update_state("WAITING", request_id, position)

    def _process_rows(self, rows, queue_name=None):
        """
        Dispatch rows to mode-specific aggregation logic.
        
        Routes incoming transaction rows to the appropriate aggregation method based
        on the configured grouper_mode. Requires temp_dir to be initialized via
        _initialize_request_paths.
        
        Args:
            rows (list): List of pipe-delimited transaction row strings.
            queue_name (str, optional): Source queue name (unused).
        
        Returns:
            None: Aggregations written directly to disk.
        
        Example:
            Query 2 aggregation (monthly item sales):
            >>> grouper = GrouperV2(..., grouper_mode='q2', ...)
            >>> rows = [
            ...     'ITEM123|2|15.50|2024-03-15 10:30:00',
            ...     'ITEM456|1|8.75|2024-03-20 14:15:00',
            ...     'ITEM123|3|23.25|2024-03-22 09:45:00'
            ... ]
            >>> grouper._process_rows(rows)
            # Calls _q2_agg, creates/updates:
            # temp/grouper_v2_q2/req-123/replica-1/2024-03.csv
            # ITEM123,5,38.75
            # ITEM456,1,8.75
            
            Query 3 aggregation (semester store totals):
            >>> grouper = GrouperV2(..., grouper_mode='q3', ...)
            >>> rows = [
            ...     'trans1|150.25|2024-03-15|STORE001',
            ...     'trans2|75.00|2024-04-20|STORE001',
            ...     'trans3|200.50|2024-07-10|STORE002'
            ... ]
            >>> grouper._process_rows(rows)
            # Calls _q3_agg, creates/updates:
            # temp/.../2024-H1_STORE001_replica-1.csv: 225.25
            # temp/.../2024-H2_STORE002_replica-1.csv: 200.50
            
            Query 4 aggregation (user visit frequency):
            >>> grouper = GrouperV2(..., grouper_mode='q4', ...)
            >>> rows = [
            ...     'trans1|amt|date|STORE001|user123',
            ...     'trans2|amt|date|STORE001|user456',
            ...     'trans3|amt|date|STORE001|user123'
            ... ]
            >>> grouper._process_rows(rows)
            # Calls _q4_agg, creates/updates:
            # temp/.../STORE001_replica-1.csv
            # user123,2
            # user456,1
        """
        if not self.temp_dir:
            logging.error("temp_dir no inicializado (falta request_id).")
            return

        if self.grouper_mode == 'q2':
            self._q2_agg(rows, self.temp_dir)
        elif self.grouper_mode == 'q3':
            self._q3_agg(rows, self.temp_dir)
        elif self.grouper_mode == 'q4':
            self._q4_agg(rows, self.temp_dir)

    def _q2_agg(self, rows, temp_dir):
        """
        Aggregate transaction items by month and menu item (Query 2).
        
        Groups transactions by month and item ID, accumulating total quantity sold
        and total revenue. Results are incrementally updated to per-month CSV files.
        
        Row format: item_id|quantity|subtotal|created_at
        
        Args:
            rows (list): List of pipe-delimited transaction item rows.
            temp_dir (str): Directory for temporary aggregation files.
        
        Example:
            Input rows:
            >>> rows = [
            ...     'ITEM123|2|15.50|2024-03-15 10:30:00',
            ...     'ITEM456|1|8.75|2024-03-20 14:15:00',
            ...     'ITEM123|3|23.25|2024-03-15 16:45:00',
            ...     'ITEM789|5|42.00|2024-04-10 09:00:00'
            ... ]
            >>> grouper._q2_agg(rows, '/tmp/grouper/req-123/replica-1')
            
            Creates/updates files:
            /tmp/grouper/req-123/replica-1/2024-03_replica-1.csv:
                ITEM123,5,38.75
                ITEM456,1,8.75
            
            /tmp/grouper/req-123/replica-1/2024-04_replica-1.csv:
                ITEM789,5,42.00
            
            Incremental aggregation:
            >>> # First batch
            >>> rows1 = ['ITEM123|2|10.00|2024-03-15']
            >>> grouper._q2_agg(rows1, temp_dir)
            # File: ITEM123,2,10.00
            
            >>> # Second batch (same month, same item)
            >>> rows2 = ['ITEM123|3|15.00|2024-03-20']
            >>> grouper._q2_agg(rows2, temp_dir)
            # File updated: ITEM123,5,25.00
        """
        idx_item, idx_quantity, idx_subtotal, idx_created = 0, 1, 2, 3
        monthly_data = defaultdict(lambda: defaultdict(lambda: [0, 0.0]))
        for row in rows:
            items = row.split('|')
            if len(items) <= idx_created:
                continue
            try:
                month = get_month_str(items[idx_created])
                item_id = items[idx_item]
                monthly_data[month][item_id][0] += int(items[idx_quantity])
                monthly_data[month][item_id][1] += float(items[idx_subtotal])
            except Exception:
                continue
        for month, data in monthly_data.items():
            self._update_q2_file(temp_dir, month, data)
        monthly_data.clear(); gc.collect()

    def _update_q2_file(self, temp_dir, month, data):
        """
        Update monthly aggregation file with new data (Query 2).
        
        Reads existing monthly aggregations, merges with new data, and writes back
        atomically. Each replica maintains separate files to avoid concurrent write
        conflicts. File format: item_id,total_quantity,total_revenue
        
        Args:
            temp_dir (str): Directory for aggregation files.
            month (str): Month string in format 'YYYY-MM'.
            data (dict): Item aggregations {item_id: [quantity, revenue]}.
        
        Example:
            Creating new monthly file:
            >>> data = {
            ...     'ITEM123': [5, 38.75],
            ...     'ITEM456': [2, 17.50]
            ... }
            >>> grouper._update_q2_file('/tmp/req-123/replica-1', '2024-03', data)
            # Creates: /tmp/req-123/replica-1/2024-03_replica-1.csv
            # ITEM123,5,38.75
            # ITEM456,2,17.50
            
            Merging with existing data:
            >>> # Existing file contains:
            >>> # ITEM123,5,38.75
            >>> # ITEM789,3,22.00
            >>> new_data = {
            ...     'ITEM123': [2, 15.00],  # Add to existing
            ...     'ITEM456': [1, 8.50]    # New item
            ... }
            >>> grouper._update_q2_file('/tmp/req-123/replica-1', '2024-03', new_data)
            # Updated file:
            # ITEM123,7,53.75  (5+2, 38.75+15.00)
            # ITEM789,3,22.00  (unchanged)
            # ITEM456,1,8.50   (new)
            
            Atomic write ensures crash consistency:
            >>> grouper._update_q2_file(temp_dir, '2024-03', large_data)
            # Writes to temporary file first
            # Atomically renames to target
            # No partial writes on crash
        """
        path = os.path.join(temp_dir, f"{month}_{self.replica_id}.csv")
        existing = {}
        if os.path.exists(path):
            with open(path, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 3:
                        existing[parts[0]] = [int(parts[1]), float(parts[2])]
        for k, v in data.items():
            if k in existing:
                existing[k][0] += v[0]
                existing[k][1] += v[1]
            else:
                existing[k] = v
        
        def write_func(temp_path):
            with open(temp_path, 'w') as f:
                for k, v in existing.items():
                    f.write(f"{k},{v[0]},{v[1]}\n")
        
        StreamProcessingWorker.atomic_write(path, write_func)

    def _q3_agg(self, rows, temp_dir):
        """
        Aggregate transactions by semester and store (Query 3).
        
        Groups transactions by semester (H1: Jan-Jun, H2: Jul-Dec) and store ID,
        accumulating total revenue. Results are incrementally updated to per-semester-store
        CSV files.
        
        Row format: transaction_id|final_amount|created_at|store_id
        
        Args:
            rows (list): List of pipe-delimited transaction rows.
            temp_dir (str): Directory for temporary aggregation files.
        
        Example:
            Input rows:
            >>> rows = [
            ...     'trans1|150.25|2024-03-15 10:30:00|STORE001',
            ...     'trans2|75.00|2024-04-20 14:15:00|STORE001',
            ...     'trans3|200.50|2024-07-10 09:00:00|STORE002',
            ...     'trans4|125.75|2024-08-05 16:30:00|STORE001'
            ... ]
            >>> grouper._q3_agg(rows, '/tmp/grouper/req-123/replica-1')
            
            Creates/updates files:
            /tmp/grouper/req-123/replica-1/2024-H1_STORE001_replica-1.csv:
                225.25
            
            /tmp/grouper/req-123/replica-1/2024-H2_STORE002_replica-1.csv:
                200.50
            
            /tmp/grouper/req-123/replica-1/2024-H2_STORE001_replica-1.csv:
                125.75
            
            Incremental aggregation:
            >>> # First batch
            >>> rows1 = ['trans1|100.00|2024-03-15|STORE001']
            >>> grouper._q3_agg(rows1, temp_dir)
            # File 2024-H1_STORE001: 100.00
            
            >>> # Second batch (same semester, same store)
            >>> rows2 = ['trans2|50.00|2024-04-20|STORE001']
            >>> grouper._q3_agg(rows2, temp_dir)
            # File updated: 150.00
        """
        idx_final, idx_created, idx_store = 1, 2, 3
        grouped = {}
        for row in rows:
            items = row.split('|')
            if len(items) < 4:
                continue
            try:
                semester = get_semester_str(items[idx_created])
                store = items[idx_store]
                amt = float(items[idx_final])
                key = f"{semester}_{store}"
                grouped[key] = grouped.get(key, 0.0) + amt
            except Exception:
                continue
        self._update_q3_file(temp_dir, grouped)
        grouped.clear(); gc.collect()

    def _update_q3_file(self, temp_dir, data):
        """
        Update semester-store aggregation files with new data (Query 3).
        
        Reads existing semester-store totals, adds new revenue, and writes back atomically.
        Each replica maintains separate files. File contains single line with total revenue.
        
        Args:
            temp_dir (str): Directory for aggregation files.
            data (dict): Aggregations {"semester_store": total_amount}.
        
        Example:
            Creating new semester-store file:
            >>> data = {'2024-H1_STORE001': 225.25}
            >>> grouper._update_q3_file('/tmp/req-123/replica-1', data)
            # Creates: /tmp/req-123/replica-1/2024-H1_STORE001_replica-1.csv
            # 225.25
            
            Merging with existing data:
            >>> # Existing file 2024-H1_STORE001_replica-1.csv contains:
            >>> # 225.25
            >>> new_data = {'2024-H1_STORE001': 100.00}
            >>> grouper._update_q3_file('/tmp/req-123/replica-1', new_data)
            # Updated file:
            # 325.25
            
            Multiple semester-store combinations:
            >>> data = {
            ...     '2024-H1_STORE001': 150.00,
            ...     '2024-H1_STORE002': 200.00,
            ...     '2024-H2_STORE001': 175.00
            ... }
            >>> grouper._update_q3_file(temp_dir, data)
            # Creates/updates 3 separate files
            
            Atomic write ensures crash consistency:
            >>> grouper._update_q3_file(temp_dir, data)
            # Each file written to temp location first
            # Atomically renamed to target
            # No partial updates on crash
        """
        for key, val in data.items():
            path = os.path.join(temp_dir, f"{key}_{self.replica_id}.csv")
            old = 0.0
            if os.path.exists(path):
                with open(path, 'r') as f:
                    try:
                        old = float(f.read().strip())
                    except:
                        pass

            total = old + val

            def write_func(temp_path):
                with open(temp_path, 'w') as f:
                    f.write(f"{total}\n")
            
            StreamProcessingWorker.atomic_write(path, write_func)

    def _q4_agg(self, rows, temp_dir):
        """
        Aggregate user visit frequency by store (Query 4).
        
        Groups transactions by store and user ID, counting the number of visits
        (transactions) each user made to each store. Results are incrementally
        updated to per-store CSV files.
        
        Row format: transaction_id|amount|date|store_id|user_id
        
        Args:
            rows (list): List of pipe-delimited transaction rows.
            temp_dir (str): Directory for temporary aggregation files.
        
        Example:
            Input rows:
            >>> rows = [
            ...     'trans1|50.00|2024-03-15|STORE001|user123',
            ...     'trans2|30.00|2024-03-20|STORE001|user456',
            ...     'trans3|75.00|2024-03-25|STORE001|user123',
            ...     'trans4|40.00|2024-03-30|STORE002|user123',
            ...     'trans5|60.00|2024-04-05|STORE001|user456'
            ... ]
            >>> grouper._q4_agg(rows, '/tmp/grouper/req-123/replica-1')
            
            Creates/updates files:
            /tmp/grouper/req-123/replica-1/STORE001_replica-1.csv:
                user123,2
                user456,2
            
            /tmp/grouper/req-123/replica-1/STORE002_replica-1.csv:
                user123,1
            
            Incremental aggregation:
            >>> # First batch
            >>> rows1 = ['trans1|50|date|STORE001|user123']
            >>> grouper._q4_agg(rows1, temp_dir)
            # File STORE001: user123,1
            
            >>> # Second batch (same store, same user)
            >>> rows2 = ['trans2|30|date|STORE001|user123']
            >>> grouper._q4_agg(rows2, temp_dir)
            # File updated: user123,2
            
            Filtering empty users:
            >>> rows = [
            ...     'trans1|50|date|STORE001|user123',
            ...     'trans2|30|date|STORE001|',  # Empty user
            ...     'trans3|40|date|STORE001|  '  # Whitespace only
            ... ]
            >>> grouper._q4_agg(rows, temp_dir)
            # File contains only: user123,1
            # Empty/whitespace users skipped
        """
        idx_store, idx_user = 3, 4
        grouped = {}
        for row in rows:
            items = row.split('|')
            if len(items) <= idx_user:
                continue
            store, user = items[idx_store], items[idx_user].strip()
            if not user:
                continue
            grouped.setdefault(store, {})
            grouped[store][user] = grouped[store].get(user, 0) + 1
        self._update_q4_file(temp_dir, grouped)
        grouped.clear(); gc.collect()

    def _update_q4_file(self, temp_dir, data):
        """
        Update store user-frequency aggregation files with new data (Query 4).
        
        Reads existing user visit counts per store, merges with new data, and writes
        back atomically. Each replica maintains separate files to avoid concurrent
        write conflicts. File format: user_id,visit_count
        
        Args:
            temp_dir (str): Directory for aggregation files.
            data (dict): Store aggregations {store_id: {user_id: count}}.
        
        Example:
            Creating new store file:
            >>> data = {
            ...     'STORE001': {'user123': 2, 'user456': 1}
            ... }
            >>> grouper._update_q4_file('/tmp/req-123/replica-1', data)
            # Creates: /tmp/req-123/replica-1/STORE001_replica-1.csv
            # user123,2
            # user456,1
            
            Merging with existing data:
            >>> # Existing file STORE001_replica-1.csv contains:
            >>> # user123,2
            >>> # user789,1
            >>> new_data = {
            ...     'STORE001': {'user123': 1, 'user456': 2}
            ... }
            >>> grouper._update_q4_file('/tmp/req-123/replica-1', new_data)
            # Updated file:
            # user123,3  (2+1)
            # user789,1  (unchanged)
            # user456,2  (new)
            
            Multiple stores:
            >>> data = {
            ...     'STORE001': {'user123': 1},
            ...     'STORE002': {'user456': 2},
            ...     'STORE003': {'user789': 1}
            ... }
            >>> grouper._update_q4_file(temp_dir, data)
            # Creates/updates 3 separate files
            
            Atomic write ensures crash consistency:
            >>> grouper._update_q4_file(temp_dir, large_data)
            # Each file written to temp location first
            # Atomically renamed to target
            # No partial writes on crash
        """
        for store, users in data.items():
            path = os.path.join(temp_dir, f"{store}_{self.replica_id}.csv")
            existing = {}
            if os.path.exists(path):
                with open(path, 'r') as f:
                    for line in f:
                        parts = line.strip().split(',')
                        if len(parts) == 2:
                            existing[parts[0]] = int(parts[1])
            for u, c in users.items():
                existing[u] = existing.get(u, 0) + c
            
            def write_func(temp_path):
                with open(temp_path, 'w') as f:
                    for u, c in existing.items():
                        f.write(f"{u},{c}\n")
            
            StreamProcessingWorker.atomic_write(path, write_func)

    def _initialize_request_paths(self, request_id):
        """
        Initialize temporary directory structure for a request.
        
        Creates isolated per-request, per-mode, per-replica directory for aggregation
        files. Only initializes once per request_id to avoid redundant directory creation.
        
        Args:
            request_id (str): Request identifier.
        
        Example:
            First call for request:
            >>> grouper = GrouperV2(..., grouper_mode='q2', replica_id='replica-1', ...)
            >>> grouper._initialize_request_paths('req-123')
            # Creates: /tmp/grouper_v2_q2/req-123/replica-1/
            # Sets: self.temp_dir = '/tmp/grouper_v2_q2/req-123/replica-1/'
            # Sets: self.request_id_initialized = True
            
            Subsequent calls (same request):
            >>> grouper._initialize_request_paths('req-123')
            # Returns immediately, no directory creation
            
            New request:
            >>> grouper._initialize_request_paths('req-456')
            # Creates: /tmp/grouper_v2_q2/req-456/replica-1/
            # Updates: self.temp_dir = '/tmp/grouper_v2_q2/req-456/replica-1/'
            
            Different modes and replicas get separate directories:
            >>> # Mode q2, replica-1
            >>> grouper1._initialize_request_paths('req-123')
            # /tmp/grouper_v2_q2/req-123/replica-1/
            
            >>> # Mode q3, replica-2
            >>> grouper2._initialize_request_paths('req-123')
            # /tmp/grouper_v2_q3/req-123/replica-2/
        """
        if self.request_id_initialized and self.current_request_id == request_id:
            return
        self.current_request_id = request_id
        mode_dir = f"grouper_v2_{self.grouper_mode}"
        self.temp_dir = os.path.join(self.base_temp_root, mode_dir, str(request_id), self.replica_id)
        os.makedirs(self.temp_dir, exist_ok=True)
        self.request_id_initialized = True

    def _cleanup_request_files(self, request_id):
        """
        Clean up temporary aggregation files for a completed request.
        
        Removes the entire request directory tree containing partial aggregation
        files from all replicas. This prevents disk space accumulation after
        requests are completed and reduced.
        
        Args:
            request_id (str): Request identifier to clean up.
        
        Example:
            >>> grouper._cleanup_request_files('req-123')
            # Removes: /tmp/grouper_v2_q2/req-123/
            # Including all replica subdirectories and CSV files
            [GrouperV2:q2] Cleaned up temporary files for request_id=req-123
        """
        import shutil
        
        mode_dir = f"grouper_v2_{self.grouper_mode}"
        request_dir = os.path.join(self.base_temp_root, mode_dir, str(request_id))
        
        if os.path.exists(request_dir):
            try:
                shutil.rmtree(request_dir)
                logging.info(f"[GrouperV2:{self.grouper_mode}] Cleaned up temporary files for request_id={request_id} at {request_dir}")
            except Exception as e:
                logging.error(f"[GrouperV2:{self.grouper_mode}] Error cleaning up temporary files for request_id={request_id}: {e}")
        else:
            logging.debug(f"[GrouperV2:{self.grouper_mode}] No temporary files found for request_id={request_id} at {request_dir}")


if __name__ == '__main__':
    def create_grouperv2():
        """
        Factory function to create GrouperV2 instance from environment configuration.
        
        Reads configuration from environment variables and config.ini to instantiate
        a GrouperV2 worker with appropriate settings. Uses hostname as replica_id.
        
        Environment Variables:
            RABBITMQ_HOST: RabbitMQ server hostname (default: 'rabbitmq').
            QUEUE_IN: Input queue name (required).
            COMPLETION_QUEUE: Output queue name (required).
            GROUPER_MODE: Aggregation mode 'q2', 'q3', or 'q4' (required).
            BASE_TEMP_DIR: Root directory for temp files (optional).
            WSM_HOST: Worker State Manager hostname (default: 'wsm').
            WSM_PORT: Worker State Manager port (default: '9000').
        
        Config.ini:
            [DEFAULT]
            BACKOFF_START: Initial WSM polling delay (default: 0.1).
            BACKOFF_MAX: Maximum WSM polling delay (default: 3.0).
        
        Returns:
            GrouperV2: Configured worker instance.
        
        Raises:
            ValueError: If GROUPER_MODE environment variable not set.
        
        Example:
            Docker Compose configuration:
            ```yaml
            grouper-q2-1:
              image: grouper_v2
              environment:
                RABBITMQ_HOST: rabbitmq
                QUEUE_IN: filter_output_q2
                COMPLETION_QUEUE: coordinator_input_q2
                GROUPER_MODE: q2
                BASE_TEMP_DIR: /app/temp
                WSM_HOST: wsm-cluster
                WSM_PORT: 9000
              hostname: grouper-q2-1
            ```
            
            Resulting configuration:
            >>> # Inside container
            >>> grouper = create_grouperv2()
            # Creates GrouperV2(
            #     queue_in='filter_output_q2',
            #     queue_out='coordinator_input_q2',
            #     rabbitmq_host='rabbitmq',
            #     grouper_mode='q2',
            #     replica_id='grouper-q2-1',
            #     backoff_start=0.1,
            #     backoff_max=3.0,
            #     wsm_nodes=['wsm-1:9000', 'wsm-2:9000']
            # )
        """
        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config = configparser.ConfigParser(); config.read(config_path)
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        queue_in = os.environ.get('QUEUE_IN')
        queue_out = os.environ.get('COMPLETION_QUEUE')
        mode = os.environ.get('GROUPER_MODE')
        replica_id = socket.gethostname()
        if not mode:
            raise ValueError("GROUPER_MODE not set")
        
        backoff_start = float(config['DEFAULT'].get('BACKOFF_START', 0.1))
        backoff_max = float(config['DEFAULT'].get('BACKOFF_MAX', 3.0))
        
        wsm_nodes = WSM_NODES[mode]
        print("WSM NODES: ", wsm_nodes)

        return GrouperV2(queue_in, queue_out, rabbitmq_host, mode, replica_id, backoff_start, backoff_max, wsm_nodes)

    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    GrouperV2.run_worker_main(create_grouperv2, config_path)
