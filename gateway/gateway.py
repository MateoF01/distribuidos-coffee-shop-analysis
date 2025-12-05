import socket
import threading
import time
import signal
import multiprocessing
from multiprocessing import Value, Manager
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue, CoffeeMessageMiddlewareExchange
import os
import glob
from shared import protocol
import logging
from shared.logging_config import initialize_log
from shared.worker import Worker

from WSM.wsm_client import WSMClient
from wsm_config import WSM_NODES
import socket

HOST = os.environ.get('GATEWAY_HOST', '0.0.0.0')
PORT = int(os.environ.get('GATEWAY_PORT', 5000))
RESULTS_QUEUE = os.environ.get('QUEUE_IN', 'results')
OUTPUT_GATEWAY_DIR = os.environ.get('GATEWAY_OUTPUT_DIR', 'output_gateway')


data_type_names = {
    protocol.DATA_TRANSACTIONS: 'transactions',
    protocol.DATA_TRANSACTION_ITEMS: 'transaction_items',
    protocol.DATA_MENU_ITEMS: 'menu_items',
    protocol.DATA_USERS: 'users',
    protocol.DATA_STORES: 'stores',
    protocol.DATA_END: 'end'
}

queue_names = {
    protocol.DATA_TRANSACTIONS: 'transactions_queue',
    protocol.DATA_TRANSACTION_ITEMS: 'transaction_items_queue',
    protocol.DATA_MENU_ITEMS: 'menu_items_queue',
    protocol.DATA_USERS: 'users_queue',
    protocol.DATA_STORES: 'stores_queue'
}

class Server:
    """
    Gateway server that receives CSV data from clients and routes it to processing queues.
    
    The Server acts as the entry point for the distributed coffee shop analysis system.
    It receives data from multiple clients, assigns request IDs, routes data to appropriate
    RabbitMQ queues, and forwards query results back to clients. The server supports
    concurrent client connections using multiprocessing and handles graceful shutdown.
    
    Public API Lifecycle:
        1. Create a Server instance with __init__()
        2. Call run() to start accepting connections
        3. Server handles shutdown automatically via signals (SIGTERM, SIGINT)
    
    Key Features:
        - Multi-process architecture for handling concurrent clients
        - Request persistence and recovery for fault tolerance
        - Result routing from workers back to appropriate clients
        - Graceful shutdown handling
    
    Attributes:
        host (str): IP address to bind the server socket.
        port (int): Port number to listen on.
        listen_backlog (int): Maximum number of queued connections.
        shutdown_flag (bool): Flag indicating server should shut down.
        requests (dict): Mapping of request_id to client socket connections.
        next_request_id (Value): Shared counter for generating unique request IDs.
        queues (dict): RabbitMQ queue connections for data routing.
        results_queue (CoffeeMessageMiddlewareQueue): Queue for receiving query results.
    
    Example:
        Basic usage:
        >>> server = Server('0.0.0.0', 5000)
        >>> server.run()
        Server listening on 0.0.0.0:5000
        [GATEWAY RECOVERY] No abandoned requests found on startup
        Result router thread started
        Connected by ('172.17.0.5', 45678)
        Client ('172.17.0.5', 45678) new request_id: 1
        ...
        
        With shared state for multiprocessing:
        >>> from multiprocessing import Manager, Value
        >>> manager = Manager()
        >>> shared_requests = manager.dict()
        >>> shared_request_id = Value('i', 1)
        >>> server = Server('0.0.0.0', 5000, 
        ...                 shared_request_id=shared_request_id,
        ...                 shared_requests=shared_requests)
        >>> server.run()
        Server listening on 0.0.0.0:5000
    """
    def __init__(self, host, port, listen_backlog=1, shared_request_id=None, shared_requests=None):
        """
        Initialize the gateway server.
        
        Args:
            host (str): IP address to bind the server socket (e.g., '0.0.0.0').
            port (int): Port number to listen on (e.g., 5000).
            listen_backlog (int, optional): Maximum number of queued connections. Defaults to 1.
            shared_request_id (Value, optional): Shared counter for request IDs across processes.
            shared_requests (dict, optional): Shared dictionary mapping request_id to connections.
        
        Example:
            >>> server = Server('0.0.0.0', 5000)
            >>> server.host
            '0.0.0.0'
            >>> server.port
            5000
            
            >>> from multiprocessing import Manager, Value
            >>> manager = Manager()
            >>> shared_id = Value('i', 1)
            >>> shared_req = manager.dict()
            >>> server = Server('localhost', 8080, listen_backlog=5,
            ...                 shared_request_id=shared_id,
            ...                 shared_requests=shared_req)
        """

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


        self.host = host
        self.port = port
        self.listen_backlog = listen_backlog
        self.shutdown_flag = False
        self._setup_socket()
        self._setup_signal_handlers()
        
        if shared_requests is not None:
            self.requests = shared_requests
        else:
            self.requests = {}
        
        if shared_request_id is not None:
            self.next_request_id = shared_request_id
        else:
            self.next_request_id = Value('i', 1)

    def _ensure_output_dir(self):
        """
        Create the output directory for gateway persistence files if it doesn't exist.
        
        The directory is used to store .active files that track in-progress requests
        for recovery purposes.
        
        Example:
            >>> server._ensure_output_dir()
            >>> os.path.exists('output_gateway')
            True
        """
        os.makedirs(OUTPUT_GATEWAY_DIR, exist_ok=True)

    def _save_active_request(self, request_id):
        """
        Persist request_id to disk as an .active file for fault-tolerant recovery.
        
        Uses atomic write operations to ensure the file is either fully written or not
        created at all, preventing partial writes during crashes.
        
        Args:
            request_id (int): The request identifier to persist.
        
        Example:
            >>> server._save_active_request(42)
            [GATEWAY PERSISTENCE] Created active request: request_id=42
            >>> os.path.exists('output_gateway/request_42.active')
            True
            >>> with open('output_gateway/request_42.active') as f:
            ...     f.read()
            '42'
        """
        self._ensure_output_dir()
        active_file = os.path.join(OUTPUT_GATEWAY_DIR, f"request_{request_id}.active")
        
        def write_func(temp_path):
            with open(temp_path, 'w') as f:
                f.write(str(request_id))
        
        try:
            Worker.atomic_write(active_file, write_func)
            logging.info(f"[GATEWAY PERSISTENCE] Created active request: request_id={request_id}")
        except Exception as e:
            logging.warning(f"[GATEWAY PERSISTENCE] Failed to save active request {request_id}: {e}")

    def _cleanup_active_request(self, request_id, queues=None):
        """
        Remove the .active persistence file when a request completes successfully.
        
        Sends DATA_END signals to all queues for this request_id to ensure workers
        clean up any resources associated with the request before removing the .active file.
        This method is idempotent - it checks if the .active file exists before attempting cleanup.
        
        Args:
            request_id (int): The request identifier whose .active file should be removed.
            queues (dict, optional): Dictionary of queue connections to use. If None, uses self.queues.
                                     IMPORTANT: When called from a subprocess, pass the subprocess's
                                     own queue connections to avoid corrupting the main process's
                                     RabbitMQ connections.
        
        Example:
            >>> server._save_active_request(42)
            [GATEWAY PERSISTENCE] Created active request: request_id=42
            >>> server._cleanup_active_request(42)
            [GATEWAY PERSISTENCE] Deleted active request: request_id=42
            >>> os.path.exists('output_gateway/request_42.active')
            False
        """
        active_file = os.path.join(OUTPUT_GATEWAY_DIR, f"request_{request_id}.active")
        
        # Check if already cleaned up (idempotent guard)
        if not os.path.exists(active_file):
            logging.debug(f"[GATEWAY CLEANUP] Request {request_id} already cleaned up, skipping")
            return
        
        # Use provided queues or fall back to self.queues (main process only!)
        queues_to_use = queues if queues is not None else self.queues
        
        for queue_name in queue_names.values():
            try:
                message = protocol.create_end_message(protocol.DATA_END, request_id, 1)
                queues_to_use[queue_name].send(message)
            except Exception as e:
                logging.error(f"[GATEWAY CLEANUP] Failed to send END to {queue_name} for request_id={request_id}: {e}")
        
        try:
            os.remove(active_file)
            logging.info(f"[GATEWAY PERSISTENCE] Deleted active request: request_id={request_id}")
        except Exception as e:
            logging.warning(f"[GATEWAY PERSISTENCE] Failed to cleanup active request {request_id}: {e}")

    def _recover_abandoned_requests(self, queues):
        """
        Recover from server crashes by detecting and canceling abandoned requests.
        
        On startup, scans for orphaned .active files from previous runs and sends
        DATA_END signals to all queues for each abandoned request to ensure workers
        don't wait indefinitely for data that will never arrive.
        
        Additionally, adjusts the next_request_id counter to be one higher than the
        highest request_id found (whether active or abandoned), ensuring no ID collisions
        when new requests are created.
        
        Args:
            queues (dict): Dictionary of queue name to CoffeeMessageMiddlewareQueue instances.
        
        Example:
            Scenario with no abandoned requests:
            >>> server._recover_abandoned_requests(queues)
            [GATEWAY RECOVERY] No abandoned requests found on startup
            
            Scenario with abandoned requests:
            >>> server._recover_abandoned_requests(queues)
            [GATEWAY RECOVERY] Found 2 abandoned request(s), sending cancellation signals...
            [GATEWAY RECOVERY] Recovered request_id=15, sent END to all queues
            [GATEWAY RECOVERY] Recovered request_id=23, sent END to all queues
            [GATEWAY RECOVERY] Adjusted next_request_id to 24 (max found: 23)
        """
        self._ensure_output_dir()
        active_files = glob.glob(os.path.join(OUTPUT_GATEWAY_DIR, "request_*.active"))
        
        if not active_files:
            logging.info("[GATEWAY RECOVERY] No abandoned requests found on startup")
            return
        
        logging.warning(f"[GATEWAY RECOVERY] Found {len(active_files)} abandoned request(s), sending cancellation signals...")
        
        max_request_id = 0
        
        for active_file in active_files:
            try:
                with open(active_file, 'r') as f:
                    request_id_str = f.read().strip()
                    request_id = int(request_id_str)
                
                max_request_id = max(max_request_id, request_id)
                
                for queue_name in queue_names.values():
                    try:
                        message = protocol.create_end_message(protocol.DATA_END, request_id, 1)
                        queues[queue_name].send(message)
                    except Exception as e:
                        logging.error(f"[GATEWAY RECOVERY] Failed to send END to {queue_name} for request_id={request_id}: {e}")
                
                try:
                    os.remove(active_file)
                except Exception as e:
                    logging.warning(f"[GATEWAY RECOVERY] Failed to remove .active file {active_file}: {e}")
                    
            except Exception as e:
                logging.error(f"[GATEWAY RECOVERY] Error processing active file {active_file}: {e}")
        
        # Adjust next_request_id to be after the highest found
        if max_request_id > 0:
            with self.next_request_id.get_lock():
                if max_request_id >= self.next_request_id.value:
                    self.next_request_id.value = max_request_id + 1
                    logging.info(f"[GATEWAY RECOVERY] Adjusted next_request_id to {self.next_request_id.value} (max found: {max_request_id})")

    def _setup_rabbitmq(self):
        """
        Initialize RabbitMQ connections for data routing and result receiving.
        
        Creates queue connections for each data type (transactions, stores, users, etc.)
        and a dedicated results queue for receiving processed query results from workers.
        
        Example:
            >>> server._setup_rabbitmq()
            >>> 'transactions_queue' in server.queues
            True
            >>> server.results_queue.queue_name
            'results'
        """
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        self.queues = {}
        for q in queue_names.values():
            self.queues[q] = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=q)
        self.results_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=RESULTS_QUEUE)

    def _setup_socket(self):
        """
        Create and configure the TCP server socket.
        
        Binds to the configured host and port and starts listening for incoming
        client connections.
        
        Example:
            >>> server._setup_socket()
            >>> server._server_socket.getsockname()
            ('0.0.0.0', 5000)
        """
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(self.listen_backlog)

    def _setup_signal_handlers(self):
        """
        Register signal handlers for graceful server shutdown.
        
        Configures handlers for SIGTERM and SIGINT to allow clean shutdown
        when the server receives termination signals.
        
        Example:
            >>> server._setup_signal_handlers()
            >>> import signal
            >>> signal.getsignal(signal.SIGTERM) == server._signal_handler
            True
        """
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """
        Handle termination signals for graceful shutdown.
        
        Args:
            signum (int): Signal number received.
            frame: Current stack frame (unused).
        
        Example:
            >>> server._signal_handler(signal.SIGTERM, None)
            Received signal 15, shutting down gateway gracefully...
            >>> server.shutdown_flag
            True
        """
        logging.info(f"Received signal {signum}, shutting down gateway gracefully...")
        self.shutdown_flag = True

    def _get_next_request_id(self):
        """
        Generate a unique request ID in a thread-safe manner.
        
        Uses a lock to ensure atomic increment across multiple processes,
        preventing race conditions when multiple clients connect simultaneously.
        
        Returns:
            int: A unique request identifier.
        
        Example:
            >>> server._get_next_request_id()
            1
            >>> server._get_next_request_id()
            2
            >>> server._get_next_request_id()
            3
        """
        with self.next_request_id.get_lock():
            rid = self.next_request_id.value
            self.next_request_id.value += 1
        return rid

    def _result_router(self):
        """
        Route query results from workers back to the appropriate client connections.
        
        This method runs in a dedicated thread in the main process and is the ONLY consumer
        of the results queue. This ensures messages are processed in order without load-balancing
        across multiple consumers. Results are forwarded directly to clients with no buffering.
        
        The router tracks DATA_END signals from all workers (5 expected per request) and sends
        a final DATA_END to the client only when all workers have completed processing.
        
        Example:
            Router processing flow:
            >>> server._result_router()
            [GATEWAY ROUTER] Starting result consumer thread
            [GATEWAY ROUTER INCOMING] DATA: data_type=10, request_id=1, position=1, rows=15, payload_size=1024
            [GATEWAY ROUTER] Forwarded DATA: data_type=10, request_id=1, size=1024
            [GATEWAY ROUTER INCOMING] END: data_type=10, request_id=1, position=2
            [GATEWAY ROUTER] Forwarded END: data_type=10, request_id=1
            [GATEWAY ROUTER] DATA_END for request_id=1 (5/5)
            [GATEWAY ROUTER] Sent final DATA_END to client for request_id=1
            [GATEWAY ROUTER] Request completed: request_id=1, elapsed_seconds=12.45
        """
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        results_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=RESULTS_QUEUE)
        
        data_end_counts = {}
        completed_requests = set()
        data_end_expected = 5
        
        message_log = {}
        
        def on_result(message):
            try:
                msg_type, data_type, request_id, position, payload = protocol.unpack_message(message)
                if msg_type == protocol.MSG_TYPE_END and data_type == protocol.DATA_END and position == 1:
                    logging.info(f"[GATEWAY ROUTER] Cleanup signal received for request_id={request_id}, ignoring in router")
                    return
                
                if request_id not in message_log:
                    message_log[request_id] = {
                        'data_messages': {},
                        'end_messages': {},
                        'total_payload_bytes': 0,
                        'first_seen': time.time()
                    }
                
                if msg_type == protocol.MSG_TYPE_DATA:
                    payload_str = payload.decode('utf-8', errors='ignore')
                    row_count = len([row for row in payload_str.split('\n') if row.strip()])
                    
                    if data_type not in message_log[request_id]['data_messages']:
                        message_log[request_id]['data_messages'][data_type] = {
                            'count': 0,
                            'rows': 0,
                            'bytes': 0
                        }
                    
                    message_log[request_id]['data_messages'][data_type]['count'] += 1
                    message_log[request_id]['data_messages'][data_type]['rows'] += row_count
                    message_log[request_id]['data_messages'][data_type]['bytes'] += len(payload)
                    message_log[request_id]['total_payload_bytes'] += len(payload)

                    logging.info(f"[GATEWAY ROUTER INCOMING] DATA: data_type={data_type}, request_id={request_id}, position={position}, rows={row_count}, payload_size={len(payload)}, msg_count={message_log[request_id]['data_messages'][data_type]['count']}, total_rows_for_type={message_log[request_id]['data_messages'][data_type]['rows']}")
                elif msg_type == protocol.MSG_TYPE_END:
                    if data_type not in message_log[request_id]['end_messages']:
                        message_log[request_id]['end_messages'][data_type] = 0
                    message_log[request_id]['end_messages'][data_type] += 1
                    logging.info(f"[GATEWAY ROUTER INCOMING] END: data_type={data_type}, request_id={request_id}, position={position}, end_count_for_this_type={message_log[request_id]['end_messages'][data_type]}")
                
                if request_id in completed_requests:
                    logging.debug(f"[GATEWAY ROUTER] Skipping message for completed request_id={request_id}")
                    return
                
                target_conn = self.requests.get(request_id)
                if target_conn is None:
                    logging.warning(f"[GATEWAY ROUTER] No socket for request_id={request_id}, dropping message")
                    return

                if msg_type == protocol.MSG_TYPE_END and data_type == protocol.DATA_END:
                    if request_id not in data_end_counts:
                        data_end_counts[request_id] = 0
                    data_end_counts[request_id] += 1
                    
                    logging.info(f"[GATEWAY ROUTER] DATA_END for request_id={request_id} ({data_end_counts[request_id]}/{data_end_expected})")
                    
                    summary = f"[GATEWAY ROUTER] Summary for request_id={request_id}:\n"
                    summary += f"  Total payload bytes: {message_log[request_id]['total_payload_bytes']}\n"
                    summary += f"  Data messages by type:\n"
                    for dtype, stats in message_log[request_id]['data_messages'].items():
                        dtype_name = {
                            protocol.Q1_RESULT: 'Q1(7)', 
                            protocol.Q2_RESULT_a: 'Q2_A(8)',
                            protocol.Q2_RESULT_b: 'Q2_B(9)',
                            protocol.Q3_RESULT: 'Q3(10)',
                            protocol.Q4_RESULT: 'Q4(11)'
                        }.get(dtype, f'type{dtype}')
                        summary += f"    {dtype_name}: {stats['count']} messages, {stats['rows']} total rows, {stats['bytes']} bytes\n"
                    summary += f"  END messages by type: {message_log[request_id]['end_messages']}"
                    logging.info(summary)

                    if data_end_counts[request_id] == data_end_expected:
                        send_success = False
                        try:
                            protocol.send_message(target_conn, protocol.MSG_TYPE_END, protocol.DATA_END, b"", 1, request_id=request_id)
                            logging.info(f"[GATEWAY ROUTER] Sent final DATA_END to client for request_id={request_id}")
                            send_success = True
                        except Exception as e:
                            logging.warning(f"[GATEWAY ROUTER] Failed to send final DATA_END to request_id={request_id}: {e}")
                        
                        completed_requests.add(request_id)
                        if request_id in self.requests:
                            del self.requests[request_id]
                        if request_id in data_end_counts:
                            del data_end_counts[request_id]
                        
                        self._cleanup_active_request(request_id)
                        
                        elapsed = time.time() - message_log[request_id]['first_seen']
                        if send_success:
                            logging.info(f"[GATEWAY ROUTER] Request completed: request_id={request_id}, elapsed_seconds={elapsed:.2f}")
                        else:
                            logging.info(f"[GATEWAY ROUTER] Request failed (connection error): request_id={request_id}, elapsed_seconds={elapsed:.2f}")
                else:
                    try:
                        protocol.send_message(target_conn, msg_type, data_type, payload, position, request_id=request_id)
                        if msg_type == protocol.MSG_TYPE_END:
                            logging.info(f"[GATEWAY ROUTER] Forwarded END: data_type={data_type}, request_id={request_id}")
                        elif msg_type == protocol.MSG_TYPE_DATA:
                            logging.info(f"[GATEWAY ROUTER] Forwarded DATA: data_type={data_type}, request_id={request_id}, size={len(payload)}")
                    except Exception as e:
                        logging.warning(f"[GATEWAY ROUTER] Error forwarding message to request_id={request_id}: {e}")
                        if request_id not in completed_requests:
                            logging.info(f"[GATEWAY ROUTER] Cleaning up failed request_id={request_id}")
                            completed_requests.add(request_id)
                            if request_id in self.requests:
                                del self.requests[request_id]
                            if request_id in data_end_counts:
                                del data_end_counts[request_id]
                            self._cleanup_active_request(request_id)
                        else:
                            logging.debug(f"[GATEWAY ROUTER] Request {request_id} already cleaned up, skipping")

            except Exception as e:
                logging.error(f"[GATEWAY ROUTER] Exception: {e}")
                import traceback
                logging.debug(traceback.format_exc())

        logging.info("[GATEWAY ROUTER] Starting result consumer thread")
        results_queue.start_consuming(on_result)

    def run(self):
        """
        Start the gateway server main loop.
        
        This method:
        1. Sets up RabbitMQ connections
        2. Recovers any abandoned requests from previous crashes
        3. Starts the result router thread
        4. Accepts client connections and spawns handler processes
        5. Handles graceful shutdown when signals are received
        
        The server runs until a shutdown signal (SIGTERM/SIGINT) is received or
        an unrecoverable error occurs.
        
        Example:
            >>> server = Server('0.0.0.0', 5000)
            >>> server.run()
            Server listening on 0.0.0.0:5000
            [GATEWAY RECOVERY] No abandoned requests found on startup
            Result router thread started
            Connected by ('172.17.0.5', 45678)
            Client ('172.17.0.5', 45678) new request_id: 1
            Received DATA message from client ('172.17.0.5', 45678): data_type=3, request_id=1
            ...
            Received signal 15, shutting down gateway gracefully...
            Shutdown flag is set. Exiting...
        """
        logging.info(f'Server listening on {self.host}:{self.port}')
        self._setup_rabbitmq()
        
        self._recover_abandoned_requests(self.queues)
        
        router_thread = threading.Thread(target=self._result_router, daemon=True)
        router_thread.start()
        time.sleep(1)
        logging.info("Result router thread started")
        
        max_processes = int(os.environ.get('GATEWAY_MAX_PROCESSES', 4))
        processes = []
        while not self.shutdown_flag:
            processes = [p for p in processes if p.is_alive()]
            if len(processes) < max_processes:
                try:
                    conn, addr = self._accept_new_connection()
                    if not self.shutdown_flag:
                        p = multiprocessing.Process(target=self._handle_client_connection, args=(conn, addr))
                        p.daemon = True
                        p.start()
                        processes.append(p)
                except OSError as e:
                    if self.shutdown_flag:
                        break
                    raise e
            else:
                time.sleep(0.1)
            if self.shutdown_flag:
                logging.info("Shutdown flag is set. Exiting...")
                break
        for p in processes:
            p.join()

    def _accept_new_connection(self):
        """
        Accept a new client connection from the listening socket.
        
        Returns:
            tuple: (connection, address) where connection is a socket object and
                   address is the client's (host, port) tuple.
        
        Example:
            >>> conn, addr = server._accept_new_connection()
            Connected by ('172.17.0.5', 45678)
            >>> addr
            ('172.17.0.5', 45678)
        """
        conn, addr = self._server_socket.accept()
        logging.info(f'Connected by {addr}')
        return conn, addr

    def _handle_client_connection(self, conn, addr):
        """
        Handle communication with a single client connection in a dedicated process.
        
        This method receives data from the client, assigns request IDs, routes messages
        to appropriate RabbitMQ queues, and manages the connection lifecycle. It supports
        multiple sequential requests from the same client connection.
        
        Args:
            conn (socket.socket): Client socket connection.
            addr (tuple): Client address as (host, port).
        
        Example:
            Client sending data:
            >>> server._handle_client_connection(conn, ('172.17.0.5', 45678))
            Client ('172.17.0.5', 45678) new request_id: 1
            Received DATA message from client ('172.17.0.5', 45678): data_type=3, request_id=1
            Sent menu_items data to queue with request_id=1
            Received END message from client ('172.17.0.5', 45678): data_type=3, request_id=1
            Sent END message for menu_items to queue with request_id=1
            ...
            Received DATA_END from client
            Cleaned up mappings for client ('172.17.0.5', 45678)
        """
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        queues = {q: CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=q) for q in queue_names.values()}

        current_request_id = None
        try:
            data_end_received = set()
            current_request_id = self._get_next_request_id()
            self.requests[current_request_id] = conn
            self._save_active_request(current_request_id)
            logging.info(f"Client {addr} new request_id: {current_request_id}")

            while True:
                msg_type, data_type, request_id, position, payload = protocol.receive_message(conn)
                if not msg_type:
                    break

                if msg_type == protocol.MSG_TYPE_DATA and data_type in data_end_received:
                    current_request_id = self._get_next_request_id()
                    self.requests[current_request_id] = conn
                    self._save_active_request(current_request_id)
                    data_end_received.clear()
                    logging.info(f"Client {addr} new request_id: {current_request_id} (new batch detected)")

                if msg_type == protocol.MSG_TYPE_DATA:
                    logging.info(f"Received DATA message from client {addr}: data_type={data_type}, request_id={current_request_id}, position={position}, payload_size={len(payload)}")
                    message = protocol.create_data_message(data_type, payload, current_request_id, position)
                elif msg_type == protocol.MSG_TYPE_END:
                    logging.info(f"Received END message from client {addr}: data_type={data_type}, request_id={current_request_id}, position={position}")
                    message = protocol.create_end_message(data_type, current_request_id, position)
                else:
                    message = protocol.create_notification_message(data_type, payload, current_request_id, position)

                if msg_type == protocol.MSG_TYPE_DATA:
                    if data_type in queue_names:
                        queues[queue_names[data_type]].send(message)
                        logging.debug(f"Sent {data_type_names.get(data_type, data_type)} data to queue with request_id={current_request_id}")
                    else:
                        logging.warning(f"Unknown data type: {data_type}")
                elif msg_type == protocol.MSG_TYPE_END:
                    if data_type == protocol.DATA_END:
                        logging.info("Received DATA_END from client")
                        data_end_received.update(queue_names.keys())
                    elif data_type in queue_names:
                        queues[queue_names[data_type]].send(message)
                        logging.debug(f"Sent END message for {data_type_names.get(data_type, data_type)} to queue with request_id={current_request_id}")

                        data_end_received.add(data_type)
                    else:
                        logging.warning(f"Unknown data type in END message: {data_type}")
                else:
                    logging.warning(f"Unknown message type: {msg_type}")
        except ConnectionError as e:
            logging.warning(f"Connection closed with {addr}: {e}")
            if current_request_id is not None:
                logging.info(f"[GATEWAY DISCONNECT] Cleaning up abandoned request_id={current_request_id} due to connection error")
                self._cleanup_active_request(current_request_id, queues=queues)
        except Exception as e:
            logging.error(f"Error in connection with {addr}: {type(e).__name__}: {e}")
            import traceback
            logging.debug(f"Traceback: {traceback.format_exc()}")
            if current_request_id is not None:
                logging.info(f"[GATEWAY DISCONNECT] Cleaning up abandoned request_id={current_request_id} due to error")
                self._cleanup_active_request(current_request_id, queues=queues)
        finally:
            to_remove = [rid for rid, c in self.requests.items() if c == conn]
            for rid in to_remove:
                del self.requests[rid]
            logging.info(f"Cleaned up mappings for client {addr}")

    def close(self):
        """
        Close all RabbitMQ connections and the server socket.
        
        Called during graceful shutdown to release all network resources.
        
        Example:
            >>> server.close()
            >>> server._server_socket.fileno()
            -1
        """
        for q in self.queues.values():
            q.close()
        self._server_socket.close()

if __name__ == '__main__':
    initialize_log(logging.INFO)
    
    manager = Manager()
    shared_requests = manager.dict()
    
    shared_request_id = Value('i', 1)
    
    server = Server(HOST, PORT, shared_request_id=shared_request_id, shared_requests=shared_requests)
    try:
        server.run()
    except KeyboardInterrupt:
        logging.info('Shutting down server.')
    except Exception as e:
        logging.error(f'Server error: {e}')
    finally:
        logging.info('Gateway shutting down...')
        server.close()
        logging.info('Gateway shutdown complete.')
