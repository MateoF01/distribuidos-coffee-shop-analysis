import socket, time, threading, os, random
from common import csv_loaders
from shared import protocol


class ConnectionRetriesExhausted(Exception):
    """Raised when connection retry attempts have been exhausted (backoff exceeded max)"""
    pass


class Client:
    """
    A distributed system client that sends CSV data to a gateway server and receives query results.
    
    The client handles multiple requests, batching of data, and manages the lifecycle of connections
    and result file writing. It processes CSV files containing transactions, menu items, stores, users,
    and transaction items, sending them to a remote gateway for processing.
    
    Public API Lifecycle:
        1. Create a Client instance with __init__()
        2. Call start_client_loop() to begin processing
        3. If shutdown event or any error occurs, call close() for cleanup
    
    The connection management is handled internally by start_client_loop(), which automatically
    establishes the socket connection, sends data, receives results, and closes resources.
    
    Attributes:
        client_id (str): Unique identifier for this client instance.
        server_address (str): Address of the gateway server in format 'host:port'.
        data_dir (str): Directory containing CSV data files to send.
        batch_max_amount (int): Maximum number of rows per batch when sending data.
        requests_amount (int): Number of times to send the complete dataset.
        conn (socket.socket): Active socket connection to the gateway (internal).
        client_results_dir (str): Base directory for storing results for this client.
        current_request_dir (str): Directory for the current request being processed.
        queries_received (set): Set of query result types received so far.
        expected_queries (set): Set of all expected query result types.
        received_data_ends (int): Count of final DATA_END signals received.
        expected_data_ends (int): Expected number of DATA_END signals (one per request).
    
    Example:
        Basic usage (typical lifecycle):
        >>> client = Client(
        ...     client_id="client1",
        ...     server_address="localhost:5000",
        ...     data_dir="/app/data",
        ...     batch_max_amount=100,
        ...     requests_amount=2
        ... )
        >>> client.start_client_loop()
        [INFO] Connected to gateway localhost:5000
        [INFO] Iteration 1/2: Sending files from data folder
        [INFO] REQ 1/2: Processing 1 files for data_type=3
        ...
        [INFO] All query results received successfully
        
        Error handling:
        >>> client = Client(
        ...     client_id="client2",
        ...     server_address="gateway:5000",
        ...     data_dir="/app/.data",
        ...     batch_max_amount=500,
        ...     requests_amount=1
        ... )
        >>> try:
        ...     client.start_client_loop()
        ... except Exception as e:
        ...     print(f"Error: {e}")
        ...     client.close()
    """
    def __init__(self, client_id, server_address, data_dir, batch_max_amount, out_dir=None, requests_amount=1,
                 initial_backoff=1.0, max_backoff=60.0, backoff_multiplier=2.0):
        """
        Initialize a new Client instance.
        
        Args:
            client_id (str): Unique identifier for this client (e.g., 'client1', 'client_docker_1').
            server_address (str): Gateway server address in format 'host:port' (e.g., 'localhost:5000').
            data_dir (str): Path to directory containing CSV data files to process.
            batch_max_amount (int): Maximum number of rows to send in each batch.
            out_dir (str, optional): Legacy parameter, not used. Results are stored in client/results/.
            requests_amount (int, optional): Number of complete dataset iterations to send. Defaults to 1.
            initial_backoff (float, optional): Initial backoff time in seconds. Defaults to 1.0.
            max_backoff (float, optional): Maximum backoff cap in seconds. Defaults to 60.0.
            backoff_multiplier (float, optional): Exponential backoff multiplier. Defaults to 2.0.
        
        Example:
            >>> client = Client(
            ...     client_id="client1",
            ...     server_address="gateway:5000",
            ...     data_dir="/app/.data",
            ...     batch_max_amount=100,
            ...     requests_amount=3
            ... )
            [DEBUG] Path construction: __file__=/app/common/client.py, ...
            [INFO] Client will save results to directory: /app/client/results/client_1
            [INFO] Client expecting query results: {10, 11, 12, 13, 14}
            [INFO] Expected final DATA_END signals: 3
        """
        self.client_id = client_id
        self.server_address = server_address
        self.data_dir = data_dir
        self.batch_max_amount = batch_max_amount
        self.conn = None
        self.requests_amount = requests_amount

        base_dir = os.path.dirname(__file__)
        app_dir = os.path.dirname(base_dir)
        
        client_results_base = os.path.join(app_dir, "client", "results")
        print(f"[DEBUG] Path construction: __file__={__file__}, base_dir={base_dir}, app_dir={app_dir}, client_results_base={client_results_base}")
        
        client_number = self.client_id.replace("client", "") if self.client_id.startswith("client") else self.client_id
        self.client_results_dir = os.path.join(client_results_base, f"client_{client_number}")
        
        os.makedirs(self.client_results_dir, exist_ok=True)
        print(f"[INFO] Client will save results to directory: {self.client_results_dir}")

        self.current_request_dir = None
        self.current_request_num = 0

        self.csv_files = {}
        
        self.queries_received = set()
        self.expected_queries = {protocol.Q1_RESULT, protocol.Q2_RESULT_a, protocol.Q2_RESULT_b, protocol.Q3_RESULT, protocol.Q4_RESULT}
        
        self.received_data_ends = 0
        self.expected_data_ends = self.requests_amount
        
        # Exponential backoff parameters for reconnection
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_multiplier = backoff_multiplier
        self.current_backoff = self.initial_backoff
        
        print(f"[INFO] Client expecting query results: {self.expected_queries}")
        print(f"[INFO] Expected final DATA_END signals: {self.expected_data_ends}")
        print(f"[INFO] Reconnection config: initial_backoff={self.initial_backoff}s, max_backoff={self.max_backoff}s, multiplier={self.backoff_multiplier}x")

    def _setup_request_directory(self, request_num):
        """
        Create and configure the directory structure for a specific request.
        
        This method creates a subdirectory for each request iteration and resets
        the query tracking for the new request.
        
        Args:
            request_num (int): The request number (1-indexed) for directory naming.
        
        Example:
            >>> client._setup_request_directory(1)
            [INFO] Created request directory: /app/client/results/client_1/request_1
            >>> client.current_request_num
            1
            >>> client.queries_received
            set()
        """
        self.current_request_num = request_num
        self.current_request_dir = os.path.join(self.client_results_dir, f"request_{request_num}")
        os.makedirs(self.current_request_dir, exist_ok=True)
        print(f"[INFO] Created request directory: {self.current_request_dir}")
        
        self.queries_received = set()

    def create_socket(self):
        """
        Internal method to establish a TCP connection to the gateway server with exponential backoff.
        
        This method is called automatically by start_client_loop() and should not be invoked
        directly by users of the Client class.
        
        Uses exponential backoff with jitter to retry connections. The backoff time doubles
        after each failed attempt up to a maximum, with random jitter to avoid thundering herd.
        After successful connection, the backoff timer is reset.
        
        Raises:
            ConnectionRetriesExhausted: If the next backoff would exceed max_backoff, indicating
                                       all reasonable retry attempts have been exhausted.
        
        Note:
            This is an internal method. Users should call start_client_loop() instead,
            which manages the connection lifecycle automatically.
        """
        host, port = self.server_address.split(":")
        attempt = 0
        
        while True:
            try:
                if self.conn:
                    try:
                        self.conn.close()
                    except Exception:
                        pass
                    
                self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.conn.connect((host, int(port)))
                print(f"[INFO] Connected to gateway {host}:{port}")
                self.reset_backoff()
                return
                
            except (ConnectionRefusedError, OSError) as e:
                attempt += 1
                
                # Add jitter: random value between 0.5 and 1.5 times the current backoff
                jitter = random.uniform(0.5, 1.5)
                wait_time = min(self.current_backoff * jitter, self.max_backoff)
                
                print(f"[WARN] Connection failed (attempt {attempt}): {e}")
                print(f"[INFO] Retrying in {wait_time:.2f} seconds (backoff: {self.current_backoff:.2f}s)...")
                
                time.sleep(wait_time)
                
                # Calculate next backoff
                next_backoff = self.current_backoff * self.backoff_multiplier
                
                # If next backoff would exceed max, we've exhausted reasonable retry attempts
                if next_backoff > self.max_backoff:
                    raise ConnectionRetriesExhausted(f"Failed to connect to gateway after {attempt} attempts (max backoff exceeded)")
                
                # Increase backoff for next attempt
                self.current_backoff = next_backoff
    
    def reset_backoff(self):
        """
        Reset the exponential backoff timer to its initial value.
        
        This method is called automatically after a successful connection to reset
        the backoff parameters for future reconnection attempts.
        """
        self.current_backoff = self.initial_backoff
        print(f"[DEBUG] Backoff timer reset to {self.initial_backoff}s")

    def close(self):
        """
        Close the client connection and cleanup resources gracefully.
        
        This method is called automatically by start_client_loop() upon completion or error.
        It can also be called directly for graceful shutdown scenarios such as signal handling
        (SIGINT, SIGTERM) or when interrupting the client execution.
        
        The method safely closes the socket connection, closes any open CSV files,
        and prints a summary of the client session including queries received and
        DATA_END signals.
        
        Example:
            Graceful shutdown with signal handling (as in main.py):
            >>> def handle_shutdown(sig, frame):
            ...     print(f"[INFO] Shutting down client...")
            ...     client.close()
            ...     sys.exit(0)
            >>> signal.signal(signal.SIGINT, handle_shutdown)
            >>> signal.signal(signal.SIGTERM, handle_shutdown)
            
            Manual cleanup on error:
            >>> try:
            ...     client.start_client_loop()
            ... except Exception as e:
            ...     print(f"Error occurred: {e}")
            ...     client.close()
            [INFO] Closing client connection
            [INFO] Client session summary:
            [INFO] - Expected queries: {10, 11, 12, 13, 14}
            [INFO] - Received queries: {10, 11, 12, 13, 14}
            [INFO] - DATA_END signals received: 1/1
            [INFO] - Client results directory: /app/client/results/client_1
            [INFO] - Current request directory: /app/client/results/client_1/request_1
            [INFO] - Total requests processed: 1
        """
        print("[INFO] Closing client connection")
        if self.conn:
            try:
                self.conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.conn.close()
            except OSError:
                pass
            self.conn = None
            
        for f in self.csv_files.values():
            try:
                f.close()
            except Exception:
                pass
        self.csv_files.clear()
        
        print(f"[INFO] Client session summary:")
        print(f"[INFO] - Expected queries: {self.expected_queries}")
        print(f"[INFO] - Received queries: {self.queries_received}")
        print(f"[INFO] - DATA_END signals received: {self.received_data_ends}/{self.expected_data_ends}")
        print(f"[INFO] - Client results directory: {self.client_results_dir}")
        print(f"[INFO] - Current request directory: {self.current_request_dir}")
        print(f"[INFO] - Total requests processed: {self.requests_amount}")

    def _listen_for_responses(self):
        """
        Background thread that listens for responses from the gateway and writes results to CSV files.
        
        This method runs in a separate thread and continuously receives messages from the gateway,
        processing data messages and end signals. It maps gateway request IDs to local request
        directories and writes query results to appropriate CSV files.
        
        The method handles:
        - MSG_TYPE_DATA: Data batches to be written to CSV files
        - MSG_TYPE_END: End signals for queries or final DATA_END signal
        
        Example:
            >>> listener_thread = threading.Thread(target=client._listen_for_responses, daemon=False)
            >>> listener_thread.start()
            [INFO] Received MSG type: 2, data_type: 10, request_id: 1, payload_size: 1024 bytes
            [INFO] Received 15 rows for data_type=10
            [INFO] REQ 1: Wrote 15 rows to Q1_results.csv
            [INFO] Query result END received for query type 10
            [INFO] Final DATA_END signal received for request_id=1 (1/1)
            [INFO] All expected results received. Closing connection.
        """
        request_id_to_local_request = {}
        request_id_queries = {}
        next_local_request = 1
        
        try:
            while True:
                try:
                    msg_type, data_type, request_id, position, payload = protocol.receive_message(self.conn)
                except ConnectionError as e:
                    if self.received_data_ends >= self.expected_data_ends:
                        print(f"[INFO] Socket closed, but all {self.expected_data_ends} DATA_END signals received. Normal completion.")
                        break
                    else:
                        print(f"[ERROR] Connection lost in listener thread: {e} (received {self.received_data_ends}/{self.expected_data_ends} DATA_ENDs)")
                        print(f"[INFO] Listener thread exiting. Main loop will handle reconnection.")
                        break
                except Exception as e:
                    print(f"[ERROR] Failed to receive message: {e}")
                    break

                if msg_type is None:
                    print("[INFO] Connection closed by server")
                    break

                print(f"[INFO] Received MSG type: {msg_type}, data_type: {data_type}, request_id: {request_id}, payload_size: {len(payload)} bytes")

                if request_id not in request_id_to_local_request:
                    request_id_to_local_request[request_id] = next_local_request
                    request_id_queries[request_id] = set()
                    print(f"[INFO] Mapping gateway request_id={request_id} to local request={next_local_request}")
                    self._setup_request_directory(next_local_request)
                    next_local_request += 1
                
                local_request_num = request_id_to_local_request[request_id]
                self.current_request_num = local_request_num
                
                if msg_type == protocol.MSG_TYPE_DATA:
                    payload_str = payload.decode("utf-8")
                    rows = [row for row in payload_str.split("\n") if row.strip()]
                    print(f"[INFO] Received {len(rows)} rows for data_type={data_type}")
                    
                    self._write_rows(data_type, rows, request_id=request_id, local_request_num=local_request_num)

                elif msg_type == protocol.MSG_TYPE_END:
                    if data_type == protocol.DATA_END:
                        self.received_data_ends += 1
                        print(f"[INFO] Final DATA_END signal received for request_id={request_id} ({self.received_data_ends}/{self.expected_data_ends})")
                        
                        if self._should_close_connection():
                            print("[INFO] All expected results received. Closing connection.")
                            break
                    elif data_type in self.expected_queries:
                        print(f"[INFO] Query result END received for query type {data_type}")
                        self.queries_received.add(data_type)
                        self._write_query_result_file(data_type)
                    else:
                        print(f"[INFO] END received for data_type={data_type}")

        except Exception as e:
            print(f"[ERROR] Listening thread crashed: {e}")
        finally:
            if self.received_data_ends >= self.expected_data_ends:
                self.close()

    def _write_rows(self, data_type, rows, request_id=None, local_request_num=None):
        """
        Write received data rows to the appropriate CSV file based on data type.
        
        Args:
            data_type (int): Protocol data type constant identifying the query result type.
            rows (list): List of data rows as strings to write.
            request_id (int, optional): Gateway request identifier for logging.
            local_request_num (int, optional): Local request number for directory determination.
        
        Example:
            >>> rows = ['store_name|total', 'Store A|1500.50', 'Store B|2300.75']
            >>> client._write_rows(protocol.Q1_RESULT, rows, request_id=1, local_request_num=1)
            [DEBUG] Writing to folder for request_id=1 (local request 1): /app/client/results/client_1/request_1
            [INFO] REQ 1: Wrote 2 rows to Q1_results.csv
            [INFO] REQ 1: Started receiving data for Q1
            
            >>> empty_rows = []
            >>> client._write_rows(protocol.Q2_RESULT_a, empty_rows)
        """
        if not rows:
            return
        
        if local_request_num is not None:
            output_dir = os.path.join(self.client_results_dir, f"request_{local_request_num}")
            os.makedirs(output_dir, exist_ok=True)
            print(f"[DEBUG] Writing to folder for request_id={request_id} (local request {local_request_num}): {output_dir}")
        elif self.current_request_dir is not None:
            output_dir = self.current_request_dir
        else:
            print("[WARN] No current request directory set, using client base directory")
            output_dir = self.client_results_dir
            
        query_names = {
            protocol.Q1_RESULT: 'Q1',
            protocol.Q2_RESULT_a: 'Q2_A', 
            protocol.Q2_RESULT_b: 'Q2_B', 
            protocol.Q3_RESULT: 'Q3',
            protocol.Q4_RESULT: 'Q4'
        }
        
        if data_type in query_names:
            query_name = query_names[data_type]
            filename = f'{query_name}_results.csv'
            filepath = os.path.join(output_dir, filename)
            
            file_exists = os.path.exists(filepath)
            
            with open(filepath, 'a', encoding='utf-8') as f:
                rows_written = 0
                for row in rows:
                    if row.strip():
                        f.write(row.strip())
                        f.write('\n')
                        rows_written += 1
                        
            print(f"[INFO] REQ {self.current_request_num}: Wrote {rows_written} rows to {filename}")
            
            if data_type not in self.queries_received:
                print(f"[INFO] REQ {self.current_request_num}: Started receiving data for {query_name}")
        else:
            print(f"[WARN] Received unexpected data_type: {data_type}")
            filepath = os.path.join(output_dir, f'unknown_{data_type}.csv')
            with open(filepath, 'a', encoding='utf-8') as f:
                for row in rows:
                    if row.strip():
                        f.write(row.strip())
                        f.write('\n')

    def _write_query_result_file(self, data_type):
        """
        Handle completion of a query result by logging the event.
        
        Args:
            data_type (int): Protocol data type constant identifying the completed query.
        
        Example:
            >>> client._write_query_result_file(protocol.Q1_RESULT)
            [INFO] Query Q1 completed
            
            >>> client._write_query_result_file(protocol.Q3_RESULT)
            [INFO] Query Q3 completed
        """
        query_names = {
            protocol.Q1_RESULT: 'Q1',
            protocol.Q2_RESULT_a: 'Q2_A', 
            protocol.Q2_RESULT_b: 'Q2_B',             
            protocol.Q3_RESULT: 'Q3', 
            protocol.Q4_RESULT: 'Q4'
        }
        
        if data_type in query_names:
            query_name = query_names[data_type]
            print(f"[INFO] Query {query_name} completed")

    def _should_close_connection(self):
        """
        Determine whether the client should close the connection.
        
        The connection should close only when all expected final DATA_END signals
        have been received (one per request iteration).
        
        Returns:
            bool: True if all DATA_END signals received, False otherwise.
        
        Example:
            >>> client.received_data_ends = 2
            >>> client.expected_data_ends = 3
            >>> client._should_close_connection()
            False
            
            >>> client.received_data_ends = 3
            >>> client._should_close_connection()
            [INFO] Received all 3 final DATA_END signals. Ready to close.
            True
        """
        if self.received_data_ends >= self.expected_data_ends:
            print(f"[INFO] Received all {self.expected_data_ends} final DATA_END signals. Ready to close.")
            return True
            
        return False

    def start_client_loop(self):
        """
        Execute the main client loop: connect, send data, and receive results.
        
        This method performs the following steps:
        1. Create socket connection to gateway (with exponential backoff retry)
        2. Start listener thread for receiving responses
        3. For each request iteration:
           - Set up request directory
           - Group CSV files by data type
           - Send files in batches
           - Send END signals for each data type
        4. Send final DATA_END signal
        5. Wait for all query results
        
        If a connection error occurs, the client will retry with exponential backoff
        and restart the entire loop from the beginning.
        
        The method blocks until all results are received or a non-connection error occurs.
        
        Raises:
            Exception: If a non-connection error occurs during execution.
        
        Example:
            >>> client = Client('client1', 'gateway:5000', '/data', 100, requests_amount=2)
            >>> client.start_client_loop()
            [INFO] Connected to gateway gateway:5000
            [INFO] Iteration 1/2: Sending files from data folder
            [INFO] REQ 1/2: Processing 1 files for data_type=3
            [INFO] REQ 1/2: Sending file /data/menu_items/menu_items.csv (type=3)
            [INFO] REQ 1/2: Sent END for data_type=3
            ...
            [INFO] Sent END FINAL - waiting for query results...
            [INFO] Waiting for results from queries: {10, 11, 12, 13, 14}
            [INFO] All query results received successfully
        """
        while True:
            try:
                self.create_socket()

                listener_thread = threading.Thread(target=self._listen_for_responses, daemon=False)
                listener_thread.start()

                for request_num in range(self.requests_amount):
                    self._setup_request_directory(request_num + 1)
                    print(f"[INFO] Iteration {request_num+1}/{self.requests_amount}: Sending files from data folder")

                    files_by_type = {}
                    for data_type, filepath in csv_loaders.iter_csv_files(self.data_dir):
                        files_by_type.setdefault(data_type, []).append(filepath)

                    for data_type, filepaths in files_by_type.items():
                        print(f"[INFO] REQ {request_num+1}/{self.requests_amount}: Processing {len(filepaths)} files for data_type={data_type}")
                        position_counter = 1
                        for filepath in filepaths:
                            print(f"[INFO] REQ {request_num+1}/{self.requests_amount}: Sending file {filepath} (type={data_type})")
                            for batch in csv_loaders.load_csv_batch(filepath, self.batch_max_amount):
                                payload = "\n".join(batch).encode()
                                protocol.send_message(self.conn, protocol.MSG_TYPE_DATA, data_type, payload, position_counter)
                                position_counter += 1
                        protocol.send_message(self.conn, protocol.MSG_TYPE_END, data_type, b"", position_counter)
                        print(f"[INFO] REQ {request_num+1}/{self.requests_amount}: Sent END for data_type={data_type}")

                protocol.send_message(self.conn, protocol.MSG_TYPE_END, protocol.DATA_END, b"", 1)
                print("[INFO] Sent END FINAL - waiting for query results...")

                print(f"[INFO] Waiting for results from queries: {self.expected_queries}")
                listener_thread.join()
                
                if self.received_data_ends >= self.expected_data_ends:
                    print("[INFO] All query results received successfully")
                    break
                else:
                    print(f"[WARN] Listener thread exited prematurely (received {self.received_data_ends}/{self.expected_data_ends} DATA_ENDs)")
                    raise ConnectionError("Listener thread exited due to connection loss")
            
            except ConnectionRetriesExhausted as e:
                print(f"[ERROR] Connection retries exhausted: {e}")
                print(f"[INFO] Unable to establish connection after multiple attempts. Shutting down.")
                self.close()
                raise
                    
            except (ConnectionError, ConnectionRefusedError, BrokenPipeError, OSError) as e:
                print(f"[ERROR] Connection error in client loop: {e}")
                print(f"[INFO] Attempting to reconnect and restart the client loop...")
                
                # Close current connection
                if self.conn:
                    try:
                        self.conn.close()
                    except Exception:
                        pass
                    self.conn = None
                
                jitter = random.uniform(0.5, 1.5)
                wait_time = min(self.current_backoff * jitter, self.max_backoff)
                
                print(f"[INFO] Waiting {wait_time:.2f}s before retry (backoff: {self.current_backoff:.2f}s)...")
                time.sleep(wait_time)
                
                self.current_backoff = min(self.current_backoff * self.backoff_multiplier, self.max_backoff)
                
                self.queries_received = set()
                continue
                
            except Exception as e:
                print(f"[ERROR] Non-connection error in client loop: {e}")
                self.close()
                raise
