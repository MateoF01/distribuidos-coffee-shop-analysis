import socket, time, threading, os
from common import csv_loaders
from shared import protocol   

class Client:
    def __init__(self, client_id, server_address, data_dir, batch_max_amount, out_dir=None, requests_amount=1):
        self.client_id = client_id
        self.server_address = server_address
        self.data_dir = data_dir
        self.batch_max_amount = batch_max_amount
        self.conn = None
        self.requests_amount = requests_amount
        self.current_request_count = 0  # Track the current request count for this client

        # Ruta de salida dentro de la carpeta client/
        # In Docker container: __file__ = /app/common/client.py, but we need /app/client/results
        base_dir = os.path.dirname(__file__)   # /app/common
        app_dir = os.path.dirname(base_dir)    # /app
        
        # Create results folder inside the client container directory (matching volume mapping)
        client_results_base = os.path.join(app_dir, "client", "results")
        # Debug print to see path construction
        print(f"[DEBUG] Path construction: __file__={__file__}, base_dir={base_dir}, app_dir={app_dir}, client_results_base={client_results_base}")
        
        # Extract client number from client_id for folder naming
        client_number = self.client_id.replace("client", "") if self.client_id.startswith("client") else self.client_id
        self.client_results_dir = os.path.join(client_results_base, f"client_{client_number}")
        
        # Create base client directory if it doesn't exist
        os.makedirs(self.client_results_dir, exist_ok=True)
        print(f"[INFO] Client will save results to directory: {self.client_results_dir}")

        # Current request directory (will be set when processing each request)
        self.current_request_dir = None
        self.current_request_num = 0

        # Diccionario para guardar archivos abiertos por data_type
        self.csv_files = {}
        
        # Track query results received per request
        self.queries_received_per_request = {}  # {request_count: set_of_received_queries}
        self.expected_queries = {protocol.Q1_RESULT, protocol.Q2_RESULT_a, protocol.Q2_RESULT_b, protocol.Q3_RESULT, protocol.Q4_RESULT}
        self.final_end_received = False
        
        print(f"[INFO] Client expecting query results: {self.expected_queries}")
        print(f"[INFO] Client will process {self.requests_amount} requests")

    def _setup_request_directory(self, request_num):
        """Setup the directory for the current request"""
        self.current_request_num = request_num
        self.current_request_count = request_num  # Update request count for protocol
        self.current_request_dir = os.path.join(self.client_results_dir, f"request_{request_num}")
        os.makedirs(self.current_request_dir, exist_ok=True)
        print(f"[INFO] Created request directory: {self.current_request_dir}")
        print(f"[INFO] Client request count: {self.current_request_count}")
        
        # Initialize query tracking for this request
        if self.current_request_count not in self.queries_received_per_request:
            self.queries_received_per_request[self.current_request_count] = set()

    def create_socket(self):
        host, port = self.server_address.split(":")
        retries = 10
        for attempt in range(retries):
            try:
                self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.conn.connect((host, int(port)))
                print(f"[INFO] Connected to gateway {host}:{port}")
                return
            except ConnectionRefusedError:
                print(f"[WARN] Gateway not ready, retrying ({attempt+1}/{retries})...")
                time.sleep(3)
        raise ConnectionError("Failed to connect to gateway after retries")

    def close(self):
        print("[INFO] Closing client connection")
        if self.conn:
            try:
                self.conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass  # Already closed
            try:
                self.conn.close()
            except OSError:
                pass
            self.conn = None
            
        # Cerrar todos los archivos abiertos
        for f in self.csv_files.values():
            try:
                f.close()
            except Exception:
                pass
        self.csv_files.clear()
        
        # Print summary
        print(f"[INFO] Client session summary:")
        print(f"[INFO] - Expected queries: {self.expected_queries}")
        print(f"[INFO] - Final END received: {self.final_end_received}")
        print(f"[INFO] - Client results directory: {self.client_results_dir}")
        print(f"[INFO] - Total requests processed: {self.requests_amount}")
        print(f"[INFO] - Per-request query completion:")
        for request_count in range(1, self.requests_amount + 1):
            if request_count in self.queries_received_per_request:
                received = self.queries_received_per_request[request_count]
                print(f"[INFO]   Request {request_count}: {len(received)}/{len(self.expected_queries)} queries completed - {received}")
            else:
                print(f"[INFO]   Request {request_count}: 0/{len(self.expected_queries)} queries completed - No data received")

    def _listen_for_responses(self):
        """Hilo que escucha respuestas del gateway y escribe CSVs por data_type"""
        try:
            while True:
                try:
                    msg_type, data_type, timestamp, request_count, payload = protocol.receive_client_message(self.conn)
                except Exception as e:
                    print(f"[ERROR] Failed to receive message: {e}")
                    break

                if msg_type is None:
                    print("[INFO] Connection closed by server")
                    break

                print(f"[INFO] Received MSG type: {msg_type}, data_type: {data_type}, request_count: {request_count}, payload_size: {len(payload)} bytes")

                if msg_type == protocol.MSG_TYPE_DATA:
                    payload_str = payload.decode("utf-8")
                    rows = [row for row in payload_str.split("\n") if row.strip()]
                    print(f"[INFO] Received {len(rows)} rows for data_type={data_type}, request_count={request_count}")
                    
                    self._write_rows(data_type, rows, request_count)

                elif msg_type == protocol.MSG_TYPE_END:
                    if data_type == protocol.DATA_END:
                        print(f"[INFO] Final END signal received for request_count={request_count}")
                        self.final_end_received = True
                    elif data_type in self.expected_queries:
                        print(f"[INFO] Query result END received for query type {data_type}, request_count={request_count}")
                        # Initialize request tracking if not exists
                        if request_count not in self.queries_received_per_request:
                            self.queries_received_per_request[request_count] = set()
                        self.queries_received_per_request[request_count].add(data_type)
                        self._write_query_result_file(data_type, request_count)
                    else:
                        print(f"[INFO] END received for data_type={data_type}, request_count={request_count}")
                    
                    # Check if we should close connection
                    if self._should_close_connection():
                        print("[INFO] All expected results received. Closing connection.")
                        break

        except Exception as e:
            print(f"[ERROR] Listening thread crashed: {e}")
        finally:
            self.close()

    def _write_rows(self, data_type, rows, request_count):
        """Escribe filas en un CSV correspondiente al data_type usando file.write"""
        if not rows:
            return
        
        # Use request_count to determine the output directory
        request_dir = os.path.join(self.client_results_dir, f"request_{request_count}")
        os.makedirs(request_dir, exist_ok=True)
        output_dir = request_dir
            
        # Map data_type to query names for output files
        query_names = {
            protocol.Q1_RESULT: 'Q1',
            protocol.Q2_RESULT_a: 'Q2_A', 
            protocol.Q2_RESULT_b: 'Q2_B', 
            protocol.Q3_RESULT: 'Q3',
            protocol.Q4_RESULT: 'Q4'
        }
        
        if data_type in query_names:
            # This is query result data
            query_name = query_names[data_type]
            filename = f'{query_name}_results.csv'
            filepath = os.path.join(output_dir, filename)
            
            # Check if file exists to determine if we need to write headers
            file_exists = os.path.exists(filepath)
            
            with open(filepath, 'a', encoding='utf-8') as f:
                rows_written = 0
                for row in rows:
                    if row.strip():
                        f.write(row.strip())
                        f.write('\n')
                        rows_written += 1
                        
            print(f"[INFO] REQ {request_count}: Wrote {rows_written} rows to {filename}")
            
            # Track that we've received data for this query (initialize if needed)
            if request_count not in self.queries_received_per_request:
                self.queries_received_per_request[request_count] = set()
            if data_type not in self.queries_received_per_request[request_count]:
                print(f"[INFO] REQ {request_count}: Started receiving data for {query_name}")
        else:
            # This is input data being echoed back (shouldn't happen normally)
            print(f"[WARN] Received unexpected data_type: {data_type} for request_count={request_count}")
            filepath = os.path.join(output_dir, f'unknown_{data_type}.csv')
            with open(filepath, 'a', encoding='utf-8') as f:
                for row in rows:
                    if row.strip():
                        f.write(row.strip())
                        f.write('\n')

    def _write_query_result_file(self, data_type, request_count):
        """Create a completion marker file for the query result"""
        query_names = {
            protocol.Q1_RESULT: 'Q1',
            protocol.Q2_RESULT_a: 'Q2_A', 
            protocol.Q2_RESULT_b: 'Q2_B',             
            protocol.Q3_RESULT: 'Q3', 
            protocol.Q4_RESULT: 'Q4'
        }
        
        if data_type in query_names:
            query_name = query_names[data_type]
            # Completion marker file creation disabled
            # marker_file = os.path.join(self.out_dir, f'{query_name}_completed.txt')
            # with open(marker_file, 'w') as f:
            #     f.write(f'Query {query_name} completed successfully\n')
            #     f.write(f'Timestamp: {time.strftime("%Y-%m-%d %H:%M:%S")}\n')
            print(f"[INFO] Query {query_name} completed for request_count={request_count}")

    def _should_close_connection(self):
        """Determine if we should close the connection based on received results"""
        # Check if all requests have completed all their expected queries
        expected_requests = set(range(1, self.requests_amount + 1))  # requests 1, 2, ..., requests_amount
        completed_requests = set()
        
        for request_count in expected_requests:
            if request_count in self.queries_received_per_request:
                received_queries = self.queries_received_per_request[request_count]
                if received_queries >= self.expected_queries:
                    completed_requests.add(request_count)
        
        # Close only if we received final END AND all requests completed all queries
        all_queries_completed = (completed_requests == expected_requests)
        
        if self.final_end_received and all_queries_completed:
            print(f"[INFO] All {self.requests_amount} requests completed all queries and final END received")
            return True
        elif self.final_end_received and not all_queries_completed:
            print(f"[WARN] Final END received but not all requests completed. Completed: {len(completed_requests)}/{len(expected_requests)}")
        elif all_queries_completed and not self.final_end_received:
            print(f"[INFO] All queries completed but still waiting for final END signal")
        
        # Log progress if not completed
        if not all_queries_completed:
            print(f"[INFO] Progress: {len(completed_requests)}/{len(expected_requests)} requests completed")
            for request_count in expected_requests:
                if request_count in self.queries_received_per_request:
                    received = self.queries_received_per_request[request_count]
                    missing = self.expected_queries - received
                    if missing:
                        print(f"[INFO] Request {request_count} still waiting for: {missing}")
                else:
                    print(f"[INFO] Request {request_count} not yet started")
            
        return False


    def start_client_loop(self):
        try:
            self.create_socket()

            # Lanzar hilo que escucha respuestas
            listener_thread = threading.Thread(target=self._listen_for_responses, daemon=False)
            listener_thread.start()

            for request_num in range(self.requests_amount):
                # Set up directory for this request
                self._setup_request_directory(request_num + 1)
                print(f"[INFO] Iteration {request_num+1}/{self.requests_amount}: Sending files from data folder")

                # Agrupar archivos por tipo
                files_by_type = {}
                for data_type, filepath in csv_loaders.iter_csv_files(self.data_dir):
                    files_by_type.setdefault(data_type, []).append(filepath)

                # Enviar archivos
                for data_type, filepaths in files_by_type.items():
                    print(f"[INFO] REQ {request_num+1}/{self.requests_amount}: Processing {len(filepaths)} files for data_type={data_type}")
                    for filepath in filepaths:
                        print(f"[INFO] REQ {request_num+1}/{self.requests_amount}: Sending file {filepath} (type={data_type})")
                        for batch in csv_loaders.load_csv_batch(filepath, self.batch_max_amount):
                            payload = "\n".join(batch).encode()
                            protocol.send_client_message(self.conn, protocol.MSG_TYPE_DATA, data_type, payload, self.current_request_count)
                            #print(f"[INFO] Sent batch of {len(batch)} rows from {filepath.name}")
                    protocol.send_client_message(self.conn, protocol.MSG_TYPE_END, data_type, b"", self.current_request_count)
                    print(f"[INFO] REQ {request_num+1}/{self.requests_amount}: Sent END for data_type={data_type} with request_count={self.current_request_count}")

            # END final
            protocol.send_client_message(self.conn, protocol.MSG_TYPE_END, protocol.DATA_END, b"", self.current_request_count)
            print(f"[INFO] Sent END FINAL with request_count={self.current_request_count} - waiting for query results...")

            # Wait for listener thread to complete
            print(f"[INFO] Waiting for results from queries: {self.expected_queries}")
            listener_thread.join()
            
            # Check if all requests completed successfully
            expected_requests = set(range(1, self.requests_amount + 1))
            completed_requests = set()
            
            for request_count in expected_requests:
                if request_count in self.queries_received_per_request:
                    received = self.queries_received_per_request[request_count]
                    if received >= self.expected_queries:
                        completed_requests.add(request_count)
            
            if completed_requests == expected_requests:
                print("[INFO] All query results received successfully for all requests")
            else:
                print(f"[WARN] Client finished but not all requests completed. Completed: {len(completed_requests)}/{len(expected_requests)} requests")
                for request_count in expected_requests:
                    if request_count not in completed_requests:
                        if request_count in self.queries_received_per_request:
                            received = self.queries_received_per_request[request_count]
                            missing = self.expected_queries - received
                            print(f"[WARN] Request {request_count} missing queries: {missing}")
                        else:
                            print(f"[WARN] Request {request_count} received no results")
                
        except Exception as e:
            print(f"[ERROR] Client loop failed: {e}")
            self.close()
            raise
