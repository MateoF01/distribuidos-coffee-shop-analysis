import random
import socket
import threading
import json
import os
import logging

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "9000"))
STATE_FILE = os.environ.get("STATE_FILE_PATH", "/app/output/worker_states.json")
USING_END_SYNC = os.environ.get("USING_END_SYNC", "0") == "1"

os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class WorkerStateManager:
    """
    Manages replicated worker state for fault-tolerant distributed processing.
    
    Tracks worker replica states (WAITING/PROCESSING/END), processed message positions,
    and coordinates exactly-once downstream notifications. Uses persistent storage with
    append-only position files and atomic state updates.
    
    Architecture:
        - worker_states: {worker_type: {replica_id: {state, request_id}}}
        - Position tracking: Append-only files per (worker_type, request_id)
        - END coordination: Tracks remaining workers per request for final notification
    
    Attributes:
        state_file (str): Main state file path (worker_states, workers_being_used, ends_by_requests).
        using_end_sync (bool): Enable multi-worker END coordination.
        output_base_dir (str): Base directory for position files.
        worker_states (dict): Current state of all worker replicas.
        workers_being_used (dict): Active worker types per request.
        ends_by_requests (dict): Remaining workers to reach END per request.
        lock (threading.Lock): Thread-safe state updates.
    
    Example:
        ```python
        # Initialize manager
        wsm = WorkerStateManager(
            state_file='/app/output/worker_states.json',
            using_end_sync=True
        )
        
        # Register replicas
        wsm.register_worker('cleaner', 'cleaner-1')
        wsm.register_worker('cleaner', 'cleaner-2')
        
        # Update state as messages are processed
        wsm.update_state('cleaner', 'cleaner-1', 'PROCESSING', request_id=123, position=1)
        wsm.update_state('cleaner', 'cleaner-1', 'WAITING', request_id=123, position=1)
        
        # Check if position already processed (duplicate detection)
        is_dup = wsm.is_position_processed('cleaner', 123, 1)  # True
        
        # Check if can send END (all replicas processed continuous stream)
        can_end = wsm.can_send_end('cleaner', 123, position=50)
        
        # Position files: /app/output/cleaner_positions/123.txt
        #   1
        #   2
        #   3
        #   ...
        ```
    """

    def __init__(self, state_file=STATE_FILE, using_end_sync=USING_END_SYNC):
        """
        Initialize WorkerStateManager with persistent storage.
        
        Args:
            state_file (str): Path to main state JSON file.
            using_end_sync (bool): Enable END coordination across workers.
        
        Example:
            ```python
            wsm = WorkerStateManager(
                state_file='/app/output/worker_states.json',
                using_end_sync=True
            )
            ```
        """
        self.state_file = state_file
        self.using_end_sync = using_end_sync
        self.output_base_dir = os.path.dirname(self.state_file)
        self.worker_states = {}
        self.workers_being_used = {}
        self.ends_by_requests = {}
        self.lock = threading.Lock()
        self._load_state()
        logging.info(f"[WSM] Archivo de estado: {self.state_file}")
        logging.info(f"[WSM] Directorio base de salida: {self.output_base_dir}")

        # Crash eligibility check (ported from Worker)
        self.crash_eligible = True
        self.processed_count = 0
        target_replica = os.environ.get("CRASH_REPLICA_ID")
        service_name = os.environ.get("WSM_NAME") # e.g. wsm_transactions

        if target_replica:
            self.crash_eligible = False
            
            # Check WSM_ID first (explicit ID for WSM services)
            wsm_id = os.environ.get("WSM_ID")
            if wsm_id and wsm_id == target_replica:
                self.crash_eligible = True
                logging.info(f"Crash enabled: WSM_ID ({wsm_id}) matches target replica ID {target_replica}")
                return

            try:
                my_hostname = socket.gethostname()
                my_ip = socket.gethostbyname(my_hostname)
                
                if service_name:
                    # Robust IP matching
                    project_name = "coffee-shop-22"
                    candidates = [
                        f"{project_name}-{service_name}-{target_replica}",
                        f"{project_name}_{service_name}_{target_replica}",
                        f"{service_name}-{target_replica}",
                        f"{service_name}_{target_replica}"
                    ]
                    
                    for candidate in candidates:
                        try:
                            target_ip = socket.gethostbyname(candidate)
                            if target_ip == my_ip:
                                self.crash_eligible = True
                                logging.info(f"Crash enabled: My IP ({my_ip}) matches target {candidate} ({target_ip})")
                                break
                        except (socket.error, UnicodeError, ValueError):
                            continue
                        
                else:
                    # Fallback to hostname suffix check
                    if my_hostname.endswith(f"-{target_replica}") or my_hostname.endswith(f"_{target_replica}"):
                         self.crash_eligible = True
                         logging.info(f"Crash enabled: My hostname ({my_hostname}) matches target replica ID {target_replica}")
                    else:
                         logging.info(f"Crash disabled: My hostname ({my_hostname}) does not match target replica ID {target_replica}")

            except Exception as e:
                logging.warning(f"Could not verify replica ID for crash target: {e}")
                self.crash_eligible = False

    def simulate_crash(self, worker_type, request_id):
        """
        Check if this WSM should crash based on probability.
        """
        self.processed_count += 1
        crash_prob = float(os.environ.get("CRASH_PROBABILITY", "0.0"))
        wait_count = int(os.environ.get("CRASH_WAIT_COUNT", "5"))
        
        if self.crash_eligible and crash_prob > 0:
            if self.processed_count <= wait_count:
                logging.info(f"Crash pending: Processed {self.processed_count}/{wait_count} messages before crash eligibility.")
                return

            if random.random() < crash_prob:
                logging.critical(f"Simulating CRASH (prob={crash_prob}) on update from {worker_type} for req={request_id}")
                os._exit(1)

    def _get_positions_dir(self, worker_type):
        """
        Get directory path for position files of a worker type.
        
        Args:
            worker_type (str): Worker type identifier.
        
        Returns:
            str: Directory path.
        
        Example:
            ```python
            dir_path = wsm._get_positions_dir('cleaner')
            # Returns: '/app/output/cleaner_positions'
            ```
        """
        return os.path.join(self.output_base_dir, f"{worker_type}_positions")
    
    def _get_positions_file(self, worker_type, request_id):
        """
        Get position file path for a worker type and request.
        
        Args:
            worker_type (str): Worker type identifier.
            request_id (int): Request identifier.
        
        Returns:
            str: Position file path.
        
        Example:
            ```python
            file_path = wsm._get_positions_file('cleaner', 123)
            # Returns: '/app/output/cleaner_positions/123.txt'
            ```
        """
        positions_dir = self._get_positions_dir(worker_type)
        return os.path.join(positions_dir, f"{request_id}.txt")
    
    def _add_position(self, worker_type, request_id, position):
        """
        Append position to crash-consistent position file.
        
        Uses append-only writes for atomicity. Each position written on new line.
        Creates directory if needed.
        
        Args:
            worker_type (str): Worker type identifier.
            request_id (int): Request identifier.
            position (int): Message position to record.
        
        Example:
            ```python
            wsm._add_position('cleaner', 123, 1)
            wsm._add_position('cleaner', 123, 2)
            
            # File: /app/output/cleaner_positions/123.txt
            # 1
            # 2
            ```
        """
        positions_dir = self._get_positions_dir(worker_type)
        os.makedirs(positions_dir, exist_ok=True)
        
        positions_file = self._get_positions_file(worker_type, request_id)
        try:
            with open(positions_file, "a", encoding="utf-8") as f:
                f.write(f"{position}\n")
        except Exception as e:
            logging.error(f"[WSM] Error agregando posición a {positions_file}: {e}")

    def _load_positions(self, worker_type, request_id):
        """
        Load processed positions from file into set.
        
        Reads append-only position file and returns set of processed positions.
        Returns empty set if file doesn't exist.
        
        Args:
            worker_type (str): Worker type identifier.
            request_id (int): Request identifier.
        
        Returns:
            set[int]: Set of processed positions.
        
        Example:
            ```python
            # File: /app/output/cleaner_positions/123.txt
            # 1
            # 2
            # 5
            
            positions = wsm._load_positions('cleaner', 123)
            # Returns: {1, 2, 5}
            ```
        """
        positions_file = self._get_positions_file(worker_type, request_id)
        if not os.path.exists(positions_file):
            return set()
        
        try:
            with open(positions_file, "r", encoding="utf-8") as f:
                positions = set()
                for line in f:
                    line = line.strip()
                    if line:
                        positions.add(int(line))
                return positions
        except Exception as e:
            logging.error(f"[WSM] Error cargando posiciones de {positions_file}: {e}")
            return set()
    
    def _load_state(self):
        """
        Load persistent state from JSON file.
        
        Restores worker_states, workers_being_used, and ends_by_requests from disk.
        Position data loaded separately from append-only files.
        
        Example:
            ```python
            # File: /app/output/worker_states.json
            # {
            #   "worker_states": {
            #     "cleaner": {
            #       "cleaner-1": {"state": "WAITING", "request_id": null},
            #       "cleaner-2": {"state": "PROCESSING", "request_id": 123}
            #     }
            #   },
            #   "workers_being_used": {"cleaner": true},
            #   "ends_by_requests": {"123": 1}
            # }
            
            wsm._load_state()
            # Restores all state structures from disk
            ```
        """
        print("COMIENZO LOAD STATE...")
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if isinstance(data, dict):
                    self.worker_states = data.get("worker_states", {})
                    self.workers_being_used = data.get("workers_being_used", {})
                    self.ends_by_requests = data.get("ends_by_requests", {})
                    logging.info(f"[WSM] Estado cargado desde {self.state_file}")
                else:
                    logging.error(f"[WSM] Formato de estado inválido en {self.state_file}")
            except Exception as e:
                logging.error(f"[WSM] Error cargando estado: {e}")

    def _save_state(self):
        """
        Persist state to JSON file atomically.
        
        Saves worker_states, workers_being_used, and ends_by_requests.
        Position data persisted separately in append-only files.
        
        Example:
            ```python
            wsm._save_state()
            # Writes to /app/output/worker_states.json
            ```
        """
        try:
            payload = {
                "worker_states": self.worker_states,
                "workers_being_used": self.workers_being_used,
                "ends_by_requests": self.ends_by_requests,
            }
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logging.error(f"[WSM] Error guardando archivo de estado: {e}")

    def _find_first_missing_position(self, pos_set):
        """
        Find first missing position in continuous sequence.
        
        Validates stream completeness by finding first gap in position sequence.
        Used to determine if END signal can be sent safely.
        
        Args:
            pos_set (set[int]): Set of processed positions.
        
        Returns:
            int: First missing position (or next expected if none missing).
        
        Example:
            ```python
            # Complete sequence
            first_missing = wsm._find_first_missing_position({1, 2, 3, 4, 5})
            # Returns: 6 (next expected)
            
            # Gap at position 4
            first_missing = wsm._find_first_missing_position({1, 2, 3, 5, 6})
            # Returns: 4
            
            # Missing from start
            first_missing = wsm._find_first_missing_position({2, 3, 5})
            # Returns: 1
            ```
        """
        if not pos_set:
            return 1
        
        sorted_positions = sorted(pos_set)
        expected = 1
        for p in sorted_positions:
            if p != expected:
                return expected
            expected += 1
        return expected

    def register_worker(self, worker_type, replica_id):
        """
        Register worker replica with initial WAITING state.
        
        Creates worker_type entry if needed. Sets replica to WAITING state.
        Updates workers_being_used for END synchronization.
        
        Args:
            worker_type (str): Worker type identifier.
            replica_id (str): Replica identifier.
        
        Example:
            ```python
            wsm.register_worker('cleaner', 'cleaner-1')
            wsm.register_worker('cleaner', 'cleaner-2')
            
            # State after:
            # worker_states = {
            #   'cleaner': {
            #     'cleaner-1': {'state': 'WAITING', 'request_id': None},
            #     'cleaner-2': {'state': 'WAITING', 'request_id': None}
            #   }
            # }
            ```
        """
        with self.lock:
            self.worker_states.setdefault(worker_type, {})
            if replica_id not in self.worker_states[worker_type]:
                if self.using_end_sync:
                    self.workers_being_used.setdefault(worker_type, True)
                self.worker_states[worker_type][replica_id] = {"state": "WAITING", "request_id": None}
                self._save_state()

    def update_state(self, worker_type, replica_id, state, request_id, position):
        """
        Update replica state and record processed position.
        
        Transitions: WAITING -> PROCESSING -> WAITING (per message).
        Records position when transitioning back to WAITING (message processed).
        Handles END state for multi-worker coordination.
        
        Args:
            worker_type (str): Worker type identifier.
            replica_id (str): Replica identifier.
            state (str): New state ('WAITING', 'PROCESSING', 'END').
            request_id (int): Request identifier.
            position (int): Message position.
        
        Example:
            ```python
            # Process message at position 1
            wsm.update_state('cleaner', 'cleaner-1', 'PROCESSING', 123, 1)
            wsm.update_state('cleaner', 'cleaner-1', 'WAITING', 123, 1)
            # Position 1 now recorded in /app/output/cleaner_positions/123.txt
            
            # Mark END
            wsm.update_state('cleaner', 'cleaner-1', 'END', 123, None)
            ```
        """
        with self.lock:
            self.worker_states.setdefault(worker_type, {})
            previous_state = self.worker_states[worker_type].get(replica_id, {}).get("state")
            if self.using_end_sync:
                self.workers_being_used.setdefault(worker_type, True)
                if request_id not in self.ends_by_requests:
                    self.ends_by_requests[request_id] = sum(
                        1 for being_used in self.workers_being_used.values() if being_used
                    )
                if state == "END":
                    self.ends_by_requests[request_id] -= 1

            if state == "PROCESSING":
                # TEST-CASE: crash si el estado es PROCESSING
                logging.debug("JAJAJAJAJAJAJAJAJAJAJAJAJAJAJAJAJAJA")
                self.simulate_crash(worker_type, request_id)

            if state == "WAITING":
                self.worker_states[worker_type][replica_id] = {"state": state, "request_id": None}
            else:
                self.worker_states[worker_type][replica_id] = {"state": state, "request_id": request_id}

            if (request_id
                and position is not None
                and state == "WAITING"
                and (previous_state == "PROCESSING" or previous_state == "END")
            ):
                self._add_position(worker_type, request_id, position)
                logging.debug(f"[WSM] {worker_type}:{replica_id} registró posición {position} para {request_id}")

            self._save_state()

    def can_send_end(self, worker_type, request_id, position):
        """
        Check if END signal can be sent for continuous stream.
        
        Validates that all positions up to (position-1) have been processed.
        Uses _find_first_missing_position to detect gaps in stream.
        
        Args:
            worker_type (str): Worker type identifier.
            request_id (int): Request identifier.
            position (int): Proposed END position.
        
        Returns:
            bool: True if stream is complete up to position, False otherwise.
        
        Example:
            ```python
            # Positions processed: {1, 2, 3, 4, 5}
            can_send = wsm.can_send_end('cleaner', 123, 6)
            # Returns: True (all positions 1-5 processed)
            
            # Positions processed: {1, 2, 4, 5}
            can_send = wsm.can_send_end('cleaner', 123, 6)
            # Returns: False (position 3 missing)
            
            # Positions processed: {1, 2, 3}
            can_send = wsm.can_send_end('cleaner', 123, 4)
            # Returns: True (stream complete up to position 3)
            ```
        """
        with self.lock:
            replicas = self.worker_states.get(worker_type, {})
            #for rid, info in replicas.items():
            #    if info["state"] == "PROCESSING" and info["request_id"] == request_id:
            #        logging.debug(f"[WSM] {worker_type}:{rid} todavía procesando {request_id}")
            #        return False

            pos_set = self._load_positions(worker_type, request_id)
            first_missing = self._find_first_missing_position(pos_set)

            if position is None:
                logging.warning(f"[WSM] can_send_end() llamado sin posición para {worker_type}:{request_id}")
                return False
            if first_missing == position:
                logging.info(f"[WSM] {worker_type}:{request_id} stream completo hasta {position-1}, END válido en {position}")
                return True
            else:
                logging.info(f"[WSM] {worker_type}:{request_id} faltan posicion {first_missing} (intentando enviar END en {position})")
                return False

    def can_send_last_end(self, worker_type, replica_id, request_id):
        """
        Check if last END can be sent after all workers finish.
        
        Coordinates final END notification when using_end_sync is enabled.
        Decrements ends_by_requests counter and checks if all workers reached END.
        
        Args:
            worker_type (str): Worker type identifier.
            replica_id (str): Replica identifier.
            request_id (int): Request identifier.
        
        Returns:
            bool: True if all workers reached END, False otherwise.
        
        Example:
            ```python
            # 3 workers, first two call can_send_last_end
            can_send = wsm.can_send_last_end('cleaner', 'cleaner-1', 123)
            # Returns: False (ends_by_requests[123] = 2)
            
            can_send = wsm.can_send_last_end('cleaner', 'cleaner-2', 123)
            # Returns: False (ends_by_requests[123] = 1)
            
            # Last worker calls can_send_last_end
            can_send = wsm.can_send_last_end('cleaner', 'cleaner-3', 123)
            # Returns: True (ends_by_requests[123] = 0, all workers done)
            ```
        """
        with self.lock:
            if self.using_end_sync:
                self.worker_states.setdefault(worker_type, {})
                self.workers_being_used.setdefault(worker_type, True)
                if request_id not in self.ends_by_requests:
                    self.ends_by_requests[request_id] = sum(
                        1 for being_used in self.workers_being_used.values() if being_used
                    )
                self.ends_by_requests[request_id] -= 1
                self.worker_states[worker_type][replica_id] = {"state": "END", "request_id": request_id}
                self._save_state()
                if self.ends_by_requests.get(request_id, 0) > 0:
                    logging.debug(f"[WSM] Aún quedan {self.ends_by_requests[request_id]} workers procesando {request_id}")
                    return False
                logging.debug(f"[WSM] Todos los workers terminaron de procesar {request_id}")
            return True

    def is_position_processed(self, worker_type, request_id, position):
        """
        Check if position was already processed (duplicate detection).
        
        Loads positions from file and checks if position exists in set.
        Used by replicas to skip duplicate messages after recovery.
        
        Args:
            worker_type (str): Worker type identifier.
            request_id (int): Request identifier.
            position (int): Position to check.
        
        Returns:
            bool: True if position already processed, False otherwise.
        
        Example:
            ```python
            # Positions file contains: 1, 2, 5
            is_processed = wsm.is_position_processed('cleaner', 123, 2)
            # Returns: True
            
            is_processed = wsm.is_position_processed('cleaner', 123, 3)
            # Returns: False
            
            is_processed = wsm.is_position_processed('cleaner', 123, 5)
            # Returns: True
            ```
        """
        with self.lock:
            pos_set = self._load_positions(worker_type, request_id)
            processed = position in pos_set
            logging.debug(f"[WSM] is_position_processed({worker_type}, {request_id}, {position}) = {processed}")
            return processed

class WSMServer:
    """
    TCP server for Worker State Manager with leader election support.
    
    Handles concurrent client connections from worker replicas. Processes state updates,
    duplicate detection queries, and END coordination. Supports LEADER/BACKUP roles for
    high availability with Bully election algorithm.
    
    Architecture:
        - Multi-threaded TCP server (one thread per client connection)
        - JSON protocol for requests/responses
        - Role-based processing (only LEADER processes state updates)
        - Hot promotion: BACKUP can become LEADER and reload state
    
    Attributes:
        host (str): Server bind address.
        port (int): Server port.
        role (str): Current role ('LEADER' or 'BACKUP').
        manager (WorkerStateManager): State manager instance.
        running (bool): Server running flag.
        server_socket (socket): TCP server socket.
    
    Example:
        ```python
        # Start as LEADER
        server = WSMServer(host='0.0.0.0', port=9000, role='LEADER')
        server.start()  # Blocks, handles client connections
        
        # Client request:
        # {"action": "register", "worker_type": "cleaner", "replica_id": "cleaner-1"}
        # Response: {"response": "OK"}
        
        # Promote BACKUP to LEADER
        backup_server.promote_to_leader()
        # Reloads state from disk, starts processing updates
        ```
    """

    def __init__(self, host=HOST, port=PORT, role="BACKUP"):
        """
        Initialize WSM TCP server.
        
        Args:
            host (str): Bind address (default: '0.0.0.0').
            port (int): Bind port (default: 9000).
            role (str): Initial role ('LEADER' or 'BACKUP').
        
        Example:
            ```python
            server = WSMServer(host='0.0.0.0', port=9000, role='LEADER')
            ```
        """
        self.host = host
        self.port = port
        self.role = role 
        self.manager = WorkerStateManager()
        self.running = False
        self.server_socket = None

    def start(self):
        """
        Start TCP server and accept client connections.
        
        Binds to configured host:port, spawns thread per client connection.
        Blocks until stop() is called.
        
        Example:
            ```python
            server = WSMServer(role='LEADER')
            threading.Thread(target=server.start, daemon=True).start()
            # Server running in background, accepting connections
            ```
        """
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(100)
        logging.info(f"[WSM] Servidor escuchando en {self.host}:{self.port}")

        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()
            except OSError:
                break

    def stop(self):
        """
        Stop TCP server gracefully.
        
        Sets running flag to False and closes server socket.
        
        Example:
            ```python
            server.stop()
            # Server stops accepting new connections
            ```
        """
        logging.info("[WSM] Deteniendo servidor líder...")
        self.running = False
        try:
            self.server_socket.close()
        except:
            pass

    def handle_client(self, conn, addr):
        """
        Handle client connection with JSON request/response protocol.
        
        Processes multiple requests over single connection until client disconnects.
        Parses JSON, dispatches to _handle_action, sends JSON response.
        
        Args:
            conn (socket): Client connection socket.
            addr (tuple): Client address.
        
        Example:
            ```python
            # Client sends:
            # {"action": "update_state", "worker_type": "cleaner", 
            #  "replica_id": "cleaner-1", "state": "PROCESSING", 
            #  "request_id": 123, "position": 1}
            
            # Server responds:
            # {"response": "OK"}
            ```
        """
        try:
            while True:
                data = conn.recv(4096)

                if not data:
                    return

                try:
                    msg = json.loads(data.decode())
                except json.JSONDecodeError:
                    conn.sendall(b'{"response": "ERROR: invalid JSON"}')
                    continue

                action = msg.get("action")
                response = self._handle_action(action, msg)
                conn.sendall(json.dumps({"response": response}).encode())

        except Exception as e:
            logging.error(f"[WSM] Error con cliente {addr}: {e}")

        finally:
            conn.close()

    def _handle_action(self, action, msg):
        """
        Dispatch action to appropriate manager method.
        
        Routes requests based on action field. Returns "NOT_LEADER" if not LEADER
        (except for is_leader check). Processes: register, update_state, can_send_end,
        can_send_last_end, is_position_processed, is_leader.
        
        Args:
            action (str): Action name.
            msg (dict): Full request message.
        
        Returns:
            str or bool: Response payload.
        
        Example:
            ```python
            response = server._handle_action('register', {
                'action': 'register',
                'worker_type': 'cleaner',
                'replica_id': 'cleaner-1'
            })
            # Returns: 'OK'
            
            response = server._handle_action('is_position_processed', {
                'action': 'is_position_processed',
                'worker_type': 'cleaner',
                'request_id': 123,
                'position': 5
            })
            # Returns: True or False
            ```
        """
        worker_type = msg.get("worker_type")
        replica_id = msg.get("replica_id")

        print("Recibo mensaje: ", msg)

        if action != "is_leader" and self.role != "LEADER":
            return "NOT_LEADER"
        if action == "register":
            self.manager.register_worker(worker_type, replica_id)
            return "OK"
        elif action == "update_state":
            self.manager.update_state(worker_type, replica_id, msg.get("state"), msg.get("request_id"), msg.get("position"))
            return "OK"
        elif action == "can_send_end":
            can_send = self.manager.can_send_end(worker_type, msg.get("request_id"), msg.get("position"))
            return "OK" if can_send else "WAIT"
        elif action == "can_send_last_end":
            can_send = self.manager.can_send_last_end(worker_type, replica_id, msg.get("request_id"))
            return "OK" if can_send else "WAIT"
        elif action == "is_position_processed":
            processed = self.manager.is_position_processed(worker_type, msg.get("request_id"), msg.get("position"))
            return processed
        elif action == "is_leader":
            return "YES" if self.role == "LEADER" else "NO"
        else:
            return "ERROR: unknown action"

    def promote_to_leader(self):
        """
        Promote server from BACKUP to LEADER role.
        
        Reloads state from disk without restarting server. Critical for hot failover
        in leader election. Allows seamless transition to active processing.
        
        Example:
            ```python
            # BACKUP detects LEADER failure
            backup_server.promote_to_leader()
            
            # Server now:
            # - role = 'LEADER'
            # - State reloaded from /app/output/worker_states.json
            # - Positions reloaded from /app/output/*_positions/*.txt
            # - Ready to process client requests
            ```
        """
        self.role = "LEADER"
        logging.info("[WSM] Promovido a LEADER → recargando estado desde disco...")
        self.manager._load_state()


if __name__ == "__main__":
    from wsm_node import WSMNode
    WSMNode().start()
