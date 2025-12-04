import socket
import json
import logging
import time
import threading
from typing import List, Tuple, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class WSMClient:
    """
    High-availability client for Worker State Manager with automatic leader discovery.
    
    Connects to WSM cluster with multiple nodes. Automatically discovers current LEADER,
    handles failover on connection loss or leader changes. Provides transparent retry
    logic for all operations.
    
    Architecture:
        - Multi-node support: List of (host, port) for all WSM nodes
        - Leader discovery: Queries all nodes with is_leader action
        - Automatic failover: Reconnects on connection loss or NOT_LEADER response
        - Transparent retry: All operations retry until successful
    
    Attributes:
        worker_type (str): Worker type identifier.
        replica_id (str): Replica identifier.
        probe_interval (float): Seconds between leader discovery attempts.
        nodes (list): List of (host, port) tuples for WSM nodes.
        sock (socket): Current connection to LEADER.
        current_node (tuple): Current LEADER (host, port).
    
    Example:
        ```python
        # Single node (for testing)
        client = WSMClient(
            worker_type='cleaner',
            replica_id='cleaner-1',
            host='wsm',
            port=9000
        )
        
        # Multi-node cluster (production)
        client = WSMClient(
            worker_type='cleaner',
            replica_id='cleaner-1',
            nodes=[
                ('wsm', 9000),
                ('wsm_2', 9000),
                ('wsm_3', 9000)
            ],
            probe_interval=1.0
        )
        
        # Operations automatically handle failover
        client.update_state('PROCESSING', request_id=123, position=1)
        # If LEADER fails, client discovers new LEADER and retries
        
        is_dup = client.is_position_processed(123, 1)
        # Returns True (position already processed)
        ```
    """

    def __init__(
        self,
        worker_type: str,
        replica_id: str,
        host: str = "wsm",
        port: int = 9000,
        nodes: Optional[List[Tuple[str, int]]] = None,
        probe_interval: float = 1.0,
    ):
        """
        Initialize WSM client with automatic leader discovery.
        
        Args:
            worker_type: Worker type identifier.
            replica_id: Replica identifier.
            host: Default host (used if nodes=None).
            port: Default port (used if nodes=None).
            nodes: List of (host, port) for all WSM nodes.
            probe_interval: Seconds between leader discovery attempts.
        
        Example:
            ```python
            # Single node
            client = WSMClient('cleaner', 'cleaner-1', 'wsm', 9000)
            
            # Multi-node cluster
            client = WSMClient(
                'cleaner',
                'cleaner-1',
                nodes=[('wsm', 9000), ('wsm_2', 9000), ('wsm_3', 9000)]
            )
            ```
        """
        self.worker_type = worker_type
        self.replica_id = replica_id
        self.probe_interval = probe_interval

        # Lista de nodos disponibles (líder + backups)
        if nodes is None:
            self.nodes: List[Tuple[str, int]] = [(host, port)]
        else:
            self.nodes = nodes

        self.sock: Optional[socket.socket] = None
        self.current_node: Optional[Tuple[str, int]] = None

        # Conectar y registrar la réplica
        self._connect_to_leader()
        self._register()

        # Start heartbeat loop
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def _find_leader_once(self) -> Optional[Tuple[str, int]]:
        """
        Probe all nodes once to find current LEADER.
        
        Sends is_leader query to each node with short timeout.
        Returns first node that responds with "YES".
        
        Returns:
            tuple or None: (host, port) of LEADER, or None if not found.
        
        Example:
            ```python
            # Nodes: wsm (BACKUP), wsm_2 (LEADER), wsm_3 (BACKUP)
            leader = client._find_leader_once()
            # Queries wsm → responds "NO"
            # Queries wsm_2 → responds "YES"
            # Returns: ('wsm_2', 9000)
            ```
        """
        probe_msg = json.dumps({"action": "is_leader"}).encode("utf-8")

        for host, port in self.nodes:
            try:
                with socket.create_connection((host, port), timeout=0.5) as s:
                    s.sendall(probe_msg)
                    data = s.recv(1024)
                    if not data:
                        continue

                    resp = json.loads(data.decode("utf-8")).get("response")
                    if resp == "YES":
                        logging.info(f"[WSMClient] Líder detectado en {host}:{port}")
                        return (host, port)
                    # Si responde "NO" o cualquier otra cosa, sigo probando
            except Exception as e:
                logging.debug(f"[WSMClient] Nodo {host}:{port} no responde como líder ({e})")

        return None

    def _connect_to_leader(self):
        """
        Block until LEADER found and connected.
        
        Repeatedly calls _find_leader_once with probe_interval delay.
        Establishes TCP connection to discovered LEADER.
        
        Example:
            ```python
            # All nodes down → loops until one becomes LEADER
            client._connect_to_leader()
            
            # LEADER found → establishes connection
            # Sets client.sock and client.current_node
            ```
        """
        while True:
            leader = self._find_leader_once()
            if leader is None:
                logging.warning("[WSMClient] No se encontró líder en ningún nodo, reintentando...")
                time.sleep(self.probe_interval)
                continue

            host, port = leader
            try:
                self.sock = socket.create_connection((host, port), timeout=5)
                self.current_node = leader
                logging.info(f"[WSMClient] Conectado al líder {host}:{port}")
                return
            except Exception as e:
                logging.warning(f"[WSMClient] No se pudo conectar al líder {host}:{port} ({e}), reintentando...")
                self._reset_connection()
                time.sleep(self.probe_interval)

    def _reset_connection(self):
        """
        Close current connection and reset state.
        
        Called on connection failure or NOT_LEADER response.
        Clears sock and current_node for reconnection.
        
        Example:
            ```python
            # Connection lost
            client._reset_connection()
            # Next _safe_request will trigger _connect_to_leader
            ```
        """
        if self.sock is not None:
            try:
                self.sock.close()
            except Exception:
                pass
        self.sock = None
        self.current_node = None

    def _safe_request(self, msg: dict):
        """
        Send request with automatic retry and failover.
        
        Handles:
        - Connection loss → reconnect to new LEADER and retry
        - NOT_LEADER response → discover new LEADER and retry
        - Network errors → discover new LEADER and retry
        
        Args:
            msg (dict): Request message.
        
        Returns:
            Any: Response payload.
        
        Example:
            ```python
            # Normal operation
            response = client._safe_request({
                'action': 'update_state',
                'worker_type': 'cleaner',
                'replica_id': 'cleaner-1',
                'state': 'PROCESSING',
                'request_id': 123,
                'position': 1
            })
            # Returns: 'OK'
            
            # LEADER fails during request
            # - Connection error caught
            # - Discovers new LEADER
            # - Retries same request
            # - Returns: 'OK' from new LEADER
            ```
        """
        payload = json.dumps(msg).encode("utf-8")

        while True:

            # Asegurarse de estar conectado a algún líder
            if self.sock is None:
                self._connect_to_leader()

            try:
                self.sock.sendall(payload)
                data = self.sock.recv(4096)
                if not data:
                    raise ConnectionError("WSM cerró la conexión")

                response = json.loads(data.decode("utf-8")).get("response")

                if response == "NOT_LEADER":
                    logging.warning("[WSMClient] Nodo actual dejó de ser líder, redescubriendo líder...")
                    self._reset_connection()
                    continue  # vuelve al while, encuentra nuevo líder y reenvía

                return response

            except Exception as e:
                logging.warning(f"[WSMClient] Error de conexión con el líder ({e}), buscando nuevo líder...")
                self._reset_connection()

    def _register(self):
        """
        Register replica with WSM (optional, state created on first update).
        
        Example:
            ```python
            client._register()
            # Sends: {"action": "register", "worker_type": "cleaner", "replica_id": "cleaner-1"}
            ```
        """
        msg = {
            "action": "register",
            "worker_type": self.worker_type,
            "replica_id": self.replica_id
        }
        try:
            self._safe_request(msg)
        except Exception as e:
            # No frenes todo si el register falla, el sistema puede funcionar igual.
            logging.warning(f"[WSMClient] Error registrando worker ({e}), se continuará igual.")

    def update_state(self, state, request_id=None, position=None):
        """
        Update replica state in WSM.
        
        Args:
            state (str): New state ('WAITING', 'PROCESSING', 'END').
            request_id (int, optional): Request identifier.
            position (int, optional): Message position.
        
        Returns:
            str: 'OK'
        
        Example:
            ```python
            client.update_state('PROCESSING', request_id=123, position=1)
            client.update_state('WAITING', request_id=123, position=1)
            # Position 1 recorded as processed
            ```
        """
        msg = {
            "action": "update_state",
            "worker_type": self.worker_type,
            "replica_id": self.replica_id,
            "state": state,
            "request_id": request_id,
            "position": position
        }
        return self._safe_request(msg)

    def can_send_end(self, request_id, position):
        """
        Check if END signal can be sent for continuous stream.
        
        Args:
            request_id (int): Request identifier.
            position (int): Proposed END position.
        
        Returns:
            bool: True if stream complete up to position-1.
        
        Example:
            ```python
            # Positions processed: {1, 2, 3, 4, 5}
            can_send = client.can_send_end(123, 6)
            # Returns: True
            
            # Gap at position 3
            can_send = client.can_send_end(123, 6)
            # Returns: False
            ```
        """
        msg = {
            "action": "can_send_end",
            "worker_type": self.worker_type,
            "request_id": request_id,
            "position": position
        }
        return self._safe_request(msg) == "OK"

    def can_send_last_end(self, request_id):
        """
        Check if last END can be sent (all workers finished).
        
        Args:
            request_id (int): Request identifier.
        
        Returns:
            bool: True if all workers reached END.
        
        Example:
            ```python
            # Last replica to reach END
            can_send = client.can_send_last_end(123)
            # Returns: True (send final notification)
            
            # Other replicas still processing
            can_send = client.can_send_last_end(123)
            # Returns: False (wait for others)
            ```
        """
        msg = {
            "action": "can_send_last_end",
            "replica_id": self.replica_id,
            "worker_type": self.worker_type,
            "request_id": request_id
        }
        return self._safe_request(msg) == "OK"

    def is_position_processed(self, request_id, position):
        """
        Check if position already processed (duplicate detection).
        
        Args:
            request_id (int): Request identifier.
            position (int): Position to check.
        
        Returns:
            bool: True if already processed.
        
        Example:
            ```python
            # After crash recovery
            is_dup = client.is_position_processed(123, 1)
            if is_dup:
                # Skip processing, already done
                pass
            else:
                # Process message
                process_message(msg)
            ```
        """
        msg = {
            "action": "is_position_processed",
            "worker_type": self.worker_type,
            "request_id": request_id,
            "position": position
        }
        return self._safe_request(msg) is True

    def _heartbeat_loop(self):
        """
        Periodically send heartbeat to WSM leader.
        """
        HEARTBEAT_INTERVAL = 3.0
        while True:
            try:
                self.send_heartbeat()
            except Exception as e:
                logging.debug(f"[WSMClient] Error sending heartbeat: {e}")
            time.sleep(HEARTBEAT_INTERVAL)

    def send_heartbeat(self):
        """
        Send heartbeat signal to WSM.
        """
        msg = {
            "action": "heartbeat",
            "worker_type": self.worker_type,
            "replica_id": self.replica_id
        }
        # Use _safe_request to handle leader failover automatically
        self._safe_request(msg)
