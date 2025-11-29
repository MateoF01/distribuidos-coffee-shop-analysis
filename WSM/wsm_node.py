import os
import socket
import json
import threading
import time
import logging

from wsm_server import WSMServer

class WSMNode:
    """
    WSM node with Bully leader election for high availability.
    
    Manages leader election among WSM replicas using Bully algorithm. Handles control plane
    (election messages) separately from data plane (WSMServer). Supports hot failover with
    state recovery from persistent storage.
    
    Architecture:
        - Control plane: Leader election on control_port (base + id)
        - Data plane: WSMServer on standard port for client requests
        - Bully algorithm: Higher ID nodes become leader after election
        - Heartbeat: BACKUP monitors LEADER, triggers election on failure
    
    Election Protocol:
        1. Node detects LEADER failure (no heartbeat)
        2. Sends ELECTION to all higher ID nodes
        3. If no OK received → becomes LEADER and broadcasts COORDINATOR
        4. If OK received → waits for COORDINATOR from winner
    
    Attributes:
        id (int): Node identifier (determines election priority).
        control_port (int): Control plane port (base + id).
        wsm_name (str): Base hostname for WSM nodes.
        peers (list): List of peer nodes with host/port.
        leader_id (int): Current LEADER node ID.
        role (str): Current role ('LEADER', 'BACKUP', 'UNKNOWN').
        running (bool): Node running flag.
        ok_received (bool): Flag for ELECTION response.
        wsm_server (WSMServer): Data plane server instance.
        leader_lock (threading.Lock): Election synchronization.
    
    Example:
        ```python
        # Environment:
        # WSM_ID=1, WSM_CONTROL_BASE_PORT=8000, WSM_NAME=wsm, WSM_REPLICAS=3
        
        # Node 1 (wsm:8001)
        node1 = WSMNode()
        node1.start()  # Becomes LEADER initially
        
        # Node 2 (wsm_2:8002) starts
        node2 = WSMNode()
        node2.start()  # Detects node1 as LEADER, becomes BACKUP
        
        # Node 1 crashes
        # Node 2 detects failure via heartbeat, starts election
        # Node 3 (wsm_3:8003) receives ELECTION from node2
        # Node 3 has higher ID → sends OK and starts own election
        # Node 3 becomes LEADER, broadcasts COORDINATOR
        # Node 2 receives COORDINATOR, becomes BACKUP
        ```
    """
    """
    WSM node with Bully leader election for high availability.
    
    Manages leader election among WSM replicas using Bully algorithm. Handles control plane
    (election messages) separately from data plane (WSMServer). Supports hot failover with
    state recovery from persistent storage.
    
    Architecture:
        - Control plane: Leader election on control_port (base + id)
        - Data plane: WSMServer on standard port for client requests
        - Bully algorithm: Higher ID nodes become leader after election
        - Heartbeat: BACKUP monitors LEADER, triggers election on failure
    
    Election Protocol:
        1. Node detects LEADER failure (no heartbeat)
        2. Sends ELECTION to all higher ID nodes
        3. If no OK received → becomes LEADER and broadcasts COORDINATOR
        4. If OK received → waits for COORDINATOR from winner
    
    Attributes:
        id (int): Node identifier (determines election priority).
        control_port (int): Control plane port (base + id).
        wsm_name (str): Base hostname for WSM nodes.
        peers (list): List of peer nodes with host/port.
        leader_id (int): Current LEADER node ID.
        role (str): Current role ('LEADER', 'BACKUP', 'UNKNOWN').
        running (bool): Node running flag.
        ok_received (bool): Flag for ELECTION response.
        wsm_server (WSMServer): Data plane server instance.
        leader_lock (threading.Lock): Election synchronization.
    
    Example:
        ```python
        # Environment:
        # WSM_ID=1, WSM_CONTROL_BASE_PORT=8000, WSM_NAME=wsm, WSM_REPLICAS=3
        
        # Node 1 (wsm:8001)
        node1 = WSMNode()
        node1.start()  # Becomes LEADER initially
        
        # Node 2 (wsm_2:8002) starts
        node2 = WSMNode()
        node2.start()  # Detects node1 as LEADER, becomes BACKUP
        
        # Node 1 crashes
        # Node 2 detects failure via heartbeat, starts election
        # Node 3 (wsm_3:8003) receives ELECTION from node2
        # Node 3 has higher ID → sends OK and starts own election
        # Node 3 becomes LEADER, broadcasts COORDINATOR
        # Node 2 receives COORDINATOR, becomes BACKUP
        ```
    """

    def __init__(self):
        """
        Initialize WSM node with configuration from environment.
        
        Environment Variables:
            WSM_ID: Node identifier (1, 2, 3, ...).
            WSM_CONTROL_BASE_PORT: Base port for control plane.
            WSM_NAME: Base hostname (node 1: wsm, node 2: wsm_2, ...).
            WSM_REPLICAS: Total number of WSM nodes.
        
        Example:
            ```python
            # Environment:
            # WSM_ID=2
            # WSM_CONTROL_BASE_PORT=8000
            # WSM_NAME=wsm
            # WSM_REPLICAS=3
            
            node = WSMNode()
            # node.id = 2
            # node.control_port = 8002
            # node.peers = [
            #   {'id': 1, 'host': 'wsm', 'port': 8001},
            #   {'id': 3, 'host': 'wsm_3', 'port': 8003}
            # ]
            ```
        """
        logging.basicConfig(level=logging.INFO, format=f"[WSM NODE %(levelname)s] %(message)s")

        self.id = int(os.getenv("WSM_ID"))
        base = int(os.getenv("WSM_CONTROL_BASE_PORT"))

        self.control_port = base + self.id
        self.wsm_name = os.getenv("WSM_NAME")
        total = int(os.getenv("WSM_REPLICAS", "3"))

        self.peers = []
        for i in range(1, total + 1):
            if i != self.id:
                host = self.wsm_name if i == 1 else f"{self.wsm_name}_{i}"
                self.peers.append({"id": i, "host": host, "port": base + i})

        self.leader_id = None
        self.role = "UNKNOWN"
        self.running = True
        self.ok_received = False

        self.wsm_server = None

        self.leader_lock = threading.Lock()

    def start(self):
        """
        Start WSM node with leader discovery and server initialization.
        
        Sequence:
        1. Start control plane listener (election messages)
        2. Try to find existing LEADER
        3. If no LEADER found → become LEADER
        4. Start WSMServer for data plane
        5. Start heartbeat loop (BACKUP only)
        6. Block main thread
        
        Example:
            ```python
            # First node starting (no existing LEADER)
            node1 = WSMNode()
            node1.start()
            # Becomes LEADER, starts WSMServer(role='LEADER')
            
            # Second node starting (LEADER exists)
            node2 = WSMNode()
            node2.start()
            # Detects node1 as LEADER
            # Becomes BACKUP, starts WSMServer(role='BACKUP')
            # Starts heartbeat monitoring node1
            ```
        """
        threading.Thread(target=self.control_listener, daemon=True).start()

        if self.try_find_leader():
            logging.info(f"📘 Ya hay líder: {self.leader_id}")
            self.role = "BACKUP"
        else:
            logging.info("👑 No existe líder → me proclamo líder")
            self.become_leader(initial=True)

        self.wsm_server = WSMServer(role=self.role)
        threading.Thread(target=self.wsm_server.start, daemon=True).start()

        logging.info(f"🚀 WSMServer iniciado como rol: {self.role}")

        threading.Thread(target=self.heartbeat_loop, daemon=True).start()

        threading.Event().wait()

    def try_find_leader(self):
        """
        Query peers to discover existing LEADER.
        
        Sends WHO_IS_LEADER to all peers with short timeout.
        Returns on first LEADER_INFO response.
        
        Returns:
            bool: True if LEADER found, False otherwise.
        
        Example:
            ```python
            # Node 3 starting, queries peers
            found = node3.try_find_leader()
            
            # If node 1 is LEADER:
            # - Sends WHO_IS_LEADER to node 1 and node 2
            # - Node 1 responds: {"type": "LEADER_INFO", "leader_id": 1}
            # - Returns True, sets node3.leader_id = 1
            
            # If no LEADER:
            # - All peers timeout or respond without LEADER_INFO
            # - Returns False
            ```
        """
        msg = {"type": "WHO_IS_LEADER", "from": self.id}

        for p in self.peers:
            try:
                with socket.create_connection((p["host"], p["port"]), timeout=0.4) as s:
                    s.sendall(json.dumps(msg).encode())
                    data = s.recv(2048)

                if not data:
                    continue

                resp = json.loads(data.decode())
                if resp.get("type") == "LEADER_INFO":
                    self.leader_id = resp["leader_id"]
                    return True

            except:
                pass

        return False

    def become_leader(self, initial=False):
        """
        Promote this node to LEADER role.
        
        Updates role, promotes WSMServer if exists, broadcasts COORDINATOR to peers
        (unless initial startup). Thread-safe with leader_lock.
        
        Args:
            initial (bool): True if initial startup (skip broadcast).
        
        Example:
            ```python
            # After winning election
            node2.become_leader(initial=False)
            # - Sets node2.leader_id = 2, role = 'LEADER'
            # - Promotes wsm_server to LEADER (reloads state)
            # - Broadcasts: {"type": "COORDINATOR", "leader_id": 2}
            
            # During initial startup (no peers yet)
            node1.become_leader(initial=True)
            # - Sets node1.leader_id = 1, role = 'LEADER'
            # - No broadcast (no peers to notify)
            ```
        """

        with self.leader_lock:
            if self.role == "LEADER":
                return

            self.leader_id = self.id
            self.role = "LEADER"
            logging.info("👑 Ahora soy el líder")

            if self.wsm_server:
                self.wsm_server.promote_to_leader()

            if not initial:
                self.broadcast({"type": "COORDINATOR", "leader_id": self.id})

    def control_listener(self):
        """
        Listen for control plane messages (election protocol).
        
        Binds to control_port, accepts connections, dispatches to handle_msg.
        Runs in background thread.
        
        Example:
            ```python
            # Node 2 control_listener receives:
            # {"type": "ELECTION", "from": 1}
            # Spawns handle_msg in new thread
            ```
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("0.0.0.0", self.control_port))
        sock.listen(32)

        while True:
            conn, _ = sock.accept()
            data = conn.recv(4096)
            if data:
                self.handle_msg(json.loads(data.decode()), conn)
            conn.close()

    def handle_msg(self, msg, conn):
        """
        Handle control plane message based on type.
        
        Message Types:
            - WHO_IS_LEADER: Query for current LEADER
            - ELECTION: Bully election message
            - OK: Response to ELECTION
            - COORDINATOR: New LEADER announcement
        
        Args:
            msg (dict): Parsed message.
            conn (socket): Connection for response.
        
        Example:
            ```python
            # WHO_IS_LEADER query
            handle_msg({"type": "WHO_IS_LEADER", "from": 3}, conn)
            # Response: {"type": "LEADER_INFO", "leader_id": 1}
            
            # ELECTION from lower ID
            handle_msg({"type": "ELECTION", "from": 1}, conn)
            # - Sends OK to node 1
            # - Starts own election (higher ID)
            
            # COORDINATOR announcement
            handle_msg({"type": "COORDINATOR", "leader_id": 3}, conn)
            # - Sets leader_id = 3
            # - Sets role = 'BACKUP'
            # - Updates wsm_server.role = 'BACKUP'
            ```
        """
        t = msg.get("type")

        if t == "WHO_IS_LEADER":
            if self.leader_id:
                conn.sendall(json.dumps({
                    "type": "LEADER_INFO",
                    "leader_id": self.leader_id
                }).encode())
            return

        if t == "ELECTION":
            sender = msg["from"]
            self.send_to(sender, {"type": "OK", "from": self.id})
            if self.id > sender:
                self.start_election()

        if t == "OK":
            self.ok_received = True

        if t == "COORDINATOR":
            self.leader_id = msg["leader_id"]
            self.role = "BACKUP"
            if self.wsm_server:
                self.wsm_server.role = "BACKUP"
            logging.info(f"📘 Nuevo líder: {self.leader_id}")

    def start_election(self):
        """
        Start Bully election by contacting higher ID nodes.
        
        Algorithm:
        1. Send ELECTION to all nodes with ID > self.id
        2. Wait 1 second for OK responses
        3. If no OK received → become LEADER
        4. If OK received → wait for COORDINATOR from winner
        
        Example:
            ```python
            # Node 2 starts election (nodes: 1, 2, 3)
            node2.start_election()
            
            # Sends ELECTION to node 3
            # Node 3 responds OK and starts own election
            # Node 2 waits for COORDINATOR
            # Node 3 wins (highest ID), broadcasts COORDINATOR
            # Node 2 receives COORDINATOR, becomes BACKUP
            
            # Node 3 starts election (highest ID)
            node3.start_election()
            # No higher nodes to contact
            # No OK received after timeout
            # Becomes LEADER immediately
            ```
        """
        with self.leader_lock:
            logging.info("🏳️ Iniciando elección Bully")

            self.ok_received = False
            higher = [p for p in self.peers if p["id"] > self.id]

            for p in higher:
                self.send_to(p["id"], {"type": "ELECTION", "from": self.id})

        time.sleep(1)

        if not self.ok_received:
            self.become_leader()

    def heartbeat_loop(self):
        """
        Monitor LEADER health (BACKUP nodes only).
        
        Sends periodic heartbeat to LEADER. On failure, triggers election.
        Runs continuously in background thread.
        
        Example:
            ```python
            # Node 2 (BACKUP) monitoring node 1 (LEADER)
            # Every 1 second:
            #   - Sends HEARTBEAT to node 1
            #   - If response → continue
            #   - If timeout → start_election()
            
            # Node 1 crashes
            # Node 2 detects failure on next heartbeat
            # Triggers election, becomes LEADER (if highest remaining)
            ```
        """
        while True:
            time.sleep(1)

            if self.role == "BACKUP" and self.leader_id:
                alive = self.send_to(self.leader_id, {"type": "HEARTBEAT"})
                if not alive:
                    logging.info("⚠️ El líder no responde → iniciando elección")
                    self.start_election()

    def send_to(self, peer_id, msg):
        """
        Send message to specific peer with timeout.
        
        Args:
            peer_id (int): Target peer ID.
            msg (dict): Message to send.
        
        Returns:
            bool: True if sent successfully, False on failure.
        
        Example:
            ```python
            success = node2.send_to(3, {"type": "ELECTION", "from": 2})
            # Connects to node 3, sends message
            # Returns: True if successful, False if timeout/error
            ```
        """
        p = next((x for x in self.peers if x["id"] == peer_id), None)
        if not p:
            return False

        try:
            with socket.create_connection((p["host"], p["port"]), timeout=0.3) as s:
                s.sendall(json.dumps(msg).encode())
            return True
        except:
            return False

    def broadcast(self, msg):
        """
        Send message to all peers.
        
        Args:
            msg (dict): Message to broadcast.
        
        Example:
            ```python
            # Node 3 becomes LEADER
            node3.broadcast({"type": "COORDINATOR", "leader_id": 3})
            # Sends to node 1 and node 2
            ```
        """
        for p in self.peers:
            self.send_to(p["id"], msg)


if __name__ == "__main__":
    WSMNode().start()
