import os
import socket
import json
import threading
import time
import logging

from wsm_server import WSMServer
import docker 


class WSMNode:
    """
    WSM node with Bully leader election for high availability.

    Uses UDP for the control plane (leader election + heartbeats) and keeps the
    data plane (WSMServer) on a separate TCP port for client requests.

    Design goals:
        - Robust startup: no elections or heartbeats until all nodes are READY
        - Lightweight UDP control-plane with Bully algorithm
        - Clear separation between control-plane and data-plane

    Control Plane (UDP):
        - READY / READY_ACK: startup synchronization (strict all-nodes-ready)
        - WHO_IS_LEADER / LEADER_INFO: leader discovery
        - ELECTION / OK / COORDINATOR: Bully algorithm messages
        - HEARTBEAT / HEARTBEAT_OK: leader liveness monitoring

    Data Plane (TCP):
        - WSMServer handles worker state management for clients
    """

    def __init__(self):
        """
        Initialize WSM node with configuration from environment.

        Environment Variables:
            WSM_ID: Node identifier (1, 2, 3, ...).
            WSM_CONTROL_BASE_PORT: Base port for control plane (UDP).
            WSM_NAME: Base hostname (node 1: wsm, node 2: wsm_2, ...).
            WSM_REPLICAS: Total number of WSM nodes.
        """
        logging.basicConfig(level=logging.INFO, format=f"[WSM NODE %(levelname)s] %(message)s")

        self.id = int(os.getenv("WSM_ID"))
        base = int(os.getenv("WSM_CONTROL_BASE_PORT"))

        self.control_port = base + self.id
        self.wsm_name = os.getenv("WSM_NAME")
        total = int(os.getenv("WSM_REPLICAS", "3"))

        # Peers configuration (other WSM nodes)
        self.peers = []
        for i in range(1, total + 1):
            if i != self.id:
                host = self.wsm_name if i == 1 else f"{self.wsm_name}_{i}"
                self.peers.append({"id": i, "host": host, "port": base + i})

        # Leader & role state
        self.leader_id = None
        self.role = "BACKUP"
        self.ok_received = False

        self.wsm_server = None
        self.leader_lock = threading.Lock()

        # Heartbeat tracking
        self.last_heartbeat_ok = time.time()

        # Strict READY synchronization
        self.ready = False
        self.ready_peers = set()
        self.total_nodes = total

        # Single UDP socket for control plane (send + receive)
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind(("0.0.0.0", self.control_port))
        self.udp.settimeout(0.2)

        # --- NEW: tracking de heartbeats de otros WSM y Docker client ---
        self.peer_heartbeats = {}  # {peer_id: last_timestamp}
        self.docker_client = None
        try:
            self.docker_client = docker.from_env()
            logging.info("[WSM NODE] Docker client initialized successfully")
        except Exception as e:
            logging.error(f"[WSM NODE] Failed to initialize Docker client: {e}")

    # ======================================================================
    # STARTUP
    # ======================================================================
    def start(self):
        """
        Start WSM node with robust UDP control plane and Bully election.

        Sequence:
        1. Start UDP control-plane listener
        2. Run strict READY handshake (all nodes must be READY)
        3. Try to discover existing leader
        4. If no leader found → run Bully election (start_election)
        5. Start WSMServer (data plane)
        6. Start heartbeat loop (BACKUP only)
        7. Block main thread
        """
        # 1) Control-plane listener (receives all UDP messages)
        threading.Thread(target=self.control_listener, daemon=True).start()

        # 2) Strict READY handshake before any election/heartbeat
        self.bootstrap_ready_phase()

        # 3) Leader discovery
        if self.try_find_leader():
            logging.info(f"📘 Existing leader found: {self.leader_id}")
            self.role = "BACKUP"
            self.last_heartbeat_ok = time.time()
        else:
            logging.info("👑 No leader present → starting Bully election")
            # Important: use Bully election instead of self-becoming leader
            self.start_election()

        # 4) Start data plane
        self.wsm_server = WSMServer(role=self.role)
        threading.Thread(target=self.wsm_server.start, daemon=True).start()
        logging.info(f"🚀 WSMServer started as role: {self.role}")

        # 5) Start heartbeat monitoring (BACKUP nodes only)
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()

        # Start peer monitor (LEADER vigila a los demás WSM)
        threading.Thread(target=self.peer_monitor_loop, daemon=True).start()

        # 6) Block forever
        threading.Event().wait()

    # ======================================================================
    # READY HANDSHAKE
    # ======================================================================
    def bootstrap_ready_phase(self):
        """
        Phase 0: Strict READY synchronization.

        No election or heartbeat is processed until:
            - This node has received READY or READY_ACK from all peers.

        Mechanism:
            - Periodically broadcast READY to all peers.
            - Peers respond with READY_ACK.
            - Both READY and READY_ACK mark a peer as "ready".
            - Once ready_peers == peers, we flip self.ready = True.
        """
        logging.info("🧑‍✈️ Waiting for all nodes to become READY...")

        msg = {"type": "READY", "from": self.id}

        while len(self.ready_peers) < len(self.peers):
            # Broadcast READY to all peers
            self.broadcast(msg)
            time.sleep(0.3)

        self.ready = True
        logging.info("✅ All nodes are READY. Control plane can start.")

    # ======================================================================
    # CONTROL LISTENER (UDP)
    # ======================================================================
    def control_listener(self):
        """
        UDP control-plane listener.

        Receives all control messages and dispatches them to handle_msg.
        """
        while True:
            try:
                data, addr = self.udp.recvfrom(4096)
                msg = json.loads(data.decode())
                threading.Thread(target=self.handle_msg, args=(msg, addr), daemon=True).start()
            except socket.timeout:
                continue

    def handle_msg(self, msg, addr):
        """
        Handle incoming control-plane message based on type.

        Message types:
            - READY / READY_ACK: startup handshake
            - WHO_IS_LEADER / LEADER_INFO
            - ELECTION / OK / COORDINATOR
            - HEARTBEAT / HEARTBEAT_OK
        """
        t = msg.get("type")

        #print(f"RECIBO MENSAJE: {t}, desde {addr}. MSG: {msg}")

        # ------------------------------------------------------------------
        # READY handshake (always processed)
        # ------------------------------------------------------------------
        if t == "READY":
            sender = msg["from"]
            self.ready_peers.add(sender)

            # Send READY_ACK back so sender also marks this node as ready
            ack = {"type": "READY_ACK", "from": self.id}
            self.udp.sendto(json.dumps(ack).encode(), addr)
            return

        if t == "READY_ACK":
            sender = msg["from"]
            self.ready_peers.add(sender)
            return

        # Until READY phase is completed, ignore all other messages
        if not self.ready:
            return

        # ------------------------------------------------------------------
        # WHO_IS_LEADER / LEADER_INFO
        # ------------------------------------------------------------------
        if t == "WHO_IS_LEADER":
            if self.leader_id is not None:
                resp = {
                    "type": "LEADER_INFO",
                    "leader_id": self.leader_id
                }
                self.udp.sendto(json.dumps(resp).encode(), addr)
            return

        if t == "LEADER_INFO":
            # Response to our leader discovery
            self.leader_id = msg["leader_id"]
            return

        # ------------------------------------------------------------------
        # ELECTION / OK / COORDINATOR
        # ------------------------------------------------------------------
        if t == "ELECTION":
            sender = msg["from"]

            # Always respond OK to the sender (we are alive)
            ok = {"type": "OK", "from": self.id}
            self.udp.sendto(json.dumps(ok).encode(), addr)

            # If we have a higher ID, we start our own election
            if self.id > sender:
                self.start_election()
            return

        if t == "OK":
            # Some higher node is alive and will take over the election
            self.ok_received = True
            return

        if t == "COORDINATOR":
            # New leader announcement
            self.leader_id = msg["leader_id"]
            self.role = "BACKUP"
            self.last_heartbeat_ok = time.time()
            if self.wsm_server:
                self.wsm_server.role = "BACKUP"
            logging.info(f"📘 New leader: {self.leader_id}")
            return

        # ------------------------------------------------------------------
        # HEARTBEAT / HEARTBEAT_OK
        # ------------------------------------------------------------------
        if t == "HEARTBEAT":
            sender = msg["from"]

            # --- NEW: si soy LEADER, registro el heartbeat del backup ---
            if self.role == "LEADER":
                self.peer_heartbeats[sender] = time.time()

            # Leader responde con HEARTBEAT_OK al backup
            resp = {"type": "HEARTBEAT_OK", "from": self.id}
            self.udp.sendto(json.dumps(resp).encode(), addr)
            return

        if t == "HEARTBEAT_OK":
            # Backup records that the leader is alive
            self.last_heartbeat_ok = time.time()
            return

    # ======================================================================
    # LEADER DISCOVERY
    # ======================================================================
    def try_find_leader(self):
        """
        Query peers to discover existing LEADER.

        Sends WHO_IS_LEADER to all peers for a short period, and returns
        True if any LEADER_INFO response is received (leader_id is set).
        """
        msg = {"type": "WHO_IS_LEADER", "from": self.id}
        payload = json.dumps(msg).encode()

        deadline = time.time() + 1.5  # total discovery window

        while time.time() < deadline and self.leader_id is None:
            for p in self.peers:
                try:
                    self.udp.sendto(payload, (p["host"], p["port"]))
                except:
                    pass
            time.sleep(0.2)

        return self.leader_id is not None

    # ======================================================================
    # BECOME LEADER
    # ======================================================================
    def become_leader(self, initial=False):
        """
        Promote this node to LEADER role.
        """
        with self.leader_lock:
            if self.role == "LEADER":
                return

            self.leader_id = self.id
            self.role = "LEADER"
            logging.info("👑 This node is now the LEADER")

            # --- NEW: inicializo marca de tiempo para todos los peers ---
            now = time.time()
            for p in self.peers:
                self.peer_heartbeats[p["id"]] = now

            if self.wsm_server:
                self.wsm_server.promote_to_leader()

            if not initial:
                self.broadcast({"type": "COORDINATOR", "leader_id": self.id})


    # ======================================================================
    # BULLY ELECTION
    # ======================================================================
    def start_election(self):
        """
        Start Bully election by contacting higher ID nodes via UDP.

        Algorithm:
        1. Send ELECTION to all nodes with ID > self.id
        2. Wait 1 second for OK responses (handled in handle_msg)
        3. If no OK received → become LEADER
        4. If OK received → wait for COORDINATOR from winner
        """
        with self.leader_lock:
            logging.info("🏳️ Starting Bully election")

            self.ok_received = False
            higher = [p for p in self.peers if p["id"] > self.id]

            msg = {"type": "ELECTION", "from": self.id}
            payload = json.dumps(msg).encode()

            for p in higher:
                try:
                    self.udp.sendto(payload, (p["host"], p["port"]))
                except:
                    pass

        time.sleep(1.0)

        if not self.ok_received:
            # No higher node answered → we are the new leader
            self.become_leader()

    # ======================================================================
    # HEARTBEAT
    # ======================================================================
    def heartbeat_loop(self):
        """
        Monitor LEADER health (BACKUP nodes only) using UDP heartbeats.

        Every second:
            - BACKUP sends HEARTBEAT to LEADER
            - LEADER responds with HEARTBEAT_OK
            - If no HEARTBEAT_OK is observed for some timeout → election
        """
        HEARTBEAT_INTERVAL = 1.0
        HEARTBEAT_TIMEOUT = 3.0  # seconds without HEARTBEAT_OK → suspect

        while True:
            time.sleep(HEARTBEAT_INTERVAL)

            if self.role != "BACKUP" or not self.leader_id:
                continue

            leader_peer = next((p for p in self.peers if p["id"] == self.leader_id), None)
            if not leader_peer:
                continue

            msg = {"type": "HEARTBEAT", "from": self.id}
            payload = json.dumps(msg).encode()

            try:
                self.udp.sendto(payload, (leader_peer["host"], leader_peer["port"]))
            except:
                pass

            # If too much time has passed without HEARTBEAT_OK, trigger election
            if time.time() - self.last_heartbeat_ok > HEARTBEAT_TIMEOUT:
                logging.info("⚠️ Leader is not responding → starting election")
                self.start_election()

    # ======================================================================
    # BROADCAST UTILITY
    # ======================================================================
    def broadcast(self, msg):
        """
        Send a UDP message to all peers.

        Args:
            msg (dict): Message to broadcast.
        """
        payload = json.dumps(msg).encode()
        for p in self.peers:
            try:
                self.udp.sendto(payload, (p["host"], p["port"]))
            except:
                pass

    # ======================================================================
    # RESTART DE OTROS WSM (solo el LEADER lo hace)
    # ======================================================================
    def restart_wsm_node(self, node_id: int):

        if not self.docker_client:
            logging.error("[WSM NODE] Cannot restart WSM node: Docker client not initialized")
            return

        if node_id == 1:
            container_name = self.wsm_name
        else:
            container_name = f"{self.wsm_name}_{node_id}"

        try:
            container = self.docker_client.containers.get(container_name)
            container.restart()
            logging.info(f"[WSM NODE] Successfully restarted WSM container: {container_name}")
        except docker.errors.NotFound:
            logging.error(f"[WSM NODE] WSM container not found for restart: {container_name}")
        except Exception as e:
            logging.error(f"[WSM NODE] Failed to restart WSM container {container_name}: {e}")

    def peer_monitor_loop(self):

        CHECK_INTERVAL = 2.0
        HEARTBEAT_TIMEOUT = 20.0  # segundos sin HEARTBEAT desde ese peer

        while True:
            time.sleep(CHECK_INTERVAL)

            if self.role != "LEADER":
                continue

            now = time.time()
            # Solo monitoreo peers que conozco (están en self.peers)
            for p in self.peers:
                peer_id = p["id"]
                last = self.peer_heartbeats.get(peer_id)

                if last is None:
                    # Todavía no recibimos ningún HEARTBEAT de este peer → esperamos
                    continue

                if now - last > HEARTBEAT_TIMEOUT:
                    logging.warning(
                        f"[WSM NODE] Peer WSM {peer_id} DEAD by TIMEOUT. "
                        f"now={now:.2f}, last={last:.2f}, delta={now-last:.2f} > {HEARTBEAT_TIMEOUT}"
                    )
                    self.restart_wsm_node(peer_id)
                    self.peer_heartbeats[peer_id] = now



if __name__ == "__main__":
    WSMNode().start()
