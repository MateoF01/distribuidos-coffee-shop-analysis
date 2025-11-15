import os
import socket
import json
import threading
import time
import logging

from wsm_server import WSMServer


class WSMNode:
    def __init__(self):
        logging.basicConfig(level=logging.INFO, format=f"[WSM NODE %(levelname)s] %(message)s")

        # ---------------------------
        # CONFIG
        # ---------------------------
        self.wsm_id = int(os.getenv("WSM_ID"))
        base_port = int(os.getenv("WSM_CONTROL_BASE_PORT"))
        self.wsm_name = os.getenv("WSM_NAME")
        self.workers_port = int(os.getenv("PORT"))

        self.control_port = base_port + self.wsm_id

        logging.info(f"▶ WSM_NAME = {self.wsm_name}")
        logging.info(f"▶ WSM_ID = {self.wsm_id}")
        logging.info(f"▶ Puerto workers = {self.workers_port}")
        logging.info(f"▶ Puerto control  = {self.control_port}")

        # ---------------------------
        # DETECCIÓN DE PEERS
        # ---------------------------
        total = int(os.getenv("WSM_REPLICAS", "3"))
        self.peers = []

        for i in range(1, total + 1):
            if i == self.wsm_id:
                continue
            host = self.wsm_name if i == 1 else f"{self.wsm_name}_{i}"
            self.peers.append({"id": i, "host": host, "port": base_port + i})

        logging.info(f"▶ Peers detectados: {self.peers}")

        # ---------------------------
        # ESTADO
        # ---------------------------
        self.leader_id = None
        self.role = "UNKNOWN"     # "LEADER" | "BACKUP"
        self.running = True

        # BULKY PROTECTION
        self.election_lock = threading.Lock()
        self.election_in_progress = False
        self.ok_received = False

        # SERVER
        self.server = WSMServer(host="0.0.0.0", port=self.workers_port)
        self.server_started = False

    # ==========================================================
    # START
    # ==========================================================
    def start(self):
        logging.info("▶ Nodo iniciando…")

        threading.Thread(target=self.control_listener, daemon=True).start()

        # → ALGORITMO CORRECTO:
        # El mayor ID se proclama líder automáticamente
        # Los demás preguntan si hay líder
        self.init_leader_logic()

        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        threading.Event().wait()

    # ==========================================================
    # LÓGICA INICIAL DE LIDERAZGO
    # ==========================================================
    def init_leader_logic(self):
        # si soy el mayor ID → líder
        if self.is_highest_id():
            logging.info("🔥 Soy el nodo con mayor ID → me proclamo líder")
            self.start_as_leader()
            return

        # si no soy el mayor → busco al líder
        logging.info("🔍 Buscando líder existente…")
        if self.try_find_leader():
            logging.info(f"📘 Detecté líder existente: {self.leader_id}")
            return

        logging.info("⚠️ No encontré líder → iniciando elección")
        self.start_election()

    def is_highest_id(self):
        return all(self.wsm_id > p["id"] for p in self.peers)

    # ==========================================================
    # BÚSQUEDA DE LÍDER AL ARRANCAR
    # ==========================================================
    def try_find_leader(self):
        msg = {"type": "WHO_IS_LEADER", "from": self.wsm_id}

        for p in self.peers:
            try:
                with socket.create_connection((p["host"], p["port"]), timeout=0.5) as s:
                    s.sendall(json.dumps(msg).encode())
                    data = s.recv(4096)

                if not data:
                    continue

                resp = json.loads(data.decode())

                if resp.get("type") == "LEADER_INFO":
                    self.set_leader(resp["leader_id"])
                    return True

            except:
                pass

        return False

    # ==========================================================
    # CONTROL LISTENER
    # ==========================================================
    def control_listener(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("0.0.0.0", self.control_port))
        sock.listen(32)

        logging.info(f"▶ Escuchando control en puerto {self.control_port}")

        while True:
            try:
                conn, _ = sock.accept()
                data = conn.recv(4096)
                if data:
                    msg = json.loads(data.decode())
                    self.handle_control_message(msg, conn)
                conn.close()
            except:
                pass

    # ==========================================================
    # MANEJO DE MENSAJES
    # ==========================================================
    def handle_control_message(self, msg, conn):
        t = msg.get("type")

        if t == "WHO_IS_LEADER":
            if self.leader_id is not None:
                conn.sendall(json.dumps({
                    "type": "LEADER_INFO",
                    "leader_id": self.leader_id
                }).encode())
            return

        if t == "ELECTION":
            sender = msg["from"]
            self.send_to(sender, {"type": "OK", "from": self.wsm_id})

            if self.wsm_id > sender:
                self.start_election()

        elif t == "OK":
            self.ok_received = True

        elif t == "COORDINATOR":
            self.set_leader(msg["leader_id"])

        elif t == "HEARTBEAT":
            pass

    # ==========================================================
    # SENDERS
    # ==========================================================
    def send_to(self, peer_id, msg):
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
        for p in self.peers:
            self.send_to(p["id"], msg)

    # ==========================================================
    # BULLY ELECTION
    # ==========================================================
    def start_election(self):
        with self.election_lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True

        logging.info("🎌 Iniciando elección Bully…")

        self.ok_received = False
        higher = [p for p in self.peers if p["id"] > self.wsm_id]

        for p in higher:
            self.send_to(p["id"], {"type": "ELECTION", "from": self.wsm_id})

        deadline = time.time() + 1.0
        while time.time() < deadline:
            if self.ok_received:
                with self.election_lock:
                    self.election_in_progress = False
                return

        self.broadcast({"type": "COORDINATOR", "leader_id": self.wsm_id})
        self.start_as_leader()

        with self.election_lock:
            self.election_in_progress = False

    # ==========================================================
    # CAMBIO DE LÍDER
    # ==========================================================
    def set_leader(self, leader_id):
        self.leader_id = leader_id

        if leader_id == self.wsm_id:
            self.start_as_leader()
        else:
            if self.role == "LEADER":
                self.server.stop()
                self.server_started = False

            logging.info(f"📘 Soy BACKUP. Líder actual: {leader_id}")
            self.role = "BACKUP"

    def start_as_leader(self):
        self.leader_id = self.wsm_id
        self.role = "LEADER"

        logging.info("👑 Ahora soy el líder del WSM")

        if not self.server_started:
            self.server_started = True
            threading.Thread(target=self.server.start, daemon=True).start()

    # ==========================================================
    # HEARTBEAT
    # ==========================================================
    def heartbeat_loop(self):
        while True:
            time.sleep(1)

            if self.role == "BACKUP" and self.leader_id is not None:
                alive = self.send_to(self.leader_id, {
                    "type": "HEARTBEAT",
                    "from": self.wsm_id
                })

                if not alive:
                    logging.info("⚠️ El líder no responde → iniciando elección")
                    self.start_election()


if __name__ == "__main__":
    WSMNode().start()
