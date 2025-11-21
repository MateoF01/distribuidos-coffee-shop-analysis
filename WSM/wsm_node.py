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

        self.id = int(os.getenv("WSM_ID"))
        base = int(os.getenv("WSM_CONTROL_BASE_PORT"))

        self.control_port = base + self.id
        self.wsm_name = os.getenv("WSM_NAME")
        total = int(os.getenv("WSM_REPLICAS", "3"))

        # Peers (control plane)
        self.peers = []
        for i in range(1, total + 1):
            if i != self.id:
                host = self.wsm_name if i == 1 else f"{self.wsm_name}_{i}"
                self.peers.append({"id": i, "host": host, "port": base + i})

        # Estado
        self.leader_id = None
        self.role = "UNKNOWN"
        self.running = True
        self.ok_received = False

        # WSM Server para este nodo (lado data plane)
        self.wsm_server = None

        self.leader_lock = threading.Lock()


    # ======================================================
    # INICIO DEL NODO
    # ======================================================
    def start(self):
        threading.Thread(target=self.control_listener, daemon=True).start()

        # Buscar si hay líder
        if self.try_find_leader():
            logging.info(f"📘 Ya hay líder: {self.leader_id}")
            self.role = "BACKUP"
        else:
            logging.info("👑 No existe líder → me proclamo líder")
            self.become_leader(initial=True)

        # Arrancar el WSMServer para este nodo
        self.wsm_server = WSMServer(role=self.role)
        threading.Thread(target=self.wsm_server.start, daemon=True).start()

        logging.info(f"🚀 WSMServer iniciado como rol: {self.role}")

        # Heartbeat en backup
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()

        # Bloquear thread principal
        threading.Event().wait()


    # ======================================================
    # DETECTAR LÍDER EXISTENTE
    # ======================================================
    def try_find_leader(self):
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


    # ======================================================
    # PROCLAMARSE LÍDER
    # ======================================================
    def become_leader(self, initial=False):

        with self.leader_lock:
            # Evitar conflictos
            if self.role == "LEADER":
                return

            self.leader_id = self.id
            self.role = "LEADER"
            logging.info("👑 Ahora soy el líder")

            # Si ya existía server, actualizarle rol
            if self.wsm_server:
                self.wsm_server.role = "LEADER"

            # Solo anunciar si no es inicio del sistema
            if not initial:
                self.broadcast({"type": "COORDINATOR", "leader_id": self.id})


    # ======================================================
    # LISTENER DE CONTROL
    # ======================================================
    def control_listener(self):
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

            # Responder OK
            self.send_to(sender, {"type": "OK", "from": self.id})

            # Si soy más grande → inicio elección
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


    # ======================================================
    # ALGORITMO BULLY
    # ======================================================
    def start_election(self):
        with self.leader_lock:
            logging.info("🏳️ Iniciando elección Bully")

            self.ok_received = False
            higher = [p for p in self.peers if p["id"] > self.id]

            for p in higher:
                self.send_to(p["id"], {"type": "ELECTION", "from": self.id})

        # Timeout para recibir OK
        time.sleep(1)

        if not self.ok_received:
            self.become_leader()


    # ======================================================
    # HEARTBEAT SOLO EN BACKUP
    # ======================================================
    def heartbeat_loop(self):
        while True:
            time.sleep(1)

            if self.role == "BACKUP" and self.leader_id:
                alive = self.send_to(self.leader_id, {"type": "HEARTBEAT"})
                if not alive:
                    logging.info("⚠️ El líder no responde → iniciando elección")
                    self.start_election()


    # ======================================================
    # ENVÍOS
    # ======================================================
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


if __name__ == "__main__":
    WSMNode().start()
