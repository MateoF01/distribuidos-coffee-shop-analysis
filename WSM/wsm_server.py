import socket
import threading
import json
import os
import logging

STATE_FILE = "worker_states.json"
HOST = "0.0.0.0"
PORT = 9000

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class WorkerStateManager:
    def __init__(self, state_file=STATE_FILE):
        self.state_file = state_file
        self.lock = threading.Lock()
        self.worker_states = self._load_state()

    # -------------------------------
    # 🔄 Persistencia
    # -------------------------------
    def _load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, "r") as f:
                return json.load(f)
        return {}

    def _save_state(self):
        with open(self.state_file, "w") as f:
            json.dump(self.worker_states, f, indent=2)

    # -------------------------------
    # 🧱 Operaciones básicas
    # -------------------------------
    def register_worker(self, replica_id):
        with self.lock:
            if replica_id not in self.worker_states:
                self.worker_states[replica_id] = {"state": "WAITING", "request_id": None}
                self._save_state()
                logging.info(f"Worker {replica_id} registrado como WAITING")

    def update_state(self, replica_id, state, request_id):
        with self.lock:
            self.worker_states[replica_id] = {"state": state, "request_id": request_id}
            self._save_state_to_file()
            logging.info(f"[WSM] {replica_id} -> {state} ({request_id})")



    def _save_state_to_file(self):
        """Guarda el estado actual en disco."""
        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(self.worker_states, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"[WSM] Error guardando archivo de estado: {e}")


    def can_send_end(self, request_id):
        """Devuelve True si no hay ningún worker procesando el mismo request_id"""
        with self.lock:
            for rid, info in self.worker_states.items():
                if info["state"] == "PROCESSING" and info["request_id"] == request_id:
                    logging.debug(f"[WSM] {rid} todavía procesando {request_id}")
                    return False
            return True


# -------------------------------
# ⚡ Servidor TCP multicliente
# -------------------------------
class WSMServer:
    def __init__(self, host=HOST, port=PORT):
        self.host = host
        self.port = port
        self.manager = WorkerStateManager()

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(10)
        logging.info(f"[WSM] Servidor escuchando en {self.host}:{self.port}")

        while True:
            conn, addr = server_socket.accept()
            threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()

    def handle_client(self, conn, addr):
        try:
            data = conn.recv(4096)
            if not data:
                return

            try:
                msg = json.loads(data.decode())
            except json.JSONDecodeError:
                conn.sendall(b'{"response": "ERROR: invalid JSON"}')
                return

            action = msg.get("action")
            response = self._handle_action(action, msg)
            conn.sendall(json.dumps({"response": response}).encode())

        except Exception as e:
            logging.error(f"[WSM] Error con cliente {addr}: {e}")
        finally:
            conn.close()

    def _handle_action(self, action, msg):
        if action == "register":
            self.manager.register_worker(msg["replica_id"])
            return "OK"
        elif action == "update_state":
            self.manager.update_state(msg["replica_id"], msg["state"], msg.get("request_id"))
            return "OK"
        elif action == "can_send_end":
            can_send = self.manager.can_send_end(msg["request_id"])
            return "OK" if can_send else "WAIT"
        else:
            return "ERROR: unknown action"


if __name__ == "__main__":
    WSMServer().start()
