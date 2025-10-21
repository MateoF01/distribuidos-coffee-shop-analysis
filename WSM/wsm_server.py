import socket
import threading
import json
import os
import logging

# -------------------------------
# ⚙️ Configuración inicial
# -------------------------------
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "9000"))
STATE_FILE = os.environ.get("STATE_FILE_PATH", "/app/output/worker_states.json")

os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class WorkerStateManager:
    def __init__(self, state_file=STATE_FILE):
        self.state_file = state_file
        self.lock = threading.Lock()
        self.worker_states = self._load_state()
        logging.info(f"[WSM] Archivo de estado: {self.state_file}")

    # -------------------------------
    # 🔄 Persistencia
    # -------------------------------
    def _load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"[WSM] Error cargando estado: {e}")
        return {}

    def _save_state(self):
        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(self.worker_states, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logging.error(f"[WSM] Error guardando archivo de estado: {e}")

    # -------------------------------
    # 🧱 Operaciones básicas
    # -------------------------------
    def register_worker(self, worker_type, replica_id):
        """
        Registra una réplica de un tipo de worker.
        """
        with self.lock:
            if worker_type not in self.worker_states:
                self.worker_states[worker_type] = {}
            if replica_id not in self.worker_states[worker_type]:
                self.worker_states[worker_type][replica_id] = {"state": "WAITING", "request_id": None}
                self._save_state()
                #logging.info(f"[WSM] {worker_type} → Replica {replica_id} registrada como WAITING")

    def update_state(self, worker_type, replica_id, state, request_id):
        """
        Actualiza el estado de una réplica específica.
        """
        with self.lock:
            if worker_type not in self.worker_states:
                self.worker_states[worker_type] = {}
            self.worker_states[worker_type][replica_id] = {"state": state, "request_id": request_id}
            self._save_state()
            #logging.info(f"[WSM] {worker_type}:{replica_id} → {state} ({request_id})")

    def can_send_end(self, worker_type, request_id):
        """
        Devuelve True si todas las réplicas de un mismo tipo de worker
        terminaron de procesar el request dado.
        """
        with self.lock:
            replicas = self.worker_states.get(worker_type, {})
            for rid, info in replicas.items():
                if info["state"] == "PROCESSING" and info["request_id"] == request_id:
                    logging.debug(f"[WSM] {worker_type}:{rid} todavía procesando {request_id}")
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
        server_socket.listen(100)
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
        worker_type = msg.get("worker_type")
        replica_id = msg.get("replica_id")

        if action == "register":
            self.manager.register_worker(worker_type, replica_id)
            return "OK"
        elif action == "update_state":
            self.manager.update_state(worker_type, replica_id, msg["state"], msg.get("request_id"))
            return "OK"
        elif action == "can_send_end":
            can_send = self.manager.can_send_end(worker_type, msg["request_id"])
            return "OK" if can_send else "WAIT"
        else:
            return "ERROR: unknown action"


if __name__ == "__main__":
    WSMServer().start()
