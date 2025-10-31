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
USING_END_SYNC = os.environ.get("USING_END_SYNC", "0") == "1"

os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class WorkerStateManager:
    def __init__(self, state_file=STATE_FILE, using_end_sync=USING_END_SYNC):
        self.state_file = state_file
        self.using_end_sync = using_end_sync
        # these three dictionaries are persisted to disk together
        self.workers_being_used = {}
        self.ends_by_requests = {}
        self.lock = threading.Lock()
        # _load_state will populate self.worker_states, self.workers_being_used and
        # self.ends_by_requests if a saved file exists. If not, defaults above remain.
        self._load_state()
        logging.info(f"[WSM] Archivo de estado: {self.state_file}")

    # -------------------------------
    # 🔄 Persistencia
    # -------------------------------
    def _load_state(self):
        """
        Load persisted state file (if present) and populate the three in-memory
        dictionaries: worker_states, workers_being_used and ends_by_requests.
        If any key is missing or the file is invalid, fall back to defaults while
        logging an error.
        """
        # initialize defaults in case file is absent or invalid
        self.worker_states = self.worker_states if hasattr(self, "worker_states") else {}
        self.workers_being_used = self.workers_being_used if hasattr(self, "workers_being_used") else {}
        self.ends_by_requests = self.ends_by_requests if hasattr(self, "ends_by_requests") else {}

        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if isinstance(data, dict):
                    self.worker_states = data.get("worker_states", {}) or {}
                    self.workers_being_used = data.get("workers_being_used", {}) or {}
                    self.ends_by_requests = data.get("ends_by_requests", {}) or {}
                    logging.info(f"[WSM] Estado cargado desde {self.state_file}")
                else:
                    logging.error(f"[WSM] Formato de estado inválido en {self.state_file}")
            except Exception as e:
                logging.error(f"[WSM] Error cargando estado: {e}")

        return self.worker_states

    def _save_state(self):
        payload = {
            "worker_states": self.worker_states,
            "workers_being_used": self.workers_being_used,
            "ends_by_requests": self.ends_by_requests,
        }
        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
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
                if self.using_end_sync:
                    if worker_type not in self.workers_being_used:
                        self.workers_being_used[worker_type] = True
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
            if self.using_end_sync:
                if worker_type not in self.workers_being_used:
                    self.workers_being_used[worker_type] = True
                if request_id not in self.ends_by_requests:
                    self.ends_by_requests[request_id] = sum(
                        1 for being_used in self.workers_being_used.values() if being_used
                    )
                if state == "END":
                    self.ends_by_requests[request_id] -= 1

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

    def can_send_last_end(self, worker_type, replica_id, request_id):
        """
        Devuelve True si todas las réplicas de un mismo tipo de worker
        terminaron de procesar el request dado y están en WAITING.
        """
        with self.lock:
            if self.using_end_sync:
                if worker_type not in self.worker_states:
                    self.worker_states[worker_type] = {}
                if self.using_end_sync:
                    if worker_type not in self.workers_being_used:
                        self.workers_being_used[worker_type] = True
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
        elif action == "can_send_last_end":
            can_send = self.manager.can_send_last_end(worker_type, replica_id, msg["request_id"])
            return "OK" if can_send else "WAIT"
        else:
            return "ERROR: unknown action"


if __name__ == "__main__":
    WSMServer().start()
