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
        self.output_base_dir = os.path.dirname(self.state_file)
        # persisted state structures
        self.worker_states = {}
        self.workers_being_used = {}
        self.ends_by_requests = {}
        # No longer store positions_by_requests in memory - will be in individual files
        self.lock = threading.Lock()
        # _load_state will populate self.worker_states, self.workers_being_used and
        # self.ends_by_requests if a saved file exists. If not, defaults above remain.
        self._load_state()
        logging.info(f"[WSM] Archivo de estado: {self.state_file}")
        logging.info(f"[WSM] Directorio base de salida: {self.output_base_dir}")

    # -------------------------------
    # 🔄 Persistencia
    # -------------------------------
    def _get_positions_dir(self, worker_type):
        """Retorna el directorio para las posiciones de un tipo de worker."""
        return os.path.join(self.output_base_dir, f"{worker_type}_positions")
    
    def _get_positions_file(self, worker_type, request_id):
        """Retorna la ruta del archivo de posiciones para un worker_type y request_id."""
        positions_dir = self._get_positions_dir(worker_type)
        return os.path.join(positions_dir, f"{request_id}.txt")
    
    def _add_position(self, worker_type, request_id, position):
        """Agrega una posición al archivo append-only."""
        positions_dir = self._get_positions_dir(worker_type)
        os.makedirs(positions_dir, exist_ok=True)
        
        positions_file = self._get_positions_file(worker_type, request_id)
        try:
            with open(positions_file, "a", encoding="utf-8") as f:
                f.write(f"{position}\n")
        except Exception as e:
            logging.error(f"[WSM] Error agregando posición a {positions_file}: {e}")

    def _load_positions(self, worker_type, request_id):
        """Carga las posiciones desde el archivo correspondiente."""
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
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if isinstance(data, dict):
                    self.worker_states = data.get("worker_states", {})
                    self.workers_being_used = data.get("workers_being_used", {})
                    self.ends_by_requests = data.get("ends_by_requests", {})
                    # Positions are now stored in separate files, no longer in main state file
                    logging.info(f"[WSM] Estado cargado desde {self.state_file}")
                else:
                    logging.error(f"[WSM] Formato de estado inválido en {self.state_file}")
            except Exception as e:
                logging.error(f"[WSM] Error cargando estado: {e}")

    def _save_state(self):
        try:
            payload = {
                "worker_states": self.worker_states,
                "workers_being_used": self.workers_being_used,
                "ends_by_requests": self.ends_by_requests,
                # positions_by_requests no longer stored in main state file
            }
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logging.error(f"[WSM] Error guardando archivo de estado: {e}")

    # -------------------------------
    # Operaciones de ayuda
    # -------------------------------
    def _find_first_missing_position(self, pos_set):
        """ Returns the first missing position in the sequence starting from 1.
        E.g. if positions = {1,2,3,5,6} → returns 4 (first missing).
        E.g. if positions = {2,3,5} → returns 1 (missing 1).
        E.g. if positions = {1,2,4,5} → returns 3 (missing 3).
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

    # -------------------------------
    # 🧱 Operaciones básicas
    # -------------------------------
    def register_worker(self, worker_type, replica_id):
        """
        Registra una réplica de un tipo de worker.
        """
        with self.lock:
            self.worker_states.setdefault(worker_type, {})
            if replica_id not in self.worker_states[worker_type]:
                if self.using_end_sync:
                    self.workers_being_used.setdefault(worker_type, True)
                self.worker_states[worker_type][replica_id] = {"state": "WAITING", "request_id": None}
                self._save_state()
                #logging.info(f"[WSM] {worker_type} → Replica {replica_id} registrada como WAITING")

    def update_state(self, worker_type, replica_id, state, request_id, position):
        """
        Actualiza el estado de una réplica específica.
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
            #logging.info(f"[WSM] {worker_type}:{replica_id} → {state} ({request_id})")

    def can_send_end(self, worker_type, request_id, position):
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
        Devuelve True si todas las réplicas de un mismo tipo de worker
        terminaron de procesar el request dado y están en WAITING.
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
        Devuelve True si la posición ya fue registrada como procesada
        por este tipo de worker en este request.
        """
        with self.lock:
            pos_set = self._load_positions(worker_type, request_id)
            processed = position in pos_set
            logging.debug(f"[WSM] is_position_processed({worker_type}, {request_id}, {position}) = {processed}")
            return processed

# -------------------------------
# ⚡ Servidor TCP multicliente
# -------------------------------
class WSMServer:
    def __init__(self, host=HOST, port=PORT):
        self.host = host
        self.port = port
        self.manager = WorkerStateManager()
        self.running = False
        self.server_socket = None

    def start(self):
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
                    break  # socket cerrado → stop

    def stop(self):
        logging.info("[WSM] Deteniendo servidor líder...")
        self.running = False
        try:
            self.server_socket.close()
        except:
            pass
    

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
        else:
            return "ERROR: unknown action"


if __name__ == "__main__":
    from wsm_node import WSMNode
    WSMNode().start()
