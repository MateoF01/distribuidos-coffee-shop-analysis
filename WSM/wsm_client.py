import socket
import json
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
class WSMClient:
    def __init__(self, worker_type, replica_id, host="wsm", port=9000):
        self.worker_type = worker_type
        self.replica_id = replica_id
        self.host = host
        self.port = port
        self.sock = None

        self._connect_with_retry()
        self._register()


    # -------------------------------
    # 🔌 Conexión persistente
    # -------------------------------
    def _connect_with_retry(self):
        """Intenta conectar infinitamente hasta que haya un WSM válido."""
        while True:
            try:
                self.sock = socket.create_connection((self.host, self.port), timeout=5)
                return
            except Exception as e:
                logging.warning(f"[WSMClient] No se pudo conectar ({e}), reintentando...")
                time.sleep(1)


    def _safe_send(self, msg):
        """Envía mensaje asegurándose de reconectar si el WSM cae."""
        payload = json.dumps(msg).encode()

        while True:
            try:
                self.sock.sendall(payload)
                data = self.sock.recv(4096)
                if not data:
                    raise ConnectionError("WSM cerró la conexión")
                return json.loads(data.decode()).get("response")

            except Exception as e:
                # cerrar socket
                try:
                    self.sock.close()
                except:
                    pass

                # reconectar infinite loop
                self._connect_with_retry()


    # -------------------------------
    # 🔁 API de alto nivel
    # -------------------------------
    def _register(self):
        msg = {
            "action": "register",
            "worker_type": self.worker_type,
            "replica_id": self.replica_id
        }
        self._safe_send(msg)

    def update_state(self, state, request_id=None, position=None):
        msg = {
            "action": "update_state",
            "worker_type": self.worker_type,
            "replica_id": self.replica_id,
            "state": state,
            "request_id": request_id,
            "position": position
        }
        return self._safe_send(msg)

    def can_send_end(self, request_id, position):
        msg = {
            "action": "can_send_end",
            "worker_type": self.worker_type,
            "request_id": request_id,
            "position": position
        }
        return self._safe_send(msg) == "OK"

    def can_send_last_end(self, request_id):
        msg = {
            "action": "can_send_last_end",
            "replica_id": self.replica_id,
            "worker_type": self.worker_type,
            "request_id": request_id
        }
        return self._safe_send(msg) == "OK"

    def is_position_processed(self, request_id, position):
        msg = {
            "action": "is_position_processed",
            "worker_type": self.worker_type,
            "request_id": request_id,
            "position": position
        }
        return self._safe_send(msg) == True
