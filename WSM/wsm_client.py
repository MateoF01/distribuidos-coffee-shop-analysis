import socket
import json
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class WSMClient:
    def __init__(self, worker_type, replica_id, host="wsm", port=9000, retry_interval=2):
        self.worker_type = worker_type
        self.replica_id = replica_id
        self.host = host
        self.port = port
        self.retry_interval = retry_interval
        self._register()


    # -------------------------------
    # 🔌 Comunicación base
    # -------------------------------
    def _send_message(self, message):
        """
        Envía un mensaje JSON al WSM y espera una respuesta.
        Devuelve el campo 'response' como string (OK, WAIT, ERROR...).
        """
        for attempt in range(3):
            try:
                with socket.create_connection((self.host, self.port), timeout=5) as sock:
                    sock.sendall(json.dumps(message).encode())
                    data = sock.recv(4096)
                    if not data:
                        raise ConnectionError("Sin respuesta del WSM")
                    response = json.loads(data.decode()).get("response")
                    return response
            except (ConnectionRefusedError, TimeoutError, OSError) as e:
                logging.warning(f"[WSMClient] Intento {attempt+1}: Error comunicando con WSM ({e})")
                time.sleep(self.retry_interval)
        logging.error("[WSMClient] No se pudo comunicar con el WSM después de 3 intentos")
        return "ERROR"

    def _register(self):
        msg = {
            "action": "register",
            "worker_type": self.worker_type,
            "replica_id": self.replica_id
        }
        self._send_message(msg)

    def update_state(self, state, request_id=None, position=None):
        msg = {
            "action": "update_state",
            "worker_type": self.worker_type,
            "replica_id": self.replica_id,
            "state": state,
            "request_id": request_id,
            "position": position
        }
        return self._send_message(msg)

    def can_send_end(self, request_id, position):
        msg = {
            "action": "can_send_end",
            "worker_type": self.worker_type,
            "request_id": request_id,
            "position": position
        }
        return self._send_message(msg) == "OK"

    def can_send_last_end(self, request_id):
        msg = {
            "action": "can_send_last_end",
            "replica_id": self.replica_id,
            "worker_type": self.worker_type,
            "request_id": request_id
        }
        return self._send_message(msg) == "OK"
