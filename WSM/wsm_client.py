import socket
import json
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class WSMClient:
    def __init__(self, replica_id, host="wsm", port=9000, retry_interval=2):
        """
        Cliente para comunicarse con el Worker State Manager (WSM).

        Args:
            replica_id (str): Identificador único de la réplica (e.g. "cleaner_1")
            host (str): Host o IP del WSM (por ejemplo, el nombre del servicio en Docker)
            port (int): Puerto TCP del WSM
            retry_interval (int): Tiempo en segundos entre reintentos de conexión
        """
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

    # -------------------------------
    # 🧱 Métodos de alto nivel
    # -------------------------------
    def _register(self):
        """Registra el worker al iniciar."""
        msg = {"action": "register", "replica_id": self.replica_id}
        resp = self._send_message(msg)
        if resp == "OK":
            logging.info(f"[WSMClient:{self.replica_id}] Registrado en WSM")
        else:
            logging.warning(f"[WSMClient:{self.replica_id}] No se pudo registrar ({resp})")

    def update_state(self, state, request_id=None):
        """
        Actualiza el estado de la réplica.
        Posibles valores de state: WAITING, PROCESSING, END
        """
        msg = {
            "action": "update_state",
            "replica_id": self.replica_id,
            "state": state,
            "request_id": request_id
        }
        resp = self._send_message(msg)
        logging.info(f"[WSMClient:{self.replica_id}] Estado actualizado: {state} ({request_id}) → {resp}")
        return resp

    def can_send_end(self, request_id):
        """
        Pregunta al WSM si puede enviar el mensaje END.
        Devuelve True si puede, False si debe esperar.
        """
        msg = {"action": "can_send_end", "request_id": request_id}
        resp = self._send_message(msg)
        logging.info(f"[WSMClient:{self.replica_id}] Consulta END({request_id}) → {resp}")
        return resp == "OK"
