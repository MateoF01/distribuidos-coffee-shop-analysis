import socket
import json
import logging
import time
from typing import List, Tuple, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class WSMClient:
    """
    Cliente para el Worker State Manager con soporte de múltiples nodos.

    - Recibe una lista de nodos (host, port).
    - Pregunta a cada uno si es líder con `{"action": "is_leader"}`.
    - Se conecta sólo al líder.
    - Si la conexión se cae o el servidor deja de ser líder, vuelve a buscar.
    """

    def __init__(
        self,
        worker_type: str,
        replica_id: str,
        host: str = "wsm",
        port: int = 9000,
        nodes: Optional[List[Tuple[str, int]]] = None,
        probe_interval: float = 1.0,
    ):
        """
        Args:
            worker_type: tipo de worker (cleaner, grouper, etc.)
            replica_id: identificación de la réplica
            host, port: se mantienen para compatibilidad; si `nodes` es None,
                        se usa [(host, port)] como única entrada.
            nodes: lista de (host, port) de todos los WSM de este grupo.
            probe_interval: tiempo (segundos) entre intentos de descubrimiento de líder.
        """
        self.worker_type = worker_type
        self.replica_id = replica_id
        self.probe_interval = probe_interval

        # Lista de nodos disponibles (líder + backups)
        if nodes is None:
            self.nodes: List[Tuple[str, int]] = [(host, port)]
        else:
            self.nodes = nodes

        self.sock: Optional[socket.socket] = None
        self.current_node: Optional[Tuple[str, int]] = None

        # Conectar y registrar la réplica
        self._connect_to_leader()
        self._register()

    # ======================================================
    # DESCUBRIMIENTO DE LÍDER
    # ======================================================
    def _find_leader_once(self) -> Optional[Tuple[str, int]]:
        """
        Hace un barrido por todos los nodos y devuelve el primero que responda
        is_leader = YES. Si ninguno lo es o no responde, devuelve None.
        """
        probe_msg = json.dumps({"action": "is_leader"}).encode("utf-8")

        for host, port in self.nodes:
            try:
                with socket.create_connection((host, port), timeout=0.5) as s:
                    s.sendall(probe_msg)
                    data = s.recv(1024)
                    if not data:
                        continue

                    resp = json.loads(data.decode("utf-8")).get("response")
                    if resp == "YES":
                        logging.info(f"[WSMClient] Líder detectado en {host}:{port}")
                        return (host, port)
                    # Si responde "NO" o cualquier otra cosa, sigo probando
            except Exception as e:
                logging.debug(f"[WSMClient] Nodo {host}:{port} no responde como líder ({e})")

        return None

    def _connect_to_leader(self):
        """
        Loop bloqueante hasta encontrar un líder accesible y conectar.
        """
        while True:
            leader = self._find_leader_once()
            if leader is None:
                logging.warning("[WSMClient] No se encontró líder en ningún nodo, reintentando...")
                time.sleep(self.probe_interval)
                continue

            host, port = leader
            try:
                self.sock = socket.create_connection((host, port), timeout=5)
                self.current_node = leader
                logging.info(f"[WSMClient] Conectado al líder {host}:{port}")
                return
            except Exception as e:
                logging.warning(f"[WSMClient] No se pudo conectar al líder {host}:{port} ({e}), reintentando...")
                self._reset_connection()
                time.sleep(self.probe_interval)

    def _reset_connection(self):
        """
        Cierra el socket actual y limpia estado local de conexión.
        """
        if self.sock is not None:
            try:
                self.sock.close()
            except Exception:
                pass
        self.sock = None
        self.current_node = None

    # ======================================================
    # ENVÍO SEGURO (REINTENTOS + REELECCIÓN)
    # ======================================================
    def _safe_request(self, msg: dict):
        """
        Envía un mensaje al WSM líder actual.
        - Si la conexión se cae → busca un nuevo líder y reintenta.
        - Si el servidor responde "NOT_LEADER" → fuerza redescubrimiento de líder.
        """
        payload = json.dumps(msg).encode("utf-8")

        while True:
            print("INICIO ENVIO DE MENSAJE: ", payload)

            # Asegurarse de estar conectado a algún líder
            if self.sock is None:
                print("No tengo la conexion abierta, busco lider...")
                self._connect_to_leader()
                print("Me conecté al nuevo lider: ", self.current_node)
                print("Y tengo que enviar el payload pendiente: ", payload)

            try:
                self.sock.sendall(payload)
                data = self.sock.recv(4096)
                if not data:
                    raise ConnectionError("WSM cerró la conexión")

                response = json.loads(data.decode("utf-8")).get("response")
                print("RESPONSE: ", response)

                if response == "NOT_LEADER":
                    logging.warning("[WSMClient] Nodo actual dejó de ser líder, redescubriendo líder...")
                    self._reset_connection()
                    continue  # vuelve al while, encuentra nuevo líder y reenvía

                return response

            except Exception as e:
                logging.warning(f"[WSMClient] Error de conexión con el líder ({e}), buscando nuevo líder...")
                self._reset_connection()
                # el loop continúa, se reconecta a un líder y reintenta

    # ======================================================
    # API DE ALTO NIVEL (COMPATIBLE CON LA VERSIÓN ANTERIOR)
    # ======================================================
    def _register(self):
        """
        Registra la réplica en el WSM (no es crítico si falla; update_state crea estado).
        """
        msg = {
            "action": "register",
            "worker_type": self.worker_type,
            "replica_id": self.replica_id
        }
        try:
            self._safe_request(msg)
        except Exception as e:
            # No frenes todo si el register falla, el sistema puede funcionar igual.
            logging.warning(f"[WSMClient] Error registrando worker ({e}), se continuará igual.")

    def update_state(self, state, request_id=None, position=None):
        msg = {
            "action": "update_state",
            "worker_type": self.worker_type,
            "replica_id": self.replica_id,
            "state": state,
            "request_id": request_id,
            "position": position
        }
        return self._safe_request(msg)

    def can_send_end(self, request_id, position):
        msg = {
            "action": "can_send_end",
            "worker_type": self.worker_type,
            "request_id": request_id,
            "position": position
        }
        return self._safe_request(msg) == "OK"

    def can_send_last_end(self, request_id):
        msg = {
            "action": "can_send_last_end",
            "replica_id": self.replica_id,
            "worker_type": self.worker_type,
            "request_id": request_id
        }
        return self._safe_request(msg) == "OK"

    def is_position_processed(self, request_id, position):
        msg = {
            "action": "is_position_processed",
            "worker_type": self.worker_type,
            "request_id": request_id,
            "position": position
        }
        return self._safe_request(msg) is True
