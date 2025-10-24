import os
import socket
import time
import logging
import configparser
from collections import defaultdict
from shared import protocol
from shared.worker import StreamProcessingWorker
from WSM.wsm_client import WSMClient


class SplitterQ1(StreamProcessingWorker):
    """
    Worker Splitter para Q1:
    - Lee filas desde una cola (replicable).
    - Genera archivos chunk por request_id.
    - Notifica a sorter_v2 cuando TODAS las réplicas finalizaron (vía WSM).
    """

    def __init__(self, queue_in, queue_out, rabbitmq_host, chunk_size, replica_id):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.replica_id = replica_id
        self.chunk_size = int(chunk_size)

        # base de temporales (puede venir por env, tiene default)
        self.base_temp_root = os.environ.get(
            'BASE_TEMP_DIR',
            os.path.join(os.path.dirname(__file__), 'temp')
        )

        # estado por request
        # buffers[request_id] -> {"rows": [...], "count": int, "dir": str, "chunk_idx": int}
        self.buffers = defaultdict(lambda: {
            "rows": [],
            "count": 0,
            "dir": None,
            "chunk_idx": 0
        })

        # WSM
        wsm_host = os.environ.get("WSM_HOST", "wsm")
        wsm_port = int(os.environ.get("WSM_PORT", "9000"))
        self.wsm_client = WSMClient(
            worker_type="splitter_q1",
            replica_id=replica_id,
            host=wsm_host,
            port=wsm_port
        )

        logging.info(f"[SplitterQ1:{self.replica_id}] init - in={queue_in}, out={queue_out}, chunk_size={self.chunk_size}")

    # ------------------------------------------------------------
    # Rutas por request
    # ------------------------------------------------------------
    def _ensure_request_dir(self, request_id):
        """
        Crea una carpeta por request_id y por réplica:
        BASE_TEMP_DIR/splitter_q1/<request_id>/<replica_id>/
        """
        buf = self.buffers[request_id]
        if buf["dir"]:
            return buf["dir"]

        req_dir = os.path.join(self.base_temp_root, str(request_id), self.replica_id)
        os.makedirs(req_dir, exist_ok=True)
        buf["dir"] = req_dir
        logging.info(f"[SplitterQ1:{self.replica_id}] dir ready for request {request_id}: {req_dir}")
        return req_dir

    # ------------------------------------------------------------
    # Escritura de chunks
    # ------------------------------------------------------------
    def _write_chunk(self, request_id):
        """
        Escribe el buffer actual a disco como chunk_<idx>.csv
        """
        buf = self.buffers[request_id]
        if buf["count"] == 0:
            return  # nada que escribir

        req_dir = self._ensure_request_dir(request_id)
        filename = f"chunk_{buf['chunk_idx']}.csv"
        path = os.path.join(req_dir, filename)

        # guardamos las filas tal cual llegan (una por línea)
        # la línea viene del protocolo como texto (por ej '|' separado). El sorter_v2
        # luego leerá estas filas para ordenar.
        with open(path, "w", newline="", encoding="utf-8") as f:
            for line in buf["rows"]:
                # aseguramos newline por cada fila
                if line.endswith("\n"):
                    f.write(line)
                else:
                    f.write(line + "\n")

        logging.info(f"[SplitterQ1:{self.replica_id}] wrote {filename} ({buf['count']} rows) for request {request_id}")

        # reset estado
        buf["rows"].clear()
        buf["count"] = 0
        buf["chunk_idx"] += 1

    def _append_row(self, request_id, row_text):
        """
        Agrega una fila al buffer; si llega al chunk_size, rota y escribe chunk.
        """
        buf = self.buffers[request_id]
        buf["rows"].append(row_text)
        buf["count"] += 1

        if buf["count"] >= self.chunk_size:
            self._write_chunk(request_id)

    # ------------------------------------------------------------
    # Ciclo de mensajes
    # ------------------------------------------------------------
    def _process_message(self, message, msg_type, data_type, request_id, timestamp, payload, queue_name=None):

        # inicializo dir al primer mensaje del request
        self._ensure_request_dir(request_id)

        # estado WSM
        self.wsm_client.update_state("PROCESSING", request_id)

        # proceso (esto invocará _process_rows / _handle_end_signal)
        super()._process_message(message, msg_type, data_type, request_id, timestamp, payload, queue_name)

        # listo por ahora
        self.wsm_client.update_state("WAITING")

    # ------------------------------------------------------------
    # Filas de datos
    # ------------------------------------------------------------
    def _process_rows(self, rows, queue_name=None):
        """
        Recibe filas decodificadas (texto), las agrupa en chunks y las escribe a disco.
        """

        request_id = self.current_request_id  # viene desde la superclase
        self._ensure_request_dir(request_id)

        for row in rows:
            row = row.strip()
            if not row:
                continue
            self._append_row(request_id, row)


    # ------------------------------------------------------------
    # END + sincronización con WSM
    # ------------------------------------------------------------
    def _handle_end_signal(self, message, msg_type, data_type, request_id, queue_name=None):
        """
        - Flushea el último chunk del request.
        - Marca END en WSM.
        - Espera hasta que WSM diga que se puede enviar END downstream.
        - Envía notificación al sorter_v2 (COMPLETION_QUEUE).
        """
        # flush final de lo pendiente
        self._write_chunk(request_id)

        # marcamos END local
        self.wsm_client.update_state("END", request_id)
        logging.info(f"[SplitterQ1:{self.replica_id}] END recibido para request {request_id}. Esperando permiso WSM...")

        # esperar permiso global del WSM (todas las réplicas listas)
        while not self.wsm_client.can_send_end(request_id):
            time.sleep(1)

        logging.info(f"[SplitterQ1:{self.replica_id}] ✅ WSM autorizó END para request {request_id}. Notificando sorter_v2...")

        # enviamos notificación downstream (cola del sorter_v2)
        if self.out_queues:
            payload = f"split_done;replica={self.replica_id}".encode("utf-8")
            noti = protocol.create_notification_message(data_type, payload, request_id)
            for q in self.out_queues:
                q.send(noti)

        # dejamos al worker en WAITING
        self.wsm_client.update_state("WAITING")

        # limpiamos buffers del request (opcional, si no se reusa)
        if request_id in self.buffers:
            del self.buffers[request_id]


# ====================
# Main
# ====================
if __name__ == '__main__':
    def create_splitter():
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        queue_in = os.environ.get('QUEUE_IN')
        queue_out = os.environ.get('COMPLETION_QUEUE')
        replica_id = socket.gethostname()
        chunk_size = int(os.environ.get('CHUNK_SIZE', 10000))
        return SplitterQ1(queue_in, queue_out, rabbitmq_host, chunk_size, replica_id)

    SplitterQ1.run_worker_main(create_splitter)
