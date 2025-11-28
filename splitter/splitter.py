import os
import socket
import time
import logging
import configparser
from collections import defaultdict
from shared import protocol
from shared.worker import StreamProcessingWorker
from WSM.wsm_client import WSMClient
from wsm_config import WSM_NODES


class SplitterQ1(StreamProcessingWorker):
    """
    Worker Splitter para Q1:
    - Lee filas desde una cola (replicable).
    - Genera archivos chunk por request_id.
    - Notifica a sorter_v2 cuando TODAS las réplicas finalizaron (vía WSM).
    """

    def __init__(self, queue_in, queue_out, rabbitmq_host, chunk_size, replica_id, backoff_start=0.1, backoff_max=3.0, wsm_nodes = None):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.replica_id = replica_id
        self.chunk_size = int(chunk_size)
        self.backoff_start = backoff_start
        self.backoff_max = backoff_max

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
            port=wsm_port,
            nodes=wsm_nodes
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
    def _write_chunk(self, request_id, new_rows=None):
        """
        Carga el último chunk existente (si hay),
        le agrega las filas nuevas, las ordena y lo reescribe.
        Si el tamaño supera chunk_size, crea un nuevo chunk.
        """
        buf = self.buffers[request_id]
        req_dir = self._ensure_request_dir(request_id)

        # determinar último chunk existente
        chunk_idx = buf["chunk_idx"]
        filename = f"chunk_{chunk_idx}.csv"
        path = os.path.join(req_dir, filename)

        # leer filas previas si el archivo ya existe
        existing_rows = []
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                existing_rows = [ln.strip() for ln in f if ln.strip()]

        # agregar las nuevas
        if new_rows:
            existing_rows.extend(new_rows)

        # si supera el tamaño, guardar chunk actual y abrir nuevo
        if len(existing_rows) > self.chunk_size:
            # cortar las primeras chunk_size filas y mantener resto para el próximo chunk
            to_write = existing_rows[:self.chunk_size]
            remaining = existing_rows[self.chunk_size:]

            # ordenar y escribir chunk actual
            to_write.sort(key=lambda ln: ln.split(",")[0])
            with open(path, "w", encoding="utf-8", newline="") as f:
                for ln in to_write:
                    f.write(ln + "\n")

            # preparar nuevo archivo
            buf["chunk_idx"] += 1
            new_filename = f"chunk_{buf['chunk_idx']}.csv"
            new_path = os.path.join(req_dir, new_filename)

            # escribir el resto (ordenado también)
            remaining.sort(key=lambda ln: ln.split(",")[0])
            with open(new_path, "w", encoding="utf-8", newline="") as f:
                for ln in remaining:
                    f.write(ln + "\n")

        else:
            # ordenar y reescribir el mismo archivo
            existing_rows.sort(key=lambda ln: ln.split(",")[0])
            with open(path, "w", encoding="utf-8", newline="") as f:
                for ln in existing_rows:
                    f.write(ln + "\n")


    def _append_row(self, request_id, row_text):
        """
        Cada mensaje se escribe directo al último chunk (reordenando).
        """
        self._write_chunk(request_id, [row_text])


    # ------------------------------------------------------------
    # Ciclo de mensajes
    # ------------------------------------------------------------
    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):

        # inicializo dir al primer mensaje del request
        self._ensure_request_dir(request_id)


        #VALIDO QUE LA POSICION HAYA SIDO PROCESADA ANTERIORMENTE, SI YA FUE PROCESADA LO DESCARTO EL MENSAJE
        if self.wsm_client.is_position_processed(request_id, position):
            logging.info(f"🔁 Mensaje duplicado detectado ({request_id}:{position}), descartando...")
            return


        # estado WSM
        self.wsm_client.update_state("PROCESSING", request_id, position)        

        # proceso (esto invocará _process_rows / _handle_end_signal)
        super()._process_message(message, msg_type, data_type, request_id, position, payload, queue_name)

        # listo por ahora
        self.wsm_client.update_state("WAITING", request_id, position)

    # ------------------------------------------------------------
    # Filas de datos
    # ------------------------------------------------------------
    def _process_rows(self, rows, queue_name=None):
        """
        Recibe filas decodificadas (texto), extrae solo los primeros dos campos (guid, amount)
        y los guarda separados por coma en los chunks.
        """
        request_id = self.current_request_id  # viene desde la superclase
        self._ensure_request_dir(request_id)

        for row in rows:
            row = row.strip()
            if not row:
                continue

            # dividir por '|'
            parts = row.split('|')

            if len(parts) < 2:
                continue  # si no tiene al menos dos campos, lo ignoramos

            guid = parts[0].strip()
            amount = parts[1].strip()

            # construir línea CSV con coma
            formatted = f"{guid},{amount}"

            self._append_row(request_id, formatted)



    # ------------------------------------------------------------
    # END + sincronización con WSM
    # ------------------------------------------------------------
    def _handle_end_signal(self, message, msg_type, data_type, request_id, position, queue_name=None):
        """
        - Flushea el último chunk del request.
        - Marca END en WSM.
        - Espera hasta que WSM diga que se puede enviar END downstream.
        - Envía notificación al sorter_v2 (COMPLETION_QUEUE).
        """

        if(data_type == 6): #si es el mensaje de final de data lo salteo para que no se repitan dos ends de la misma request
            logging.info(f"[Splitter:{self.replica_id}] Ignorando END END para request {request_id}. ")
            return

        # flush final de lo pendiente
        self._write_chunk(request_id)

        # marcamos END local
        self.wsm_client.update_state("END", request_id, position)
        print(f"[{self.replica_id}] END")

        logging.info(f"[SplitterQ1:{self.replica_id}] END recibido para request {request_id}. Esperando permiso WSM...")

        # esperar permiso global del WSM con exponential backoff
        backoff = self.backoff_start
        total_wait = 0.0
        
        while not self.wsm_client.can_send_end(request_id, position):
            if total_wait >= self.backoff_max:
                error_msg = f"[SplitterQ1:{self.replica_id}] Timeout esperando permiso WSM para END de {request_id} después de {total_wait:.2f}s"
                logging.error(error_msg)
                raise TimeoutError(error_msg)
            
            logging.info(f"[SplitterQ1:{self.replica_id}] Esperando permiso... (backoff={backoff:.3f}s, total={total_wait:.2f}s)")
            time.sleep(backoff)
            total_wait += backoff
            backoff = min(backoff * 2, self.backoff_max - total_wait) if total_wait < self.backoff_max else 0

        logging.info(f"[SplitterQ1:{self.replica_id}] ✅ WSM autorizó END para request {request_id}. Notificando sorter_v2...")

        # enviamos notificación downstream (cola del sorter_v2)
        if self.out_queues:
            payload = f"split_done;replica={self.replica_id}".encode("utf-8")
            noti = protocol.create_notification_message(data_type, payload, request_id)
            for q in self.out_queues:
                q.send(noti)

        # dejamos al worker en WAITING
        self.wsm_client.update_state("WAITING", request_id, position)

        # limpiamos buffers del request (opcional, si no se reusa)
        if request_id in self.buffers:
            del self.buffers[request_id]


# ====================
# Main
# ====================
if __name__ == '__main__':
    def create_splitter():
        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)
        
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        queue_in = os.environ.get('QUEUE_IN')
        queue_out = os.environ.get('COMPLETION_QUEUE')
        replica_id = socket.gethostname()
        chunk_size = int(os.environ.get('CHUNK_SIZE', 10000))
        
        # Load backoff configuration from DEFAULT section
        backoff_start = float(config['DEFAULT'].get('BACKOFF_START', 0.1))
        backoff_max = float(config['DEFAULT'].get('BACKOFF_MAX', 3.0))
        
        key = 'q1' #Solo hay un tipo en el splliter
        wsm_nodes = WSM_NODES[key]
        print("WSM NODES: ", wsm_nodes)

        return SplitterQ1(queue_in, queue_out, rabbitmq_host, chunk_size, replica_id, backoff_start, backoff_max, wsm_nodes)

    SplitterQ1.run_worker_main(create_splitter)
