import os
import csv
import heapq
import logging
from glob import glob
from shared import protocol
from shared.worker import StreamProcessingWorker


class SorterV2(StreamProcessingWorker):
    """
    Sorter V2:
    - Espera una señal split_done (desde splitter_q1).
    - Fusiona y ordena todos los chunks.
    - Genera un único archivo final q1_sorted.csv.
    - Notifica al siguiente componente (q1_sender).
    """

    def __init__(self, queue_in, queue_out, rabbitmq_host, base_temp_root, sort_columns):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.base_temp_root = base_temp_root
        self.sort_columns = [int(x) for x in sort_columns.split(",")]
        logging.info(f"[SorterV2] Inicializado - in={queue_in}, out={queue_out}")

    # ------------------------------------------------------------
    # 🔔 Recepción de mensajes
    # ------------------------------------------------------------
    def _process_message(self, message, msg_type, data_type,
                         request_id, timestamp, payload, queue_name=None):
        if msg_type != protocol.MSG_TYPE_NOTIFICATION:
            logging.warning(f"[SorterV2] Mensaje inesperado tipo {msg_type}")
            return

        logging.info(f"[SorterV2] Señal recibida split_done para request {request_id}")
        self._process_sort(request_id)
        self._send_completion_signal(request_id)

    # ------------------------------------------------------------
    # 🧠 Ordenamiento y merge de chunks
    # ------------------------------------------------------------
    def _process_sort(self, request_id):
        splitter_root = os.path.join(self.base_temp_root, "splitter_q1", str(request_id))
        pattern = os.path.join(splitter_root, "*", "chunk_*.csv")
        chunk_files = sorted(glob(pattern))

        if not chunk_files:
            logging.error(f"[SorterV2] ❌ No hay chunks para request {request_id}")
            return

        output_dir = os.path.join(self.base_temp_root, "q1_sorted", str(request_id))
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "q1_sorted.csv")

        logging.info(f"[SorterV2] 🔄 Ordenando {len(chunk_files)} chunks -> {output_path}")

        heap = []
        file_handles = []

        # abrir todos los chunks y cargar la primera línea de cada uno al heap
        for i, path in enumerate(chunk_files):
            f = open(path, "r", encoding="utf-8")
            reader = csv.reader(f)
            file_handles.append(f)
            try:
                row = next(reader)
                key = tuple(row[c] for c in self.sort_columns)
                heapq.heappush(heap, (key, i, row, reader))
            except StopIteration:
                f.close()

        # merge sort
        with open(output_path, "w", newline="", encoding="utf-8") as out:
            writer = csv.writer(out)
            while heap:
                key, idx, row, reader = heapq.heappop(heap)
                writer.writerow(row)
                try:
                    nxt = next(reader)
                    nxt_key = tuple(nxt[c] for c in self.sort_columns)
                    heapq.heappush(heap, (nxt_key, idx, nxt, reader))
                except StopIteration:
                    file_handles[idx].close()

        logging.info(f"[SorterV2] ✅ Orden completado para request {request_id}")

        # cerrar todos los archivos
        for f in file_handles:
            if not f.closed:
                f.close()

    # ------------------------------------------------------------
    # 📤 Notificación final
    # ------------------------------------------------------------
    def _send_completion_signal(self, request_id):
        if self.out_queues:
            payload = f"sort_done;request={request_id}".encode("utf-8")
            msg = protocol.create_notification_message(
                protocol.MSG_TYPE_NOTIFICATION, payload, request_id
            )
            for q in self.out_queues:
                q.send(msg)
            logging.info(f"[SorterV2] sort_done enviado para request {request_id}")
        else:
            logging.warning("[SorterV2] Sin cola de salida configurada.")


# ==============================================================
# 🏁 Main
# ==============================================================
if __name__ == "__main__":
    def create_sorter_v2():
        rabbitmq_host = os.environ.get("RABBITMQ_HOST", "rabbitmq")
        queue_in = os.environ.get("QUEUE_IN")
        queue_out = os.environ.get("COMPLETION_QUEUE")
        base_temp_root = os.environ.get("BASE_TEMP_DIR", "/app/temp")
        sort_columns = os.environ.get("SORT_COLUMNS", "0")

        if not queue_in or not queue_out:
            raise ValueError("QUEUE_IN y COMPLETION_QUEUE son requeridos")

        return SorterV2(queue_in, queue_out, rabbitmq_host,
                        base_temp_root, sort_columns)

    SorterV2.run_worker_main(create_sorter_v2)
