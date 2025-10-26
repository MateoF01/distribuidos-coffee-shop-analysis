import os
import csv
import heapq
import logging
from glob import glob
from shared import protocol
from shared.worker import SignalProcessingWorker


class SorterV2(SignalProcessingWorker):
    """
    Sorter V2:
    - Espera una señal split_done (desde splitter_q1).
    - Fusiona y ordena todos los chunks de todas las réplicas.
    - Genera un único archivo final q1_sorted.csv.
    - Envía una notificación sort_done al siguiente componente (q1_sender).
    """

    def __init__(self, queue_in, queue_out, rabbitmq_host, base_temp_root, sort_columns="0"):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.base_temp_root = base_temp_root
        self.sort_columns = [int(x) for x in sort_columns.split(",")]
        os.makedirs(self.base_temp_root, exist_ok=True)
        logging.info(f"[SorterV2] Inicializado - in={queue_in}, out={queue_out}, sort_columns={self.sort_columns}")

    # ======================================================
    # CORE: Ejecutado al recibir señal NOTI (split_done)
    # ======================================================
    def _process_signal(self, request_id):
        splitter_root = os.path.join(self.base_temp_root, "splitter_q1", str(request_id))
        pattern = os.path.join(splitter_root, "*", "chunk_*.csv")
        chunk_files = sorted(glob(pattern))

        if not chunk_files:
            logging.error(f"[SorterV2] ❌ No hay chunks para request {request_id} en {splitter_root}")
            return

        output_dir = os.path.join("/app/output", str(request_id))
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "q1.csv")

        logging.info(f"[SorterV2] 🔄 Ordenando {len(chunk_files)} chunks -> {output_path}")

        heap = []
        file_handles = []

        # Abrir todos los chunks y cargar la primera línea de cada uno al heap
        for i, path in enumerate(chunk_files):
            try:
                f = open(path, "r", encoding="utf-8")
                reader = csv.reader(f)
                file_handles.append(f)
                row = next(reader, None)
                if row:
                    key = tuple(row[c] for c in self.sort_columns)
                    heapq.heappush(heap, (key, i, row, reader))
            except Exception as e:
                logging.error(f"[SorterV2] Error abriendo {path}: {e}")

        # Merge sort
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
                except Exception as e:
                    logging.error(f"[SorterV2] Error leyendo {chunk_files[idx]}: {e}")

        for f in file_handles:
            if not f.closed:
                f.close()

        logging.info(f"[SorterV2] ✅ Orden completado para request {request_id}")
        self._notify_completion(protocol.DATA_TRANSACTIONS, request_id)

    # ======================================================
    # 🧾 Notificación de finalización (ya la trae SignalProcessingWorker)
    # ======================================================
    # Usa self._notify_completion(data_type, request_id)


# ==============================================================
# 🏁 MAIN
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

        return SorterV2(queue_in, queue_out, rabbitmq_host, base_temp_root, sort_columns)

    SorterV2.run_worker_main(create_sorter_v2)
