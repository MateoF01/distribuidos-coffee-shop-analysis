import os
import csv
import logging
import gc
from collections import defaultdict
from shared.worker import SignalProcessingWorker


class ReducerV2(SignalProcessingWorker):
    """
    Reducer para Q2.
    Combina los resultados parciales del GrouperV2, agrupando por prefijo de archivo (ej. '2024_01'),
    y genera un archivo reducido por grupo dentro del output_dir.
    """

    def __init__(self, queue_in, queue_out, rabbitmq_host, input_dir, output_dir, reducer_mode="q2"):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.reducer_mode = reducer_mode.lower()

        os.makedirs(self.output_dir, exist_ok=True)

    # ======================================================
    # CORE: Ejecutado al recibir señal NOTI
    # ======================================================

    def _process_signal(self):
        if not os.path.exists(self.input_dir):
            logging.warning(f"[Reducer Q2] Input directory not found: {self.input_dir}")
            return

        logging.info(f"[Reducer Q2] Iniciando reducción en {self.input_dir}")

        # Agrupar archivos por prefijo (antes del último '_')
        groups = defaultdict(list)
        for filename in os.listdir(self.input_dir):
            if not filename.endswith(".csv"):
                continue
            prefix = "_".join(filename.split("_")[:-1])  # ej. 2024_01_hostA -> 2024_01
            groups[prefix].append(os.path.join(self.input_dir, filename))

        logging.info(f"[Reducer Q2] Se encontraron {len(groups)} grupos.")

        for prefix, filepaths in groups.items():
            combined = self._reduce_q2(filepaths)
            output_path = os.path.join(self.output_dir, f"{prefix}.csv")

            try:
                with open(output_path, "w", newline="") as f:
                    writer = csv.writer(f)
                    for row in combined:
                        writer.writerow(row)
                logging.info(f"[Reducer Q2] Grupo {prefix} reducido correctamente ({len(combined)} filas).")
            except Exception as e:
                logging.error(f"[Reducer Q2] Error al escribir {output_path}: {e}")

        gc.collect()
        logging.info("[Reducer Q2] Reducción completa de todos los grupos.")

    # ======================================================
    # Lógica de reducción Q2
    # ======================================================

    def _reduce_q2(self, filepaths):
        """
        Merge de archivos parciales:
        item_id, quantity, subtotal → suma total por item_id.
        """
        combined = defaultdict(lambda: [0, 0.0])

        for fpath in filepaths:
            try:
                with open(fpath, "r") as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if len(row) != 3:
                            continue
                        item_id, qty, subtotal = row
                        combined[item_id][0] += int(qty)
                        combined[item_id][1] += float(subtotal)
            except Exception as e:
                logging.error(f"[Reducer Q2] Error leyendo {fpath}: {e}")

        return [(item_id, qty, subtotal) for item_id, (qty, subtotal) in combined.items()]


# ======================================================
# MAIN
# ======================================================

if __name__ == "__main__":
    import configparser

    def create_reducer_v2():
        config_path = os.path.join(os.path.dirname(__file__), "config.ini")
        config = configparser.ConfigParser()
        config.read(config_path)

        rabbitmq_host = os.environ.get("RABBITMQ_HOST", "rabbitmq")
        queue_in = os.environ.get("QUEUE_IN")
        queue_out = os.environ.get("COMPLETION_QUEUE")
        input_dir = os.environ.get("INPUT_DIR", "/app/temp/grouper_q2")
        output_dir = os.environ.get("OUTPUT_DIR", "/app/temp/reduced_q2")

        return ReducerV2(queue_in, queue_out, rabbitmq_host, input_dir, output_dir, reducer_mode="q2")

    ReducerV2.run_worker_main(create_reducer_v2)
