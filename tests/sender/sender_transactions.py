import os
import time
import logging
from shared import protocol
from shared.worker import Worker

class SenderWorker(Worker):
    """
    Sender que envía batches reales de data desde data_full/transactions/
    a la cola indicada y luego manda el END.
    """

    def __init__(self, queue_out, rabbitmq_host, data_dir="data_full/transactions", batch_size=1):
        super().__init__(queue_in=None, queue_out=queue_out, rabbitmq_host=rabbitmq_host)
        self.request_id = 1
        self.data_type = protocol.DATA_TRANSACTIONS
        self.data_dir = data_dir
        self.batch_size = batch_size

    def _load_transaction_data(self):
        """Lee todos los CSV de data_full/transactions y devuelve una lista de filas."""
        all_rows = []
        if not os.path.exists(self.data_dir):
            logging.error(f"[SENDER] Carpeta no encontrada: {self.data_dir}")
            return all_rows

        for filename in sorted(os.listdir(self.data_dir)):
            if filename.endswith(".csv"):
                file_path = os.path.join(self.data_dir, filename)
                logging.info(f"[SENDER] Leyendo {file_path}...")
                with open(file_path, "r", encoding="utf-8") as f:
                    next(f, None)  # omitir encabezado si existe
                    for line in f:
                        line = line.strip()
                        if line:
                            all_rows.append(line)
        logging.info(f"[SENDER] Total de filas cargadas: {len(all_rows)}")
        return all_rows

    def run(self):
        """Override run() para enviar los datos y luego el END."""
        logging.info("[SENDER] Iniciando envío de batches al cleaner...")

        rows = self._load_transaction_data()
        if not rows:
            logging.warning("[SENDER] No se encontraron filas para enviar.")
            return

        total = len(rows)
        batch_count = 0

        for i in range(0, total, self.batch_size):
            batch_rows = rows[i:i+self.batch_size]
            payload = "\n".join(batch_rows).encode("utf-8")
            msg = protocol.pack_message(protocol.MSG_TYPE_DATA, self.data_type, payload, self.request_id)

            for q in self.out_queues:
                q.send(msg)

            batch_count += 1
            logging.info(f"[SENDER] Enviado batch {batch_count} ({len(batch_rows)} filas)")
            time.sleep(0.1)

        # END message
        end_msg = protocol.create_end_message(self.data_type, self.request_id)
        for q in self.out_queues:
            q.send(end_msg)

        logging.info(f"[SENDER] ✅ Todos los mensajes enviados. Total batches: {batch_count}")
        time.sleep(1) #Esto es para que el test mande el end sin cerrar el canal antes

    def _process_message(self, *args, **kwargs):
        """No recibe mensajes, solo envía."""
        pass


if __name__ == "__main__":
    Worker.setup_logging()
    sender = SenderWorker(
        queue_out="transactions_queue",
        rabbitmq_host="rabbitmq",
        data_dir="data/transactions"
    )
    sender.run()
