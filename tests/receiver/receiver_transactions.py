import os
import logging
from datetime import datetime
from shared import protocol
from shared.worker import Worker

class ReceiverWorker(Worker):
    """
    Worker de test que recibe mensajes del cleaner, los imprime
    y los guarda en un archivo local.
    """

    def __init__(self, queue_in, rabbitmq_host, output_dir="/app/output"):
        super().__init__(queue_in=queue_in, queue_out=None, rabbitmq_host=rabbitmq_host)
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.received_file = os.path.join(
            self.output_dir, f"receiver_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )
        self.state_file = os.path.join(self.output_dir, "receiver_state.txt")

    def _process_message(self, message, msg_type, data_type, request_id, timestamp, payload, queue_name=None):
        """Guarda cada mensaje recibido en un archivo."""
        payload_str = payload.decode("utf-8") if payload else "(sin payload)"
        log_entry = (
            f"[RECEIVER] msg_type={msg_type}, data_type={data_type}, "
            f"request_id={request_id}, rows={len(payload_str.splitlines())}\n"
        )

        # Mostrar por consola y guardar en archivo
        logging.info(log_entry.strip())
        with open(self.received_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
            f.write(payload_str + "\n")

        # Si es END, actualizar estado
        if msg_type == protocol.MSG_TYPE_END:
            logging.info(f"[RECEIVER] 🚀 END recibido para request_id={request_id}")
            with open(self.state_file, "a", encoding="utf-8") as f:
                f.write(f"END recibido para request_id={request_id}\n")

if __name__ == "__main__":
    Worker.setup_logging()
    receiver = ReceiverWorker(queue_in="transactions_cleaned", rabbitmq_host="rabbitmq")
    receiver.run()
