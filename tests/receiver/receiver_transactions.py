import os
import json
import logging
from datetime import datetime
from shared import protocol
from shared.worker import Worker

OUTPUT_DIR = "/app/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


class ReceiverWorker(Worker):
    """
    Worker que recibe mensajes procesados y los guarda en un archivo JSON,
    registrando también los END para verificar determinismo.
    """
    def __init__(self, queue_in, rabbitmq_host):
        super().__init__(queue_in=queue_in, queue_out=None, rabbitmq_host=rabbitmq_host)
        self.received_data = []
        self.output_path = os.path.join(
            OUTPUT_DIR, f"receiver_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

    def _process_message(self, message, msg_type, data_type, request_id, timestamp, payload, queue_name=None):
        payload_str = payload.decode("utf-8", errors="ignore")
        record = {
            "msg_type": msg_type,
            "data_type": data_type,
            "request_id": request_id,
            "payload": payload_str,
        }

        logging.info(f"[RECEIVER] DATA recibido ({len(payload)} bytes) → {payload_str}")
        self.received_data.append(record)
        self._flush_to_disk()

    def _handle_end_signal(self, message, msg_type, data_type, request_id, queue_name=None):
        """Sobrescribe el manejo base del END para loguearlo y guardarlo."""
        logging.info(f"[RECEIVER] END recibido para request_id={request_id}")

        record = {
            "msg_type": msg_type,
            "data_type": data_type,
            "request_id": request_id,
            "payload": "END"
        }
        self.received_data.append(record)
        self._flush_to_disk()

        # No hay colas downstream, pero igual llamamos al padre
        # (por consistencia, marca el END como procesado)
        super()._handle_end_signal(message, msg_type, data_type, request_id, queue_name)

    def _flush_to_disk(self):
        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(self.received_data, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    Worker.setup_logging()
    queue_in = os.getenv("QUEUE_IN", "transactions_cleaned")
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")

    receiver = ReceiverWorker(queue_in=queue_in, rabbitmq_host=rabbitmq_host)
    receiver.run()
