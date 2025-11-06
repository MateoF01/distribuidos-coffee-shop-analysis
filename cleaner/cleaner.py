import os
import socket
import time
import logging
import configparser
from shared import protocol
from shared.worker import StreamProcessingWorker
from WSM.wsm_client import WSMClient


class Cleaner(StreamProcessingWorker):
    def __init__(self, queue_in, queue_out, columns_have, columns_want, rabbitmq_host, keep_when_empty=None):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.columns_have = columns_have
        self.columns_want = columns_want
        self.keep_indices = [self.columns_have.index(col) for col in self.columns_want]
        self.keep_when_empty = [self.columns_want.index(col) for col in keep_when_empty] if keep_when_empty else []

        # 🔗 Conexión con el Worker State Manager
        replica_id = socket.gethostname()
        wsm_host = os.environ.get("WSM_HOST", "wsm")
        wsm_port = int(os.environ.get("WSM_PORT", "9000"))
        self.wsm_client = WSMClient(
            worker_type="cleaner",
            replica_id=replica_id,
            host=wsm_host,
            port=wsm_port
        )

        logging.info(f"[Cleaner:{replica_id}] Inicializado - input: {queue_in}, output: {queue_out}")

    # ------------------------------------------------------------
    # 🔁 Lógica de procesamiento normal (DATA)
    # ------------------------------------------------------------
    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
        """Procesa mensajes de datos (no END)."""

        #VALIDO QUE LA POSICION HAYA SIDO PROCESADA ANTERIORMENTE, SI LO DESCARTO EL MENSAJE
        if self.wsm_client.is_position_processed(request_id, position):
            logging.info(f"🔁 Mensaje duplicado detectado ({request_id}:{position}), descartando...")
            return


        # 1️⃣ Marcar inicio de procesamiento
        self.wsm_client.update_state("PROCESSING", request_id, position)

        rows = payload.decode("utf-8").split("\n")
        cleaned_rows = self._process_rows(rows)

        if cleaned_rows:
            new_payload = "\n".join(cleaned_rows).encode("utf-8")
            new_msg = protocol.pack_message(msg_type, data_type, new_payload, request_id, position)
            for q in self.out_queues:
                q.send(new_msg)

        # 2️⃣ Marcar fin de procesamiento
        self.wsm_client.update_state("WAITING", request_id, position)

    # ------------------------------------------------------------
    # 🧩 Lógica de END sincronizado (sobrescribe el padre)
    # ------------------------------------------------------------
    def _handle_end_signal(self, message, msg_type, data_type, request_id, position, queue_name=None):
        """
        Extiende el manejo base del END.
        Primero sincroniza con el WSM, y luego llama a la implementación del padre,
        que reenvía el END automáticamente a las colas de salida.
        """
        # Registrar estado END en el WSM
        self.wsm_client.update_state("END", request_id, position)
        logging.info(f"[Cleaner] Recibido END para request {request_id}. Consultando WSM...")

        # Esperar permiso del WSM para enviar END
        while not self.wsm_client.can_send_end(request_id, position):
            logging.info(f"[Cleaner] Esperando permiso para reenviar END de {request_id}...")
            time.sleep(1)

        logging.info(f"[Cleaner] ✅ Permiso otorgado para enviar END de {request_id}")

        # Llamar al manejo normal del END (reenvío a colas de salida)
        super()._handle_end_signal(message, msg_type, data_type, request_id, position, queue_name)

        # Volver a estado de espera
        self.wsm_client.update_state("WAITING", request_id, position)

    # ------------------------------------------------------------
    # 🧽 Limpieza de datos
    # ------------------------------------------------------------
    def _process_rows(self, rows, queue_name=None):
        """Filtra y limpia las filas del dataset."""
        filtered_rows = []
        for row in rows:
            if row.strip():
                cleaned_row = self._filter_row(row)
                if cleaned_row:
                    filtered_rows.append(cleaned_row)
        return filtered_rows

    def _filter_row(self, row):
        items = row.split('|')

        if len(items) < max(self.keep_indices) + 1:
            logging.warning(f"Row has insufficient columns: {row}")
            return None

        try:
            selected = [items[i] for i in self.keep_indices]
        except IndexError as e:
            logging.error(f"Index error processing row: {row} - {e}")
            return None

        # Convertir user_id a int si corresponde
        if 'user_id' in self.columns_want:
            user_id_idx = self.columns_want.index('user_id')
            if selected[user_id_idx] != '':
                try:
                    selected[user_id_idx] = str(int(float(selected[user_id_idx])))
                except Exception as e:
                    logging.warning(f"No se pudo convertir user_id '{selected[user_id_idx]}' a int: {e}")

        # Eliminar filas vacías si no están en keep_when_empty
        if any(selected[i] == '' and i not in self.keep_when_empty for i in range(len(selected))):
            return None

        return '|'.join(selected)


# ------------------------------------------------------------
# 🚀 Entry point del worker
# ------------------------------------------------------------
if __name__ == '__main__':
    def create_cleaner():
        queue_in = os.environ.get('QUEUE_IN')
        queue_out = os.environ.get('QUEUE_OUT')
        data_type = os.environ.get('DATA_TYPE')
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)

        if data_type not in config:
            raise ValueError(f"Unknown data type: {data_type}")

        columns_have = [col.strip() for col in config[data_type]['have'].split(',')]
        columns_want = [col.strip() for col in config[data_type]['want'].split(',')]
        keep_when_empty_str = config[data_type].get('keep_when_empty', '').strip()
        keep_when_empty = [col.strip() for col in keep_when_empty_str.split(',')] if keep_when_empty_str else None

        return Cleaner(queue_in, queue_out, columns_have, columns_want, rabbitmq_host, keep_when_empty)

    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Cleaner.run_worker_main(create_cleaner, config_path)
