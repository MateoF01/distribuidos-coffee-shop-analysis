import os
import signal
import sys
import threading
import struct
import configparser
import logging
import time
import socket
from shared.logging_config import initialize_log
from shared.worker import StreamProcessingWorker
from shared import protocol
from WSM.wsm_client import WSMClient
from wsm_config import WSM_NODES



class Filter(StreamProcessingWorker):
    def __init__(self, queue_in, queue_out, rabbitmq_host, backoff_start=0.1, backoff_max=3.0, wsm_nodes = None):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.backoff_start = backoff_start
        self.backoff_max = backoff_max

        # 🔗 Conexión con el Worker State Manager
        self.replica_id = socket.gethostname()
        wsm_host = os.environ.get("WSM_HOST", "wsm")
        wsm_port = int(os.environ.get("WSM_PORT", "9000"))

        self.wsm_client = WSMClient(
            worker_type="filter",
            replica_id=self.replica_id,
            host=wsm_host,
            port=wsm_port,
            nodes=wsm_nodes,
        )

        logging.info(f"[Filter:{self.replica_id}] Inicializado - input: {queue_in}, output: {queue_out}")

    # ------------------------------------------------------------
    # 🔁 Procesamiento de mensajes de datos
    # ------------------------------------------------------------
    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
        """Procesa mensajes de datos (no END)."""
        
        #VALIDO QUE LA POSICION HAYA SIDO PROCESADA ANTERIORMENTE, SI YA FUE PROCESADA LO DESCARTO EL MENSAJE
        if self.wsm_client.is_position_processed(request_id, position):
            logging.info(f"🔁 Mensaje duplicado detectado ({request_id}:{position}), descartando...")
            return

        
        # 1️⃣ Notificar al WSM que esta réplica está procesando
        self.wsm_client.update_state("PROCESSING", request_id, position)

        # Decodificar las filas
        rows = payload.decode("utf-8").split("\n")
        dic_queue_row = self._process_rows(rows, queue_name)

        # Enviar resultados filtrados a las colas correspondientes
        self._send_complex_results(dic_queue_row, msg_type, data_type, request_id, position)

        # 2️⃣ Volver a estado de espera
        self.wsm_client.update_state("WAITING", request_id, position)

    # ------------------------------------------------------------
    # 🧩 Manejo de END sincronizado
    # ------------------------------------------------------------
    def _handle_end_signal(self, message, msg_type, data_type, request_id, position, queue_name=None):
        """Maneja el END: sincroniza con el WSM antes de reenviarlo."""
        self.wsm_client.update_state("END", request_id, position)
        logging.info(f"[Filter:{self.replica_id}] Recibido END para request {request_id}. Esperando permiso del WSM...")

        # Esperar permiso del WSM para reenviar END con exponential backoff
        backoff = self.backoff_start
        total_wait = 0.0
        
        while not self.wsm_client.can_send_end(request_id, position):
            if total_wait >= self.backoff_max:
                error_msg = f"[Filter:{self.replica_id}] Timeout esperando permiso WSM para END de {request_id} después de {total_wait:.2f}s"
                logging.error(error_msg)
                raise TimeoutError(error_msg)
            
            logging.info(f"[Filter:{self.replica_id}] Esperando permiso para reenviar END de {request_id}... (backoff={backoff:.3f}s, total={total_wait:.2f}s)")
            time.sleep(backoff)
            total_wait += backoff
            backoff = min(backoff * 2, self.backoff_max - total_wait) if total_wait < self.backoff_max else 0

        logging.info(f"[Filter:{self.replica_id}] ✅ Permiso otorgado por el WSM para reenviar END de {request_id}")

        # Reenviar END a las colas de salida (manejo base)
        super()._handle_end_signal(message, msg_type, data_type, request_id, position, queue_name)

        # Volver a estado de espera
        self.wsm_client.update_state("WAITING", request_id, position)

    # ------------------------------------------------------------
    # 🧠 Lógica de filtrado
    # ------------------------------------------------------------
    def _process_rows(self, rows, queue_name=None):
        """Procesa filas y devuelve un diccionario {queue_name: [rows]}."""
        dic_queue_row = {}

        for row in rows:
            if not row.strip():
                continue

            rows_queues = self._filter_row(row) or []
            for filtered_row, target_queue_name in rows_queues:
                dic_queue_row.setdefault(target_queue_name, []).append(filtered_row)

        return dic_queue_row

    def _send_complex_results(self, dic_queue_row, msg_type, data_type, request_id, position):
        """Envía los resultados filtrados a las colas correspondientes."""
        for queue_name, filtered_rows in dic_queue_row.items():
            new_payload_str = '\n'.join(filtered_rows)
            new_payload = new_payload_str.encode('utf-8')
            new_message = protocol.create_data_message(data_type, new_payload, request_id, position)

            for q in self.out_queues:
                if q.queue_name == queue_name:
                    q.send(new_message)

    # Método base que redefinen los hijos
    def _filter_row(self, row: str):
        return [(row, self.out_queues[0].queue_name if self.out_queues else "default")]


# ====================
# Filters específicos
# ====================

class TemporalFilter(Filter):
    def __init__(self, queue_in, queue_out, rabbitmq_host, data_type, col_index, config, backoff_start=0.1, backoff_max=3.0, wsm_nodes = None):
        super().__init__(queue_in, queue_out, rabbitmq_host, backoff_start, backoff_max, wsm_nodes)
        self.data_type = data_type
        self.col_index = col_index
        self.rules = []

        for section in config.sections():
            rule_data_type = config[section].get("DATA_TYPE")
            if rule_data_type != self.data_type:
                continue

            queue_out = config[section]["QUEUE_OUT"].strip()
            year_start = int(config[section].get("YEAR_START", 0))
            year_end = int(config[section].get("YEAR_END", 9999))
            hour_start = int(config[section].get("HOUR_START", 0))
            hour_end = int(config[section].get("HOUR_END", 23))

            self.rules.append({
                "queue_out": queue_out,
                "year_start": year_start,
                "year_end": year_end,
                "hour_start": hour_start,
                "hour_end": hour_end
            })

    def _route(self, row: str, year, hour):
        result = []
        for rule in self.rules:
            if rule["year_start"] <= year <= rule["year_end"] and \
               rule["hour_start"] <= hour <= rule["hour_end"]:
                result.append((row, rule["queue_out"]))
        return result

    def _filter_row(self, row: str):
        parts = row.split('|')
        if "created_at" not in self.col_index or len(parts) <= self.col_index["created_at"]:
            return None

        created_at_str = parts[self.col_index["created_at"]].strip()
        try:
            year = int(created_at_str[0:4])
            hour = int(created_at_str[11:13])
        except Exception:
            return None

        return self._route(row, year, hour)


class AmountFilter(Filter):
    def __init__(self, queue_in, queue_out, rabbitmq_host, min_amount, col_index, backoff_start=0.1, backoff_max=3.0, wsm_nodes = None):
        super().__init__(queue_in, queue_out, rabbitmq_host, backoff_start, backoff_max, wsm_nodes)
        self.min_amount = float(min_amount)
        self.col_index = col_index

    def _filter_row(self, row: str):
        parts = row.split('|')

        if "final_amount" not in self.col_index or len(parts) <= self.col_index["final_amount"]:
            return None
        try:
            amount = float(parts[self.col_index["final_amount"]])
        except Exception:
            return None

        if amount < self.min_amount:
            return None
        return [(row, self.out_queues[0].queue_name)]


# ====================
# Main
# ====================

if __name__ == '__main__':
    def create_filter():
        queue_in = os.environ.get('QUEUE_IN')
        queue_out = os.environ.get('QUEUE_OUT')
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

        filter_type = os.environ.get('FILTER_TYPE', 'temporal')  # temporal o amount
        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)

        data_type = os.environ.get('DATA_TYPE')
        if data_type not in config:
            raise ValueError(f"Unknown data type: {data_type}")

        columns_have = [c.strip() for c in config[data_type]['have'].split(',')]
        col_index = {c: i for i, c in enumerate(columns_have)}

        # Load backoff configuration from DEFAULT section
        backoff_start = float(config['DEFAULT'].get('BACKOFF_START', 0.1))
        backoff_max = float(config['DEFAULT'].get('BACKOFF_MAX', 3.0))

        key = filter_type + '_' + data_type
        wsm_nodes = WSM_NODES[key]
        print("WSM NODES: ", wsm_nodes)

        if filter_type == 'temporal':
            temporal_config_path = os.path.join(os.path.dirname(__file__), 'temporal_filter_config.ini')
            temporal_config = configparser.ConfigParser()
            temporal_config.read(temporal_config_path)
            return TemporalFilter(queue_in, queue_out, rabbitmq_host, data_type, col_index, temporal_config, backoff_start, backoff_max, wsm_nodes)

        elif filter_type == 'amount':
            min_amount = os.environ.get('MIN_AMOUNT')
            return AmountFilter(queue_in, queue_out, rabbitmq_host, min_amount, col_index, backoff_start, backoff_max, wsm_nodes)

        else:
            raise ValueError(f"Unknown FILTER_TYPE: {filter_type}")

    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Filter.run_worker_main(create_filter, config_path)
