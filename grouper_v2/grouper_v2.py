import os
import socket
import time
import logging
import configparser
import gc
from collections import defaultdict
from datetime import datetime
from shared import protocol
from shared.worker import StreamProcessingWorker
from WSM.wsm_client import WSMClient
from wsm_config import WSM_NODES


def get_month_str(dt_str):
    dt = datetime.strptime(dt_str[:7], '%Y-%m')
    return dt.strftime('%Y-%m')

def get_semester_str(dt_str):
    dt = datetime.strptime(dt_str[:7], '%Y-%m')
    year = dt.strftime('%Y')
    month = dt.month
    sem = 'H1' if 1 <= month <= 6 else 'H2'
    return f'{year}-{sem}'


class GrouperV2(StreamProcessingWorker):
    """Agrupa transacciones por usuario o tienda y acumula totales."""

    def __init__(self, queue_in, queue_out, rabbitmq_host, grouper_mode, replica_id, backoff_start=0.1, backoff_max=3.0, wsm_nodes = None):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.grouper_mode = grouper_mode
        self.replica_id = replica_id
        self.backoff_start = backoff_start
        self.backoff_max = backoff_max
        self.base_temp_root = os.environ.get('BASE_TEMP_DIR', os.path.join(os.path.dirname(__file__), 'temp'))
        self.temp_dir = None
        self.current_request_id = None
        self.request_id_initialized = False

        # 🔗 Conexión con el Worker State Manager
        wsm_host = os.environ.get("WSM_HOST", "wsm")
        wsm_port = int(os.environ.get("WSM_PORT", "9000"))
        self.wsm_client = WSMClient(
            worker_type="grouper",
            replica_id=replica_id,
            host=wsm_host,
            port=wsm_port,
            nodes=wsm_nodes
        )

        logging.info(f"[GrouperV2:{self.grouper_mode}:{replica_id}] Inicializado - input: {queue_in}, output: {queue_out}")

    # ------------------------------------------------------------
    # 🔁 Procesamiento de mensajes
    # ------------------------------------------------------------
    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
        self._initialize_request_paths(request_id)

        #VALIDO QUE LA POSICION HAYA SIDO PROCESADA ANTERIORMENTE, SI YA FUE PROCESADA LO DESCARTO EL MENSAJE
        if self.wsm_client.is_position_processed(request_id, position):
            logging.info(f"🔁 Mensaje duplicado detectado ({request_id}:{position}), descartando...")
            return

        # 1️⃣ Notificar inicio de procesamiento
        self.wsm_client.update_state("PROCESSING", request_id, position)

        # Delegar al procesamiento normal (que invoca a _process_rows)
        super()._process_message(message, msg_type, data_type, request_id, position, payload, queue_name)

        # 2️⃣ Marcar espera
        self.wsm_client.update_state("WAITING", request_id, position)

    # ------------------------------------------------------------
    # 🧩 Manejo del END sincronizado
    # ------------------------------------------------------------
    def _handle_end_signal(self, message, msg_type, data_type, request_id, position, queue_name=None):
        if data_type == protocol.DATA_END:
            logging.info(f"[GrouperV2:{self.grouper_mode}] Recibido DATA_END para request {request_id} en cola {queue_name}.")
            return
        
        self.wsm_client.update_state("END", request_id, position)
        logging.info(f"[GrouperV2:{self.grouper_mode}] Recibido END para request {request_id}. Esperando permiso del WSM...")

        # Esperar permiso del WSM con exponential backoff
        backoff = self.backoff_start
        total_wait = 0.0
        
        while not self.wsm_client.can_send_end(request_id, position):
            if total_wait >= self.backoff_max:
                error_msg = f"[GrouperV2:{self.grouper_mode}] Timeout esperando permiso WSM para END de {request_id} después de {total_wait:.2f}s"
                logging.error(error_msg)
                raise TimeoutError(error_msg)
            
            logging.info(f"[GrouperV2:{self.grouper_mode}] Esperando permiso... (backoff={backoff:.3f}s, total={total_wait:.2f}s)")
            time.sleep(backoff)
            total_wait += backoff
            backoff = min(backoff * 2, self.backoff_max - total_wait) if total_wait < self.backoff_max else 0

        logging.info(f"[GrouperV2:{self.grouper_mode}] ✅ Permiso otorgado por el WSM para reenviar END de {request_id}")

        # Enviar notificación downstream
        noti_payload = f"completed_by={self.replica_id}".encode("utf-8")
        noti_message = protocol.create_notification_message(data_type, noti_payload, request_id)
        for q in self.out_queues:
            q.send(noti_message)

        self.wsm_client.update_state("WAITING", request_id, position)

    # ------------------------------------------------------------
    # 🧠 Lógica de agrupamiento
    # ------------------------------------------------------------
    def _process_rows(self, rows, queue_name=None):
        if not self.temp_dir:
            logging.error("temp_dir no inicializado (falta request_id).")
            return

        if self.grouper_mode == 'q2':
            self._q2_agg(rows, self.temp_dir)
        elif self.grouper_mode == 'q3':
            self._q3_agg(rows, self.temp_dir)
        elif self.grouper_mode == 'q4':
            self._q4_agg(rows, self.temp_dir)

    # === Q2 ===
    def _q2_agg(self, rows, temp_dir):
        idx_item, idx_quantity, idx_subtotal, idx_created = 0, 1, 2, 3
        monthly_data = defaultdict(lambda: defaultdict(lambda: [0, 0.0]))
        for row in rows:
            items = row.split('|')
            if len(items) <= idx_created:
                continue
            try:
                month = get_month_str(items[idx_created])
                item_id = items[idx_item]
                monthly_data[month][item_id][0] += int(items[idx_quantity])
                monthly_data[month][item_id][1] += float(items[idx_subtotal])
            except Exception:
                continue
        for month, data in monthly_data.items():
            self._update_q2_file(temp_dir, month, data)
        monthly_data.clear(); gc.collect()

    def _update_q2_file(self, temp_dir, month, data):
        path = os.path.join(temp_dir, f"{month}_{self.replica_id}.csv")
        existing = {}
        if os.path.exists(path):
            with open(path, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 3:
                        existing[parts[0]] = [int(parts[1]), float(parts[2])]
        for k, v in data.items():
            if k in existing:
                existing[k][0] += v[0]
                existing[k][1] += v[1]
            else:
                existing[k] = v
        with open(path, 'w') as f:
            for k, v in existing.items():
                f.write(f"{k},{v[0]},{v[1]}\n")

    # === Q3 ===
    def _q3_agg(self, rows, temp_dir):
        idx_final, idx_created, idx_store = 1, 2, 3
        grouped = {}
        for row in rows:
            items = row.split('|')
            if len(items) < 4:
                continue
            try:
                semester = get_semester_str(items[idx_created])
                store = items[idx_store]
                amt = float(items[idx_final])
                key = f"{semester}_{store}"
                grouped[key] = grouped.get(key, 0.0) + amt
            except Exception:
                continue
        self._update_q3_file(temp_dir, grouped)
        grouped.clear(); gc.collect()

    def _update_q3_file(self, temp_dir, data):
        for key, val in data.items():
            path = os.path.join(temp_dir, f"{key}_{self.replica_id}.csv")
            old = 0.0
            if os.path.exists(path):
                with open(path, 'r') as f:
                    try:
                        old = float(f.read().strip())
                    except:
                        pass

            total = old + val

            with open(path, 'w') as f:
                f.write(f"{total}\n")

    # === Q4 ===
    def _q4_agg(self, rows, temp_dir):
        idx_store, idx_user = 3, 4
        grouped = {}
        for row in rows:
            items = row.split('|')
            if len(items) <= idx_user:
                continue
            store, user = items[idx_store], items[idx_user].strip()
            if not user:
                continue
            grouped.setdefault(store, {})
            grouped[store][user] = grouped[store].get(user, 0) + 1
        self._update_q4_file(temp_dir, grouped)
        grouped.clear(); gc.collect()

    def _update_q4_file(self, temp_dir, data):
        for store, users in data.items():
            path = os.path.join(temp_dir, f"{store}_{self.replica_id}.csv")
            existing = {}
            if os.path.exists(path):
                with open(path, 'r') as f:
                    for line in f:
                        parts = line.strip().split(',')
                        if len(parts) == 2:
                            existing[parts[0]] = int(parts[1])
            for u, c in users.items():
                existing[u] = existing.get(u, 0) + c
            with open(path, 'w') as f:
                for u, c in existing.items():
                    f.write(f"{u},{c}\n")

    # ------------------------------------------------------------
    # 📂 Inicialización de rutas por request
    # ------------------------------------------------------------
    def _initialize_request_paths(self, request_id):
        if self.request_id_initialized and self.current_request_id == request_id:
            return
        self.current_request_id = request_id
        mode_dir = f"grouper_v2_{self.grouper_mode}"
        self.temp_dir = os.path.join(self.base_temp_root, mode_dir, str(request_id), self.replica_id)
        os.makedirs(self.temp_dir, exist_ok=True)
        self.request_id_initialized = True


# ====================
# Main
# ====================

if __name__ == '__main__':
    def create_grouperv2():
        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config = configparser.ConfigParser(); config.read(config_path)
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        queue_in = os.environ.get('QUEUE_IN')
        queue_out = os.environ.get('COMPLETION_QUEUE')
        mode = os.environ.get('GROUPER_MODE')
        replica_id = socket.gethostname()
        if not mode:
            raise ValueError("GROUPER_MODE not set")
        
        # Load backoff configuration from DEFAULT section
        backoff_start = float(config['DEFAULT'].get('BACKOFF_START', 0.1))
        backoff_max = float(config['DEFAULT'].get('BACKOFF_MAX', 3.0))
        
        wsm_nodes = WSM_NODES[mode]
        print("WSM NODES: ", wsm_nodes)

        return GrouperV2(queue_in, queue_out, rabbitmq_host, mode, replica_id, backoff_start, backoff_max, wsm_nodes)

    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    GrouperV2.run_worker_main(create_grouperv2, config_path)
