import queue
from statistics import mode
from shared.worker import StreamProcessingWorker
import logging
import os
import configparser
import gc
from collections import defaultdict
from datetime import datetime
import socket
from shared import protocol



def get_month_str(dt_str):
    dt = datetime.strptime(dt_str[:7], '%Y-%m')
    return dt.strftime('%Y-%m')  # e.g., 2024-01

def get_semester_str(dt_str):
    dt = datetime.strptime(dt_str[:7], '%Y-%m')
    year = dt.strftime('%Y')
    month = dt.month
    sem = 'H1' if 1 <= month <= 6 else 'H2'
    return f'{year}-{sem}'

class GrouperV2(StreamProcessingWorker):
    """Agrupa transacciones por usuario o tienda y acumula totales."""

    def __init__(self, queue_in, queue_out, rabbitmq_host, grouper_mode, replica_id):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.grouper_mode = grouper_mode
        self.temp_dir = ''
        self.replica_id = replica_id

        # raíz para outputs temporales (se puede parametrizar vía env si querés)
        self.base_temp_root = os.environ.get('BASE_TEMP_DIR', os.path.join(os.path.dirname(__file__), 'temp'))
        # ej: <repo>/grouper_v2/temp
        self.temp_dir = None
        self.current_request_id = None
        self.request_id_initialized = False

    def _process_message(self, message, msg_type, data_type, request_id, timestamp, payload, queue_name=None):
        """
        Intercepta TODOS los mensajes para:
          - inicializar carpetas por request
          - luego delegar al flujo normal del StreamProcessingWorker
        """
        self._initialize_request_paths(request_id)
        # delegamos al comportamiento del padre (que terminará llamando a _process_rows)
        return super()._process_message(message, msg_type, data_type, request_id, timestamp, payload, queue_name=queue_name)

    def _process_rows(self, rows, queue_name=None):
        """Agrupa las filas recibidas y acumula sumas."""
        if not self.temp_dir:
            logging.error("temp_dir no inicializado (falta request_id).")
            return

        if self.grouper_mode == 'q2':
            self._q2_agg(rows, self.temp_dir)
        elif self.grouper_mode == 'q3':
            self._q3_agg(rows, self.temp_dir)
        elif self.grouper_mode == 'q4':
            self._q4_agg(rows, self.temp_dir)

    def _q2_agg(self, rows, temp_dir):
        # Use pre-compiled indices
        idx_item = 0
        idx_quantity = 1
        idx_subtotal = 2
        idx_created = 3
        
        # Process by month to reduce memory usage
        monthly_data = defaultdict(lambda: defaultdict(lambda: [0, 0.0]))
        
        # Process rows more efficiently
        for row in rows:

            items = row.split('|')
            if len(items) <= max(idx_item, idx_quantity, idx_subtotal, idx_created):
                continue
            
            try:
                month = get_month_str(items[idx_created])
                item_id = items[idx_item]
                quantity = int(items[idx_quantity])
                subtotal = float(items[idx_subtotal])
                
                monthly_data[month][item_id][0] += quantity
                monthly_data[month][item_id][1] += subtotal
            except (ValueError, IndexError):
                continue
        
        # Process each month separately to minimize memory usage
        for month, items_dict in monthly_data.items():
            self._update_q2_file(temp_dir, month, items_dict)
        
        # Clear data and force garbage collection
        monthly_data.clear()
        del monthly_data
        gc.collect()

    def _update_q2_file(self, temp_dir, month, new_data):
        """Update monthly file efficiently"""
        fpath = os.path.join(temp_dir, f'{month}_{self.replica_id}.csv')
        
        # Read existing data efficiently
        existing_data = {}
        if os.path.exists(fpath):
            with open(fpath, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 3:
                        existing_data[parts[0]] = [int(parts[1]), float(parts[2])]
        
        # Merge new data with existing
        for item_id, vals in new_data.items():
            if item_id in existing_data:
                existing_data[item_id][0] += vals[0]
                existing_data[item_id][1] += vals[1]
            else:
                existing_data[item_id] = vals
        
        # Write all data at once
        with open(fpath, 'w') as f:
            for item_id, vals in existing_data.items():
                f.write(f'{item_id},{vals[0]},{vals[1]}\n')

    def _q3_agg(self, rows, temp_dir):
        # Use pre-compiled indices if available

        #transaction_id,final_amount,created_at,store_id,user_id
        idx_final = 1
        idx_created = 2
        idx_store = 3
        
        # Use regular dict for better memory efficiency
        grouped = {}
        min_len = max(idx_final, idx_created, idx_store) + 1
        
        for row in rows:
            items = row.split('|')
            if len(items) < min_len:
                continue
            
            try:
                semester = get_semester_str(items[idx_created])
                store_id = items[idx_store]
                final_amount = float(items[idx_final])
                
                key = f"{semester}_{store_id}"
                grouped[key] = grouped.get(key, 0.0) + final_amount
            except (ValueError, IndexError):
                continue
        
        # Batch update files
        self._update_q3_file(temp_dir, grouped)
        
        # Clear data
        grouped.clear()
        del grouped
        gc.collect()
    
    def _update_q3_file(self,temp_dir, grouped_data):
        """Batch update Q3 files efficiently"""
        for key, new_total in grouped_data.items():
            fpath = os.path.join(temp_dir, f'{key}_{self.replica_id}.csv')
            
            # Read existing value
            old_total = 0.0
            if os.path.exists(fpath):
                try:
                    with open(fpath, 'r') as f:
                        old_total = float(f.read().strip())
                except (ValueError, IOError):
                    old_total = 0.0
            
            # Write updated total
            with open(fpath, 'w') as f:
                f.write(f'{old_total + new_total}\n')

    def _q4_agg(self, rows, temp_dir):

        #transaction_id,final_amount,created_at,store_id,user_id
        idx_store = 3
        idx_user = 4
        
        # Use regular dict for better memory efficiency
        grouped = {}
        min_len = max(idx_store, idx_user) + 1
        
        for row in rows:
            items = row.split('|')
            if len(items) < min_len:
                continue
            
            store_id = items[idx_store]
            user_id = items[idx_user].strip()
            
            if not user_id:  # Skip empty user_id
                continue
            
            if store_id not in grouped:
                grouped[store_id] = {}
            
            grouped[store_id][user_id] = grouped[store_id].get(user_id, 0) + 1
        
        # Batch update files
        self._update_q4_file(temp_dir, grouped)
        
        # Clear data
        grouped.clear()
        del grouped
        gc.collect()

    def _update_q4_file(self, temp_dir, grouped_data):
        """Batch update Q4 files efficiently"""
        for store_id, users in grouped_data.items():
            fpath = os.path.join(temp_dir, f'{store_id}_{self.replica_id}.csv')
            
            # Read existing data efficiently
            existing_users = {}
            if os.path.exists(fpath):
                with open(fpath, 'r') as f:
                    for line in f:
                        parts = line.strip().split(',')
                        if len(parts) == 2 and parts[0].strip():
                            existing_users[parts[0]] = int(parts[1])
            
            # Merge new data
            for user_id, count in users.items():
                existing_users[user_id] = existing_users.get(user_id, 0) + count
            
            # Write all data at once
            with open(fpath, 'w') as f:
                for user_id, count in existing_users.items():
                    f.write(f'{user_id},{count}\n')



    #tuve que sobreescribir este metodo porque no hay clase para recibir rows y enviar una notificacion
    def _handle_end_signal(self, message, msg_type, data_type, request_id, queue_name=None):
        logging.info(f"[{self.replica_id}] END recibido — enviando notificación de completado con request_id={request_id}.")

        noti_payload = f"completed_by={self.replica_id}".encode("utf-8")
        noti_message = protocol.create_notification_message(data_type, noti_payload, request_id)

        for q in self.out_queues:
            q.send(noti_message)

        logging.info(f"[{self.replica_id}] Notificación enviada a reducer(s): {[q.queue_name for q in self.out_queues]} con request_id={request_id}")

    def _initialize_request_paths(self, request_id: int):
        """Crea/actualiza las rutas de salida para este request."""
        if self.request_id_initialized and self.current_request_id == request_id:
            return

        self.current_request_id = request_id

        # carpeta por modo + request + replica (para que 2 réplicas no pisen archivos)
        # ej: <base>/grouper_v2_q4/123/<hostname>/
        mode_dir = f"grouper_v2_{self.grouper_mode}"
        self.temp_dir = os.path.join(self.base_temp_root, mode_dir, str(request_id), self.replica_id)
        os.makedirs(self.temp_dir, exist_ok=True)

        self.request_id_initialized = True
        logging.info(f"[GrouperV2:{self.grouper_mode}] temp_dir={self.temp_dir} (request_id={request_id})")

# ====================
# Main
# ====================

if __name__ == '__main__':
    
    def create_grouperv2():
        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        queue_in = os.environ.get('QUEUE_IN')
        queue_out = os.environ.get('COMPLETION_QUEUE')
        mode = os.environ.get('GROUPER_MODE')
        replica_id = socket.gethostname()

        
        if mode:
            return GrouperV2(queue_in, queue_out,rabbitmq_host, mode, replica_id)
        else:
            logging.error('Unknown or missing GROUPER_MODE. Set GROUPER_MODE to q2, q3, or q4.')
            raise ValueError(f"Unknown GROUPER_MODE: {mode}")

    # Use the Worker base class main entry point
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    GrouperV2.run_worker_main(create_grouperv2, config_path)