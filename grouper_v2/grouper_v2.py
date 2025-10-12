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

BASE_TEMP_DIR = os.path.join(os.path.dirname(__file__), 'temp/grouper_v2_q2')
os.makedirs(BASE_TEMP_DIR, exist_ok=True)

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
        self.temp_dir = BASE_TEMP_DIR
        self.replica_id = replica_id

    def _process_rows(self, rows, queue_name=None):
        """Agrupa las filas recibidas y acumula sumas."""
        if self.grouper_mode == 'q2':
            self._q2_agg(rows, self.temp_dir)

    def _q2_agg(self, rows, temp_dir):
        print("RECIBO ROWS: ", rows)
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


    #tuve que sobreescribir este metodo porque no hay clase para recibir rows y enviar una notificacion
    def _handle_end_signal(self, message, msg_type, data_type, queue_name=None):
        logging.info(f"[{self.replica_id}] END recibido — enviando notificación de completado.")

        noti_payload = f"completed_by={self.replica_id}".encode("utf-8")
        noti_message = protocol.pack_message(protocol.MSG_TYPE_NOTI, data_type, noti_payload)

        for q in self.out_queues:
            q.send(noti_message)

        logging.info(f"[{self.replica_id}] Notificación enviada a reducer(s): {[q.queue_name for q in self.out_queues]}")
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