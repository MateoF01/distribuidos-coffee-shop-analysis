import os
import sys
import signal
import threading
import configparser
import time
from collections import defaultdict
from datetime import datetime
import logging
import gc
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue
from shared import protocol
from shared.logging_config import initialize_log

BASE_TEMP_DIR = os.path.join(os.path.dirname(__file__), 'temp')
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

class Grouper:
    def __init__(self, queue_in, groupby, agg, rabbitmq_host, columns, temp_dir=None, completion_queue=None):
        self.queue_in = queue_in
        self.groupby = groupby
        self.agg = agg
        self.rabbitmq_host = rabbitmq_host
        self.columns = columns
        self.completion_queue = completion_queue
        # Use a subfolder for each query (by queue_in)
        self.query_id = queue_in
        self.temp_dir = temp_dir or os.path.join(BASE_TEMP_DIR, self.query_id)
        os.makedirs(self.temp_dir, exist_ok=True)
        self._running = False
        self._shutdown_event = threading.Event()
        self.counter = 0  # Counter for processed rows
        
        # Optimization: batch processing - load from config if available
        try:
            config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
            perf_config = configparser.ConfigParser()
            perf_config.read(config_path)
            self.batch_size = int(perf_config.get('PERFORMANCE', 'batch_size', fallback=10000))
            self.memory_limit = int(perf_config.get('PERFORMANCE', 'memory_limit', fallback=50000))
        except Exception:
            self.batch_size = 10000
            self.memory_limit = 50000
        
        self.row_buffer = []
        
        logging.info(f"[Grouper:{self.query_id}] Initialized with batch_size={self.batch_size}, memory_limit={self.memory_limit}")
        
        # Pre-compile column indices for faster lookup
        self.column_indices = {col: idx for idx, col in enumerate(columns)}
        
        self.in_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_in)
        # Initialize completion queue if provided
        self.out_queue = None
        if self.completion_queue:
            self.out_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=completion_queue)

    def run(self):
        self._running = True
        logging.info(f"[Grouper:{self.query_id}] Starting grouper, listening on queue: {self.queue_in}")
        def on_message(message):
            if not self._running:
                return
            try:
                if not isinstance(message, bytes) or len(message) < 6:
                    logging.warning(f"Invalid message format or too short: {message}")
                    return
                msg_type, data_type, timestamp, payload = protocol.unpack_message(message)

                if msg_type == protocol.MSG_TYPE_END:
                    if data_type == protocol.DATA_END:
                        logging.info(f'[Grouper:{self.query_id}] END signal (DATA_END) received. Total rows processed: {self.counter}')
                        self._final_flush()
                        # self.stop()
                        return
                    else:
                        logging.info(f"[Grouper:{self.query_id}] END signal received for data_type:{data_type}. Total rows processed: {self.counter}")
                        self._final_flush()
                        self._send_completion_signal()
                        return

                try:
                    payload_str = payload.decode('utf-8')
                except Exception as e:
                    logging.error(f"[Grouper:{self.query_id}] Failed to decode payload: {e}")
                    return
                
                # More efficient row processing
                if payload_str.strip():  # Only process if payload has content
                    rows = [row for row in payload_str.split('\n') if row.strip()]
                    if rows:  # Only process if we have valid rows
                        self.process_rows(rows)
            except Exception as e:
                logging.error(f"[Grouper:{self.query_id}] Error processing message: {e} - Message: {message}")

        #print(f"Grouper listening on {self.queue_in}")
        self.in_queue.start_consuming(on_message)
        try:
            self._shutdown_event.wait()
        except KeyboardInterrupt:
            logging.info(f"[Grouper:{self.query_id}] Keyboard interrupt received, shutting down...")
            self.stop()

    def stop(self):
        self._running = False
        self._shutdown_event.set()
        try:
            self.in_queue.stop_consuming()
        except Exception as e:
            logging.error(f"[Grouper:{self.query_id}] Error stopping consumer: {e}")
        self.close()

    def close(self):
        self.in_queue.close()
        if self.out_queue:
            self.out_queue.close()

    def _send_completion_signal(self):
        """Send completion signal to the next stage if completion queue is configured"""
        if self.out_queue:
            try:
                logging.info(f"[Grouper:{self.query_id}] Sending completion signal to {self.completion_queue}")
                completion_message = protocol.pack_message(protocol.MSG_TYPE_NOTI, protocol.DATA_END, b"", time.time())
                self.out_queue.send(completion_message)
                logging.info(f"[Grouper:{self.query_id}] Completion signal sent successfully")
            except Exception as e:
                logging.error(f"[Grouper:{self.query_id}] Error sending completion signal: {e}")

    def process_rows(self, rows):
        # Filter out empty rows efficiently
        valid_rows = [row for row in rows if row.strip()]
        
        # Add to buffer
        self.row_buffer.extend(valid_rows)
        
        # Update counter and log progress
        self.counter += len(valid_rows)
        if self.counter % 5000 == 0 or (self.counter - len(valid_rows)) // 5000 != self.counter // 5000:
            logging.info(f"[Grouper:{self.query_id}] Processed {self.counter} rows")
        
        # Process in batches to manage memory
        if len(self.row_buffer) >= self.batch_size:
            self._flush_buffer()
    
    def _flush_buffer(self):
        """Process and flush the current buffer"""
        if not self.row_buffer:
            return
        
        buffer_size = len(self.row_buffer)
        logging.debug(f"[Grouper:{self.query_id}] Flushing buffer with {buffer_size} rows")
        
        # Process current buffer
        self.agg(self.row_buffer, self.temp_dir, self.columns, self.column_indices)
        
        # Clear buffer and force garbage collection
        self.row_buffer.clear()
        gc.collect()
        
        logging.debug(f"[Grouper:{self.query_id}] Buffer flush completed")
    
    def _final_flush(self):
        """Final flush when ending processing"""
        self._flush_buffer()
        logging.info(f"[Grouper:{self.query_id}] Final flush completed")

 # --- Q2: transaction_items_filtered_Q2, groupby month, item_id, sum quantity/subtotal ---
def q2_agg(rows, temp_dir, columns, column_indices=None):
    # Use pre-compiled indices if available
    if column_indices:
        idx_item = column_indices['item_id']
        idx_quantity = column_indices['quantity']
        idx_subtotal = column_indices['subtotal']
        idx_created = column_indices['created_at']
    else:
        idx_item = columns.index('item_id')
        idx_quantity = columns.index('quantity')
        idx_subtotal = columns.index('subtotal')
        idx_created = columns.index('created_at')
    
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
        _update_monthly_file(temp_dir, month, items_dict)
    
    # Clear data and force garbage collection
    monthly_data.clear()
    del monthly_data
    gc.collect()

def _update_monthly_file(temp_dir, month, new_data):
    """Update monthly file efficiently"""
    fpath = os.path.join(temp_dir, f'{month}.csv')
    
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

 # --- Q3: transactions_filtered_Q3, groupby semester+store_id, sum final_amount ---
def q3_agg(rows, temp_dir, columns, column_indices=None):
    # Use pre-compiled indices if available
    if column_indices:
        idx_final = column_indices['final_amount']
        idx_created = column_indices['created_at']
        idx_store = column_indices['store_id']
    else:
        idx_final = columns.index('final_amount')
        idx_created = columns.index('created_at')
        idx_store = columns.index('store_id')
    
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
    _batch_update_q3_files(temp_dir, grouped)
    
    # Clear data
    grouped.clear()
    del grouped
    gc.collect()

def _batch_update_q3_files(temp_dir, grouped_data):
    """Batch update Q3 files efficiently"""
    for key, new_total in grouped_data.items():
        fpath = os.path.join(temp_dir, f'{key}.csv')
        
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

# --- Q4: transactions_filtered_Q4, groupby store_id, count user_id ---
def q4_agg(rows, temp_dir, columns, column_indices=None):
    # Use pre-compiled indices if available
    if column_indices:
        idx_store = column_indices['store_id']
        idx_user = column_indices['user_id']
    else:
        idx_store = columns.index('store_id')
        idx_user = columns.index('user_id')
    
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
    _batch_update_q4_files(temp_dir, grouped)
    
    # Clear data
    grouped.clear()
    del grouped
    gc.collect()

def _batch_update_q4_files(temp_dir, grouped_data):
    """Batch update Q4 files efficiently"""
    for store_id, users in grouped_data.items():
        fpath = os.path.join(temp_dir, f'{store_id}.csv')
        
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

if __name__ == '__main__':
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_path)
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    queue_in = os.environ.get('QUEUE_IN')
    mode = os.environ.get('GROUPER_MODE')
    
    # Initialize logging
    logging_level = os.environ.get('LOGGING_LEVEL', config.get('DEFAULT', 'LOGGING_LEVEL', fallback='INFO'))
    initialize_log(logging_level)

    if not queue_in:
        logging.error('QUEUE_IN environment variable is required.')
        sys.exit(1)
    
    if mode == 'q2':
        section = 'transaction_items_filtered_Q2'
        columns = [col.strip() for col in config[section]['columns'].split(',')]
        completion_queue = os.environ.get('COMPLETION_QUEUE')
        grouper = Grouper(queue_in, groupby='month', agg=q2_agg, rabbitmq_host=rabbitmq_host, columns=columns, completion_queue=completion_queue)
    elif mode == 'q3':
        section = 'transactions_filtered_Q3'
        columns = [col.strip() for col in config[section]['columns'].split(',')]
        completion_queue = os.environ.get('COMPLETION_QUEUE')
        grouper = Grouper(queue_in, groupby='semester_store', agg=q3_agg, rabbitmq_host=rabbitmq_host, columns=columns, completion_queue=completion_queue)
    elif mode == 'q4':
         section = 'transactions_filtered_Q4'
         columns = [col.strip() for col in config[section]['columns'].split(',')]
         completion_queue = os.environ.get('COMPLETION_QUEUE')
         grouper = Grouper(queue_in, groupby='store_user', agg=q4_agg, rabbitmq_host=rabbitmq_host, columns=columns, completion_queue=completion_queue)
    else:
        logging.error('Unknown or missing GROUPER_MODE. Set GROUPER_MODE to q2, q3, or q4.')
        sys.exit(1)
    def signal_handler(signum, frame):
        #print(f'Received signal {signum}, shutting down grouper gracefully...')
        grouper.stop()
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    try:
        #print(f"Starting grouper in mode {mode}...")
        grouper.run()
    except KeyboardInterrupt:
        logging.info('Keyboard interrupt received, shutting down grouper.')
        grouper.stop()
    except Exception as e:
        logging.error(f'Error in grouper: {e}')
        grouper.stop()
    finally:
        logging.info('Grouper shutdown complete.')
