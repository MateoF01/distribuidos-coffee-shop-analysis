import os
import sys
import signal
import threading
import configparser
from collections import defaultdict
from datetime import datetime
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue
from shared import protocol

BASE_TEMP_DIR = os.path.join(os.path.dirname(__file__), 'temp')
os.makedirs(BASE_TEMP_DIR, exist_ok=True)

def get_month_str(dt_str):
    dt = datetime.strptime(dt_str[:7], '%Y-%m')
    return dt.strftime('%b%y').lower()  # e.g., jan24

def get_semester_str(dt_str):
    dt = datetime.strptime(dt_str[:7], '%Y-%m')
    year = dt.strftime('%y')
    month = dt.month
    sem = '1sem' if 1 <= month <= 6 else '2sem'
    return f'{sem}_{year}'

class Grouper:
    def __init__(self, queue_in, groupby, agg, rabbitmq_host, columns, temp_dir=None):
        self.queue_in = queue_in
        self.groupby = groupby
        self.agg = agg
        self.rabbitmq_host = rabbitmq_host
        self.columns = columns
        # Use a subfolder for each query (by queue_in)
        self.query_id = queue_in
        self.temp_dir = temp_dir or os.path.join(BASE_TEMP_DIR, self.query_id)
        os.makedirs(self.temp_dir, exist_ok=True)
        self._running = False
        self._shutdown_event = threading.Event()
        self.in_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_in)

    def run(self):
        self._running = True
        def on_message(message):
            if not self._running:
                return
            try:
                #print(f"[Grouper:{self.query_id}] Received message from queue: {self.queue_in}")
                if not isinstance(message, bytes) or len(message) < 6:
                    print(f"Invalid message format or too short: {message}")
                    return
                msg_type, data_type, payload = protocol._unpack_message(message)

                if msg_type == protocol.MSG_TYPE_END:
                    if data_type == protocol.DATA_END:
                        print('End-of-data signal received. Closing grouper for this queue.')
                        self.stop()
                        return
                    else:
                        print(f"Sent end-of-data msg_type:{msg_type} signal to grouper")
                        return

                try:
                    payload_str = payload.decode('utf-8')
                except Exception as e:
                    print(f"Failed to decode payload: {e}")
                    return
                rows = payload_str.split('\n')
                rows = [row for row in rows if row.strip()]
                self.process_rows(rows)
            except Exception as e:
                print(f"Error processing message: {e} - Message: {message}")

        #print(f"Grouper listening on {self.queue_in}")
        self.in_queue.start_consuming(on_message)
        try:
            self._shutdown_event.wait()
        except KeyboardInterrupt:
            print("Keyboard interrupt received, shutting down...")
            self.stop()

    def stop(self):
        self._running = False
        self._shutdown_event.set()
        try:
            self.in_queue.stop_consuming()
        except Exception as e:
            print(f"Error stopping consumer: {e}")
        self.close()

    def close(self):
        self.in_queue.close()

    def process_rows(self, rows):
        self.agg(rows, self.temp_dir, self.columns)

 # --- Q2: transaction_items_filtered_Q2, groupby month, item_id, sum quantity/subtotal ---
def q2_agg(rows, temp_dir, columns):
    # columns: item_id,quantity,subtotal,created_at
    idx_item = columns.index('item_id')
    idx_quantity = columns.index('quantity')
    idx_subtotal = columns.index('subtotal')
    idx_created = columns.index('created_at')
    # {month: {item_id: [total_quantity, total_subtotal]}}
    grouped = defaultdict(lambda: defaultdict(lambda: [0, 0.0]))
    #print(f"[Q2 AGG] Processing {len(rows)} rows. Columns: {columns}")
    for i, row in enumerate(rows):
        items = row.split('|')
        if len(items) < 4:
            #print(f"[Q2 AGG] Skipping row {i} (not enough fields): {row}")
            continue
        month = get_month_str(items[idx_created])
        item_id = items[idx_item]
        try:
            quantity = int(items[idx_quantity])
            subtotal = float(items[idx_subtotal])
        except Exception as e:
            #print(f"[Q2 AGG] Skipping row {i} (parse error): {row} | Error: {e}")
            continue
        grouped[month][item_id][0] += quantity
        grouped[month][item_id][1] += subtotal
    for month, items_dict in grouped.items():
        fpath = os.path.join(temp_dir, f'{month}.csv')
        #print(f"[Q2 AGG] Writing to {fpath} ({len(items_dict)} items)")
        # Read old data if exists
        old = {}
        if os.path.exists(fpath):
            with open(fpath, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 3:
                        old[parts[0]] = [int(parts[1]), float(parts[2])]
        # Update
        for item_id, vals in items_dict.items():
            if item_id in old:
                vals[0] += old[item_id][0]
                vals[1] += old[item_id][1]
            old[item_id] = vals
        # Write
        with open(fpath, 'w') as f:
            for item_id, vals in old.items():
                f.write(f'{item_id},{vals[0]},{vals[1]}\n')
    #print(f"[Q2 AGG] Finished writing files for {len(grouped)} months.")

 # --- Q3: transactions_filtered_Q3, groupby semester+store_id, sum final_amount ---
def q3_agg(rows, temp_dir, columns):
    # columns: transaction_id,final_amount,created_at,store_id,user_id
    idx_final = columns.index('final_amount')
    idx_created = columns.index('created_at')
    idx_store = columns.index('store_id')
    grouped = defaultdict(float)  # (semester, store_id) -> total
    for row in rows:
        items = row.split('|')
        if len(items) < 5:
            continue
        semester = get_semester_str(items[idx_created])
        store_id = items[idx_store]
        try:
            final_amount = float(items[idx_final])
        except Exception:
            continue
        key = (semester, store_id)
        grouped[key] += final_amount
    for (semester, store_id), total in grouped.items():
        fname = f'{semester}_{store_id}.csv'
        fpath = os.path.join(temp_dir, fname)
        # Read old value if exists
        old = 0.0
        if os.path.exists(fpath):
            with open(fpath, 'r') as f:
                try:
                    old = float(f.read().strip())
                except Exception:
                    old = 0.0
        total += old
        with open(fpath, 'w') as f:
            f.write(f'{total}\n')

# --- Q4: transactions_filtered_Q4, groupby store_id, count user_id ---
def q4_agg(rows, temp_dir, columns):
    idx_store = columns.index('store_id')
    idx_user = columns.index('user_id')
    grouped = defaultdict(lambda: defaultdict(int))  # store_id -> user_id -> count
    for i, row in enumerate(rows):
        items = row.split('|')
        if len(items) < 5:
            #print(f"[Q4 AGG] Skipping row {i} (not enough fields): {row}")
            continue
        store_id = items[idx_store]
        user_id = items[idx_user]
        if not user_id.strip():
            # Empty user_id, skip
            continue
        grouped[store_id][user_id] += 1
    for store_id, users in grouped.items():
        fpath = os.path.join(temp_dir, f'{store_id}.csv')
        # Read old data if exists
        old = defaultdict(int)
        if os.path.exists(fpath):
            with open(fpath, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 2 and parts[0].strip():
                        old[parts[0]] = int(parts[1])
        # Update
        for user_id, count in users.items():
            old[user_id] += count
        # Write
        with open(fpath, 'w') as f:
            for user_id, count in old.items():
                f.write(f'{user_id},{count}\n')

if __name__ == '__main__':
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_path)
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    mode = os.environ.get('GROUPER_MODE')
    if mode == 'q2':
        section = 'transaction_items_filtered_Q2'
        queue_in = section
        columns = [col.strip() for col in config[section]['columns'].split(',')]
        grouper = Grouper(queue_in, groupby='month', agg=q2_agg, rabbitmq_host=rabbitmq_host, columns=columns)
    elif mode == 'q3':
        section = 'transactions_filtered_Q3'
        queue_in = section
        columns = [col.strip() for col in config[section]['columns'].split(',')]
        grouper = Grouper(queue_in, groupby='semester_store', agg=q3_agg, rabbitmq_host=rabbitmq_host, columns=columns)
    elif mode == 'q4':
         section = 'transactions_filtered_Q4'
         queue_in = section
         columns = [col.strip() for col in config[section]['columns'].split(',')]
         grouper = Grouper(queue_in, groupby='store_user', agg=q4_agg, rabbitmq_host=rabbitmq_host, columns=columns)
    else:
        print('Unknown or missing GROUPER_MODE. Set GROUPER_MODE to q2 or q3.')
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
        print('Keyboard interrupt received, shutting down grouper.')
        grouper.stop()
    except Exception as e:
        print(f'Error in grouper: {e}')
        grouper.stop()
    finally:
        print('Grouper shutdown complete.')
