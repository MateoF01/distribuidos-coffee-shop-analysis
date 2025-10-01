import os
import signal
import sys
import threading
import struct
import configparser
import csv
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue


class Joiner:
    def __init__(self, queue_in, queue_out, output_file, columns_want, rabbitmq_host, query_type, multiple_queues=None):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.output_file = output_file
        self.columns_want = columns_want
        self.rabbitmq_host = rabbitmq_host
        self.query_type = query_type

        self._running = False
        self._shutdown_event = threading.Event()
        self._lock = threading.Lock()
        self._csv_initialized = False
        self._rows_written = 0

        self.multiple_queues = multiple_queues or [queue_in]
        self.in_queues = {}
        self.out_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_out) if queue_out else None

        self.end_received = {queue: False for queue in self.multiple_queues}

        self.temp_dir = os.path.join(os.path.dirname(output_file), 'temp')
        os.makedirs(self.temp_dir, exist_ok=True)

        for queue_name in self.multiple_queues:
            self.in_queues[queue_name] = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_name)

        # strategies
        self.strategies = {
            "q1": self._process_q1,
            "q4": self._process_q4,
            "q2": self._process_q2,  # Placeholder
        }

    # =====================
    # Utils
    # =====================

    def _initialize_csv(self):
        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(self.columns_want)
            self._csv_initialized = True
            print(f"Initialized CSV file {self.output_file} with headers: {self.columns_want}")
        except Exception as e:
            print(f"Error initializing CSV file {self.output_file}: {e}")

    def _write_rows_to_csv(self, rows_data):
        try:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(rows_data)
            self._rows_written += len(rows_data)
            print(f"Wrote {len(rows_data)} rows to {self.output_file}. Total rows written: {self._rows_written}")
        except Exception as e:
            print(f"Error writing rows to CSV file {self.output_file}: {e}")

    def _save_to_temp_file(self, queue_name, rows_data):
        temp_file = os.path.join(self.temp_dir, f"{queue_name}.csv")
        try:
            file_exists = os.path.exists(temp_file)
            with open(temp_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if not file_exists:
                    if queue_name == 'stores_cleaned_q4':
                        writer.writerow(['store_id', 'store_name'])
                    elif queue_name == 'users_cleaned':
                        writer.writerow(['user_id', 'birthdate'])
                    elif queue_name == 'resultados_groupby_q4':
                        writer.writerow(['store_id', 'user_id', 'purchase_qty'])
                writer.writerows(rows_data)
            print(f"Saved {len(rows_data)} rows to temp file: {temp_file}")
        except Exception as e:
            print(f"Error saving to temp file {temp_file}: {e}")

    def _send_sort_request(self):
        try:
            if self.out_queue:
                header = struct.pack('>BBI', 3, 0, 0)
                self.out_queue.send(header)
                print(f"Sent sort request for {self.output_file}")
        except Exception as e:
            print(f"Error sending sort request: {e}")

    # =====================
    # Dispatcher
    # =====================

    def _process_joined_data(self):
        if self.query_type not in self.strategies:
            raise ValueError(f"Unsupported query_type: {self.query_type}")
        self.strategies[self.query_type]()  # Ejecuta la estrategia correspondiente

    # =====================
    # Strategies
    # =====================

    def _process_q1(self):
        """
        Q1: transaction_id y final_amount de una sola cola
        """
        print("Processing Q1 join...")
        self._send_sort_request()

    def _process_q4(self):
        """
        Q4: join entre resultados_groupby_q4, stores_cleaned_q4 y users_cleaned
        """
        print("Processing Q4 join...")

        stores_lookup, users_lookup = {}, {}

        # Stores
        stores_file = os.path.join(self.temp_dir, 'stores_cleaned_q4.csv')
        if os.path.exists(stores_file):
            with open(stores_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f); next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        stores_lookup[row[0]] = row[1]
        print(f"Loaded {len(stores_lookup)} store mappings")

        # Users
        users_file = os.path.join(self.temp_dir, 'users_cleaned.csv')
        if os.path.exists(users_file):
            with open(users_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f); next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        users_lookup[row[0]] = row[1]
        print(f"Loaded {len(users_lookup)} user mappings")

        # Main
        main_file = os.path.join(self.temp_dir, 'resultados_groupby_q4.csv')
        if not os.path.exists(main_file):
            print("Main file for Q4 not found")
            return

        self._initialize_csv()
        processed_rows = []

        with open(main_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f); next(reader, None)
            for row in reader:
                if len(row) >= 3:
                    store_id, user_id, purchase_qty = row[0], row[1], row[2]
                    store_name = stores_lookup.get(store_id, store_id)
                    birthdate = users_lookup.get(user_id, user_id)
                    processed_rows.append([store_name, birthdate])

        if processed_rows:
            self._write_rows_to_csv(processed_rows)
        self._send_sort_request()
        print(f"Q4 join complete. {len(processed_rows)} rows written.")

    def _process_q2(self):
        """
        Q2: placeholder para lógica futura
        """
        print("Processing Q2 join... (TODO)")
        self._send_sort_request()

    # =====================
    # Principal Loop 
    # =====================

    def run(self):
        self._running = True

        def create_message_handler(queue_name):
            def on_message(message):
                if not self._running:
                    return
                try:
                    if not isinstance(message, bytes) or len(message) < 6:
                        return

                    header = message[:6]
                    msg_type, data_type, payload_len = struct.unpack('>BBI', header)
                    payload = message[6:]

                    if msg_type == 2:  # END
                        with self._lock:
                            if not self.end_received[queue_name]:
                                self.end_received[queue_name] = True
                                if all(self.end_received.values()):
                                    self._process_joined_data()
                        return

                    try:
                        payload_str = payload.decode('utf-8')
                    except Exception as e:
                        print(f"Decode error: {e}")
                        return

                    rows = payload_str.split('\n')
                    processed_rows = []
                    for row in rows:
                        if row.strip():
                            if queue_name == 'resultados_groupby_q4':
                                items = row.strip().split(',')
                                if len(items) >= 3:
                                    processed_rows.append(items)
                            else:
                                items = row.strip().split('|')
                                if len(items) >= 2:
                                    processed_rows.append(items)

                    if processed_rows:
                        with self._lock:
                            if len(self.multiple_queues) > 1:
                                self._save_to_temp_file(queue_name, processed_rows)
                            else:
                                final_rows = [[r[0], r[1]] for r in processed_rows if len(r) >= 2]
                                if not self._csv_initialized:
                                    self._initialize_csv()
                                if final_rows:
                                    self._write_rows_to_csv(final_rows)
                except Exception as e:
                    print(f"Error processing message: {e}")

            return on_message

        for queue_name in self.multiple_queues:
            handler = create_message_handler(queue_name)
            print(f"Joiner listening on {queue_name}")
            self.in_queues[queue_name].start_consuming(handler)

        self._shutdown_event.wait()

    def stop(self):
        self._running = False
        self._shutdown_event.set()
        for q in self.in_queues.values():
            try:
                q.stop_consuming()
            except:
                pass
        self.close()

    def close(self):
        for q in self.in_queues.values():
            try:
                q.close()
            except:
                pass
        if self.out_queue:
            self.out_queue.close()


if __name__ == '__main__':
    queue_in = os.environ.get('QUEUE_IN')
    queue_out = os.environ.get('QUEUE_OUT')
    output_file = os.environ.get('OUTPUT_FILE')
    query_type = os.environ.get('QUERY_TYPE')
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

    multiple_queues_str = os.environ.get('MULTIPLE_QUEUES')
    multiple_queues = [q.strip() for q in multiple_queues_str.split(',')] if multiple_queues_str else [queue_in]

    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_path)

    if query_type not in config:
        raise ValueError(f"Unknown query type: {query_type}")

    columns_want = [col.strip() for col in config[query_type]['columns'].split(',')]

    joiner = Joiner(queue_in, queue_out, output_file, columns_want, rabbitmq_host, query_type, multiple_queues)

    def signal_handler(signum, frame):
        joiner.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        print(f"Starting joiner for {query_type} query...")
        joiner.run()
    except KeyboardInterrupt:
        joiner.stop()
    finally:
        print("Joiner shutdown complete.")
