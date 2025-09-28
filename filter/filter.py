import os
import signal
import sys
import threading
import struct
import configparser

from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue

class Filter:
    def __init__(self, queue_in, queue_out, rabbitmq_host):
        if isinstance(queue_out, str):
            queue_out = [q.strip() for q in queue_out.split(',')]
        self.queue_in = queue_in
        self.queue_out = queue_out
        self._running = False
        self._shutdown_event = threading.Event()
        self.in_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_in)
        self.out_queues = [CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=q) for q in self.queue_out]

    # Método genérico que cada hijo va a redefinir
    def _filter_row(self, row: str):
        return row

    def run(self):
        def on_message(message):
            if not self._running:
                return
            try:
                if not isinstance(message, bytes) or len(message) < 6:
                    return
                header = message[:6]
                msg_type, data_type, payload_len = struct.unpack('>BBI', header)
                payload = message[6:]

                if msg_type == 2:
                    for q in self.out_queues:
                        q.send(message)
                    if data_type == 6:
                        self.stop()
                    return

                payload_str = payload.decode('utf-8')
                rows = payload_str.split('\n')

                dic_queue_row = {}

                for row in rows:
                    rows_queues = self._filter_row(row) or []

                    print("[ROWS_QUEUES: ] ", rows_queues)

                    for row, queue_name in rows_queues:
                        if queue_name not in dic_queue_row:
                            dic_queue_row[queue_name] = []
                        
                        dic_queue_row[queue_name].append(row)
                        

                print("DIC_QUE_ROW: ", dic_queue_row)

                if not dic_queue_row:
                    return

                for queue_name in dic_queue_row.keys():
                    
                    filtered_rows = dic_queue_row[queue_name]

                    new_payload_str = '\n'.join(filtered_rows)
                    new_payload = new_payload_str.encode('utf-8')
                    new_payload_len = len(new_payload)
                    new_header = struct.pack('>BBI', msg_type, data_type, new_payload_len)
                    new_message = new_header + new_payload

                    print("NEW_MESSAGE: ", new_message)

                    for q in self.out_queues:
                        if q.queue_name == queue_name:
                            q.send(new_message)

            except Exception as e:
                print(f"Error processing message: {e} - Message: {message}")

        self._running = True
        self.in_queue.start_consuming(on_message)
        self._shutdown_event.wait()

    def stop(self):
        self._running = False
        self._shutdown_event.set()
        try:
            self.in_queue.stop_consuming()
        except Exception:
            pass
        self.close()

    def close(self):
        self.in_queue.close()
        for q in self.out_queues:
            q.close()


# ====================
# Filters
# ====================

class TemporalFilter(Filter):
    def __init__(self, queue_in, queue_out, rabbitmq_host, data_type, col_index, config):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.data_type = data_type
        self.col_index = col_index
        self.rules = []  # Each rule is a dic with the conditions

        # Load rules
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

        print(f"[TEMPORAL FILTER] Loaded rules for {self.data_type}: {self.rules}")

    def _route(self, row: str, year, hour):
        result = []  # [(queue_name, row)]
        for rule in self.rules:
            if rule["year_start"] <= year <= rule["year_end"] and \
               rule["hour_start"] <= hour <= rule["hour_end"]:
                result.append((row, rule["queue_out"]))
        return result

    def _filter_row(self, row: str):

        print("[TEMPORAL FILTER] ROW: ", row)

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
    def __init__(self, queue_in, queue_out, rabbitmq_host, min_amount, col_index):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.min_amount = float(min_amount)
        self.col_index = col_index

    def _filter_row(self, row: str):
        
        print("[AMOUNT FILTER] ROW: ", row)

        parts = row.split('|')

        if "final_amount" not in self.col_index or len(parts) <= self.col_index["final_amount"]:
            return None
        try:
            amount = float(parts[self.col_index["final_amount"]])
        except Exception:
            return None

        if amount < self.min_amount:
            return None
        return [(row, queue_out)]


# ====================
# Main
# ====================

if __name__ == '__main__':
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


    if filter_type == 'temporal':
        temporal_config_path = os.path.join(os.path.dirname(__file__), 'temporal_filter_config.ini')
        temporal_config = configparser.ConfigParser()
        temporal_config.read(temporal_config_path)
        f = TemporalFilter(queue_in, queue_out, rabbitmq_host, data_type, col_index, temporal_config)

    elif filter_type == 'amount':
        min_amount = os.environ.get('MIN_AMOUNT')
        f = AmountFilter(queue_in, queue_out, rabbitmq_host, min_amount, col_index)

    else:
        raise ValueError(f"Unknown FILTER_TYPE: {filter_type}")

    # Signal handling
    def signal_handler(signum, frame):
        print(f"Signal {signum}, shutting down filter gracefully...")
        f.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print(f"Starting {filter_type} filter...")
    f.run()
