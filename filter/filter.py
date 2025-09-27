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
                filtered_rows = [self._filter_row(row) for row in rows if row.strip()]
                filtered_rows = [row for row in filtered_rows if row]

                print("FILTERED_ROWS: ", filtered_rows)

                if not filtered_rows:
                    return

                new_payload_str = '\n'.join(filtered_rows)
                new_payload = new_payload_str.encode('utf-8')
                new_payload_len = len(new_payload)
                new_header = struct.pack('>BBI', msg_type, data_type, new_payload_len)
                new_message = new_header + new_payload

                print("NEW_MESSAGE: ", new_message)

                for q in self.out_queues:
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
    def __init__(self, queue_in, queue_out, rabbitmq_host, year_start, year_end, hour_start, hour_end, col_index):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.year_start = int(year_start)
        self.year_end = int(year_end)
        self.hour_start = int(hour_start)
        self.hour_end = int(hour_end)
        self.col_index = col_index

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

        if not (self.year_start <= year <= self.year_end):
            return None
        if not (self.hour_start <= hour <= self.hour_end):
            return None
        return row


class AmountFilter(Filter):
    def __init__(self, queue_in, queue_out, rabbitmq_host, min_amount, col_index):
        super().__init__(queue_in, queue_out, rabbitmq_host)
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
        return row


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
        year_start =os.environ.get('YEAR_START')
        year_end = os.environ.get('YEAR_END')
        hour_start = os.environ.get('HOUR_START')
        hour_end = os.environ.get('HOUR_END')
        f = TemporalFilter(queue_in, queue_out, rabbitmq_host, year_start, year_end, hour_start, hour_end, col_index)

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
