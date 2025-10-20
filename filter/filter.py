import os
import signal
import sys
import threading
import struct
import configparser
import logging
import time
from shared.logging_config import initialize_log
from shared.worker import StreamProcessingWorker
from shared import protocol


class Filter(StreamProcessingWorker):
    def __init__(self, queue_in, queue_out, rabbitmq_host):
        super().__init__(queue_in, queue_out, rabbitmq_host)

    def _process_rows(self, rows, queue_name=None):
        """Process rows and return results organized by queue."""
        dic_queue_row = {}
        
        for row in rows:
            if not row.strip():
                continue
                
            rows_queues = self._filter_row(row) or []
            
            for filtered_row, target_queue_name in rows_queues:
                if target_queue_name not in dic_queue_row:
                    dic_queue_row[target_queue_name] = []
                dic_queue_row[target_queue_name].append(filtered_row)
        
        return dic_queue_row
    
    def _send_complex_results(self, dic_queue_row, msg_type, data_type, request_id, timestamp):
        """Send filtered results to appropriate queues."""
        import time
        for queue_name, filtered_rows in dic_queue_row.items():
            new_payload_str = '\n'.join(filtered_rows)
            new_payload = new_payload_str.encode('utf-8')
            # Use per-hop timestamps: generate new timestamp for this forwarding step
            current_timestamp = time.time()
            new_message = protocol.create_data_message(data_type, new_payload, request_id, current_timestamp)

            for q in self.out_queues:
                if q.queue_name == queue_name:
                    q.send(new_message)

    # Método genérico que cada hijo va a redefinir
    def _filter_row(self, row: str):
        return [(row, self.out_queues[0].queue_name if self.out_queues else "default")]




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

    def _route(self, row: str, year, hour):
        result = []  # [(row, queue_name)]
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

        if filter_type == 'temporal':
            temporal_config_path = os.path.join(os.path.dirname(__file__), 'temporal_filter_config.ini')
            temporal_config = configparser.ConfigParser()
            temporal_config.read(temporal_config_path)
            return TemporalFilter(queue_in, queue_out, rabbitmq_host, data_type, col_index, temporal_config)

        elif filter_type == 'amount':
            min_amount = os.environ.get('MIN_AMOUNT')
            return AmountFilter(queue_in, queue_out, rabbitmq_host, min_amount, col_index)

        else:
            raise ValueError(f"Unknown FILTER_TYPE: {filter_type}")

    # Use the Worker base class main entry point
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Filter.run_worker_main(create_filter, config_path)
