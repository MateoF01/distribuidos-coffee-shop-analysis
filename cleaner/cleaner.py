import os
import signal
import sys
import threading
import configparser
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue
from shared import protocol
import logging
from shared.logging_config import initialize_log
from shared.worker import StreamProcessingWorker

class Cleaner(StreamProcessingWorker):
    def __init__(self, queue_in, queue_out, columns_have, columns_want, rabbitmq_host, keep_when_empty=None):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.columns_have = columns_have
        self.columns_want = columns_want
        self.keep_indices = [self.columns_have.index(col) for col in self.columns_want]
        self.keep_when_empty = [self.columns_want.index(col) for col in keep_when_empty] if keep_when_empty else []

    def _process_rows(self, rows, queue_name=None):
        """Process rows by filtering and cleaning them."""
        filtered_rows = []
        for row in rows:
            if row.strip():
                cleaned_row = self._filter_row(row)
                if cleaned_row:
                    filtered_rows.append(cleaned_row)
        return filtered_rows
    
    def _filter_row(self, row):
        items = row.split('|')

        if len(items) < max(self.keep_indices) + 1:
            logging.warning(f"Row has insufficient columns: {row} (expected {max(self.keep_indices) + 1}, got {len(items)})")
            return None

        try:
            selected = [items[i] for i in self.keep_indices]
        except IndexError as e:
            logging.error(f"Index error processing row: {row} - {e}")
            return None

        # Convert user_id to int if present in columns_want
        if 'user_id' in self.columns_want:
            user_id_idx = self.columns_want.index('user_id')
            if selected[user_id_idx] != '':
                try:
                    # Remove .0 if present (float to int string)
                    float_val = float(selected[user_id_idx])
                    int_val = int(float_val)
                    selected[user_id_idx] = str(int_val)
                except Exception as e:
                    logging.warning(f"Could not convert user_id '{selected[user_id_idx]}' to int in row: {row} - {e}")

        if any(selected[i] == '' and i not in self.keep_when_empty for i in range(len(selected))):
            return None

        return '|'.join(selected)



if __name__ == '__main__':
    def create_cleaner():
        # Configure via environment variables
        queue_in = os.environ.get('QUEUE_IN')
        queue_out = os.environ.get('QUEUE_OUT')
        data_type = os.environ.get('DATA_TYPE')
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)

        if data_type not in config:
            raise ValueError(f"Unknown data type: {data_type}")
        
        columns_have = [col.strip() for col in config[data_type]['have'].split(',')]
        columns_want = [col.strip() for col in config[data_type]['want'].split(',')]
        keep_when_empty_str = config[data_type].get('keep_when_empty', '').strip()
        keep_when_empty = [col.strip() for col in keep_when_empty_str.split(',')] if keep_when_empty_str else None

        return Cleaner(queue_in, queue_out, columns_have, columns_want, rabbitmq_host, keep_when_empty)
    
    # Use the Worker base class main entry point
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Cleaner.run_worker_main(create_cleaner, config_path)
