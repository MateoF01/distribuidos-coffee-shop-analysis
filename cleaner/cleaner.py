import os
import signal
import sys
import threading
import configparser
import time
import heapq
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue, CoffeeMessageMiddlewareExchange
from shared import protocol
import logging
from shared.logging_config import initialize_log
from shared.worker import StreamProcessingWorker

class Cleaner(StreamProcessingWorker):
    def __init__(self, queue_in, queue_out, columns_have, columns_want, rabbitmq_host, keep_when_empty=None, exchange_name=None, timeout_seconds=5.0):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.columns_have = columns_have
        self.columns_want = columns_want
        self.keep_indices = [self.columns_have.index(col) for col in self.columns_want]
        self.keep_when_empty = [self.columns_want.index(col) for col in keep_when_empty] if keep_when_empty else []
        
        # Exchange subscriber for END messages
        self.exchange_name = exchange_name
        self.end_exchange = None
        self.end_messages_heap = []  # Heap for all END messages - ordered by timestamp (timestamp, request_id, data_type)
        self.lock = threading.Lock()
        
        # Timeout mechanism for processing remaining END messages
        self.timeout_seconds = timeout_seconds  # Configurable timeout from config
        self.last_process_time = time.time()
        self.timeout_timer = None
        self.timeout_cancelled = threading.Event()  # Flag to cancel timeout processing
        
        # Race condition detection counters
        self.data_messages_count = 0
        self.end_messages_count = 0
        self.held_back_messages_count = 0
        self.processed_messages_count = 0
        
        if self.exchange_name:
            self._setup_exchange_subscriber()
        
        logging.info(f"Cleaner initialized - input: {queue_in}, output: {queue_out}, exchange: {exchange_name}")
    
    def _setup_exchange_subscriber(self):
        """Set up exchange subscriber for END messages."""
        try:
            self.end_exchange = CoffeeMessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=self.exchange_name,
                route_keys=[]
            )
            # Start consuming END messages in a separate thread
            def on_end_message(message):
                self._handle_end_message(message)
            
            threading.Thread(
                target=lambda: self.end_exchange.start_consuming(on_end_message),
                daemon=True
            ).start()
            logging.info(f"Started consuming END messages from exchange: {self.exchange_name}")
            
            # Start the timeout timer
            self._reset_timeout_timer()
        except Exception as e:
            logging.error(f"Failed to setup exchange subscriber: {e}")
    
    def _handle_end_message(self, message):
        """Handle END messages from exchange."""
        try:
            msg_type, data_type, request_id, timestamp, payload = protocol.unpack_message(message)
            with self.lock:
                # Add all END messages to the single heap, ordered by timestamp
                heapq.heappush(self.end_messages_heap, (timestamp, request_id, data_type))
                self.end_messages_count += 1
                logging.info(f"[RACE_DETECTION] Received END message #{self.end_messages_count} for request_id {request_id} (data_type {data_type}) with timestamp {timestamp}")
                logging.info(f"[RACE_DETECTION] Current stats - Data: {self.data_messages_count}, END: {self.end_messages_count}, Held: {self.held_back_messages_count}, Processed: {self.processed_messages_count}")
        except Exception as e:
            logging.error(f"Error handling END message: {e}")

    def _process_remaining_end_messages(self):
        """Process all remaining END messages in the heap when timeout occurs."""
        # Check if timeout processing was cancelled before starting
        if self.timeout_cancelled.is_set():
            logging.debug("[TIMEOUT] Timeout processing cancelled before starting")
            return
            
        with self.lock:
            processed_count = 0
            while self.end_messages_heap and not self.timeout_cancelled.is_set():
                end_timestamp, end_request_id, end_data_type = heapq.heappop(self.end_messages_heap)
                logging.info(f"[TIMEOUT] Processing END message from heap due to timeout: timestamp={end_timestamp}, request_id={end_request_id}, data_type={end_data_type}")
                # Process the END message outside the lock
                self.lock.release()
                try:
                    # Check again if cancelled while we were outside the lock
                    if self.timeout_cancelled.is_set():
                        logging.info(f"[TIMEOUT] Timeout processing cancelled, stopping after {processed_count} messages")
                        # Put the message back in the heap since we didn't process it
                        with self.lock:
                            heapq.heappush(self.end_messages_heap, (end_timestamp, end_request_id, end_data_type))
                        return
                    
                    self._handle_data_end_message(protocol.MSG_TYPE_END, end_data_type, end_request_id, end_timestamp, b"")
                    processed_count += 1
                finally:
                    self.lock.acquire()
            
            if processed_count > 0:
                if self.timeout_cancelled.is_set():
                    logging.info(f"[TIMEOUT] Processed {processed_count} END messages from heap before cancellation")
                else:
                    logging.info(f"[TIMEOUT] Processed {processed_count} END messages from heap due to timeout")
    
    def _reset_timeout_timer(self):
        """Reset the timeout timer."""
        # Signal any running timeout processing to stop
        self.timeout_cancelled.set()
        
        # Cancel existing timer if any
        if self.timeout_timer:
            self.timeout_timer.cancel()
        
        # Clear the cancellation flag for the new timer
        self.timeout_cancelled.clear()
        
        # Start new timer
        self.timeout_timer = threading.Timer(self.timeout_seconds, self._process_remaining_end_messages)
        self.timeout_timer.start()
        self.last_process_time = time.time()
        logging.debug("[TIMEOUT] Timer reset - any running timeout processing will be cancelled")

    def _process_message(self, message, msg_type, data_type, request_id, timestamp, payload, queue_name=None):
        """Override to add timestamp comparison logic."""
        # Reset timeout timer when any message is processed
        if self.exchange_name:
            self._reset_timeout_timer()
        
        if msg_type == protocol.MSG_TYPE_END:
            # Handle END messages with timestamp comparison
            logging.info(f"[RACE_DETECTION] Processing END message for request_id {request_id} (data_type {data_type}) with timestamp {timestamp}")
            self._handle_data_end_message(msg_type, data_type, request_id, timestamp, payload)
        else:
            # For data messages, check if we should send based on END timestamp
            with self.lock:
                self.data_messages_count += 1
                
            # Check if exchange_name is configured
            if self.exchange_name is None:
                # No exchange configured, process normally
                with self.lock:
                    self.processed_messages_count += 1
                logging.debug(f"[RACE_DETECTION] Processing data message #{self.data_messages_count} (request_id: {request_id}, data_type: {data_type}, timestamp: {timestamp}) - SENDING")
                super()._process_message(message, msg_type, data_type, request_id, timestamp, payload, queue_name)
            else:
                # Exchange configured, process END messages with lower timestamps first
                with self.lock:
                    # Process all END messages with timestamps lower than current data message
                    while (self.end_messages_heap and 
                           self.end_messages_heap[0][0] < timestamp):  # heap[0][0] is the timestamp
                        end_timestamp, end_request_id, end_data_type = heapq.heappop(self.end_messages_heap)
                        logging.info(f"[RACE_DETECTION] Processing END message from heap: timestamp={end_timestamp}, request_id={end_request_id}, data_type={end_data_type}")
                        # Process the END message outside the lock
                        self.lock.release()
                        try:
                            self._handle_data_end_message(protocol.MSG_TYPE_END, end_data_type, end_request_id, end_timestamp, b"")
                        finally:
                            self.lock.acquire()
                    
                    # Now process the data message
                    self.processed_messages_count += 1
                
                logging.debug(f"[RACE_DETECTION] Processing data message #{self.data_messages_count} (request_id: {request_id}, data_type: {data_type}, timestamp: {timestamp}) - SENDING")
                super()._process_message(message, msg_type, data_type, request_id, timestamp, payload, queue_name)
            # Log periodic statistics
            if self.data_messages_count % 5000 == 0:
                with self.lock:
                    logging.info(f"[RACE_DETECTION] Periodic stats - Data messages: {self.data_messages_count}, Processed: {self.processed_messages_count}, Held back: {self.held_back_messages_count}, END messages: {self.end_messages_count}")
    

    def _handle_data_end_message(self, msg_type, data_type, request_id, timestamp, payload):
        """Handle END messages - always forward with new timestamp."""
        with self.lock:
            current_stats = f"Data: {self.data_messages_count}, Processed: {self.processed_messages_count}, Held: {self.held_back_messages_count}, END: {self.end_messages_count}"
            
        if data_type == protocol.DATA_END:
            # For DATA_END, always forward with new timestamp
            new_message = protocol.create_end_message(data_type, request_id)
            for q in self.out_queues:
                q.send(new_message)
            logging.info(f"[RACE_DETECTION] Forwarded DATA_END message - Final stats: {current_stats}")
        else:
            # For specific data type END messages, forward with new timestamp
            new_message = protocol.create_end_message(data_type, request_id)
            for q in self.out_queues:
                q.send(new_message)
            logging.info(f"[RACE_DETECTION] Forwarded END message for data_type {data_type} - Current stats: {current_stats}")

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
        exchange_name = os.environ.get('EXCHANGE_NAME')  # Optional exchange for END messages

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
        
        # Read timeout from config with default fallback
        timeout_seconds = float(config[data_type].get('timeout_seconds', '5.0'))

        return Cleaner(queue_in, queue_out, columns_have, columns_want, rabbitmq_host, keep_when_empty, exchange_name, timeout_seconds)
    
    # Use the Worker base class main entry point
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Cleaner.run_worker_main(create_cleaner, config_path)
