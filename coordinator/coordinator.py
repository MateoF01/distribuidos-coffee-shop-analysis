import os
import signal
import sys
import threading
import struct
import configparser
import logging
import time
from collections import defaultdict
from shared.logging_config import initialize_log
from shared.worker import StreamProcessingWorker
from shared import protocol

from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue

class Coordinator(StreamProcessingWorker):
    def __init__(self, queue_in, queue_out, rabbitmq_host, num_replicas):
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.num_replicas = num_replicas
        # Track END messages by (msg_type, data_type, request_id) tuple to distinguish different END types and request IDs
        self.end_messages_received = defaultdict(int)  
        self.end_messages_forwarded = set()  # Track which (msg_type, data_type, request_id) have already had END forwarded
        self.data_messages_buffer = []  # Buffer for data messages
        self.data_messages_count = defaultdict(int)  # Track data messages by data_type
        self.lock = threading.Lock()
        
    def _process_message(self, message, msg_type, data_type, request_id, timestamp, payload, queue_name=None):
        """Process messages from multiple replicas."""
        with self.lock:
            if msg_type == protocol.MSG_TYPE_END:
                self._handle_end_message(msg_type, data_type, request_id, timestamp, payload)
            elif msg_type == protocol.MSG_TYPE_DATA:
                self._handle_data_message(msg_type, data_type, request_id, timestamp, payload)
            else:
                # Forward other message types directly
                self._forward_message(msg_type, data_type, request_id, timestamp, payload)
    
    def _handle_data_message(self, msg_type, data_type, request_id, timestamp, payload):
        """Handle data messages - forward immediately."""
        # Count data messages
        self.data_messages_count[data_type] += 1
        
        # Data messages are forwarded immediately with per-hop timestamps
        new_message = protocol.create_data_message(data_type, payload, request_id)
        for q in self.out_queues:
            q.send(new_message)
        
        # Log data message with count every 1000 messages, and always log the first few
        if self.data_messages_count[data_type] <= 10 or self.data_messages_count[data_type] % 1000 == 0:
            logging.info(f"Forwarded data message #{self.data_messages_count[data_type]} (data_type: {data_type}, request_id: {request_id}, payload_size: {len(payload)} bytes)")
        else:
            logging.debug(f"Forwarded data message #{self.data_messages_count[data_type]} (data_type: {data_type}, request_id: {request_id})")
    
    def _handle_end_message(self, msg_type, data_type, request_id, timestamp, payload):
        """Handle END messages - wait for all replicas before forwarding."""
        # Use (msg_type, data_type, request_id) tuple to distinguish different types of END messages and request IDs
        end_key = (msg_type, data_type, request_id)
        self.end_messages_received[end_key] += 1
        logging.debug(f"Received END message {self.end_messages_received[end_key]}/{self.num_replicas} for msg_type: {msg_type}, data_type: {data_type}, request_id: {request_id}")
        
        # Only forward END message once after receiving from all replicas for this specific request_id
        if (self.end_messages_received[end_key] >= self.num_replicas and 
            end_key not in self.end_messages_forwarded):
            # All replicas have sent END message for this request_id, forward it once
            new_message = protocol.create_end_message(data_type, request_id)
            for q in self.out_queues:
                q.send(new_message)
            
            # Log END message with data message count summary
            total_data_msgs = self.data_messages_count[data_type]
            logging.info(f"Forwarded END message for msg_type: {msg_type}, data_type: {data_type}, request_id: {request_id} after receiving from all {self.num_replicas} replicas. Total data messages forwarded: {total_data_msgs}")
            
            # Mark this (msg_type, data_type, request_id) as having END forwarded
            self.end_messages_forwarded.add(end_key)
    
    def _forward_message(self, msg_type, data_type, request_id, timestamp, payload):
        """Forward other message types directly."""
        if msg_type == protocol.MSG_TYPE_DATA:
            new_message = protocol.create_data_message(data_type, payload, request_id)
        elif msg_type == protocol.MSG_TYPE_END:
            new_message = protocol.create_end_message(data_type, request_id)
        else:
            new_message = protocol.create_notification_message(data_type, payload, request_id)
        for q in self.out_queues:
            q.send(new_message)
        logging.debug(f"Forwarded message (msg_type: {msg_type}, data_type: {data_type}, request_id: {request_id})")
    
    def _process_rows(self, rows, queue_name=None):
        """Not used in coordinator - messages are handled in _process_message."""
        return []

def create_coordinator():
    """Create coordinator instance from environment variables."""
    queue_in = os.environ.get('QUEUE_IN')
    queue_out = os.environ.get('QUEUE_OUT')
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    num_replicas = int(os.environ.get('NUM_REPLICAS', '1'))
    
    return Coordinator(queue_in, queue_out, rabbitmq_host, num_replicas)

if __name__ == "__main__":
    initialize_log(logging.INFO)
    
    # Use the Worker base class main entry point
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Coordinator.run_worker_main(create_coordinator, config_path)