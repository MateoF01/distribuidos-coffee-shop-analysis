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
from shared.worker import Worker
from shared import protocol
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue

class Coordinator(Worker):

  def __init__(self, queue_in, queue_out, rabbitmq_host):
    super().__init__(queue_in, queue_out, rabbitmq_host)

    self.messages_by_request_id_counter = defaultdict(int)
    # Track END messages by (msg_type, data_type, request_id) tuple to distinguish different END types and request IDs
    self.end_messages_received = defaultdict(int)  
    self.end_messages_forwarded = set()  # Track which (msg_type, data_type, request_id) have already had END forwarded
    self.data_messages_buffer = []  # Buffer for data messages
    self.data_messages_count = defaultdict(int)  # Track data messages by data_type
    self.lock = threading.Lock()

  def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
    self.messages_by_request_id_counter[request_id] = self.messages_by_request_id_counter.get(request_id, 0) + 1
    new_position = self.messages_by_request_id_counter[request_id]
    self._forward_message(msg_type, data_type, request_id, new_position, payload)

    # def _handle_data_message(self, msg_type, data_type, request_id, timestamp, payload):
    #   # Count data messages
    #   self.data_messages_count[data_type] += 1
    #   # Data messages are forwarded immediately with per-hop timestamps
    #   new_message = protocol.create_data_message(data_type, payload, request_id)
    #   for q in self.out_queues:
    #     q.send(new_message)
    #   # Log data message with count every 1000 messages, and always log the first few
    #   if self.data_messages_count[data_type] <= 10 or self.data_messages_count[data_type] % 1000 == 0:
    #     logging.info(f"Forwarded data message #{self.data_messages_count[data_type]} (data_type: {data_type}, request_id: {request_id}, payload_size: {len(payload)} bytes)")
    #   else:
    #     logging.debug(f"Forwarded data message #{self.data_messages_count[data_type]} (data_type: {data_type}, request_id: {request_id})")

    # def _handle_end_message(self, msg_type, data_type, request_id, timestamp, payload):
    #     new_message = protocol.create_end_message(data_type, request_id)
    #     for q in self.out_queues:
    #       q.send(new_message)
    #     # Log END message with data message count summary
    #     total_data_msgs = self.data_messages_count[data_type]
    #     logging.info(f"Forwarded END message for msg_type: {msg_type}, data_type: {data_type}, request_id: {request_id}. Total data messages forwarded: {total_data_msgs}")

  def _handle_end_signal(self, message, msg_type, data_type, request_id, position, queue_name=None):
    self.messages_by_request_id_counter[request_id] = self.messages_by_request_id_counter.get(request_id, 0) + 1
    new_position = self.messages_by_request_id_counter[request_id]
    self._forward_message(msg_type, data_type, request_id, new_position, b'')


  def _forward_message(self, msg_type, data_type, request_id, position, payload):
    """Forward other message types directly."""
    new_message = None
    if msg_type == protocol.MSG_TYPE_DATA:
      new_message = protocol.create_data_message(data_type, payload, request_id, position)
    elif msg_type == protocol.MSG_TYPE_END:
      new_message = protocol.create_end_message(data_type, request_id, position)
    else:
      new_message = protocol.create_notification_message(data_type, payload, request_id, position)

    for q in self.out_queues:
      logging.info(f"Sending to queue: {q.queue_name}")
      q.send(new_message)

    logging.info(f"Forwarded message (msg_type: {msg_type}, data_type: {data_type}, request_id: {request_id}, position: {position}, payload_size: {len(payload)} bytes)")

  def _process_rows(self, rows, queue_name=None):
    """Not used in coordinator - messages are handled in _process_message."""
    return []

def create_coordinator():
  """Create coordinator instance from environment variables."""
  queue_in = os.environ.get('QUEUE_IN')
  queue_out = os.environ.get('QUEUE_OUT')
  rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

  return Coordinator(queue_in, queue_out, rabbitmq_host)

if __name__ == "__main__":

  config_path = os.path.join(os.path.dirname(__file__), 'config.ini')

  Coordinator.run_worker_main(create_coordinator, config_path)