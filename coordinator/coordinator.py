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
    
    # Persistence paths
    self.state_dir = os.path.join(os.path.dirname(__file__), '..', 'output_coordinator')
    self.positions_dir = os.path.join(self.state_dir, 'positions')
    self.assigned_dir = os.path.join(self.state_dir, 'assigned')
    
    # Create directories if they don't exist
    os.makedirs(self.positions_dir, exist_ok=True)
    os.makedirs(self.assigned_dir, exist_ok=True)

  def _load_assigned_position_for_request(self, request_id):
    """
    Lazy-load the assigned position for a request_id from disk.
    If the file exists, load it into the counter.
    If not, the counter stays at 0 (default from defaultdict).
    """
    if request_id not in self.messages_by_request_id_counter or self.messages_by_request_id_counter[request_id] == 0:
      assigned_file = os.path.join(self.assigned_dir, f'{request_id}.txt')
      if os.path.exists(assigned_file):
        try:
          with open(assigned_file, 'r') as f:
            content = f.read().strip()
            if content:
              last_assigned_position = int(content)
              self.messages_by_request_id_counter[request_id] = last_assigned_position
              logging.info(f"Recovered assigned position for request_id={request_id}: {last_assigned_position}")
        except Exception as e:
          logging.warning(f"Failed to load assigned position from {assigned_file}: {e}")


  def _check_and_persist_input_position(self, request_id, position):
    """
    Check if position already persisted for this request_id by reading from disk.
    If not persisted, add it atomically.
    Returns True if position is new (not duplicate), False if it's a duplicate.
    """
    positions_file = os.path.join(self.positions_dir, f'{request_id}.txt')
    
    with self.lock:
      # Read current positions from disk
      current_positions = set()
      if os.path.exists(positions_file):
        try:
          with open(positions_file, 'r') as f:
            for line in f:
              line = line.strip()
              if line:
                current_positions.add(int(line))
        except Exception as e:
          logging.error(f"Error reading positions file for request_id={request_id}: {e}")
          raise
      
      # Check if position already exists
      if position in current_positions:
        logging.info(f"Duplicate position received: request_id={request_id}, position={position}")
        return False
      
      # Add new position and persist atomically
      current_positions.add(position)
      
      def write_all_positions(path):
        with open(path, 'w') as f:
          for pos in sorted(current_positions):
            f.write(f'{pos}\n')
      
      try:
        Worker.atomic_write(positions_file, write_all_positions)
        logging.info(f"Persisted input position: request_id={request_id}, position={position}")
        return True
      except Exception as e:
        logging.error(f"Error persisting position for request_id={request_id}, position={position}: {e}")
        raise

  def _persist_assigned_position(self, request_id, new_position):
    """Persist the assigned new_position atomically."""
    assigned_file = os.path.join(self.assigned_dir, f'{request_id}.txt')
    
    def write_assigned(path):
      with open(path, 'w') as f:
        f.write(f'{new_position}\n')
    
    try:
      Worker.atomic_write(assigned_file, write_assigned)
      logging.debug(f"Persisted assigned position: request_id={request_id}, new_position={new_position}")
    except Exception as e:
      logging.error(f"Error persisting assigned position for request_id={request_id}, new_position={new_position}: {e}")
      raise

  def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
    # FIRST: Lazy-load assigned position for this request_id if needed
    self._load_assigned_position_for_request(request_id)
    
    # SECOND: Check if position already persisted (duplicate detection)
    if not self._check_and_persist_input_position(request_id, position):
      # This is a duplicate, discard the message
      logging.warning(f"Discarding duplicate message: request_id={request_id}, position={position}, msg_type={msg_type}")
      return
    
    self.messages_by_request_id_counter[request_id] = self.messages_by_request_id_counter.get(request_id, 0) + 1
    new_position = self.messages_by_request_id_counter[request_id]
    
    # Persist the assigned new_position before forwarding
    self._persist_assigned_position(request_id, new_position)
    
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
    # FIRST: Lazy-load assigned position for this request_id if needed
    self._load_assigned_position_for_request(request_id)
    
    # SECOND: Check if position already persisted (duplicate detection)
    if not self._check_and_persist_input_position(request_id, position):
      # This is a duplicate, discard the message
      logging.warning(f"Discarding duplicate END signal: request_id={request_id}, position={position}")
      return
    
    self.messages_by_request_id_counter[request_id] = self.messages_by_request_id_counter.get(request_id, 0) + 1
    new_position = self.messages_by_request_id_counter[request_id]
    
    # Persist the assigned new_position before forwarding
    self._persist_assigned_position(request_id, new_position)
    
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