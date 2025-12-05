import os
import threading
import logging
from collections import defaultdict
from shared.worker import Worker
from shared import protocol

from WSM.wsm_client import WSMClient
from wsm_config import WSM_NODES
import socket


class Coordinator(Worker):
  """
  Message coordinator that reorders distributed worker outputs into sequential positions.
  
  The Coordinator is placed AFTER distributed stream processing workers (cleaners, filters,
  etc.) to consolidate their outputs and assign monotonically increasing position numbers.
  Since multiple worker replicas process messages in parallel, their outputs may arrive
  out of order. The Coordinator ensures total ordering within each request by:
  - Receiving messages from multiple distributed workers
  - Assigning sequential positions to reorder messages
  - Detecting and discarding duplicate messages
  - Persisting state for crash recovery
  
  The coordinator maintains two types of persistent state:
  1. Input positions: Tracks which positions have been received (for duplicate detection)
  2. Assigned positions: Tracks the last position assigned to each request
  
  Attributes:
      messages_by_request_id_counter (defaultdict): Counter for assigned positions per request.
      end_messages_received (defaultdict): Count of END messages received per message type.
      end_messages_forwarded (set): Track which END messages have been forwarded.
      data_messages_buffer (list): Buffer for data messages (currently unused).
      data_messages_count (defaultdict): Count of data messages by data type.
      state_dir (str): Directory for persistent state storage.
      positions_dir (str): Directory for input position tracking.
      assigned_dir (str): Directory for assigned position tracking.
  
  Example:
      Pipeline placement:
      Gateway -> Cleaner (3 replicas) -> Coordinator -> Joiner
      
      The Cleaner replicas process messages in parallel and send to Coordinator:
      - Cleaner-1 sends: (req-123, pos=5) at t=1
      - Cleaner-2 sends: (req-123, pos=3) at t=2
      - Cleaner-3 sends: (req-123, pos=4) at t=3
      
      Coordinator reorders and assigns sequential positions:
      >>> coordinator = Coordinator(
      ...     queue_in='cleaner_output',
      ...     queue_out='joiner_input',
      ...     rabbitmq_host='rabbitmq'
      ... )
      >>> # Receives pos=5, assigns new_position=1
      >>> # Receives pos=3, assigns new_position=2
      >>> # Receives pos=4, assigns new_position=3
      
      State persistence after crash:
      >>> # On restart, recovers last assigned positions from disk
      >>> coordinator = Coordinator('input', 'output', 'rabbitmq')
      >>> # Continues from last position: next message gets position=4
  """

  def __init__(self, queue_in, queue_out, rabbitmq_host):
    """
    Initialize the Coordinator with persistent state directories.
    
    Args:
        queue_in (str): Input RabbitMQ queue name (receives from distributed workers).
        queue_out (str|list): Output queue name(s) (sends to downstream aggregators).
        rabbitmq_host (str): RabbitMQ server hostname.
    
    Example:
        Receiving from distributed cleaners and sending to joiner:
        >>> coordinator = Coordinator(
        ...     queue_in='cleaner_output',
        ...     queue_out='joiner_input',
        ...     rabbitmq_host='rabbitmq'
        ... )
        
        Receiving from distributed filters and sending to reducer:
        >>> coordinator = Coordinator(
        ...     queue_in='filter_output',
        ...     queue_out='reducer_input',
        ...     rabbitmq_host='rabbitmq'
        ... )
    """



    
    service_name = f"coordinator_{os.environ.get('DATA_TYPE', '')}"
    super().__init__(queue_in, queue_out, rabbitmq_host, service_name=service_name)

    # --- WSM Heartbeat Integration ----
    self.replica_id = socket.gethostname()

    worker_type_key = "coordinator"

    # Read WSM host/port (OPTIONAL for single-node; required if you specify wsm host in compose)
    wsm_host = os.environ.get("WSM_HOST", None)
    wsm_port = int(os.environ.get("WSM_PORT", "0")) if os.environ.get("WSM_PORT") else None

    # Load multi-node config if exists
    wsm_nodes = WSM_NODES.get(worker_type_key)

    # Create client in heartbeat-only mode
    self.wsm_client = WSMClient(
        worker_type=worker_type_key,
        replica_id=self.replica_id,
        host=wsm_host,
        port=wsm_port,
        nodes=wsm_nodes
    )

    logging.info(f"[Coordinator] Heartbeat WSM client ready for {worker_type_key}, replica={self.replica_id}")


    self.messages_by_request_id_counter = defaultdict(int)
    self.end_messages_received = defaultdict(int)  
    self.end_messages_forwarded = set()
    self.data_messages_buffer = []
    self.data_messages_count = defaultdict(int)
    self.lock = threading.Lock()
    
    self.state_dir = os.path.join(os.path.dirname(__file__), '..', 'output_coordinator')
    self.positions_dir = os.path.join(self.state_dir, 'positions')
    self.assigned_dir = os.path.join(self.state_dir, 'assigned')
    
    os.makedirs(self.positions_dir, exist_ok=True)
    os.makedirs(self.assigned_dir, exist_ok=True)

  def _load_assigned_position_for_request(self, request_id):
    """
    Lazy-load the last assigned position for a request from persistent storage.
    
    Reads the assigned position file from disk if it exists and the counter
    is not yet initialized. This allows the coordinator to recover its state
    after a restart without loading all request states at initialization.
    
    Args:
        request_id (str): Unique request identifier.
    
    Example:
        >>> coordinator._load_assigned_position_for_request('req-123')
        Recovered assigned position for request_id=req-123: 42
        >>> coordinator.messages_by_request_id_counter['req-123']
        42
        
        No file exists:
        >>> coordinator._load_assigned_position_for_request('req-999')
        >>> coordinator.messages_by_request_id_counter['req-999']
        0
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
    Check for duplicate positions and persist new positions atomically.
    
    Reads the current set of received positions from disk, checks if the given
    position is a duplicate, and if not, adds it to the set and persists atomically.
    This ensures exactly-once processing of messages even across restarts.
    
    Args:
        request_id (str): Unique request identifier.
        position (int): Position number from the incoming message.
    
    Returns:
        bool: True if position is new (should be processed), False if duplicate.
    
    Example:
        First message:
        >>> coordinator._check_and_persist_input_position('req-123', 5)
        Persisted input position: request_id=req-123, position=5
        True
        
        Duplicate message:
        >>> coordinator._check_and_persist_input_position('req-123', 5)
        Duplicate position received: request_id=req-123, position=5
        False
        
        After restart:
        >>> # File exists on disk with position 5
        >>> coordinator._check_and_persist_input_position('req-123', 5)
        Duplicate position received: request_id=req-123, position=5
        False
    """
    positions_file = os.path.join(self.positions_dir, f'{request_id}.txt')
    
    with self.lock:
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
      
      if position in current_positions:
        logging.info(f"Duplicate position received: request_id={request_id}, position={position}")
        return False
      
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
    """
    Atomically persist the assigned position for a request.
    
    Writes the new position to disk using atomic file operations to ensure
    consistency even in case of crashes during the write operation.
    
    Args:
        request_id (str): Unique request identifier.
        new_position (int): The position number being assigned.
    
    Raises:
        Exception: If atomic write fails.
    
    Example:
        >>> coordinator._persist_assigned_position('req-123', 10)
        Persisted assigned position: request_id=req-123, new_position=10
    """
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
    """
    Process data messages by assigning new sequential positions.
    
    The coordinator performs the following steps:
    1. Lazy-load the last assigned position for this request (if needed)
    2. Check if the incoming position is a duplicate (discard if so)
    3. Assign a new sequential position
    4. Persist the new assigned position
    5. Forward the message with the new position
    
    Args:
        message (bytes): Raw message from RabbitMQ.
        msg_type (int): Message type identifier.
        data_type (str): Type of data being processed.
        request_id (str): Unique request identifier.
        position (int): Original position from sender.
        payload (bytes): Message payload.
        queue_name (str, optional): Source queue name.
    
    Example:
        First message for req-123:
        >>> coordinator._process_message(
        ...     message=raw_msg,
        ...     msg_type=1,
        ...     data_type='users',
        ...     request_id='req-123',
        ...     position=1,
        ...     payload=b'data'
        ... )
        Persisted assigned position: request_id=req-123, new_position=1
        Forwarded message (msg_type: 1, data_type: users, request_id: req-123, position: 1)
        
        Duplicate message:
        >>> coordinator._process_message(
        ...     message=raw_msg,
        ...     msg_type=1,
        ...     data_type='users',
        ...     request_id='req-123',
        ...     position=1,
        ...     payload=b'data'
        ... )
        Duplicate position received: request_id=req-123, position=1
        Discarding duplicate message: request_id=req-123, position=1, msg_type=1
    """
    self._load_assigned_position_for_request(request_id)
    
    if not self._check_and_persist_input_position(request_id, position):
      logging.warning(f"Discarding duplicate message: request_id={request_id}, position={position}, msg_type={msg_type}")
      return
    
    self.messages_by_request_id_counter[request_id] = self.messages_by_request_id_counter.get(request_id, 0) + 1
    new_position = self.messages_by_request_id_counter[request_id]
    
    self._forward_message(msg_type, data_type, request_id, new_position, payload)

    # TEST-CASE: Completar
    # Aca duplicariamos el mensaje pero no haria la deduplication el coordinator
    #self.simulate_crash(queue_name, request_id)

    self._persist_assigned_position(request_id, new_position)
    

  def _handle_end_signal(self, message, msg_type, data_type, request_id, position, queue_name=None):
    """
    Handle END signals by assigning new sequential positions.
    
    Similar to _process_message, but specifically for END signals. Ensures
    that END signals also receive sequential positions and duplicate END
    signals are properly detected and discarded.
    
    Args:
        message (bytes): Raw END message.
        msg_type (int): Message type identifier.
        data_type (str): Type of data being processed.
        request_id (str): Unique request identifier.
        position (int): Original position from sender.
        queue_name (str, optional): Source queue name.
    
    Example:
        >>> coordinator._handle_end_signal(
        ...     message=end_msg,
        ...     msg_type=2,
        ...     data_type='users',
        ...     request_id='req-123',
        ...     position=100
        ... )
        Persisted assigned position: request_id=req-123, new_position=100
        Forwarded message (msg_type: 2, data_type: users, request_id: req-123, position: 100)
    """
    self._load_assigned_position_for_request(request_id)
    
    if not self._check_and_persist_input_position(request_id, position):
      logging.warning(f"Discarding duplicate END signal: request_id={request_id}, position={position}")
      return
    
    self.messages_by_request_id_counter[request_id] = self.messages_by_request_id_counter.get(request_id, 0) + 1
    new_position = self.messages_by_request_id_counter[request_id]
    
    self._persist_assigned_position(request_id, new_position)
    
    self._forward_message(msg_type, data_type, request_id, new_position, b'')


  def _forward_message(self, msg_type, data_type, request_id, position, payload):
    """
    Forward messages to output queues with assigned positions.
    
    Creates the appropriate message type based on msg_type and sends it to
    all configured output queues.
    
    Args:
        msg_type (int): Message type identifier (DATA, END, or NOTIFICATION).
        data_type (str): Type of data being processed.
        request_id (str): Unique request identifier.
        position (int): Assigned sequential position.
        payload (bytes): Message payload.
    
    Example:
        Forward data message:
        >>> coordinator._forward_message(
        ...     msg_type=1,
        ...     data_type='users',
        ...     request_id='req-123',
        ...     position=5,
        ...     payload=b'user_data'
        ... )
        Sending to queue: cleaner_input
        Forwarded message (msg_type: 1, data_type: users, request_id: req-123, position: 5)
        
        Forward END message:
        >>> coordinator._forward_message(
        ...     msg_type=2,
        ...     data_type='users',
        ...     request_id='req-123',
        ...     position=100,
        ...     payload=b''
        ... )
    """
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
    """
    Not implemented in Coordinator.
    
    The Coordinator processes complete messages rather than individual rows,
    so this method is not used. All processing happens in _process_message
    and _handle_end_signal.
    
    Args:
        rows (list): List of CSV row strings (unused).
        queue_name (str, optional): Source queue name (unused).
    
    Returns:
        list: Empty list.
    """
    return []

def create_coordinator():
  """
  Factory function to create a Coordinator instance from environment variables.
  
  Reads configuration from:
  - Environment variables: QUEUE_IN, QUEUE_OUT, RABBITMQ_HOST
  
  Returns:
      Coordinator: Configured Coordinator instance ready to run.
  
  Example:
      Environment setup:
      $ export QUEUE_IN=gateway_output
      $ export QUEUE_OUT=cleaner_input
      $ export RABBITMQ_HOST=rabbitmq
      
      Running:
      >>> coordinator = create_coordinator()
      >>> coordinator.run()
  """
  queue_in = os.environ.get('QUEUE_IN')
  queue_out = os.environ.get('QUEUE_OUT')
  rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

  return Coordinator(queue_in, queue_out, rabbitmq_host)

if __name__ == "__main__":
  config_path = os.path.join(os.path.dirname(__file__), 'config.ini')

  Coordinator.run_worker_main(create_coordinator, config_path)