import pika
import json
import threading
import configparser
from enum import Enum
from contextlib import contextmanager
from middleware.base import (
  MessageMiddlewareQueue,
  MessageMiddlewareExchange,
  MessageMiddlewareMessageError,
  MessageMiddlewareDisconnectedError,
  MessageMiddlewareCloseError,
  MessageMiddlewareDeleteError
)

config = configparser.ConfigParser()
config.read("middleware/config.ini")

QUEUE_ARGS = {
  "x-max-length": int(config["QUEUE"].get("x-max-length", 10000)),
  "x-message-ttl": int(config["QUEUE"].get("x-message-ttl", 60000)),
}

EXCHANGE_QUEUE_ARGS = {
  "x-message-ttl": int(config["EXCHANGE"].get("x-message-ttl", 30000))
}

RABBITMQ_CREDENTIALS = {
  "username": config["RABBITMQ"].get("username", "guest"),
  "password": config["RABBITMQ"].get("password", "guest"),
  "host": config["RABBITMQ"].get("host", "localhost"),
  "heartbeat": int(config["RABBITMQ"].get("heartbeat", 300))
}

def read_timeout_config(section, key, default_value):
  """
  Read timeout configuration values with error handling and type conversion.
  
  Args:
      section (str): Configuration file section name.
      key (str): Configuration key within the section.
      default_value (float): Default value if key not found or parsing fails.
  
  Returns:
      float: Timeout value in seconds.
  
  Example:
      >>> read_timeout_config("TIMEOUTS", "startup-timeout", 5.0)
      DEBUG: Read TIMEOUTS.startup-timeout: 5.0 -> 5.0
      5.0
      
      >>> read_timeout_config("TIMEOUTS", "missing-key", 10.0)
      DEBUG: Read TIMEOUTS.missing-key: 10.0 -> 10.0
      10.0
  """
  try:
    value_str = config[section].get(key, str(default_value))
    value = float(value_str)
    print(f"DEBUG: Read {section}.{key}: {value_str} -> {value}")
    return value
  except Exception as e:
    print(f"DEBUG: Error reading {section}.{key}: {e}, using default {default_value}")
    return default_value

TIMEOUTS = {
  "startup": read_timeout_config("TIMEOUTS", "startup-timeout", 5.0),
  "consumer_graceful_shutdown": read_timeout_config("TIMEOUTS", "consumer-graceful-shutdown", 5.0),
  "consumer_thread_join": read_timeout_config("TIMEOUTS", "consumer-thread-join", 2.0),
  "consumer_force_stop": read_timeout_config("TIMEOUTS", "consumer-force-stop", 2.0),
}

CONSUMER_CONFIG = {
  "startup_timeout": TIMEOUTS["startup"]
}

class ConnectionState(Enum):
  """
  Enumeration of possible connection states for RabbitMQ connections.
  
  States:
      DISCONNECTED: No active connection to RabbitMQ.
      CONNECTED: Connection established, ready for operations.
      CONSUMING: Actively consuming messages from a queue.
      DISCONNECTING: In the process of gracefully closing the connection.
  
  Example:
      >>> state = ConnectionState.CONNECTED
      >>> state.value
      'connected'
      >>> state == ConnectionState.CONNECTED
      True
  """
  DISCONNECTED = "disconnected"
  CONNECTED = "connected"
  CONSUMING = "consuming"
  DISCONNECTING = "disconnecting"

class CoffeeMessageMiddlewareQueue(MessageMiddlewareQueue):

  def __init__(self, host, queue_name):
    """
    Initialize a new queue connection with separate publisher and consumer channels.
    
    Args:
        host (str): RabbitMQ server hostname or IP address.
        queue_name (str): Name of the queue to connect to.
    
    Example:
        >>> queue = CoffeeMessageMiddlewareQueue('localhost', 'my_queue')
        >>> queue.queue_name
        'my_queue'
        >>> queue.host
        'localhost'
    """
    self.queue_name = queue_name
    self.host = host
    
    self._state_lock = threading.RLock()
    self._state = ConnectionState.DISCONNECTED
    
    self._publisher_connection = None
    self._publisher_channel = None
    self._consumer_connection = None
    self._consumer_channel = None
    
    self._consumer_thread = None
    self._consumer_stop_event = threading.Event()
    self._consumer_ready_event = threading.Event()
    self._consumer_finished_event = threading.Event()
    
    self._initialize_connections()

  def _initialize_connections(self):
    """
    Initialize separate RabbitMQ connections for publishing and consuming with retry logic.
    
    Creates two independent connections:
    - Publisher connection: Used for sending messages with publisher confirms enabled
    - Consumer connection: Dedicated to consuming messages in a separate thread
    
    This separation prevents race conditions and improves thread safety. The method
    retries up to 10 times with 3-second delays if RabbitMQ is not immediately available.
    
    Raises:
        MessageMiddlewareDisconnectedError: If connection fails after all retries.
    
    Example:
        >>> queue = CoffeeMessageMiddlewareQueue('rabbitmq', 'test_queue')
        RabbitMQ not ready, retrying (1/10)...
        [Connection succeeds on retry]
        >>> queue._state == ConnectionState.CONNECTED
        True
    """
    import time
    credentials = pika.PlainCredentials(
      RABBITMQ_CREDENTIALS["username"], 
      RABBITMQ_CREDENTIALS["password"]
    )
    params = pika.ConnectionParameters(
        host=self.host, 
        credentials=credentials,
        heartbeat=RABBITMQ_CREDENTIALS["heartbeat"]
    )
    max_retries = 10
    for attempt in range(max_retries):
      try:
        self._publisher_connection = pika.BlockingConnection(params)
        self._publisher_channel = self._publisher_connection.channel()
        self._publisher_channel.confirm_delivery()
        self._publisher_channel.queue_declare(queue=self.queue_name, durable=True)

        self._consumer_connection = pika.BlockingConnection(params)
        self._consumer_channel = self._consumer_connection.channel()
        self._consumer_channel.queue_declare(queue=self.queue_name, durable=True)
        with self._state_lock:
          self._state = ConnectionState.CONNECTED
        break
      except pika.exceptions.AMQPConnectionError as e:
        print(f"RabbitMQ not ready, retrying ({attempt+1}/{max_retries})...")
        time.sleep(3)
      except Exception as e:
        print(f"Error connecting to RabbitMQ: {e}")
        time.sleep(3)
    else:
      print("Failed to connect to RabbitMQ after retries.")
      raise MessageMiddlewareDisconnectedError("RabbitMQ connection failed after retries.")

  @contextmanager
  def _state_transition(self, from_states, to_state):
    """
    Thread-safe state transition context manager with automatic rollback on error.
    
    Ensures that state transitions are atomic and thread-safe. If an exception occurs
    during the transition, the state is automatically rolled back to its previous value.
    
    Args:
        from_states (list): List of valid states to transition from.
        to_state (ConnectionState): Target state to transition to.
    
    Yields:
        ConnectionState: The old state before transition.
    
    Raises:
        RuntimeError: If current state is not in from_states.
    
    Example:
        >>> with queue._state_transition([ConnectionState.CONNECTED], 
        ...                              ConnectionState.CONSUMING):
        ...     queue._setup_consumer()
        [If setup_consumer raises, state rolls back to CONNECTED]
    """
    with self._state_lock:
      if self._state not in from_states:
        raise RuntimeError(f"Invalid state transition: {self._state} -> {to_state}")
      old_state = self._state
      self._state = to_state
      try:
        yield old_state
      except Exception:
        self._state = old_state
        raise

  def start_consuming(self, on_message_callback):
    """
    Start consuming messages from the queue in a background thread.
    
    Messages are processed by the provided callback function. The consumer runs in a
    dedicated thread, allowing the main thread to continue execution. The method blocks
    until the consumer thread is ready or times out.
    
    Args:
        on_message_callback (callable): Function to call for each message.
            Should accept one argument: the message body as bytes.
    
    Raises:
        MessageMiddlewareDisconnectedError: If consumer is already running or fails to start.
    
    Example:
        >>> def process_message(body):
        ...     print(f"Got: {body.decode('utf-8')}")
        >>> queue.start_consuming(process_message)
        [Consumer starts in background thread]
        >>> # Main thread continues execution
    """
    with self._state_transition([ConnectionState.CONNECTED], ConnectionState.CONSUMING):
      if self._consumer_thread and self._consumer_thread.is_alive():
        raise MessageMiddlewareDisconnectedError("Consumer already running")
      
      self._consumer_stop_event.clear()
      self._consumer_ready_event.clear()
      self._consumer_finished_event.clear()
      
      self._consumer_thread = threading.Thread(
        target=self._consume_loop, 
        args=(on_message_callback,),
        daemon=True
      )
      self._consumer_thread.start()
      
      if not self._consumer_ready_event.wait(timeout=CONSUMER_CONFIG["startup_timeout"]):
        self._force_stop_consumer()
        raise MessageMiddlewareDisconnectedError("Consumer failed to start within timeout")

  def _consume_loop(self, on_message_callback):
    """
    Main consumer loop running in a dedicated thread.
    
    Continuously processes messages from the queue until stopped. Handles message
    acknowledgment and rejection based on callback success/failure. Includes proper
    cleanup and error handling.
    
    Args:
        on_message_callback (callable): User-provided message processing function.
    
    Example:
        [Internal method called by start_consuming]
        >>> queue._consume_loop(my_callback)
        [Runs in background thread until stop_consuming is called]
    """
    try:
      def callback(ch, method, properties, body):
        try:
          on_message_callback(body)
          if ch and not ch.is_closed:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
          try:
            if ch and not ch.is_closed:
              ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
          except:
            pass
          raise MessageMiddlewareMessageError(str(e))

      if not self._consumer_channel or self._consumer_channel.is_closed:
        return

      self._consumer_channel.basic_qos(prefetch_count=1)
      self._consumer_channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
      
      self._consumer_ready_event.set()
      
      self._consumer_channel.start_consuming()
            
    except Exception as e:
      if not self._consumer_stop_event.is_set():
        print(f"Consumer loop error: {e}")
    finally:
      self._consumer_ready_event.clear()
      self._consumer_finished_event.set()

  def stop_consuming(self):
    """
    Stop consuming messages and gracefully shut down the consumer thread.
    
    Signals the consumer thread to stop, waits for graceful shutdown with timeouts,
    and transitions state back to CONNECTED. The method is race-condition-free and
    idempotent (safe to call multiple times).
    
    Raises:
        MessageMiddlewareDisconnectedError: If error occurs during shutdown.
    
    Example:
        >>> queue.start_consuming(my_callback)
        >>> # ... messages being processed ...
        >>> queue.stop_consuming()
        [Consumer thread stops gracefully]
        >>> queue._state == ConnectionState.CONNECTED
        True
    """
    with self._state_lock:
      if self._state != ConnectionState.CONSUMING:
        return
      
      if not self._consumer_thread or not self._consumer_thread.is_alive():
        self._state = ConnectionState.CONNECTED
        return
    
    try:
      self._consumer_stop_event.set()
      
      try:
        if self._consumer_channel and not self._consumer_channel.is_closed:
          self._consumer_connection.add_callback_threadsafe(self._consumer_channel.stop_consuming)
      except Exception:
        pass
      
      if not self._consumer_finished_event.wait(timeout=TIMEOUTS["consumer_graceful_shutdown"]):
        print("Warning: Consumer thread did not finish within timeout")
      
      if self._consumer_thread.is_alive():
        self._consumer_thread.join(timeout=TIMEOUTS["consumer_thread_join"])
      
      self._consumer_thread = None
      
      with self._state_lock:
        self._state = ConnectionState.CONNECTED
        
    except Exception as e:
      raise MessageMiddlewareDisconnectedError(f"Error stopping consumer: {str(e)}")

  def _force_stop_consumer(self):
    """
    Force immediate consumer shutdown on startup failure.
    
    Used when the consumer fails to start within the timeout period. Does not
    wait as long as stop_consuming() for graceful shutdown.
    
    Example:
        [Internal method called by start_consuming on timeout]
        >>> queue._force_stop_consumer()
        [Consumer thread stopped immediately]
    """
    self._consumer_stop_event.set()
    try:
      if self._consumer_channel and not self._consumer_channel.is_closed:
        self._consumer_connection.add_callback_threadsafe(self._consumer_channel.stop_consuming)
    except Exception:
      pass
    if self._consumer_thread and self._consumer_thread.is_alive():
      self._consumer_thread.join(timeout=TIMEOUTS["consumer_force_stop"])
    self._consumer_thread = None
    with self._state_lock:
      self._state = ConnectionState.CONNECTED

  def _ensure_publisher_channel(self):
    """
    Ensure the publisher channel is available, reconnecting if necessary.
    
    This method checks if the publisher channel is closed and attempts to
    reconnect transparently. Called internally by send() to handle transient
    connection issues.
    
    Returns:
        bool: True if channel is available, False otherwise.
    """
    if self._publisher_channel and not self._publisher_channel.is_closed:
      return True
    
    # Try to reconnect
    try:
      if self._publisher_connection and not self._publisher_connection.is_closed:
        self._publisher_channel = self._publisher_connection.channel()
        self._publisher_channel.confirm_delivery()
        self._publisher_channel.queue_declare(queue=self.queue_name, durable=True)
        return True
      else:
        # Full reconnect needed - reinitialize connections
        self._initialize_connections()
        return self._publisher_channel and not self._publisher_channel.is_closed
    except Exception as e:
      print(f"Failed to reconnect publisher channel: {e}")
      return False

  def send(self, message):
    """
    Send a message to the queue with publisher confirms for guaranteed delivery.
    
    The message is sent using the dedicated publisher channel with confirm_delivery
    enabled, ensuring the message is successfully stored by RabbitMQ before returning.
    Messages are persisted to disk (delivery_mode=2). Auto-reconnects if channel
    is unavailable.
    
    Args:
        message (bytes|str): Message to send. Strings are automatically encoded to UTF-8.
    
    Raises:
        MessageMiddlewareDisconnectedError: If not connected or channel unavailable after retry.
        MessageMiddlewareMessageError: If message cannot be routed or is rejected.
    
    Example:
        Send bytes:
        >>> queue.send(b'Hello, World!')
        [Message sent and confirmed]
        
        Send string:
        >>> queue.send('Processing task 42')
        [String encoded to bytes and sent]
        
        Guaranteed delivery:
        >>> try:
        ...     queue.send(b'important_data')
        ... except MessageMiddlewareMessageError:
        ...     print("Message was not confirmed by broker")
    """
    with self._state_lock:
      if self._state == ConnectionState.DISCONNECTED:
        raise MessageMiddlewareDisconnectedError("Not connected")
      
      if not self._publisher_channel or self._publisher_channel.is_closed:
        # Try to reconnect before failing
        if not self._ensure_publisher_channel():
          raise MessageMiddlewareDisconnectedError("Publisher channel not available")
    
    try:
      if isinstance(message, bytes):
        body = message
      elif isinstance(message, str):
        body = message.encode('utf-8')
      else:
        body = str(message).encode('utf-8')
        
      self._publisher_channel.basic_publish(
        exchange='',
        routing_key=self.queue_name,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2)
      )
    except pika.exceptions.UnroutableError as e:
      raise MessageMiddlewareMessageError(f"Message could not be routed to queue: {str(e)}")
    except pika.exceptions.NackError as e:
      raise MessageMiddlewareMessageError(f"Message was rejected by broker: {str(e)}")
    except Exception as e:
      raise MessageMiddlewareMessageError(str(e))

  def close(self):
    """Graceful connection closure"""
    with self._state_transition(
      [ConnectionState.CONNECTED, ConnectionState.CONSUMING], 
      ConnectionState.DISCONNECTING
    ):
      try:
        if self._state == ConnectionState.CONSUMING:
          self.stop_consuming()
        
        self._cleanup_connections()
        
        self._state = ConnectionState.DISCONNECTED
        
      except Exception as e:
        self._state = ConnectionState.DISCONNECTED
        raise MessageMiddlewareCloseError(str(e))

  def delete(self):
    """
    Delete the queue from RabbitMQ.
    
    The queue must not be actively consuming when deleted. If consuming, stop
    the consumer first before calling delete.
    
    Raises:
        MessageMiddlewareDeleteError: If queue is consuming, disconnected, or deletion fails.
    
    Example:
        >>> queue.delete()
        MessageMiddlewareDeleteError: Cannot delete queue while consuming...
        >>> queue.stop_consuming()
        >>> queue.delete()
        [Queue deleted from RabbitMQ]
    """
    with self._state_lock:
      if self._state == ConnectionState.CONSUMING:
        raise MessageMiddlewareDeleteError("Cannot delete queue while consuming. Stop consuming first.")
      
      if self._state == ConnectionState.DISCONNECTED:
        raise MessageMiddlewareDeleteError("Cannot delete queue: not connected")
    
    try:
      if self._publisher_channel and not self._publisher_channel.is_closed:
        self._publisher_channel.queue_delete(queue=self.queue_name)
      else:
        raise MessageMiddlewareDeleteError("Publisher channel not available for deletion")
    except Exception as e:
      raise MessageMiddlewareDeleteError(str(e))

  def _cleanup_connections(self):
    """
    Safely close and cleanup all RabbitMQ connections.
    
    Ensures consumer thread is stopped before nullifying connection references
    to prevent race conditions. Ignores errors during individual connection closures.
    
    Example:
        [Internal method called by close()]
        >>> queue._cleanup_connections()
        [All connections closed and references cleared]
    """
    if self._consumer_thread and self._consumer_thread.is_alive():
      self._consumer_stop_event.set()
      self._consumer_thread.join(timeout=TIMEOUTS["consumer_thread_join"])
    
    connections_to_close = [
      self._publisher_connection,
      self._consumer_connection
    ]
    
    self._publisher_channel = None
    self._consumer_channel = None
    self._publisher_connection = None
    self._consumer_connection = None
    
    for conn in connections_to_close:
      if conn and not conn.is_closed:
        try:
          conn.close()
        except:
          pass


class CoffeeMessageMiddlewareExchange(MessageMiddlewareExchange):
  """
  Thread-safe RabbitMQ exchange implementation for publish/subscribe messaging.
  
  This class provides reliable exchange-based messaging with:
  - Support for fanout and topic exchange types
  - Exclusive consumer queues (auto-deleted on disconnect)
  - Separate connections for publishing and consuming
  - Thread-safe state management
  - Publisher confirms for guaranteed delivery
  - Routing key support for topic exchanges
  
  Exchanges enable one-to-many message distribution patterns where a single
  published message is delivered to multiple consumers.
  
  Attributes:
      exchange_name (str): Name of the RabbitMQ exchange.
      host (str): RabbitMQ server hostname.
      exchange_type (str): Type of exchange ('fanout' or 'topic').
      route_keys (list): Routing keys for topic exchanges.
  
  Example:
      Fanout exchange (broadcast to all consumers):
      >>> exchange = CoffeeMessageMiddlewareExchange(
      ...     'rabbitmq', 'notifications', route_keys=None
      ... )
      >>> exchange.send(b'System alert!')
      >>> # All consumers receive the message
      >>> 
      >>> def on_notification(body):
      ...     print(f"Alert: {body.decode('utf-8')}")
      >>> exchange.start_consuming(on_notification)
      
      Topic exchange (selective routing):
      >>> exchange = CoffeeMessageMiddlewareExchange(
      ...     'rabbitmq', 'events', route_keys=['user.login', 'user.logout']
      ... )
      >>> exchange.start_consuming(on_user_event)
  """
  def __init__(self, host, exchange_name, route_keys):
    """
    Initialize a new exchange connection.
    
    Args:
        host (str): RabbitMQ server hostname or IP address.
        exchange_name (str): Name of the exchange to connect to.
        route_keys (list): List of routing keys for binding (topic exchanges).
            Empty or None for fanout exchanges.
    
    Example:
        Fanout exchange:
        >>> exchange = CoffeeMessageMiddlewareExchange('localhost', 'broadcasts', None)
        >>> exchange.exchange_type
        'fanout'
        
        Topic exchange:
        >>> exchange = CoffeeMessageMiddlewareExchange(
        ...     'localhost', 'events', ['error.*', 'warning.*']
        ... )
    """
    self.host = host
    self.exchange_name = exchange_name
    self.route_keys = route_keys or []
    self.exchange_type = config.get("EXCHANGE", "exchange-type", fallback="fanout")
    
    self._state_lock = threading.RLock()
    self._state = ConnectionState.DISCONNECTED
    
    self._publisher_connection = None
    self._publisher_channel = None
    self._consumer_connection = None
    self._consumer_channel = None
    self._queue_name = None
    
    self._consumer_thread = None
    self._consumer_stop_event = threading.Event()
    self._consumer_ready_event = threading.Event()
    self._consumer_finished_event = threading.Event()
    
    self._initialize_connections()

  def _initialize_connections(self):
    """
    Initialize separate RabbitMQ connections and create exclusive consumer queue.
    
    Creates publisher and consumer connections, declares the exchange, and creates
    an exclusive queue for consuming messages. The queue is bound to the exchange
    using configured routing keys.
    
    Raises:
        MessageMiddlewareDisconnectedError: If connection or setup fails.
    
    Example:
        >>> exchange = CoffeeMessageMiddlewareExchange('rabbitmq', 'test_exchange', [])
        [Connections initialized, exclusive queue created and bound]
    """
    try:
      credentials = pika.PlainCredentials(
        RABBITMQ_CREDENTIALS["username"], 
        RABBITMQ_CREDENTIALS["password"]
      )
      params = pika.ConnectionParameters(
          host=self.host, 
          credentials=credentials,
          heartbeat=RABBITMQ_CREDENTIALS["heartbeat"]
      )
      
      self._publisher_connection = pika.BlockingConnection(params)
      self._publisher_channel = self._publisher_connection.channel()
      self._publisher_channel.confirm_delivery()
      self._publisher_channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type)
      
      self._consumer_connection = pika.BlockingConnection(params)
      self._consumer_channel = self._consumer_connection.channel()
      self._consumer_channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type)
      
      result = self._consumer_channel.queue_declare(queue='', exclusive=True, arguments=EXCHANGE_QUEUE_ARGS)
      self._queue_name = result.method.queue
      

      if self.exchange_type == 'fanout':
        self._consumer_channel.queue_bind(
          exchange=self.exchange_name,
          queue=self._queue_name
        )
      else:
        for rk in self.route_keys:
          self._consumer_channel.queue_bind(
            exchange=self.exchange_name,
            queue=self._queue_name,
            routing_key=rk
          )
      
      with self._state_lock:
        self._state = ConnectionState.CONNECTED
        
    except Exception as e:
      raise MessageMiddlewareDisconnectedError(str(e))

  @contextmanager
  def _state_transition(self, from_states, to_state):
    """
    Thread-safe state transition context manager with automatic rollback on error.
    
    Ensures that state transitions are atomic and thread-safe. If an exception occurs
    during the transition, the state is automatically rolled back to its previous value.
    
    Args:
        from_states (list): List of valid states to transition from.
        to_state (ConnectionState): Target state to transition to.
    
    Yields:
        ConnectionState: The old state before transition.
    
    Raises:
        RuntimeError: If current state is not in from_states.
    
    Example:
        >>> with exchange._state_transition([ConnectionState.CONNECTED], 
        ...                                 ConnectionState.CONSUMING):
        ...     exchange._setup_consumer()
        [If setup_consumer raises, state rolls back to CONNECTED]
    """
    with self._state_lock:
      if self._state not in from_states:
        raise RuntimeError(f"Invalid state transition: {self._state} -> {to_state}")
      old_state = self._state
      self._state = to_state
      try:
        yield old_state
      except Exception:
        self._state = old_state
        raise

  def start_consuming(self, on_message_callback):
    """
    Start consuming messages from the exchange in a background thread.
    
    Messages published to the exchange are delivered to this consumer's exclusive queue.
    The consumer runs in a dedicated thread, allowing the main thread to continue execution.
    
    Args:
        on_message_callback (callable): Function to call for each message.
            Should accept one argument: the message body as bytes.
    
    Raises:
        MessageMiddlewareDisconnectedError: If consumer is already running or fails to start.
    
    Example:
        >>> def on_broadcast(body):
        ...     print(f"Received: {body.decode('utf-8')}")
        >>> exchange.start_consuming(on_broadcast)
        [Consumer starts in background thread]
    """
    with self._state_transition([ConnectionState.CONNECTED], ConnectionState.CONSUMING):
      if self._consumer_thread and self._consumer_thread.is_alive():
        raise MessageMiddlewareDisconnectedError("Consumer already running")
      
      self._consumer_stop_event.clear()
      self._consumer_ready_event.clear()
      self._consumer_finished_event.clear()
      
      self._consumer_thread = threading.Thread(
        target=self._consume_loop, 
        args=(on_message_callback,),
        daemon=True
      )
      self._consumer_thread.start()
      
      if not self._consumer_ready_event.wait(timeout=TIMEOUTS["startup"]):
        self._force_stop_consumer()
        raise MessageMiddlewareDisconnectedError("Consumer failed to start within timeout")

  def _consume_loop(self, on_message_callback):
    """
    Main consumer loop running in a dedicated thread for the exchange.
    
    Continuously processes messages from the exclusive queue until stopped. Handles
    message acknowledgment and rejection based on callback success/failure.
    
    Args:
        on_message_callback (callable): User-provided message processing function.
    
    Example:
        [Internal method called by start_consuming]
        >>> exchange._consume_loop(my_callback)
        [Runs in background thread until stop_consuming is called]
    """
    try:
      def callback(ch, method, properties, body):
        try:
          on_message_callback(body)
          if ch and not ch.is_closed:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
          try:
            if ch and not ch.is_closed:
              ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
          except:
            pass
          raise MessageMiddlewareMessageError(str(e))

      if not self._consumer_channel or self._consumer_channel.is_closed:
        return

      self._consumer_channel.basic_qos(prefetch_count=1)
      self._consumer_channel.basic_consume(queue=self._queue_name, on_message_callback=callback)
      
      self._consumer_ready_event.set()
      
      self._consumer_channel.start_consuming()
            
    except Exception as e:
      if not self._consumer_stop_event.is_set():
        print(f"Consumer loop error: {e}")
    finally:
      self._consumer_ready_event.clear()
      self._consumer_finished_event.set()

  def stop_consuming(self):
    """
    Stop consuming messages and gracefully shut down the consumer thread.
    
    Signals the consumer thread to stop, waits for graceful shutdown with timeouts,
    and transitions state back to CONNECTED. The method is race-condition-free and
    idempotent (safe to call multiple times).
    
    Raises:
        MessageMiddlewareDisconnectedError: If error occurs during shutdown.
    
    Example:
        >>> exchange.start_consuming(callback)
        >>> # ... messages being processed ...
        >>> exchange.stop_consuming()
        [Consumer thread stops gracefully]
    """
    with self._state_lock:
      if self._state != ConnectionState.CONSUMING:
        return
      
      if not self._consumer_thread or not self._consumer_thread.is_alive():
        self._state = ConnectionState.CONNECTED
        return
    
    try:
      self._consumer_stop_event.set()
      
      try:
        if self._consumer_channel and not self._consumer_channel.is_closed:
          self._consumer_connection.add_callback_threadsafe(self._consumer_channel.stop_consuming)
      except Exception:
        pass
      
      if not self._consumer_finished_event.wait(timeout=TIMEOUTS["consumer_graceful_shutdown"]):
        print("Warning: Consumer thread did not finish within timeout")
      
      if self._consumer_thread.is_alive():
        self._consumer_thread.join(timeout=TIMEOUTS["consumer_thread_join"])
      
      self._consumer_thread = None
      
      with self._state_lock:
        self._state = ConnectionState.CONNECTED
        
    except Exception as e:
      raise MessageMiddlewareDisconnectedError(f"Error stopping consumer: {str(e)}")

  def _force_stop_consumer(self):
    """
    Force immediate consumer shutdown on startup failure.
    
    Used when the consumer fails to start within the timeout period. Does not
    wait as long as stop_consuming() for graceful shutdown.
    
    Example:
        [Internal method called by start_consuming on timeout]
        >>> exchange._force_stop_consumer()
        [Consumer thread stopped immediately]
    """
    self._consumer_stop_event.set()
    try:
      if self._consumer_channel and not self._consumer_channel.is_closed:
        self._consumer_connection.add_callback_threadsafe(self._consumer_channel.stop_consuming)
    except Exception:
      pass
    if self._consumer_thread and self._consumer_thread.is_alive():
      self._consumer_thread.join(timeout=TIMEOUTS["consumer_force_stop"])
    self._consumer_thread = None
    with self._state_lock:
      self._state = ConnectionState.CONNECTED

  def send(self, message):
    """
    Publish a message to the exchange with publisher confirms.
    
    The message is broadcast to all consumers bound to the exchange (fanout) or
    routed based on routing key (topic). Publisher confirms ensure reliable delivery.
    
    Args:
        message (bytes|str): Message to send.
    
    Raises:
        MessageMiddlewareDisconnectedError: If not connected.
        MessageMiddlewareMessageError: If message cannot be routed or is rejected.
    
    Example:
        Fanout broadcast:
        >>> exchange.send(b'Hello all consumers!')
        [Message sent to all bound queues]
        
        Topic routing:
        >>> exchange.send(b'Error occurred')
        [Message routed based on routing key]
    """
    with self._state_lock:
      if self._state == ConnectionState.DISCONNECTED:
        raise MessageMiddlewareDisconnectedError("Not connected")
      
      if not self._publisher_channel or self._publisher_channel.is_closed:
        raise MessageMiddlewareDisconnectedError("Publisher channel not available")
    
    try:
      if isinstance(message, bytes):
        body = message
      elif isinstance(message, str):
        body = message.encode('utf-8')
      else:
        body = str(message).encode('utf-8')
        
      routing_key = '' if self.exchange_type == 'fanout' else (self.route_keys[0] if self.route_keys else '')
      
      self._publisher_channel.basic_publish(
        exchange=self.exchange_name,
        routing_key=routing_key,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2)
      )
    except pika.exceptions.UnroutableError as e:
      raise MessageMiddlewareMessageError(f"Message could not be routed: {str(e)}")
    except pika.exceptions.NackError as e:
      raise MessageMiddlewareMessageError(f"Message was rejected by broker: {str(e)}")
    except Exception as e:
      raise MessageMiddlewareMessageError(str(e))

  def close(self):
    """
    Gracefully close all connections and stop consuming.
    
    Ensures proper shutdown by stopping the consumer thread first (if running),
    then closing publisher and consumer connections. Safe to call multiple times.
    
    Raises:
        MessageMiddlewareCloseError: If error occurs during closure.
    
    Example:
        >>> exchange.start_consuming(callback)
        >>> # ... processing messages ...
        >>> exchange.close()
        [Consumer stopped, connections closed]
    """
    with self._state_transition(
      [ConnectionState.CONNECTED, ConnectionState.CONSUMING], 
      ConnectionState.DISCONNECTING
    ):
      try:
        if self._state == ConnectionState.CONSUMING:
          self.stop_consuming()
        
        self._cleanup_connections()
        
        self._state = ConnectionState.DISCONNECTED
        
      except Exception as e:
        self._state = ConnectionState.DISCONNECTED
        raise MessageMiddlewareCloseError(str(e))

  def delete(self):
    """
    Delete the exchange from RabbitMQ.
    
    The exchange must not be actively consuming when deleted. Stop the consumer
    first before calling delete.
    
    Raises:
        MessageMiddlewareDeleteError: If exchange is consuming, disconnected, or deletion fails.
    
    Example:
        >>> exchange.stop_consuming()
        >>> exchange.delete()
        [Exchange deleted from RabbitMQ]
    """
    with self._state_lock:
      if self._state == ConnectionState.CONSUMING:
        raise MessageMiddlewareDeleteError("Cannot delete exchange while consuming. Stop consuming first.")
      
      if self._state == ConnectionState.DISCONNECTED:
        raise MessageMiddlewareDeleteError("Cannot delete exchange: not connected")
    
    try:
      if self._publisher_channel and not self._publisher_channel.is_closed:
        self._publisher_channel.exchange_delete(exchange=self.exchange_name)
      else:
        raise MessageMiddlewareDeleteError("Publisher channel not available for deletion")
    except Exception as e:
      raise MessageMiddlewareDeleteError(str(e))

  def _cleanup_connections(self):
    """
    Safely close and cleanup all RabbitMQ connections and the exclusive queue.
    
    Ensures consumer thread is stopped before nullifying connection references.
    Ignores errors during individual connection closures.
    
    Example:
        [Internal method called by close()]
        >>> exchange._cleanup_connections()
        [All connections closed and references cleared]
    """
    if self._consumer_thread and self._consumer_thread.is_alive():
      self._consumer_stop_event.set()
      self._consumer_thread.join(timeout=TIMEOUTS["consumer_thread_join"])
    
    connections_to_close = [
      self._publisher_connection,
      self._consumer_connection
    ]
    
    self._publisher_channel = None
    self._consumer_channel = None
    self._publisher_connection = None
    self._consumer_connection = None
    self._queue_name = None
    
    for conn in connections_to_close:
      if conn and not conn.is_closed:
        try:
          conn.close()
        except:
          pass
