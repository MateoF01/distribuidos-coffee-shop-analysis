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

# Load config from config.ini
config = configparser.ConfigParser()
config.read("middleware/config.ini")

QUEUE_ARGS = {
  "x-max-length": int(config["QUEUE"].get("x-max-length", 100000)),
  "x-message-ttl": int(config["QUEUE"].get("x-message-ttl", 60000)),
}

EXCHANGE_QUEUE_ARGS = {
  "x-message-ttl": int(config["EXCHANGE"].get("x-message-ttl", 30000))
}

RABBITMQ_CREDENTIALS = {
  "username": config["RABBITMQ"].get("username", "guest"),
  "password": config["RABBITMQ"].get("password", "guest"),
  "host": config["RABBITMQ"].get("host", "localhost")
}

# Read all timeout configurations
def read_timeout_config(section, key, default_value):
  """Helper function to read timeout values with error handling"""
  try:
    value_str = config[section].get(key, str(default_value))
    value = float(value_str)
    print(f"DEBUG: Read {section}.{key}: {value_str} -> {value}")
    return value
  except Exception as e:
    print(f"DEBUG: Error reading {section}.{key}: {e}, using default {default_value}")
    return default_value

# Timeout configurations
TIMEOUTS = {
  "startup": read_timeout_config("TIMEOUTS", "startup-timeout", 5.0),
  "consumer_graceful_shutdown": read_timeout_config("TIMEOUTS", "consumer-graceful-shutdown", 5.0),
  "consumer_thread_join": read_timeout_config("TIMEOUTS", "consumer-thread-join", 2.0),
  "consumer_force_stop": read_timeout_config("TIMEOUTS", "consumer-force-stop", 2.0),
  "data_events_time_limit": read_timeout_config("TIMEOUTS", "data-events-time-limit", 0.1)
}

# Legacy support for existing consumer config
CONSUMER_CONFIG = {
  "startup_timeout": TIMEOUTS["startup"]
}

class ConnectionState(Enum):
  DISCONNECTED = "disconnected"
  CONNECTED = "connected"
  CONSUMING = "consuming"
  DISCONNECTING = "disconnecting"

class CoffeeMessageMiddlewareQueue(MessageMiddlewareQueue):

  def __init__(self, host=None, queue_name=None):
    self.queue_name = queue_name
    self.host = host or RABBITMQ_CREDENTIALS["host"]
    
    # Thread-safe state management
    self._state_lock = threading.RLock()
    self._state = ConnectionState.DISCONNECTED
    
    # Separate connections for publish and consume to avoid races
    self._publisher_connection = None
    self._publisher_channel = None
    self._consumer_connection = None
    self._consumer_channel = None
    
    # Consumer thread management with proper synchronization
    self._consumer_thread = None
    self._consumer_stop_event = threading.Event()
    self._consumer_ready_event = threading.Event()
    self._consumer_finished_event = threading.Event()
    
    self._initialize_connections()

  def _initialize_connections(self):
    """Initialize separate connections for publisher and consumer with retry logic"""
    import time
    credentials = pika.PlainCredentials(
      RABBITMQ_CREDENTIALS["username"], 
      RABBITMQ_CREDENTIALS["password"]
    )
    params = pika.ConnectionParameters(host=self.host, credentials=credentials)
    max_retries = 10
    for attempt in range(max_retries):
      try:
        # Separate publisher connection (thread-safe for publishing only)
        self._publisher_connection = pika.BlockingConnection(params)
        self._publisher_channel = self._publisher_connection.channel()
        self._publisher_channel.queue_declare(queue=self.queue_name, durable=True, arguments=QUEUE_ARGS)
        # Separate consumer connection (dedicated to consuming only)
        self._consumer_connection = pika.BlockingConnection(params)
        self._consumer_channel = self._consumer_connection.channel()
        self._consumer_channel.queue_declare(queue=self.queue_name, durable=True, arguments=QUEUE_ARGS)
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
    """Thread-safe state transitions with rollback on error"""
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
    with self._state_transition([ConnectionState.CONNECTED], ConnectionState.CONSUMING):
      if self._consumer_thread and self._consumer_thread.is_alive():
        raise MessageMiddlewareDisconnectedError("Consumer already running")
      
      # Reset synchronization events
      self._consumer_stop_event.clear()
      self._consumer_ready_event.clear()
      self._consumer_finished_event.clear()
      
      # Start consumer in dedicated thread
      self._consumer_thread = threading.Thread(
        target=self._consume_loop, 
        args=(on_message_callback,),
        daemon=True
      )
      self._consumer_thread.start()
      
      # Wait for consumer to be ready with timeout
      if not self._consumer_ready_event.wait(timeout=CONSUMER_CONFIG["startup_timeout"]):
        self._force_stop_consumer()
        raise MessageMiddlewareDisconnectedError("Consumer failed to start within timeout")

  def _consume_loop(self, on_message_callback):
    try:
      def callback(ch, method, properties, body):
        try:
          on_message_callback(body)
          # Check if channel is still open before acknowledging
          if ch and not ch.is_closed:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
          # Try to reject message if channel is still open
          try:
            if ch and not ch.is_closed:
              ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
          except:
            pass  # Ignore rejection errors
          raise MessageMiddlewareMessageError(str(e))

      # Check if channel is still available before setting up
      if not self._consumer_channel or self._consumer_channel.is_closed:
        return

      self._consumer_channel.basic_qos(prefetch_count=1)
      self._consumer_channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
      
      # Signal that consumer is ready
      self._consumer_ready_event.set()
      
      # Start consuming with stop event checking
      while not self._consumer_stop_event.is_set():
        try:
          # Check if connection is still available before processing
          if not self._consumer_connection or self._consumer_connection.is_closed:
            break
          self._consumer_connection.process_data_events(time_limit=TIMEOUTS["data_events_time_limit"])
        except Exception as e:
          if not self._consumer_stop_event.is_set():
            raise MessageMiddlewareDisconnectedError(str(e))
          break
            
    except Exception as e:
      if not self._consumer_stop_event.is_set():
        print(f"Consumer loop error: {e}")
    finally:
      # Cancel consuming if channel is still available
      try:
        if self._consumer_channel and not self._consumer_channel.is_closed:
          self._consumer_channel.stop_consuming()
      except:
        pass  # Ignore cleanup errors
      self._consumer_ready_event.clear()
      self._consumer_finished_event.set()

  def stop_consuming(self):
    """Race-condition-free consumer shutdown"""
    with self._state_lock:
      if self._state != ConnectionState.CONSUMING:
        return  # Already stopped or not consuming
      
      if not self._consumer_thread or not self._consumer_thread.is_alive():
        self._state = ConnectionState.CONNECTED
        return
    
    try:
      # Signal consumer to stop
      self._consumer_stop_event.set()
      
      # Wait for consumer thread to finish gracefully
      if not self._consumer_finished_event.wait(timeout=TIMEOUTS["consumer_graceful_shutdown"]):
        print("Warning: Consumer thread did not finish within timeout")
      
      # Wait for thread to actually terminate
      if self._consumer_thread.is_alive():
        self._consumer_thread.join(timeout=TIMEOUTS["consumer_thread_join"])
      
      self._consumer_thread = None
      
      with self._state_lock:
        self._state = ConnectionState.CONNECTED
        
    except Exception as e:
      raise MessageMiddlewareDisconnectedError(f"Error stopping consumer: {str(e)}")

  def _force_stop_consumer(self):
    """Force stop consumer on startup failure"""
    self._consumer_stop_event.set()
    if self._consumer_thread and self._consumer_thread.is_alive():
      self._consumer_thread.join(timeout=TIMEOUTS["consumer_force_stop"])
    self._consumer_thread = None
    with self._state_lock:
      self._state = ConnectionState.CONNECTED

  def send(self, message):
    """Thread-safe publishing using dedicated publisher channel - sends raw bytes"""
    with self._state_lock:
      if self._state == ConnectionState.DISCONNECTED:
        raise MessageMiddlewareDisconnectedError("Not connected")
      
      if not self._publisher_channel or self._publisher_channel.is_closed:
        raise MessageMiddlewareDisconnectedError("Publisher channel not available")
    
    try:
      # Convert message to bytes
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
    except Exception as e:
      raise MessageMiddlewareMessageError(str(e))

  def close(self):
    """Graceful connection closure"""
    with self._state_transition(
      [ConnectionState.CONNECTED, ConnectionState.CONSUMING], 
      ConnectionState.DISCONNECTING
    ):
      try:
        # Stop consumer first if running
        if self._state == ConnectionState.CONSUMING:
          self.stop_consuming()
        
        # Close connections safely
        self._cleanup_connections()
        
        self._state = ConnectionState.DISCONNECTED
        
      except Exception as e:
        self._state = ConnectionState.DISCONNECTED
        raise MessageMiddlewareCloseError(str(e))

  def delete(self):
    with self._state_lock:
      # Ensure we're not consuming before deleting
      if self._state == ConnectionState.CONSUMING:
        raise MessageMiddlewareDeleteError("Cannot delete queue while consuming. Stop consuming first.")
      
      if self._state == ConnectionState.DISCONNECTED:
        raise MessageMiddlewareDeleteError("Cannot delete queue: not connected")
    
    try:
      # Use publisher channel for deletion (safer than consumer channel)
      if self._publisher_channel and not self._publisher_channel.is_closed:
        self._publisher_channel.queue_delete(queue=self.queue_name)
      else:
        raise MessageMiddlewareDeleteError("Publisher channel not available for deletion")
    except Exception as e:
      raise MessageMiddlewareDeleteError(str(e))

  def _cleanup_connections(self):
    """Safe connection cleanup - only after consumer is stopped"""
    # Ensure consumer is stopped first
    if self._consumer_thread and self._consumer_thread.is_alive():
      self._consumer_stop_event.set()
      self._consumer_thread.join(timeout=TIMEOUTS["consumer_thread_join"])
    
    connections_to_close = [
      self._publisher_connection,
      self._consumer_connection
    ]
    
    # Only nullify after ensuring consumer thread is stopped
    self._publisher_channel = None
    self._consumer_channel = None
    self._publisher_connection = None
    self._consumer_connection = None
    
    for conn in connections_to_close:
      if conn and not conn.is_closed:
        try:
          conn.close()
        except:
          pass  # Ignore cleanup errors


class CoffeeMessageMiddlewareExchange(MessageMiddlewareExchange):
  def __init__(self, host=None, exchange_name=None, route_keys=None, exchange_type='fanout'):
    self.host = host or RABBITMQ_CREDENTIALS["host"]
    self.exchange_name = exchange_name
    self.route_keys = route_keys or []
    self.exchange_type = exchange_type
    
    # Thread-safe state management
    self._state_lock = threading.RLock()
    self._state = ConnectionState.DISCONNECTED
    
    # Separate connections for publish and consume to avoid races
    self._publisher_connection = None
    self._publisher_channel = None
    self._consumer_connection = None
    self._consumer_channel = None
    self._queue_name = None  # Exclusive queue for consuming
    
    # Consumer thread management with proper synchronization
    self._consumer_thread = None
    self._consumer_stop_event = threading.Event()
    self._consumer_ready_event = threading.Event()
    self._consumer_finished_event = threading.Event()
    
    self._initialize_connections()

  def _initialize_connections(self):
    """Initialize separate connections for publisher and consumer"""
    try:
      credentials = pika.PlainCredentials(
        RABBITMQ_CREDENTIALS["username"], 
        RABBITMQ_CREDENTIALS["password"]
      )
      params = pika.ConnectionParameters(host=self.host, credentials=credentials)
      
      # Separate publisher connection
      self._publisher_connection = pika.BlockingConnection(params)
      self._publisher_channel = self._publisher_connection.channel()
      self._publisher_channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type)
      
      # Separate consumer connection with exclusive queue
      self._consumer_connection = pika.BlockingConnection(params)
      self._consumer_channel = self._consumer_connection.channel()
      self._consumer_channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type)
      
      # Create exclusive queue for consuming
      result = self._consumer_channel.queue_declare(queue='', exclusive=True, arguments=EXCHANGE_QUEUE_ARGS)
      self._queue_name = result.method.queue
      
      # Bind queue to exchange
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
    """Thread-safe state transitions with rollback on error"""
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
    """Race-condition-free consumer startup"""
    with self._state_transition([ConnectionState.CONNECTED], ConnectionState.CONSUMING):
      if self._consumer_thread and self._consumer_thread.is_alive():
        raise MessageMiddlewareDisconnectedError("Consumer already running")
      
      # Reset synchronization events
      self._consumer_stop_event.clear()
      self._consumer_ready_event.clear()
      self._consumer_finished_event.clear()
      
      # Start consumer in dedicated thread
      self._consumer_thread = threading.Thread(
        target=self._consume_loop, 
        args=(on_message_callback,),
        daemon=True
      )
      self._consumer_thread.start()
      
      # Wait for consumer to be ready with timeout
      if not self._consumer_ready_event.wait(timeout=TIMEOUTS["startup"]):
        self._force_stop_consumer()
        raise MessageMiddlewareDisconnectedError("Consumer failed to start within timeout")

  def _consume_loop(self, on_message_callback):
    """Dedicated consumer loop with proper error handling"""
    try:
      def callback(ch, method, properties, body):
        try:
          on_message_callback(body)
          # Check if channel is still open before acknowledging
          if ch and not ch.is_closed:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
          # Try to reject message if channel is still open
          try:
            if ch and not ch.is_closed:
              ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
          except:
            pass  # Ignore rejection errors
          raise MessageMiddlewareMessageError(str(e))

      # Check if channel is still available before setting up
      if not self._consumer_channel or self._consumer_channel.is_closed:
        return

      self._consumer_channel.basic_qos(prefetch_count=1)
      self._consumer_channel.basic_consume(queue=self._queue_name, on_message_callback=callback)
      
      # Signal that consumer is ready
      self._consumer_ready_event.set()
      
      # Start consuming with stop event checking
      while not self._consumer_stop_event.is_set():
        try:
          # Check if connection is still available before processing
          if not self._consumer_connection or self._consumer_connection.is_closed:
            break
          self._consumer_connection.process_data_events(time_limit=TIMEOUTS["data_events_time_limit"])
        except Exception as e:
          if not self._consumer_stop_event.is_set():
            raise MessageMiddlewareDisconnectedError(str(e))
          break
            
    except Exception as e:
      if not self._consumer_stop_event.is_set():
        print(f"Consumer loop error: {e}")
    finally:
      # Cancel consuming if channel is still available
      try:
        if self._consumer_channel and not self._consumer_channel.is_closed:
          self._consumer_channel.stop_consuming()
      except:
        pass  # Ignore cleanup errors
      self._consumer_ready_event.clear()
      self._consumer_finished_event.set()

  def stop_consuming(self):
    """Race-condition-free consumer shutdown"""
    with self._state_lock:
      if self._state != ConnectionState.CONSUMING:
        return  # Already stopped or not consuming
      
      if not self._consumer_thread or not self._consumer_thread.is_alive():
        self._state = ConnectionState.CONNECTED
        return
    
    try:
      # Signal consumer to stop
      self._consumer_stop_event.set()
      
      # Wait for consumer thread to finish gracefully
      if not self._consumer_finished_event.wait(timeout=TIMEOUTS["consumer_graceful_shutdown"]):
        print("Warning: Consumer thread did not finish within timeout")
      
      # Wait for thread to actually terminate
      if self._consumer_thread.is_alive():
        self._consumer_thread.join(timeout=TIMEOUTS["consumer_thread_join"])
      
      self._consumer_thread = None
      
      with self._state_lock:
        self._state = ConnectionState.CONNECTED
        
    except Exception as e:
      raise MessageMiddlewareDisconnectedError(f"Error stopping consumer: {str(e)}")

  def _force_stop_consumer(self):
    """Force stop consumer on startup failure"""
    self._consumer_stop_event.set()
    if self._consumer_thread and self._consumer_thread.is_alive():
      self._consumer_thread.join(timeout=TIMEOUTS["consumer_force_stop"])
    self._consumer_thread = None
    with self._state_lock:
      self._state = ConnectionState.CONNECTED

  def send(self, message):
    """Thread-safe publishing using dedicated publisher channel"""
    with self._state_lock:
      if self._state == ConnectionState.DISCONNECTED:
        raise MessageMiddlewareDisconnectedError("Not connected")
      
      if not self._publisher_channel or self._publisher_channel.is_closed:
        raise MessageMiddlewareDisconnectedError("Publisher channel not available")
    
    try:
      # Convert message to bytes
      if isinstance(message, bytes):
        body = message
      elif isinstance(message, str):
        body = message.encode('utf-8')
      else:
        body = str(message).encode('utf-8')
        
      # For fanout exchanges, routing key should be empty
      routing_key = '' if self.exchange_type == 'fanout' else (self.route_keys[0] if self.route_keys else '')
      self._publisher_channel.basic_publish(
        exchange=self.exchange_name,
        routing_key=routing_key,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2)
      )
    except Exception as e:
      raise MessageMiddlewareMessageError(str(e))

  def close(self):
    """Graceful connection closure"""
    with self._state_transition(
      [ConnectionState.CONNECTED, ConnectionState.CONSUMING], 
      ConnectionState.DISCONNECTING
    ):
      try:
        # Stop consumer first if running
        if self._state == ConnectionState.CONSUMING:
          self.stop_consuming()
        
        # Close connections safely
        self._cleanup_connections()
        
        self._state = ConnectionState.DISCONNECTED
        
      except Exception as e:
        self._state = ConnectionState.DISCONNECTED
        raise MessageMiddlewareCloseError(str(e))

  def delete(self):
    """Race-condition-free exchange deletion"""
    with self._state_lock:
      # Ensure we're not consuming before deleting
      if self._state == ConnectionState.CONSUMING:
        raise MessageMiddlewareDeleteError("Cannot delete exchange while consuming. Stop consuming first.")
      
      if self._state == ConnectionState.DISCONNECTED:
        raise MessageMiddlewareDeleteError("Cannot delete exchange: not connected")
    
    try:
      # Use publisher channel for deletion (safer than consumer channel)
      if self._publisher_channel and not self._publisher_channel.is_closed:
        self._publisher_channel.exchange_delete(exchange=self.exchange_name)
      else:
        raise MessageMiddlewareDeleteError("Publisher channel not available for deletion")
    except Exception as e:
      raise MessageMiddlewareDeleteError(str(e))

  def _cleanup_connections(self):
    """Safe connection cleanup - only after consumer is stopped"""
    # Ensure consumer is stopped first
    if self._consumer_thread and self._consumer_thread.is_alive():
      self._consumer_stop_event.set()
      self._consumer_thread.join(timeout=TIMEOUTS["consumer_thread_join"])
    
    connections_to_close = [
      self._publisher_connection,
      self._consumer_connection
    ]
    
    # Only nullify after ensuring consumer thread is stopped
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
          pass  # Ignore cleanup errors
