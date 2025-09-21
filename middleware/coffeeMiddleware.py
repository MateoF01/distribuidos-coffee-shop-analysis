import pika
import json
import threading
import configparser
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
  "x-max-length": int(config["QUEUE"].get("x-max-length", 10000)),
  "x-message-ttl": int(config["QUEUE"].get("x-message-ttl", 60000)),
}

EXCHANGE_QUEUE_ARGS = {
  "x-message-ttl": int(config["EXCHANGE"].get("x-message-ttl", 30000))
}

class CoffeeMessageMiddlewareQueue(MessageMiddlewareQueue):
  def __init__(self, host, queue_name):
    self.queue_name = queue_name
    self.host = host
    try:
      self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
      self._channel = self._connection.channel()
      self._channel.queue_declare(queue=self.queue_name, durable=True, arguments=QUEUE_ARGS)
    except Exception as e:
      raise MessageMiddlewareDisconnectedError(str(e))

    self._consume_thread = None
    self._stopping = False

  def start_consuming(self, on_message_callback):

    def _consume():

      def callback(ch, method, properties, body):
        try:
          message = json.loads(body)
          on_message_callback(message)
          ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
          raise MessageMiddlewareMessageError(str(e))

      self._channel.basic_qos(prefetch_count=1)
      self._channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
      try:
        self._channel.start_consuming()
      except Exception as e:
        if not self._stopping:
          raise MessageMiddlewareDisconnectedError(str(e))

    self._consume_thread = threading.Thread(target=_consume)
    self._consume_thread.start()

  def stop_consuming(self):
    self._stopping = True
    try:
      if self._channel.is_open:
        self._channel.stop_consuming()
      if self._consume_thread:
        self._consume_thread.join()
    except Exception as e:
      raise MessageMiddlewareDisconnectedError(str(e))

  def send(self, message):
    try:
      self._channel.basic_publish(
        exchange='',
        routing_key=self.queue_name,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)  # make message persistent
      )
    except Exception as e:
      raise MessageMiddlewareMessageError(str(e))

  def close(self):
    try:
      self._connection.close()
    except Exception as e:
      raise MessageMiddlewareCloseError(str(e))

  def delete(self):
    try:
      self._channel.queue_delete(queue=self.queue_name)
    except Exception as e:
      raise MessageMiddlewareDeleteError(str(e))


class CoffeeMessageMiddlewareExchange(MessageMiddlewareExchange):
  def __init__(self, host, exchange_name, route_keys=None, exchange_type='fanout'):
    self.host = host
    self.exchange_name = exchange_name
    self.route_keys = route_keys or []
    self.exchange_type = exchange_type
    self._consume_thread = None
    self._stopping = False

    try:
      self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
      self._channel = self._connection.channel()
      self._channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type)

      # Create an exclusive queue for consuming
      result = self._channel.queue_declare(queue='', exclusive=True, arguments=EXCHANGE_QUEUE_ARGS)
      self._queue_name = result.method.queue

      for rk in self.route_keys:
        self._channel.queue_bind(
          exchange=self.exchange_name,
          queue=self._queue_name,
          routing_key=rk
        )
    except Exception as e:
      raise MessageMiddlewareDisconnectedError(str(e))

  def start_consuming(self, on_message_callback):
    def _consume():
      def callback(ch, method, properties, body):
        try:
          message = json.loads(body)
          on_message_callback(message)
          ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
          raise MessageMiddlewareMessageError(str(e))

      self._channel.basic_qos(prefetch_count=1)
      self._channel.basic_consume(queue=self._queue_name, on_message_callback=callback)
      try:
        self._channel.start_consuming()
      except Exception as e:
        if not self._stopping:
          raise MessageMiddlewareDisconnectedError(str(e))

    self._consume_thread = threading.Thread(target=_consume)
    self._consume_thread.start()

  def stop_consuming(self):
    self._stopping = True
    try:
      if self._channel.is_open:
        self._channel.stop_consuming()
      if self._consume_thread:
        self._consume_thread.join()
    except Exception as e:
      raise MessageMiddlewareDisconnectedError(str(e))

  def send(self, message):
    try:
      self._channel.basic_publish(
        exchange=self.exchange_name,
        routing_key=self.route_keys[0] if self.route_keys else '',
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)
      )
    except Exception as e:
      raise MessageMiddlewareMessageError(str(e))

  def close(self):
    try:
      self._connection.close()
    except Exception as e:
      raise MessageMiddlewareCloseError(str(e))

  def delete(self):
    try:
      self._channel.exchange_delete(exchange=self.exchange_name)
    except Exception as e:
      raise MessageMiddlewareDeleteError(str(e))
