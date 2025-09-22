import time
import pytest
import tempfile
import os
from middleware.coffeeMiddleware import (
  CoffeeMessageMiddlewareQueue,
  CoffeeMessageMiddlewareExchange
)

# Use a short delay to give consumers time to process messages
DELAY = 0.5

@pytest.fixture
def rabbitmq_host():
  return "localhost"  # Connect to localhost when running tests outside Docker

@pytest.fixture
def unique_queue_name():
  """Generate unique queue name for test isolation"""
  return f"test_queue_{os.getpid()}_{id(object())}"

@pytest.fixture  
def unique_exchange_name():
  """Generate unique exchange name for test isolation"""
  return f"test_exchange_{os.getpid()}_{id(object())}"


def test_queue_1_to_1(rabbitmq_host, unique_queue_name):
  received_messages = []

  consumer = CoffeeMessageMiddlewareQueue(rabbitmq_host, unique_queue_name)
  producer = CoffeeMessageMiddlewareQueue(rabbitmq_host, unique_queue_name)

  def callback(message):
    received_messages.append(message)

  consumer.start_consuming(callback)
  producer.send({"data": "hello"})

  time.sleep(DELAY)
  consumer.stop_consuming()
  consumer.delete()

  assert received_messages == [{"data": "hello"}]


def test_queue_1_to_n(rabbitmq_host, unique_queue_name):
  received_1 = []
  received_2 = []

  producer = CoffeeMessageMiddlewareQueue(rabbitmq_host, unique_queue_name)
  consumer1 = CoffeeMessageMiddlewareQueue(rabbitmq_host, unique_queue_name)
  consumer2 = CoffeeMessageMiddlewareQueue(rabbitmq_host, unique_queue_name)

  def callback1(msg): received_1.append(msg)
  def callback2(msg): received_2.append(msg)

  consumer1.start_consuming(callback1)
  consumer2.start_consuming(callback2)

  # Send multiple messages for distribution
  for i in range(4):
    producer.send({"msg": i})

  time.sleep(DELAY * 4)
  consumer1.stop_consuming()
  consumer2.stop_consuming()
  
  # Cleanup resources
  try:
    consumer1.close()
    consumer2.close()
    producer.delete()
  except:
    pass

  total_received = len(received_1) + len(received_2)
  assert total_received == 4
  assert received_1 != [] or received_2 != []  # At least one consumed something


def test_exchange_1_to_1(rabbitmq_host, unique_exchange_name):
  received = []

  producer = CoffeeMessageMiddlewareExchange(
      rabbitmq_host, unique_exchange_name, exchange_type="fanout"
  )
  consumer = CoffeeMessageMiddlewareExchange(
      rabbitmq_host, unique_exchange_name, exchange_type="fanout"
  )

  def callback(msg): received.append(msg)

  consumer.start_consuming(callback)  # Now blocks until ready internally
  producer.send({"event": "update"})

  time.sleep(DELAY)
  consumer.stop_consuming()
  consumer.delete()

  assert received == [{"event": "update"}]


def test_exchange_1_to_n(rabbitmq_host, unique_exchange_name):
  received_1 = []
  received_2 = []

  producer = CoffeeMessageMiddlewareExchange(
      rabbitmq_host, unique_exchange_name, exchange_type="fanout"
  )
  consumer1 = CoffeeMessageMiddlewareExchange(
      rabbitmq_host, unique_exchange_name, exchange_type="fanout"
  )
  consumer2 = CoffeeMessageMiddlewareExchange(
      rabbitmq_host, unique_exchange_name, exchange_type="fanout"
  )

  consumer1.start_consuming(lambda msg: received_1.append(msg))  # Blocks until ready
  consumer2.start_consuming(lambda msg: received_2.append(msg))  # Blocks until ready
  
  producer.send({"broadcast": True})

  time.sleep(DELAY)
  consumer1.stop_consuming()
  consumer2.stop_consuming()
  
  # Cleanup resources
  try:
    consumer1.close()
    consumer2.close()
    producer.delete()
  except:
    pass

  assert received_1 == [{"broadcast": True}]
  assert received_2 == [{"broadcast": True}]
