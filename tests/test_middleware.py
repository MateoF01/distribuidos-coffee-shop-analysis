import time
import pytest
from middleware.coffeeMiddleware import (
  CoffeeMessageMiddlewareQueue,
  CoffeeMessageMiddlewareExchange
)

# Use a short delay to give consumers time to process messages
DELAY = 0.3

@pytest.fixture
def rabbitmq_host():
  return "rabbitmq"  # Must match the service name in docker-compose


def test_queue_1_to_1(rabbitmq_host):
  queue_name = "test_queue_1_to_1"

  received_messages = []

  consumer = CoffeeMessageMiddlewareQueue(rabbitmq_host, queue_name)
  producer = CoffeeMessageMiddlewareQueue(rabbitmq_host, queue_name)

  def callback(message):
    received_messages.append(message)

  consumer.start_consuming(callback)
  producer.send({"data": "hello"})

  time.sleep(DELAY)
  consumer.stop_consuming()
  consumer.delete()

  assert received_messages == [{"data": "hello"}]


def test_queue_1_to_n(rabbitmq_host):
  queue_name = "test_queue_1_to_n"

  received_1 = []
  received_2 = []

  producer = CoffeeMessageMiddlewareQueue(rabbitmq_host, queue_name)
  consumer1 = CoffeeMessageMiddlewareQueue(rabbitmq_host, queue_name)
  consumer2 = CoffeeMessageMiddlewareQueue(rabbitmq_host, queue_name)

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
  consumer1.delete()

  total_received = len(received_1) + len(received_2)
  assert total_received == 4
  assert received_1 != [] or received_2 != []  # At least one consumed something


def test_exchange_1_to_1(rabbitmq_host):
  exchange_name = "test_exchange_1_to_1"

  received = []

  producer = CoffeeMessageMiddlewareExchange(
      rabbitmq_host, exchange_name, route_keys=[""], exchange_type="fanout"
  )
  consumer = CoffeeMessageMiddlewareExchange(
      rabbitmq_host, exchange_name, route_keys=[""], exchange_type="fanout"
  )

  def callback(msg): received.append(msg)

  consumer.start_consuming(callback)
  producer.send({"event": "update"})

  time.sleep(DELAY)
  consumer.stop_consuming()
  consumer.delete()

  assert received == [{"event": "update"}]


def test_exchange_1_to_n(rabbitmq_host):
  exchange_name = "test_exchange_1_to_n"

  received_1 = []
  received_2 = []

  producer = CoffeeMessageMiddlewareExchange(
      rabbitmq_host, exchange_name, exchange_type="fanout"
  )
  consumer1 = CoffeeMessageMiddlewareExchange(
      rabbitmq_host, exchange_name, exchange_type="fanout"
  )
  consumer2 = CoffeeMessageMiddlewareExchange(
      rabbitmq_host, exchange_name, exchange_type="fanout"
  )

  consumer1.start_consuming(lambda msg: received_1.append(msg))
  consumer2.start_consuming(lambda msg: received_2.append(msg))

  producer.send({"broadcast": True})

  time.sleep(DELAY)
  consumer1.stop_consuming()
  consumer2.stop_consuming()
  consumer1.delete()
  consumer2.delete()

  assert received_1 == [{"broadcast": True}]
  assert received_2 == [{"broadcast": True}]
