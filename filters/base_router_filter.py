import struct
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue

class BaseRouterFilter:
    def __init__(self, queue_in, rabbitmq_host, routing_queues: dict):
        """
        queue_in: nombre de la cola de entrada
        rabbitmq_host: host de RabbitMQ
        routing_queues: dict con { 'alias': 'nombre_cola_rabbitmq' }
        """
        self.queue_in = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_in)
        self.queues_out = {
            alias: CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=qname)
            for alias, qname in routing_queues.items()
        }

    def route(self, row: str, data_type: int) -> list:
        """
        Implementar en cada subclase.
        Devuelve una lista de aliases de colas de salida.
        """
        raise NotImplementedError

    def run(self):
        def on_message(message):
            try:
                header = message[:6]
                msg_type, data_type, payload_len = struct.unpack(">BBI", header)
                payload = message[6:].decode("utf-8")

                print("PAYLOAD: ", payload)

                rows = payload.split("\n")
                for row in rows:
                    if not row.strip():
                        continue

                    targets = self.route(row, data_type)
                    for target in targets:
                        if target not in self.queues_out:
                            print(f"[WARN] Unknown route {target}, skipping...")
                            continue

                        new_payload = row.encode("utf-8")
                        new_header = struct.pack(">BBI", msg_type, data_type, len(new_payload))
                        self.queues_out[target].send(new_header + new_payload)
                        print(f"[OK] Routed row → {target} ({self.queues_out[target].queue_name})")

            except Exception as e:
                print(f"[ERROR] Processing message: {e}")

        print(f"[START] RouterFilter listening on {self.queue_in.queue_name}")
        self.queue_in.start_consuming(on_message)
