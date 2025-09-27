from .base_router_filter import BaseRouterFilter

class AmountFilter(BaseRouterFilter):
    def __init__(self, queue_in, rabbitmq_host, routing_queues, threshold=15.0):
        super().__init__(queue_in, rabbitmq_host, routing_queues)
        self.threshold = threshold

    def route(self, row: str, data_type: int):
        """
        Ejemplo de enrutamiento:
        - Si data_type == 1 (transactions):
            → Si final_amount > threshold → va a Q_amount_high
            → Si final_amount <= threshold → va a Q_amount_low
        - Otros data_type se ignoran.
        """
        if data_type != 1:
            return []

        parts = row.split("|")
        if len(parts) < 2:
            return []

        try:
            amount = float(parts[1])
        except:
            return []

        if amount > self.threshold:
            return ["Q_amount_high"]
        else:
            return ["Q_amount_low"]
