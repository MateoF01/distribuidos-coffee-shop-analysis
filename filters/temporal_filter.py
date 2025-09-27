from datetime import datetime
from .base_router_filter import BaseRouterFilter

class TemporalFilter(BaseRouterFilter):
    def __init__(self, queue_in, rabbitmq_host, routing_queues,
                 year_range, hour_range):
        super().__init__(queue_in, rabbitmq_host, routing_queues)
        self.year_range = year_range
        self.hour_range = hour_range

    def route(self, row: str, data_type: int):
        """
        Reglas:
        - data_type == 1 (transactions):
            → Si año entre year_range y hora entre hour_range
              → va a Q1 y Q3
        - data_type == 2 (transaction_items):
            → Si año entre year_range
              → va a Q2 y Q4
        """
        parts = row.split("|")
        created_at = None

        if data_type == 1 and len(parts) >= 3:  # transactions
            created_at = parts[2]
        elif data_type == 2 and len(parts) >= 6:  # transaction_items
            created_at = parts[5]

        if not created_at:
            return []

        try:
            dt = datetime.fromisoformat(created_at.strip())
        except:
            return []

        if not (self.year_range[0] <= dt.year <= self.year_range[1]):
            return []

        if data_type == 1 and (self.hour_range[0] <= dt.hour <= self.hour_range[1]):
            return ["Q1_transactions_filtered", "Q3_transactions_filtered"]

        if data_type == 2:
            return ["Q2_transaction_items_cleaned", "Q4_transaction_items_filtered"]

        return []
