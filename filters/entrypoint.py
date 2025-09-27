import os
from filters.temporal_filter import TemporalFilter
from filters.amount_filter import AmountFilter

if __name__ == "__main__":
    rabbit_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    queue_in = os.getenv("QUEUE_IN")
    mode = os.getenv("FILTER_MODE", "temporal")

    if mode == "temporal":
        year_start = int(os.getenv("YEAR_START"))
        year_end = int(os.getenv("YEAR_END"))
        hour_start = int(os.getenv("HOUR_START"))
        hour_end = int(os.getenv("HOUR_END"))

        routing = {
            "Q1_transactions_filtered": "Q1_transactions_filtered",
            "Q3_transactions_filtered": "Q3_transactions_filtered",
            "Q2_transaction_items_filtered": "Q2_transaction_items_filtered",
            "Q4_transaction_items_filtered": "Q4_transaction_items_filtered",
        }

        filt = TemporalFilter(
            queue_in=queue_in,
            rabbitmq_host=rabbit_host,
            routing_queues=routing,
            year_range=(year_start, year_end),
            hour_range=(hour_start, hour_end),
        )

    elif mode == "amount":
        threshold = float(os.getenv("AMOUNT_THRESHOLD"))

        routing = {
            "Q_amount_high": "Q_amount_high",
            "Q_amount_low": "Q_amount_low",
        }

        filt = AmountFilter(
            queue_in=queue_in,
            rabbitmq_host=rabbit_host,
            routing_queues=routing,
            threshold=threshold,
        )
    else:
        raise ValueError(f"Unknown FILTER_MODE={mode}")

    filt.run()
