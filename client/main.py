import os
import signal
import sys
from common.client import Client


def main():
    
    server_address = os.getenv("SERVER_ADDRESS", "gateway:5000")
    client_id = os.getenv("CLIENT_ID", "client1")
    data_dir = os.getenv("DATA_DIR", "/app/.data")
    batch_max_amount = int(os.getenv("BATCH_MAX_AMOUNT", "100"))

    client = Client(
        client_id=client_id,
        server_address=server_address,
        data_dir=data_dir,
        batch_max_amount=batch_max_amount,
    )

    # Signal handler
    def handle_shutdown(sig, frame):
        print(f"[INFO] Shutting down client {client_id}...")
        client.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Start
    client.start_client_loop()


if __name__ == "__main__":
    main()
