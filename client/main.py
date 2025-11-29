import os
import signal
import sys
import socket
from common.client import Client

def get_client_id():
    """
    Generate a unique client identifier based on the container hostname.
    
    In Docker Compose environments, hostnames follow the pattern <service>_<replica_number>.
    This function returns the hostname to be used as the client ID.
    
    Returns:
        str: The hostname of the container.
    
    Example:
        >>> get_client_id()
        'client_1'
        
        >>> get_client_id()
        'client_docker_replica_3'
    """
    hostname = socket.gethostname()
    return hostname

def main():
    """
    Main entry point for the client application.
    
    Initializes the client with configuration from environment variables and starts
    the data transmission loop. Sets up signal handlers for graceful shutdown.
    
    Environment Variables:
        SERVER_ADDRESS (str): Gateway server address (default: 'gateway:5000')
        CLIENT_ID (str): Unique client identifier (default: auto-generated from hostname)
        DATA_DIR (str): Directory containing CSV data files (default: '/app/.data')
        BATCH_MAX_AMOUNT (int): Maximum rows per batch (default: 100)
        REQUESTS_PER_CLIENT (int): Number of complete data iterations (default: 1)
    
    Example:
        Environment configuration:
        SERVER_ADDRESS=gateway:5000
        CLIENT_ID=client1
        DATA_DIR=/app/.data
        BATCH_MAX_AMOUNT=500
        REQUESTS_PER_CLIENT=3
        
        >>> main()
        [DEBUG] Path construction: __file__=/app/common/client.py, ...
        [INFO] Client will save results to directory: /app/client/results/client_1
        [INFO] Connected to gateway gateway:5000
        [INFO] Iteration 1/3: Sending files from data folder
        ...
        [INFO] All query results received successfully
    """
    
    server_address = os.getenv("SERVER_ADDRESS", "gateway:5000")
    client_id = os.getenv("CLIENT_ID", get_client_id())
    data_dir = os.getenv("DATA_DIR", "/app/.data")
    batch_max_amount = int(os.getenv("BATCH_MAX_AMOUNT", "100"))
    requests_amount = int(os.getenv("REQUESTS_PER_CLIENT", "1"))

    client = Client(
        client_id=client_id,
        server_address=server_address,
        data_dir=data_dir,
        batch_max_amount=batch_max_amount,
        requests_amount=requests_amount
    )

    def handle_shutdown(sig, frame):
        print(f"[INFO] Shutting down client {client_id}...")
        client.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    client.start_client_loop()


if __name__ == "__main__":
    main()
