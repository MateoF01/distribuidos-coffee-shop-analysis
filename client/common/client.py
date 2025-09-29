import socket, time
from common import csv_loaders
from shared import protocol    

class Client:
    def __init__(self, client_id, server_address, data_dir, batch_max_amount):
        self.client_id = client_id
        self.server_address = server_address
        self.data_dir = data_dir
        self.batch_max_amount = batch_max_amount
        self.conn = None



    def create_socket(self):
        host, port = self.server_address.split(":")
        retries = 10
        #Si el gateway no está listo espero y reintento
        for attempt in range(retries):
            try:
                self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.conn.connect((host, int(port)))
                print(f"[INFO] Connected to gateway {host}:{port}")
                return
            except ConnectionRefusedError:
                print(f"[WARN] Gateway not ready, retrying ({attempt+1}/{retries})...")
                time.sleep(3)
        raise ConnectionError("Failed to connect to gateway after retries")


    def close(self):
        if self.conn:
            self.conn.close()
    
    def start_client_loop(self):
        self.create_socket()

        # Group files by data type
        files_by_type = {}
        for data_type, filepath in csv_loaders.iter_csv_files(self.data_dir):
            if data_type not in files_by_type:
                files_by_type[data_type] = []
            files_by_type[data_type].append(filepath)

        # Process all files for each data type
        for data_type, filepaths in files_by_type.items():
            print(f"[INFO] Processing {len(filepaths)} files for data type {data_type}")
            
            for filepath in filepaths:
                print(f"[INFO] Sending file {filepath} (type={data_type})")

                # Enviar en batches
                for batch in csv_loaders.load_csv_batch(filepath, self.batch_max_amount):
                    payload = "\n".join(batch).encode()
                    protocol.send_message(self.conn, protocol.MSG_TYPE_DATA, data_type, payload)
                    print(f"[INFO] Sent batch of {len(batch)} rows from {filepath.name}")

            # Send END only after all files of this data type are processed
            protocol.send_message(self.conn, protocol.MSG_TYPE_END, data_type, b"")
            print(f"[INFO] Sent END for data type {data_type} (processed {len(filepaths)} files)")

        # === END FINAL ===
        protocol.send_message(self.conn, protocol.MSG_TYPE_END, protocol.DATA_END, b"")
        print("[INFO] Sent END FINAL")

        self.close()