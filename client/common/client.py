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

        # === Iterate .data ===
        for data_type, filepath in csv_loaders.iter_csv_files(self.data_dir):
            print(f"[INFO] Sending file {filepath} (type={data_type})")

            # Send batches
            for batch in csv_loaders.load_csv_batch(filepath, self.batch_max_amount):
                payload = "\n".join(batch).encode()
                protocol.send_message(self.conn, protocol.MSG_TYPE_DATA, data_type, payload)
                print(f"[INFO] Sent batch of {len(batch)} rows from {filepath.name}")

            # END type
            protocol.send_message(self.conn, protocol.MSG_TYPE_END, data_type, b"")
            print(f"[INFO] Sent END for {filepath.name}")

        # === FINAL END ===
        protocol.send_message(self.conn, protocol.MSG_TYPE_END, protocol.DATA_END, b"")
        print("[INFO] Sent END FINAL")

        # === WAIT RESPONSE ===

        while True:
            msg_type, data_type, payload = protocol.recv_message(self.conn)
            if msg_type == protocol.MSG_TYPE_END and data_type == protocol.DATA_END:
                print("[INFO] Received FINAL END from server. Closing...")
                break

            if msg_type == protocol.MSG_TYPE_DATA:
                # Decodificar resultado como texto
                lines = payload.decode().splitlines()
                filename = f"{self.data_dir}/query_{data_type}.csv"

                with open(filename, "w", newline="") as f:
                    writer = csv.writer(f)
                    for line in lines:
                        writer.writerow(line.split("|"))  # suponiendo que el server separa con "|"

                print(f"[INFO] Saved result for query {data_type} in {filename}")



        self.close()