import socket, time, threading, os
from common import csv_loaders
from shared import protocol   
from pathlib import Path


class Client:
    def __init__(self, client_id, server_address, data_dir, batch_max_amount, out_dir=None):
        self.client_id = client_id
        self.server_address = server_address
        self.data_dir = data_dir
        self.batch_max_amount = batch_max_amount
        self.conn = None

        # Ruta de salida dentro de la carpeta client/
        base_dir = os.path.dirname(__file__)   # carpeta client/common
        client_dir = os.path.dirname(base_dir) # sube a carpeta client/
        self.out_dir = out_dir or os.path.join(client_dir, "results")

        # Crear carpeta si no existe
        os.makedirs(self.out_dir, exist_ok=True)

        # Diccionario para guardar archivos abiertos por data_type
        self.csv_files = {}

    def create_socket(self):
        host, port = self.server_address.split(":")
        retries = 10
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
        # Cerrar todos los archivos abiertos
        for f in self.csv_files.values():
            try:
                f.close()
            except Exception:
                pass

    def _listen_for_responses(self):
        """Hilo que escucha respuestas del gateway y escribe CSVs por data_type"""
        try:
            while True:
                msg_type, data_type, payload = protocol.receive_message(self.conn)

                print(f"MSG type: {msg_type}, data_type{data_type}, payload: {payload}")

                if msg_type is None:
                    break

                if msg_type == protocol.MSG_TYPE_DATA:
                    payload_str = payload.decode("utf-8")
                    rows = payload_str.split("\n")
                    print(f"[INFO] Recibidas {len(rows)} filas para data_type={data_type}")
                    self._write_rows(data_type, rows)

                elif msg_type == protocol.MSG_TYPE_END:
                    if data_type == protocol.DATA_END:
                        print("[INFO] END FINAL recibido. Cerrando listener.")
                        self.close()
                        break
                    else:
                        print(f"[INFO] END recibido para data_type={data_type}")

        except Exception as e:
            print(f"[ERROR] Listening thread crashed: {e}")

    def _write_rows(self, data_type, rows):
        """Escribe filas en un CSV correspondiente al data_type usando file.write"""
        with open(f'{Path(self.data_dir)}/{data_type}.csv', 'a') as f:
            for row in rows:
                f.write(row)
                f.write('\n')


    def start_client_loop(self):
        self.create_socket()

        # Lanzar hilo que escucha respuestas
        t = threading.Thread(target=self._listen_for_responses, daemon=True)
        t.start()

        # Agrupar archivos por tipo
        files_by_type = {}
        for data_type, filepath in csv_loaders.iter_csv_files(self.data_dir):
            files_by_type.setdefault(data_type, []).append(filepath)

        # Enviar archivos
        for data_type, filepaths in files_by_type.items():
            print(f"[INFO] Processing {len(filepaths)} files for data_type={data_type}")
            for filepath in filepaths:
                print(f"[INFO] Sending file {filepath} (type={data_type})")
                for batch in csv_loaders.load_csv_batch(filepath, self.batch_max_amount):
                    payload = "\n".join(batch).encode()
                    protocol.send_message(self.conn, protocol.MSG_TYPE_DATA, data_type, payload)
                    print(f"[INFO] Sent batch of {len(batch)} rows from {filepath.name}")
            protocol.send_message(self.conn, protocol.MSG_TYPE_END, data_type, b"")
            print(f"[INFO] Sent END for data_type={data_type}")

        # END final
        protocol.send_message(self.conn, protocol.MSG_TYPE_END, protocol.DATA_END, b"")
        print("[INFO] Sent END FINAL")

        # Esperar a que termine el hilo listener
        t.join()
