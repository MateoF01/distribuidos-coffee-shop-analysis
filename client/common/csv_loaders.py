import csv
import os
from pathlib import Path

# Mapear nombres de archivo → tipo de dato del protocolo
FILE_TYPE_MAP = {
    "transactions": 1,
    "transaction_items": 2,
    "menu_items": 3,
    "users": 4,
    "stores": 5,
}

#Itera los archivos que hay en la carpeta data
def iter_csv_files(data_dir: str):

    for file in sorted(Path(data_dir).glob("*.csv")):
        fname = file.stem.lower()  # sin extensión
        if '_sample' in fname:
            fname = fname.replace('_sample', '')
        for prefix, data_type in FILE_TYPE_MAP.items():
            if fname.startswith(prefix):
                yield data_type, file
                break


#Lee el archivo y genera el batch sin cargar todo en memoria
def load_csv_batch(path: str, batch_size: int):

    with open(path, newline="") as csvfile:
        reader = csv.reader(csvfile)
        batch = []
        for row in reader:
            batch.append("|".join(row))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
