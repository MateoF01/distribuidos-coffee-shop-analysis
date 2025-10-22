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

    # Desired order
    ordered_prefixes = ["menu_items", "stores", "users", "transactions", "transaction_items"]
    files_by_type = {prefix: [] for prefix in ordered_prefixes}

    for file in sorted(Path(data_dir).rglob("*.csv")):
        fname = file.stem.lower()
        if '_sample' in fname:
            fname = fname.replace('_sample', '')
        for prefix in ordered_prefixes:
            if fname.startswith(prefix):
                files_by_type[prefix].append(file)
                break

    for prefix in ordered_prefixes:
        data_type = FILE_TYPE_MAP[prefix]
        for file in files_by_type[prefix]:
            yield data_type, file


#Lee el archivo y genera el batch sin cargar todo en memoria
def load_csv_batch(path: str, batch_size: int):

    with open(path, newline="") as csvfile:
        reader = csv.reader(csvfile)

        # --- saltar la primera fila (headers) ---
        next(reader, None)

        batch = []
        for row in reader:
            batch.append("|".join(row))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
