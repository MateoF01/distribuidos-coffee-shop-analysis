import csv
from pathlib import Path

FILE_TYPE_MAP = {
    "transactions": 1,
    "transaction_items": 2,
    "menu_items": 3,
    "users": 4,
    "stores": 5,
}

def iter_csv_files(data_dir: str):
    """
    Iterate over CSV files in a directory, yielding them in a specific order with their data types.
    
    Files are processed in the following order:
    1. menu_items
    2. stores
    3. users
    4. transactions
    5. transaction_items
    
    This ordering ensures that reference data (menu, stores, users) is sent before
    transactional data that depends on it.
    
    Args:
        data_dir (str): Path to the directory containing CSV files.
    
    Yields:
        tuple: (data_type, file_path) where:
            - data_type (int): Protocol constant identifying the file type (1-5)
            - file_path (Path): Path object pointing to the CSV file
    
    Example:
        >>> for data_type, filepath in iter_csv_files('/app/data'):
        ...     print(f"Type {data_type}: {filepath.name}")
        Type 3: menu_items.csv
        Type 5: stores.csv
        Type 4: users.csv
        Type 1: transactions_202401.csv
        Type 1: transactions_202501.csv
        Type 2: transaction_items_202401.csv
        Type 2: transaction_items_202501.csv
        
        >>> files = list(iter_csv_files('/data'))
        >>> len(files)
        7
        >>> files[0]
        (3, PosixPath('/data/menu_items/menu_items.csv'))
    """

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


def load_csv_batch(path: str, batch_size: int):
    """
    Read a CSV file in batches without loading the entire file into memory.
    
    This generator function yields batches of rows from a CSV file, with each row
    converted to a pipe-delimited string. The CSV header row is automatically skipped.
    This approach is memory-efficient for processing large CSV files.
    
    Args:
        path (str): Path to the CSV file to read.
        batch_size (int): Maximum number of rows per batch.
    
    Yields:
        list: A list of strings, each representing a pipe-delimited row from the CSV.
              Each batch contains up to batch_size rows.
    
    Example:
        >>> for batch in load_csv_batch('/data/transactions.csv', batch_size=3):
        ...     print(f"Batch of {len(batch)} rows:")
        ...     for row in batch:
        ...         print(f"  {row}")
        Batch of 3 rows:
          1|2024-01-01|100.50|store1
          2|2024-01-01|250.75|store2
          3|2024-01-02|175.00|store1
        Batch of 2 rows:
          4|2024-01-02|300.25|store3
          5|2024-01-03|125.50|store2
        
        >>> batches = list(load_csv_batch('/data/menu_items.csv', 100))
        >>> len(batches)
        5
        >>> len(batches[0])
        100
        >>> batches[0][0]
        'item1|Coffee|3.50|Beverages'
    """

    with open(path, newline="") as csvfile:
        reader = csv.reader(csvfile)

        next(reader, None)

        batch = []
        for row in reader:
            batch.append("|".join(row))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
