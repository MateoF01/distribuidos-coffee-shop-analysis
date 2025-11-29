import os
import csv
import heapq
import logging
from glob import glob
from shared import protocol
from shared.worker import SignalProcessingWorker


class SorterV2(SignalProcessingWorker):
    """
    Multi-replica merge sorter using k-way heap merge.
    
    Receives notification signals, merges sorted chunks from multiple replicas,
    and produces a single sorted output file. Uses min-heap for efficient k-way
    merge across all replica chunks.
    
    Architecture:
        - Input: Notification signals with request_id
        - Reads: Sorted chunks from all replicas
        - Merges: K-way merge using min-heap
        - Output: Single sorted CSV file + completion notification
    
    Attributes:
        base_temp_root (str): Base directory for temporary files.
        sort_columns (list): Column indices for multi-column sorting.
    
    Example:
        Input structure:
        /app/temp/splitter_q1/req-123/
            replica-1/
                chunk_0.csv: [sorted rows 0-999]
                chunk_1.csv: [sorted rows 1000-1999]
            replica-2/
                chunk_0.csv: [sorted rows 0-999]
                chunk_1.csv: [sorted rows 1000-1999]
        
        Output:
        /app/output/req-123/q1.csv: [all rows merged and sorted]
        
        Sends: Completion notification to downstream worker
    """

    def __init__(self, queue_in, queue_out, rabbitmq_host, base_temp_root, sort_columns="0"):
        """
        Initialize sorter with directories and sort configuration.
        
        Args:
            queue_in (str): Input queue for notification signals.
            queue_out (str): Output queue for completion notifications.
            rabbitmq_host (str): RabbitMQ server hostname.
            base_temp_root (str): Base directory for temporary files.
            sort_columns (str, optional): Comma-separated column indices. Defaults to "0".
        
        Example:
            >>> sorter = SorterV2(
            ...     queue_in='sorter_notifications_q1',
            ...     queue_out='sender_notifications_q1',
            ...     rabbitmq_host='rabbitmq',
            ...     base_temp_root='/app/temp',
            ...     sort_columns='0,1'
            ... )
        """
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.base_temp_root = base_temp_root
        self.sort_columns = [int(x) for x in sort_columns.split(",")]
        os.makedirs(self.base_temp_root, exist_ok=True)
        logging.info(f"[SorterV2] Inicializado - in={queue_in}, out={queue_out}, sort_columns={self.sort_columns}")

    def _process_signal(self, request_id):
        """
        Process notification signal by merging all replica chunks.
        
        Performs k-way merge across all sorted chunks from all replicas using
        a min-heap. Writes output atomically and sends completion notification.
        
        Args:
            request_id (str): Request identifier.
        
        Example:
            Input: Notification for req-123
            
            Finds chunks:
            /app/temp/splitter_q1/req-123/replica-1/chunk_0.csv
            /app/temp/splitter_q1/req-123/replica-1/chunk_1.csv
            /app/temp/splitter_q1/req-123/replica-2/chunk_0.csv
            /app/temp/splitter_q1/req-123/replica-2/chunk_1.csv
            
            Merges to:
            /app/output/req-123/q1.csv
            
            Sends: Completion notification with DATA_TRANSACTIONS
        """
        splitter_root = os.path.join(self.base_temp_root, "splitter_q1", str(request_id))
        pattern = os.path.join(splitter_root, "*", "chunk_*.csv")
        chunk_files = sorted(glob(pattern))

        if not chunk_files:
            logging.error(f"[SorterV2] ❌ No hay chunks para request {request_id} en {splitter_root}")
            return

        output_dir = os.path.join("/app/output", str(request_id))
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "q1.csv")

        logging.info(f"[SorterV2] 🔄 Ordenando {len(chunk_files)} chunks -> {output_path}")

        heap = []
        file_handles = []

        for i, path in enumerate(chunk_files):
            try:
                f = open(path, "r", encoding="utf-8")
                reader = csv.reader(f)
                file_handles.append(f)
                row = next(reader, None)
                if row:
                    key = tuple(row[c] for c in self.sort_columns)
                    heapq.heappush(heap, (key, i, row, reader))
            except Exception as e:
                logging.error(f"[SorterV2] Error abriendo {path}: {e}")

        def merge_write_func(temp_path):
            with open(temp_path, "w", newline="", encoding="utf-8") as out:
                writer = csv.writer(out)
                
                writer.writerow(['transaction_id', 'final_amount'])

                while heap:
                    key, idx, row, reader = heapq.heappop(heap)
                    writer.writerow(row)
                    try:
                        nxt = next(reader)
                        nxt_key = tuple(nxt[c] for c in self.sort_columns)
                        heapq.heappush(heap, (nxt_key, idx, nxt, reader))
                    except StopIteration:
                        file_handles[idx].close()
                    except Exception as e:
                        logging.error(f"[SorterV2] Error leyendo {chunk_files[idx]}: {e}")

        SignalProcessingWorker.atomic_write(output_path, merge_write_func)

        for f in file_handles:
            if not f.closed:
                f.close()

        logging.info(f"[SorterV2] ✅ Orden completado para request {request_id}")
        self._notify_completion(protocol.DATA_TRANSACTIONS, request_id)


if __name__ == "__main__":
    def create_sorter_v2():
        """
        Factory function to create SorterV2 from environment configuration.
        
        Environment Variables:
            RABBITMQ_HOST: RabbitMQ hostname (default: rabbitmq).
            QUEUE_IN: Input queue for notifications (required).
            COMPLETION_QUEUE: Output queue for completions (required).
            BASE_TEMP_DIR: Base temp directory (default: /app/temp).
            SORT_COLUMNS: Comma-separated column indices (default: "0").
        
        Returns:
            SorterV2: Configured sorter instance.
        
        Raises:
            ValueError: If required environment variables missing.
        """
        rabbitmq_host = os.environ.get("RABBITMQ_HOST", "rabbitmq")
        queue_in = os.environ.get("QUEUE_IN")
        queue_out = os.environ.get("COMPLETION_QUEUE")
        base_temp_root = os.environ.get("BASE_TEMP_DIR", "/app/temp")
        sort_columns = os.environ.get("SORT_COLUMNS", "0")

        if not queue_in or not queue_out:
            raise ValueError("QUEUE_IN y COMPLETION_QUEUE son requeridos")

        return SorterV2(queue_in, queue_out, rabbitmq_host, base_temp_root, sort_columns)

    SorterV2.run_worker_main(create_sorter_v2)
