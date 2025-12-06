import os
import csv
import heapq
import logging
from glob import glob
from shared import protocol
from shared.worker import SignalProcessingWorker

from WSM.wsm_client import WSMClient
from wsm_config import WSM_NODES
import socket


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

        # --- WSM Heartbeat Integration ----
        self.replica_id = socket.gethostname()

        worker_type_key = "coordinator"

        # Read WSM host/port (OPTIONAL for single-node; required if you specify wsm host in compose)
        wsm_host = os.environ.get("WSM_HOST", None)
        wsm_port = int(os.environ.get("WSM_PORT", "0")) if os.environ.get("WSM_PORT") else None

        # Load multi-node config if exists
        wsm_nodes = WSM_NODES.get(worker_type_key)

        # Create client in heartbeat-only mode
        self.wsm_client = WSMClient(
            worker_type=worker_type_key,
            replica_id=self.replica_id,
            host=wsm_host,
            port=wsm_port,
            nodes=wsm_nodes
        )

        logging.info(f"[Coordinator] Heartbeat WSM client ready for {worker_type_key}, replica={self.replica_id}")

        self.base_temp_root = base_temp_root
        self.sort_columns = [int(x) for x in sort_columns.split(",")]
        os.makedirs(self.base_temp_root, exist_ok=True)
        logging.info(f"[SorterV2] Inicializado - in={queue_in}, out={queue_out}, sort_columns={self.sort_columns}")

    def _process_signal(self, request_id, data_type):
        """
        Process notification signal by merging all replica chunks.
        
        Performs k-way merge across all sorted chunks from all replicas using
        a min-heap. Writes output atomically and sends completion notification.
        Handles DATA_END for cleanup during abnormal termination.
        
        Args:
            request_id (str): Request identifier.
            data_type (int): Data type from notification.
        
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
        if data_type == protocol.DATA_END:
            logging.info(f"[SorterV2] Received DATA_END for request_id={request_id}")
            self._cleanup_request_files(request_id)
            cleanup_message = protocol.create_notification_message(protocol.DATA_END, b"", request_id)
            for q in self.out_queues:
                q.send(cleanup_message)
            logging.info(f"[SorterV2] Forwarded DATA_END notification for request_id={request_id}")
            return
        
        splitter_root = os.path.join(self.base_temp_root, "splitter_q1", str(request_id))
        pattern = os.path.join(splitter_root, "*", "chunk_*.csv")
        chunk_files = sorted(glob(pattern))

        if not chunk_files:
            logging.info(f"[SorterV2] ⚠️ No hay chunks para request {request_id} en {splitter_root} (stream vacío). Enviando notificación.")
            self._notify_completion(protocol.DATA_TRANSACTIONS, request_id)
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

                last_written_key = None

                while heap:
                    key, idx, row, reader = heapq.heappop(heap)
                    
                    # Deduplication logic (handle cross-replica duplicates)
                    if key != last_written_key:
                        writer.writerow(row)
                        last_written_key = key
                    
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

    def _cleanup_request_files(self, request_id):
        """
        Clean up output files for abnormal termination.
        
        Removes:
        - Output directory containing sorted result file
        
        Called during DATA_END handling to free disk space.
        
        Args:
            request_id (str): Request identifier to clean up.
        
        Example:
            >>> sorter._cleanup_request_files('req-123')
            # Removes:
            #   /app/output/req-123/ (sorted output file)
        """
        import shutil

        output_dir = os.path.join("/app/output", str(request_id))
        if os.path.exists(output_dir):
            try:
                shutil.rmtree(output_dir)
                logging.info(f"[SorterV2] Cleaned up output directory for request_id={request_id} at {output_dir}")
            except Exception as e:
                logging.error(f"[SorterV2] Error removing output directory for request_id={request_id} at {output_dir}: {e}")
        else:
            logging.debug(f"[SorterV2] Output directory already removed for request_id={request_id} at {output_dir}")


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
