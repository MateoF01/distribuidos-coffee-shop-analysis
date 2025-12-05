import os
import socket
import time
import logging
import configparser
from collections import defaultdict
from shared import protocol
from shared.worker import StreamProcessingWorker
from WSM.wsm_client import WSMClient
from wsm_config import WSM_NODES


class SplitterQ1(StreamProcessingWorker):
    """
    Replicated stream splitter with sorted chunk generation.
    
    Reads transaction rows from queue, splits data into sorted chunks, and uses
    WSM for replica coordination. Ensures exactly-once downstream notification
    when all replicas complete processing.
    
    Architecture:
        - Input: Transaction rows from replicated queue
        - Processing: Extract transaction_id and amount, sort into chunks
        - Chunks: Incrementally sorted files per replica
        - Coordination: WSM tracks replica progress and END signals
        - Output: Notification to sorter when all replicas finish
    
    Attributes:
        replica_id (str): Unique replica identifier (typically hostname).
        chunk_size (int): Maximum rows per chunk file.
        base_temp_root (str): Base directory for temporary chunk files.
        buffers (dict): Per-request buffering state.
        wsm_client (WSMClient): Client for replica coordination.
        backoff_start (float): Initial backoff seconds.
        backoff_max (float): Maximum backoff seconds.
    
    Example:
        Input messages (3 replicas receive same data):
        - MSG 1: guid1|100.50|...
        - MSG 2: guid2|200.75|...
        - END signal
        
        Each replica creates:
        /app/temp/splitter_q1/req-123/replica-1/
            chunk_0.csv: guid1,100.50\nguid2,200.75 (sorted)
        
        WSM coordination:
        - Each replica marks END in WSM
        - Only one replica sends notification when all reach END
        - Downstream sorter receives single notification
    """

    def __init__(self, queue_in, queue_out, rabbitmq_host, chunk_size, replica_id, backoff_start=0.1, backoff_max=3.0, wsm_nodes = None):
        """
        Initialize splitter with chunk configuration and WSM coordination.
        
        Args:
            queue_in (str): Input queue name.
            queue_out (str): Output queue for notifications.
            rabbitmq_host (str): RabbitMQ server hostname.
            chunk_size (int): Maximum rows per chunk file.
            replica_id (str): Unique replica identifier.
            backoff_start (float, optional): Initial backoff seconds. Defaults to 0.1.
            backoff_max (float, optional): Maximum backoff seconds. Defaults to 3.0.
            wsm_nodes (list, optional): WSM node endpoints.
        
        Example:
            >>> splitter = SplitterQ1(
            ...     queue_in='transactions_cleaned_q1',
            ...     queue_out='sorter_notifications_q1',
            ...     rabbitmq_host='rabbitmq',
            ...     chunk_size=10000,
            ...     replica_id='splitter-q1-1',
            ...     wsm_nodes=['wsm:9000', 'wsm:9001', 'wsm:9002']
            ... )
        """
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.replica_id = replica_id
        self.chunk_size = int(chunk_size)
        self.backoff_start = backoff_start
        self.backoff_max = backoff_max

        self.base_temp_root = os.environ.get(
            'BASE_TEMP_DIR',
            os.path.join(os.path.dirname(__file__), 'temp')
        )

        self.buffers = defaultdict(lambda: {
            "rows": [],
            "count": 0,
            "dir": None,
            "chunk_idx": 0
        })

        wsm_host = os.environ.get("WSM_HOST", "wsm")
        wsm_port = int(os.environ.get("WSM_PORT", "9000"))
        self.wsm_client = WSMClient(
            worker_type="splitter_q1",
            replica_id=replica_id,
            host=wsm_host,
            port=wsm_port,
            nodes=wsm_nodes
        )

        logging.info(f"[SplitterQ1:{self.replica_id}] init - in={queue_in}, out={queue_out}, chunk_size={self.chunk_size}")

    def _ensure_request_dir(self, request_id):
        """
        Create per-request, per-replica directory structure.
        
        Args:
            request_id (str): Request identifier.
        
        Returns:
            str: Directory path for this request and replica.
        
        Example:
            >>> splitter._ensure_request_dir('req-123')
            '/app/temp/splitter_q1/req-123/splitter-q1-1/'
        """
        buf = self.buffers[request_id]
        if buf["dir"]:
            return buf["dir"]

        req_dir = os.path.join(self.base_temp_root, str(request_id), self.replica_id)
        os.makedirs(req_dir, exist_ok=True)
        buf["dir"] = req_dir
        logging.info(f"[SplitterQ1:{self.replica_id}] dir ready for request {request_id}: {req_dir}")
        return req_dir

    def _write_chunk(self, request_id, new_rows=None):
        """
        Incrementally write and sort chunk files with atomic writes.
        
        Loads existing chunk, appends new rows, sorts, and rewrites. Splits into
        new chunk if size exceeds chunk_size. Uses atomic writes for crash recovery.
        
        Args:
            request_id (str): Request identifier.
            new_rows (list, optional): New rows to append.
        
        Example:
            Current chunk_0.csv: 800 rows
            new_rows: 300 rows
            chunk_size: 1000
            
            Result:
            - chunk_0.csv: 1000 sorted rows
            - chunk_1.csv: 100 sorted rows
        """
        buf = self.buffers[request_id]
        req_dir = self._ensure_request_dir(request_id)

        chunk_idx = buf["chunk_idx"]
        filename = f"chunk_{chunk_idx}.csv"
        path = os.path.join(req_dir, filename)

        existing_rows = []
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                existing_rows = [ln.strip() for ln in f if ln.strip()]

        if new_rows:
            existing_rows.extend(new_rows)

        if len(existing_rows) > self.chunk_size:
            to_write = existing_rows[:self.chunk_size]
            remaining = existing_rows[self.chunk_size:]

            to_write.sort(key=lambda ln: ln.split(",")[0])
            
            def write_chunk_func(temp_path):
                with open(temp_path, "w", encoding="utf-8", newline="") as f:
                    for ln in to_write:
                        f.write(ln + "\n")
            
            StreamProcessingWorker.atomic_write(path, write_chunk_func)

            buf["chunk_idx"] += 1
            new_filename = f"chunk_{buf['chunk_idx']}.csv"
            new_path = os.path.join(req_dir, new_filename)

            remaining.sort(key=lambda ln: ln.split(",")[0])
            
            def write_remaining_func(temp_path):
                with open(temp_path, "w", encoding="utf-8", newline="") as f:
                    for ln in remaining:
                        f.write(ln + "\n")
            
            StreamProcessingWorker.atomic_write(new_path, write_remaining_func)

        else:
            existing_rows.sort(key=lambda ln: ln.split(",")[0])
            
            def write_current_func(temp_path):
                with open(temp_path, "w", encoding="utf-8", newline="") as f:
                    for ln in existing_rows:
                        f.write(ln + "\n")
            
            StreamProcessingWorker.atomic_write(path, write_current_func)

    def _append_row(self, request_id, row_text):
        """
        Append single row to current chunk.
        
        Args:
            request_id (str): Request identifier.
            row_text (str): CSV row text.
        """
        self._write_chunk(request_id, [row_text])

    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
        """
        Process message with WSM duplicate detection and state tracking.
        
        Args:
            message (bytes): Raw message.
            msg_type (int): Message type.
            data_type (int): Data type.
            request_id (str): Request identifier.
            position (int): Position number.
            payload (bytes): Message payload.
            queue_name (str, optional): Source queue name.
        
        Example:
            Position 5 arrives twice (replica failure/retry):
            - First: Processes and updates WSM
            - Second: Detected as duplicate, discarded
        """
        self._ensure_request_dir(request_id)

        if self.wsm_client.is_position_processed(request_id, position):
            logging.info(f"🔁 Mensaje duplicado detectado ({request_id}:{position}), descartando...")
            return

        self.wsm_client.update_state("PROCESSING", request_id, position)        

        super()._process_message(message, msg_type, data_type, request_id, position, payload, queue_name)

        self.wsm_client.update_state("WAITING", request_id, position)

    def _process_rows(self, rows, queue_name=None):
        """
        Process data rows: extract transaction_id and amount, format as CSV.
        
        Args:
            rows (list): List of row strings (pipe-delimited).
            queue_name (str, optional): Source queue name.
        
        Example:
            Input row: "guid1|100.50|2024-01|..."
            Extracted: "guid1,100.50"
            Written to: chunk_0.csv (sorted)
        """
        request_id = self.current_request_id  # viene desde la superclase
        self._ensure_request_dir(request_id)

        for row in rows:
            row = row.strip()
            if not row:
                continue

            parts = row.split('|')

            if len(parts) < 2:
                continue

            guid = parts[0].strip()
            amount = parts[1].strip()

            formatted = f"{guid},{amount}"

            self._append_row(request_id, formatted)

    def _handle_end_signal(self, message, msg_type, data_type, request_id, position, queue_name=None):
        """
        Handle END signal with WSM coordination for exactly-once notification.
        
        Flushes final chunk, marks END in WSM, waits for all replicas to reach END,
        then sends single notification downstream. Handles DATA_END for cleanup.
        
        Args:
            message (bytes): Raw message.
            msg_type (int): Message type.
            data_type (int): Data type.
            request_id (str): Request identifier.
            position (int): Position number.
            queue_name (str, optional): Source queue name.
        
        Example:
            3 replicas processing request req-123:
            - Replica 1: Reaches END, marks in WSM, waits
            - Replica 2: Reaches END, marks in WSM, waits
            - Replica 3: Reaches END, marks in WSM
            - WSM authorizes one replica to send notification
            - Single notification sent to sorter
            
            DATA_END handling:
            - Cleans up WSM state for request
            - Removes chunk files for request
            - Forwards DATA_END to downstream
        """
        if data_type == protocol.DATA_END:
            logging.info(f"[SplitterQ1:{self.replica_id}] Manejo de DATA_END para request {request_id}")
            self.wsm_client.cleanup_request(request_id)
            self._cleanup_request_files(request_id)
            cleanup_message = protocol.create_notification_message(protocol.DATA_END, b"", request_id)
            for q in self.out_queues:
                q.send(cleanup_message)
            logging.info(f"[SplitterQ1:{self.replica_id}] Sent cleanup notification for request {request_id}")
            return

        self._write_chunk(request_id)

        self.wsm_client.update_state("END", request_id, position)
        print(f"[{self.replica_id}] END")

        logging.info(f"[SplitterQ1:{self.replica_id}] END recibido para request {request_id}. Esperando permiso WSM...")

        backoff = self.backoff_start
        total_wait = 0.0
        
        while not self.wsm_client.can_send_end(request_id, position):
            if total_wait >= self.backoff_max:
                error_msg = f"[SplitterQ1:{self.replica_id}] Timeout esperando permiso WSM para END de {request_id} después de {total_wait:.2f}s"
                logging.error(error_msg)
                raise TimeoutError(error_msg)
            
            logging.info(f"[SplitterQ1:{self.replica_id}] Esperando permiso... (backoff={backoff:.3f}s, total={total_wait:.2f}s)")
            time.sleep(backoff)
            total_wait += backoff
            backoff = min(backoff * 2, self.backoff_max - total_wait) if total_wait < self.backoff_max else 0

        logging.info(f"[SplitterQ1:{self.replica_id}] ✅ WSM autorizó END para request {request_id}. Notificando sorter_v2...")

        if self.out_queues:
            payload = f"split_done;replica={self.replica_id}".encode("utf-8")
            noti = protocol.create_notification_message(data_type, payload, request_id)
            for q in self.out_queues:
                q.send(noti)

        self.wsm_client.update_state("WAITING", request_id, position)

        if request_id in self.buffers:
            del self.buffers[request_id]

    def _cleanup_request_files(self, request_id):
        """
        Clean up entire request directory including all replica subdirectories.
        
        Removes complete request directory tree containing all replica chunk files
        when request is aborted. Called during DATA_END handling.
        
        Args:
            request_id (str): Request identifier to clean up.
        
        Example:
            >>> splitter._cleanup_request_files('req-123')
            # Removes entire directory tree:
            #   /app/temp/splitter_q1/req-123/
            #     splitter-q1-1/chunk_0.csv
            #     splitter-q1-1/chunk_1.csv
            #     splitter-q1-2/chunk_0.csv
            #     splitter-q1-3/chunk_0.csv
        """
        import shutil
        
        request_dir = os.path.join(self.base_temp_root, str(request_id))
        
        if os.path.exists(request_dir):
            try:
                shutil.rmtree(request_dir)
                logging.info(f"[SplitterQ1:{self.replica_id}] Cleaned up entire request directory for request_id={request_id} at {request_dir}")
            except Exception as e:
                logging.error(f"[SplitterQ1:{self.replica_id}] Error removing directory for request_id={request_id} at {request_dir}: {e}")
        else:
            logging.debug(f"[SplitterQ1:{self.replica_id}] Request directory already removed for request_id={request_id} at {request_dir}")
        
        # Clean up buffer tracking
        if request_id in self.buffers:
            del self.buffers[request_id]
            logging.debug(f"[SplitterQ1:{self.replica_id}] Cleared buffer for request_id={request_id}")


if __name__ == '__main__':
    def create_splitter():
        """
        Factory function to create SplitterQ1 from environment configuration.
        
        Environment Variables:
            RABBITMQ_HOST: RabbitMQ hostname (default: rabbitmq).
            QUEUE_IN: Input queue name (required).
            COMPLETION_QUEUE: Output queue for notifications (required).
            CHUNK_SIZE: Rows per chunk (default: 10000).
            BASE_TEMP_DIR: Base directory for chunks (optional).
            WSM_HOST: WSM server hostname (default: wsm).
            WSM_PORT: WSM server port (default: 9000).
            BACKOFF_START: Initial backoff seconds (from config, default: 0.1).
            BACKOFF_MAX: Maximum backoff seconds (from config, default: 3.0).
        
        Returns:
            SplitterQ1: Configured splitter instance.
        """
        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)
        
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        queue_in = os.environ.get('QUEUE_IN')
        queue_out = os.environ.get('COMPLETION_QUEUE')
        replica_id = socket.gethostname()
        chunk_size = int(os.environ.get('CHUNK_SIZE', 10000))
        
        backoff_start = float(config['DEFAULT'].get('BACKOFF_START', 0.1))
        backoff_max = float(config['DEFAULT'].get('BACKOFF_MAX', 3.0))
        
        key = 'q1'
        wsm_nodes = WSM_NODES[key]
        print("WSM NODES: ", wsm_nodes)

        return SplitterQ1(queue_in, queue_out, rabbitmq_host, chunk_size, replica_id, backoff_start, backoff_max, wsm_nodes)

    SplitterQ1.run_worker_main(create_splitter)
