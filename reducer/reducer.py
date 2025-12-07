from asyncio import protocols
import os
import csv
import logging
import gc
from shared import protocol
from collections import defaultdict
from shared.worker import SignalProcessingWorker


from WSM.wsm_client import WSMClient
from wsm_config import WSM_NODES
import socket

class ReducerV2(SignalProcessingWorker):
    """
    Distributed reducer for combining partial aggregation results.
    
    Receives notification signals from groupers, combines partial results from multiple
    replicas, and produces final reduced output files. Supports query-specific reduction
    strategies (Q2: item aggregation, Q3: store TPV, Q4: user visit counts).
    
    Architecture:
        - Input: Notification signals with request_id
        - Reads: Partial CSV files from grouper replicas
        - Groups: Files by common prefix (e.g., month, semester)
        - Combines: Aggregates values using query-specific logic
        - Output: Final reduced CSV files per group
    
    Attributes:
        input_dir (str): Base directory for partial files from groupers.
        output_dir (str): Base directory for final reduced files.
        reducer_mode (str): Query type ('q2', 'q3', 'q4').
        current_request_id (int): Latest request ID processed.
    
    Example:
        Environment setup:
        >>> REDUCER_MODE=q2
        >>> INPUT_DIR=/app/temp/grouper_q2
        >>> OUTPUT_DIR=/app/temp/reduced_q2
        
        Input structure:
        /app/temp/grouper_q2/req-123/
            replica-1/
                2024-01_replica-1.csv: item1,10,50.0\nitem2,5,25.0
                2024-02_replica-1.csv: item1,8,40.0
            replica-2/
                2024-01_replica-2.csv: item1,12,60.0
        
        Output after reduction:
        /app/temp/reduced_q2/req-123/
            2024-01.csv: item1,22,110.0\nitem2,5,25.0
            2024-02.csv: item1,8,40.0
    """

    def __init__(self, queue_in, queue_out, rabbitmq_host, input_dir, output_dir, reducer_mode="q2", service_name=None):
        """
        Initialize reducer with directories and query mode.
        
        Args:
            queue_in (str): Queue name for notification signals.
            queue_out (str): Completion queue for downstream notifications.
            rabbitmq_host (str): RabbitMQ server hostname.
            input_dir (str): Directory containing partial files from groupers.
            output_dir (str): Directory for final reduced files.
            reducer_mode (str, optional): Query type ('q2', 'q3', 'q4'). Defaults to 'q2'.
        
        Example:
            >>> reducer = ReducerV2(
            ...     queue_in='reducer_notifications_q2',
            ...     queue_out='sorter_q2',
            ...     rabbitmq_host='rabbitmq',
            ...     input_dir='/app/temp/grouper_q2',
            ...     output_dir='/app/temp/reduced_q2',
            ...     reducer_mode='q2'
            ... )
        """
        super().__init__(queue_in, queue_out, rabbitmq_host, service_name=service_name)

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



        self.input_dir = input_dir
        self.output_dir = output_dir
        self.reducer_mode = reducer_mode.lower()
        self.current_request_id = 0

        os.makedirs(self.output_dir, exist_ok=True)

    def _process_signal(self, request_id, data_type):
        """
        Process notification signal by combining partial results from all replicas.
        
        Groups partial CSV files by prefix, applies query-specific reduction logic,
        and writes final reduced files. Sends completion notification downstream.
        
        Args:
            request_id (str): Request identifier.
            data_type (int): Data type constant from the notification message.
        
        Example:
            Input: Notification signal for req-123
            
            Reads from:
            /app/temp/grouper_q2/req-123/
                replica-1/2024-01_replica-1.csv
                replica-1/2024-02_replica-1.csv
                replica-2/2024-01_replica-2.csv
            
            Groups by prefix:
            - 2024-01: [replica-1 file, replica-2 file]
            - 2024-02: [replica-1 file]
            
            Writes to:
            /app/temp/reduced_q2/req-123/
                2024-01.csv
                2024-02.csv
        """
        
        if data_type == protocol.DATA_END:
            logging.info(f"[Reducer {self.reducer_mode.upper()}] Manejo de DATA_END para request {request_id}")
            self._cleanup_request_files(request_id, self.output_dir)
            cleanup_message = protocol.create_notification_message(protocol.DATA_END, b"", request_id)
            for q in self.out_queues:
                q.send(cleanup_message)
            logging.info(f"[Reducer {self.reducer_mode.upper()}] Forwarded DATA_END notification for request {request_id}")
            return
        
        base_input = self.input_dir
        base_output = self.output_dir

        request_path = os.path.join(base_input, str(request_id))
        if not os.path.exists(request_path):
            logging.warning(f"[Reducer {self.reducer_mode.upper()}] No existe input_dir para request_id={request_id}")
            return

        replicas = [
            os.path.join(request_path, r)
            for r in os.listdir(request_path)
            if os.path.isdir(os.path.join(request_path, r))
        ]

        if not replicas:
            logging.warning(f"[Reducer {self.reducer_mode.upper()}] No hay réplicas dentro de {request_path}")
            return

        logging.info(f"[Reducer {self.reducer_mode.upper()}] Iniciando reducción para request_id={request_id} ({len(replicas)} réplicas detectadas)")

        output_request_dir = os.path.join(base_output, str(request_id))
        os.makedirs(output_request_dir, exist_ok=True)

        groups = defaultdict(list)

        for replica_dir in replicas:
            for filename in os.listdir(replica_dir):
                if not filename.endswith(".csv"):
                    continue
                prefix = "_".join(filename.split("_")[:-1])
                groups[prefix].append(os.path.join(replica_dir, filename))

        logging.info(f"[Reducer {self.reducer_mode.upper()}] Se encontraron {len(groups)} grupos para combinar.")

        for prefix, filepaths in groups.items():
            if self.reducer_mode == "q2":
                combined = self._reduce_q2(filepaths)
            elif self.reducer_mode == "q3":
                combined = self._reduce_q3(filepaths)
            elif self.reducer_mode == "q4":
                combined = self._reduce_q4(filepaths)
            else:
                logging.error(f"[Reducer] Modo desconocido: {self.reducer_mode}")
                return

            output_path = os.path.join(output_request_dir, f"{prefix}.csv")

            try:
                with open(output_path, "w", newline="") as f:
                    writer = csv.writer(f)
                    for row in combined:
                        writer.writerow(row)
                logging.info(f"[Reducer {self.reducer_mode.upper()}] Grupo {prefix} reducido correctamente ({len(combined)} filas).")
            except Exception as e:
                logging.error(f"[Reducer {self.reducer_mode.upper()}] Error al escribir {output_path}: {e}")

        gc.collect()
        logging.info(f"[Reducer {self.reducer_mode.upper()}] Reducción completa para request_id={request_id}.")
        
        self.simulate_crash(None, request_id)

        self._notify_completion(data_type, request_id)

    def _reduce_q2(self, filepaths):
        """
        Q2 reduction: sum quantity and subtotal by item_id with deduplication.
        
        Args:
            filepaths (list): List of partial CSV file paths.
        
        Returns:
            list: List of tuples (item_id, total_quantity, total_subtotal).
        """
        combined = defaultdict(lambda: [0, 0.0])
        # Track seen positions PER KEY to ignore duplicates
        # Actually tracked as (item_id, position) tuples
        seen = set()

        for fpath in filepaths:
            try:
                with open(fpath, "r") as f:
                    reader = csv.reader(f)
                    for row in reader:
                        # item_id, qty, subtotal, position
                        if len(row) < 4:
                            continue
                        
                        item_id = row[0]
                        qty = int(row[1])
                        subtotal = float(row[2])
                        pos = row[3]
                        
                        unique_key = (item_id, pos)
                        if unique_key in seen:
                            continue
                        
                        seen.add(unique_key)
                        combined[item_id][0] += qty
                        combined[item_id][1] += subtotal
            except Exception as e:
                logging.error(f"[Reducer Q2] Error leyendo {fpath}: {e}")

        return [(item_id, qty, subtotal) for item_id, (qty, subtotal) in combined.items()]

    def _reduce_q3(self, filepaths):
        """
        Q3 reduction: sum all partial TPV values with deduplication.
        
        Args:
            filepaths (list): List of partial CSV file paths.
        
        Returns:
            list: Single-row list containing the total TPV [[total]].
        """
        total = 0.0
        # Track seen positions to ignore duplicate batches
        seen_positions = set()

        for fpath in filepaths:
            try:
                with open(fpath, "r") as f:
                    reader = csv.reader(f)
                    for row in reader:
                        # value, position
                        if not row or len(row) < 2:
                            continue
                        
                        val = float(row[0])
                        pos = row[1]
                        
                        # Since Q3 splits files by Key (Semester_Store), 
                        # duplicate Position means duplicate batch for this key.
                        # Note: Different replicas might process DIFFERENT messages for the same key.
                        # BUT they are processing a SERIAL STREAM of messages.
                        # Wait, replicas share the queue.
                        # Message 1 (Pos 5) -> Replica 1 -> Writes (Val, 5)
                        # Message 2 (Pos 6) -> Replica 2 -> Writes (Val, 6)
                        # Message 1 (Pos 5) -> RETRY Replica 2 -> Writes (Val, 5)
                        # Deduplication by Position is correct because Position is unique 
                        # identifier of the INPUT MESSAGE in the stream.
                        
                        if pos in seen_positions:
                            continue
                        
                        seen_positions.add(pos)
                        total += val
            except Exception as e:
                logging.error(f"[Reducer Q3] Error leyendo {fpath}: {e}")

        s = f"{total:.10f}".rstrip('0').rstrip('.')
        if '.' in s and len(s.split('.')[1]) > 2:
            total = int(total * 100 + 0.5) / 100.0

        return [[total]]



    def _reduce_q4(self, filepaths):
        """
        Q4 reduction: sum visit counts by user_id with deduplication.
        
        Args:
            filepaths (list): List of partial CSV file paths.
        
        Returns:
            list: List of tuples (user_id, total_count).
        """
        combined = defaultdict(int)
        # Track seen (user_id, position)
        seen = set()

        for fpath in filepaths:
            try:
                with open(fpath, "r") as f:
                    reader = csv.reader(f)
                    for row in reader:
                        # user_id, count, position
                        if len(row) < 3:
                            continue
                        
                        user_id = row[0]
                        count = int(row[1])
                        pos = row[2]
                        
                        unique_key = (user_id, pos)
                        if unique_key in seen:
                            continue
                            
                        seen.add(unique_key)
                        combined[user_id] += count
            except Exception as e:
                logging.error(f"[Reducer Q4] Error leyendo {fpath}: {e}")

        return [(user_id, count) for user_id, count in combined.items()]

    def _cleanup_request_files(self, request_id, base_dir):
        """
        Clean up temporary files for a completed request.
        
        Removes the entire request directory tree containing aggregation files.
        This prevents disk space accumulation after requests are completed.
        
        Args:
            request_id (str): Request identifier to clean up.
            base_dir (str): Base directory (input_dir or output_dir).
        
        Example:
            >>> reducer._cleanup_request_files('req-123', '/app/temp/reduced_q2')
            # Removes: /app/temp/reduced_q2/req-123/
            [Reducer Q2] Cleaned up files for request_id=req-123 at /app/temp/reduced_q2/req-123
        """
        import shutil
        
        request_dir = os.path.join(base_dir, str(request_id))
        
        if os.path.exists(request_dir):
            try:
                shutil.rmtree(request_dir)
                logging.info(f"[Reducer {self.reducer_mode.upper()}] Cleaned up files for request_id={request_id} at {request_dir}")
            except Exception as e:
                logging.error(f"[Reducer {self.reducer_mode.upper()}] Error cleaning up files for request_id={request_id} at {request_dir}: {e}")
        else:
            logging.debug(f"[Reducer {self.reducer_mode.upper()}] No files found for request_id={request_id} at {request_dir}")


if __name__ == "__main__":
    import configparser

    def create_reducer_v2():
        """
        Factory function to create ReducerV2 from environment configuration.
        
        Environment Variables:
            RABBITMQ_HOST: RabbitMQ server hostname (default: rabbitmq).
            QUEUE_IN: Input queue for notification signals.
            COMPLETION_QUEUE: Output queue for completion notifications.
            REDUCER_MODE: Query type ('q2', 'q3', 'q4', default: 'q2').
            INPUT_DIR: Directory with partial grouper files (default: /app/temp/grouper_{mode}).
            OUTPUT_DIR: Directory for reduced files (default: /app/temp/reduced_{mode}).
        
        Returns:
            ReducerV2: Configured reducer instance.
        
        Example:
            >>> REDUCER_MODE=q2 QUEUE_IN=reducer_q2 python reducer.py
        """
        config_path = os.path.join(os.path.dirname(__file__), "config.ini")
        config = configparser.ConfigParser()
        config.read(config_path)

        rabbitmq_host = os.environ.get("RABBITMQ_HOST", "rabbitmq")
        queue_in = os.environ.get("QUEUE_IN")
        queue_out = os.environ.get("COMPLETION_QUEUE")
        reducer_mode = os.environ.get("REDUCER_MODE", "q2").lower()

        input_dir = os.environ.get("INPUT_DIR", f"/app/temp/grouper_{reducer_mode}")
        output_dir = os.environ.get("OUTPUT_DIR", f"/app/temp/reduced_{reducer_mode}")
        
        service_name = f"reducer_{reducer_mode}"

        return ReducerV2(queue_in, queue_out, rabbitmq_host, input_dir, output_dir, reducer_mode, service_name=service_name)

    ReducerV2.run_worker_main(create_reducer_v2)
