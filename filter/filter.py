import os
import configparser
import logging
import time
import socket
from shared.worker import StreamProcessingWorker
from shared import protocol
from WSM.wsm_client import WSMClient
from wsm_config import WSM_NODES

class Filter(StreamProcessingWorker):
    def __init__(self, queue_in, queue_out, rabbitmq_host, backoff_start=0.1, backoff_max=3.0, wsm_nodes = None):
        """
        Initialize the Filter worker with WSM coordination.
        
        Args:
            queue_in (str): Input RabbitMQ queue name.
            queue_out (str|list): Output queue name(s).
            rabbitmq_host (str): RabbitMQ server hostname.
            backoff_start (float, optional): Initial backoff delay in seconds. Default: 0.1.
            backoff_max (float, optional): Maximum backoff delay in seconds. Default: 3.0.
            wsm_nodes (list, optional): List of WSM node addresses for coordination.
        
        Example:
            >>> filter_worker = Filter(
            ...     queue_in='raw_data',
            ...     queue_out=['queue_a', 'queue_b'],
            ...     rabbitmq_host='rabbitmq',
            ...     backoff_start=0.5,
            ...     backoff_max=5.0
            ... )
        """
        super().__init__(queue_in, queue_out, rabbitmq_host)
        self.backoff_start = backoff_start
        self.backoff_max = backoff_max

        self.replica_id = socket.gethostname()
        wsm_host = os.environ.get("WSM_HOST", "wsm")
        wsm_port = int(os.environ.get("WSM_PORT", "9000"))

        self.wsm_client = WSMClient(
            worker_type="filter",
            replica_id=self.replica_id,
            host=wsm_host,
            port=wsm_port,
            nodes=wsm_nodes,
        )

        logging.info(f"[Filter:{self.replica_id}] Inicializado - input: {queue_in}, output: {queue_out}")

    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
        """
        Process data messages by filtering and routing rows to appropriate queues.
        
        Coordinates with WSM to detect duplicates and track processing state.
        Filters each row according to the implemented logic and routes results
        to the correct output queues.
        
        Args:
            message (bytes): Raw message from RabbitMQ.
            msg_type (int): Message type identifier.
            data_type (str): Type of data being processed.
            request_id (str): Unique request identifier.
            position (int): Message position in the stream.
            payload (bytes): Message payload containing CSV rows.
            queue_name (str, optional): Source queue name.
        
        Example:
            [Internal method called by StreamProcessingWorker]
            >>> filter._process_message(
            ...     message=raw_msg,
            ...     msg_type=1,
            ...     data_type='transactions',
            ...     request_id='req-123',
            ...     position=5,
            ...     payload=b'row1|data\nrow2|data'
            ... )
            [Rows filtered and routed to appropriate queues]
        """
        
        if self.wsm_client.is_position_processed(request_id, position):
            logging.info(f"🔁 Mensaje duplicado detectado ({request_id}:{position}), descartando...")
            return

        self.wsm_client.update_state("PROCESSING", request_id, position)

        rows = payload.decode("utf-8").split("\n")
        dic_queue_row = self._process_rows(rows, queue_name)

        self._send_complex_results(dic_queue_row, msg_type, data_type, request_id, position)

        self.wsm_client.update_state("WAITING", request_id, position)

    def _handle_end_signal(self, message, msg_type, data_type, request_id, position, queue_name=None):
        """
        Handle END signal with WSM coordination using exponential backoff.
        
        Synchronizes with the Worker State Manager to ensure all replicas have
        processed their messages before forwarding the END signal. Uses exponential
        backoff to poll WSM for permission to send END.
        
        Args:
            message (bytes): Raw END message.
            msg_type (int): Message type identifier.
            data_type (str): Type of data being processed.
            request_id (str): Unique request identifier.
            position (int): END signal position.
            queue_name (str, optional): Source queue name.
        
        Raises:
            TimeoutError: If WSM permission not granted within backoff_max timeout.
        
        Example:
            [Internal method called by StreamProcessingWorker]
            >>> filter._handle_end_signal(
            ...     message=end_msg,
            ...     msg_type=2,
            ...     data_type='transactions',
            ...     request_id='req-123',
            ...     position=100
            ... )
            [Filter:worker-1] Recibido END para request req-123. Esperando permiso del WSM...
            [Filter:worker-1] Esperando permiso para reenviar END... (backoff=0.100s)
            [Filter:worker-1] ✅ Permiso otorgado por el WSM para reenviar END
        """

        if data_type == protocol.DATA_END:
            logging.info(f"[Filter] Manejo de DATA_END para request {request_id}")
            self.wsm_client.cleanup_request(request_id)
            super()._handle_end_signal(message, msg_type, data_type, request_id, position, queue_name)
            return

        self.wsm_client.update_state("END", request_id, position)
        logging.info(f"[Filter:{self.replica_id}] Recibido END para request {request_id}. Esperando permiso del WSM...")

        backoff = self.backoff_start
        total_wait = 0.0
        
        while not self.wsm_client.can_send_end(request_id, position):
            if total_wait >= self.backoff_max:
                error_msg = f"[Filter:{self.replica_id}] Timeout esperando permiso WSM para END de {request_id} después de {total_wait:.2f}s"
                logging.error(error_msg)
                raise TimeoutError(error_msg)
            
            logging.info(f"[Filter:{self.replica_id}] Esperando permiso para reenviar END de {request_id}... (backoff={backoff:.3f}s, total={total_wait:.2f}s)")
            time.sleep(backoff)
            total_wait += backoff
            backoff = min(backoff * 2, self.backoff_max - total_wait) if total_wait < self.backoff_max else 0

        logging.info(f"[Filter:{self.replica_id}] ✅ Permiso otorgado por el WSM para reenviar END de {request_id}")

        super()._handle_end_signal(message, msg_type, data_type, request_id, position, queue_name)

        self.wsm_client.update_state("WAITING", request_id, position)

    def _process_rows(self, rows, queue_name=None):
        """
        Process multiple rows and organize them by target queue.
        
        Filters each row using _filter_row() and groups results by destination
        queue name.
        
        Args:
            rows (list): List of CSV row strings.
            queue_name (str, optional): Source queue name (unused in base class).
        
        Returns:
            dict: Dictionary mapping queue names to lists of filtered rows.
                Format: {queue_name: [row1, row2, ...]}
        
        Example:
            >>> rows = ['row1|data', 'row2|data', 'row3|data']
            >>> filter._process_rows(rows)
            {'queue_a': ['row1|data'], 'queue_b': ['row2|data', 'row3|data']}
        """
        dic_queue_row = {}

        for row in rows:
            if not row.strip():
                continue

            rows_queues = self._filter_row(row) or []
            for filtered_row, target_queue_name in rows_queues:
                dic_queue_row.setdefault(target_queue_name, []).append(filtered_row)

        return dic_queue_row

    def _send_complex_results(self, dic_queue_row, msg_type, data_type, request_id, position):
        """
        Send filtered results to their corresponding output queues.
        
        Takes the dictionary of filtered rows organized by queue and sends
        each group to its designated output queue.
        
        Args:
            dic_queue_row (dict): Dictionary mapping queue names to row lists.
            msg_type (int): Message type identifier.
            data_type (str): Type of data being processed.
            request_id (str): Unique request identifier.
            position (int): Message position in the stream.
        
        Example:
            >>> results = {
            ...     'high_priority': ['row1|urgent', 'row2|urgent'],
            ...     'low_priority': ['row3|normal']
            ... }
            >>> filter._send_complex_results(
            ...     results, msg_type=1, data_type='transactions',
            ...     request_id='req-123', position=5
            ... )
            [Messages sent to high_priority and low_priority queues]
        """
        for queue_name, filtered_rows in dic_queue_row.items():
            new_payload_str = '\n'.join(filtered_rows)
            new_payload = new_payload_str.encode('utf-8')
            new_message = protocol.create_data_message(data_type, new_payload, request_id, position)

            for q in self.out_queues:
                if q.queue_name == queue_name:
                    q.send(new_message)

    def _filter_row(self, row: str):
        """
        Filter a single row and determine its routing.
        
        Base implementation that routes all rows to the first output queue.
        Subclasses should override this method to implement custom filtering logic.
        
        Args:
            row (str): CSV row string to filter.
        
        Returns:
            list: List of tuples (filtered_row, target_queue_name), or None to discard.
        
        Example:
            Base implementation:
            >>> filter._filter_row('data|row')
            [('data|row', 'default')]
            
            Custom implementation (in subclass):
            >>> class CustomFilter(Filter):
            ...     def _filter_row(self, row):
            ...         if 'important' in row:
            ...             return [(row, 'priority_queue')]
            ...         return [(row, 'normal_queue')]
        """
        return [(row, self.out_queues[0].queue_name if self.out_queues else "default")]


class TemporalFilter(Filter):
    """
    Filter that routes data based on temporal criteria (year and hour ranges).
    
    Routes rows to different queues based on timestamp values extracted from
    the 'created_at' column. Multiple routing rules can be configured, and a
    single row can be routed to multiple queues if it matches multiple rules.
    
    Attributes:
        data_type (str): Type of data being filtered.
        col_index (dict): Mapping of column names to their indices.
        rules (list): List of routing rules with year/hour ranges and target queues.
    
    Example:
        Route by year:
        >>> config = configparser.ConfigParser()
        >>> config.add_section('rule1')
        >>> config['rule1'] = {
        ...     'DATA_TYPE': 'transactions',
        ...     'QUEUE_OUT': 'transactions_2024',
        ...     'YEAR_START': '2024',
        ...     'YEAR_END': '2024'
        ... }
        >>> filter = TemporalFilter(
        ...     queue_in='input',
        ...     queue_out=['transactions_2024', 'transactions_2025'],
        ...     rabbitmq_host='rabbitmq',
        ...     data_type='transactions',
        ...     col_index={'created_at': 2},
        ...     config=config
        ... )
        
        Route by hour (business hours):
        >>> config['rule2'] = {
        ...     'DATA_TYPE': 'transactions',
        ...     'QUEUE_OUT': 'business_hours',
        ...     'HOUR_START': '9',
        ...     'HOUR_END': '17'
        ... }
    """
    def __init__(self, queue_in, queue_out, rabbitmq_host, data_type, col_index, config, backoff_start=0.1, backoff_max=3.0, wsm_nodes = None):
        super().__init__(queue_in, queue_out, rabbitmq_host, backoff_start, backoff_max, wsm_nodes)
        self.data_type = data_type
        self.col_index = col_index
        self.rules = []

        for section in config.sections():
            rule_data_type = config[section].get("DATA_TYPE")
            if rule_data_type != self.data_type:
                continue

            queue_out = config[section]["QUEUE_OUT"].strip()
            year_start = int(config[section].get("YEAR_START", 0))
            year_end = int(config[section].get("YEAR_END", 9999))
            hour_start = int(config[section].get("HOUR_START", 0))
            hour_end = int(config[section].get("HOUR_END", 23))

            self.rules.append({
                "queue_out": queue_out,
                "year_start": year_start,
                "year_end": year_end,
                "hour_start": hour_start,
                "hour_end": hour_end
            })

    def _route(self, row: str, year, hour):
        """
        Route a row to queues based on year and hour matching rules.
        
        Evaluates all configured rules and returns the row paired with each
        matching queue. A single row can match multiple rules and be sent to
        multiple queues.
        
        Args:
            row (str): CSV row string.
            year (int): Extracted year from created_at column.
            hour (int): Extracted hour from created_at column.
        
        Returns:
            list: List of tuples (row, queue_name) for all matching rules.
        
        Example:
            >>> filter._route('txn_data', year=2024, hour=10)
            [('txn_data', 'txn_2024'), ('txn_data', 'business_hours')]
        """
        result = []
        for rule in self.rules:
            if rule["year_start"] <= year <= rule["year_end"] and \
               rule["hour_start"] <= hour <= rule["hour_end"]:
                result.append((row, rule["queue_out"]))
        return result

    def _filter_row(self, row: str):
        """
        Extract timestamp from row and route based on temporal rules.
        
        Parses the 'created_at' column to extract year and hour, then routes
        the row according to configured temporal rules.
        
        Args:
            row (str): CSV row string with '|' delimiter.
        
        Returns:
            list: List of tuples (row, queue_name) for matching rules, or None if invalid.
        
        Example:
            Row format: 'id|name|2024-03-15 14:30:00|amount'
            >>> filter._filter_row('123|John|2024-03-15 14:30:00|50.00')
            [('123|John|2024-03-15 14:30:00|50.00', 'txn_2024')]
            
            Invalid timestamp:
            >>> filter._filter_row('123|John|invalid|50.00')
            None
        """
        parts = row.split('|')
        if "created_at" not in self.col_index or len(parts) <= self.col_index["created_at"]:
            return None

        created_at_str = parts[self.col_index["created_at"]].strip()
        try:
            year = int(created_at_str[0:4])
            hour = int(created_at_str[11:13])
        except Exception:
            return None

        return self._route(row, year, hour)


class AmountFilter(Filter):
    """
    Filter that routes data based on minimum amount threshold.
    
    Filters rows by checking if the 'final_amount' column meets a minimum
    threshold. Rows below the threshold are discarded.
    
    Attributes:
        min_amount (float): Minimum amount threshold.
        col_index (dict): Mapping of column names to their indices.
    
    Example:
        Filter high-value transactions:
        >>> filter = AmountFilter(
        ...     queue_in='all_transactions',
        ...     queue_out='high_value_transactions',
        ...     rabbitmq_host='rabbitmq',
        ...     min_amount=100.0,
        ...     col_index={'final_amount': 3}
        ... )
        >>> filter._filter_row('123|John|2024-03-15|150.00')
        [('123|John|2024-03-15|150.00', 'high_value_transactions')]
        >>> filter._filter_row('124|Jane|2024-03-15|50.00')
        None
    """
    def __init__(self, queue_in, queue_out, rabbitmq_host, min_amount, col_index, backoff_start=0.1, backoff_max=3.0, wsm_nodes = None):
        super().__init__(queue_in, queue_out, rabbitmq_host, backoff_start, backoff_max, wsm_nodes)
        self.min_amount = float(min_amount)
        self.col_index = col_index

    def _filter_row(self, row: str):
        """
        Filter row based on amount threshold.
        
        Extracts the 'final_amount' value and checks if it meets the minimum
        threshold. Returns the row if it passes, None otherwise.
        
        Args:
            row (str): CSV row string with '|' delimiter.
        
        Returns:
            list: List with single tuple (row, queue_name) if amount >= min_amount,
                None if amount below threshold or parsing fails.
        
        Example:
            Amount above threshold:
            >>> filter.min_amount = 100.0
            >>> filter._filter_row('123|data|150.00')
            [('123|data|150.00', 'output_queue')]
            
            Amount below threshold:
            >>> filter._filter_row('124|data|50.00')
            None
            
            Invalid amount:
            >>> filter._filter_row('125|data|invalid')
            None
        """
        parts = row.split('|')

        if "final_amount" not in self.col_index or len(parts) <= self.col_index["final_amount"]:
            return None
        try:
            amount = float(parts[self.col_index["final_amount"]])
        except Exception:
            return None

        if amount < self.min_amount:
            return None
        return [(row, self.out_queues[0].queue_name)]


if __name__ == '__main__':
    def create_filter():
        """
        Factory function to create a Filter instance from environment variables and config.
        
        Reads configuration from:
        - Environment variables: QUEUE_IN, QUEUE_OUT, RABBITMQ_HOST, FILTER_TYPE, DATA_TYPE, MIN_AMOUNT
        - config.ini: Column mappings, backoff settings
        - temporal_filter_config.ini: Temporal routing rules (for temporal filters)
        - wsm_config.py: WSM node addresses
        
        Supports two filter types:
        - 'temporal': Routes based on year and hour ranges
        - 'amount': Routes based on minimum amount threshold
        
        Returns:
            Filter: Configured Filter instance (TemporalFilter or AmountFilter).
        
        Raises:
            ValueError: If DATA_TYPE not in config or FILTER_TYPE unknown.
        
        Example:
            Temporal filter:
            $ export FILTER_TYPE=temporal
            $ export DATA_TYPE=transactions
            $ export QUEUE_IN=raw_txn
            $ export QUEUE_OUT=filtered_txn
            >>> filter = create_filter()
            >>> filter.run()
            
            Amount filter:
            $ export FILTER_TYPE=amount
            $ export MIN_AMOUNT=100.0
            $ export DATA_TYPE=transactions
            >>> filter = create_filter()
        """
        queue_in = os.environ.get('QUEUE_IN')
        queue_out = os.environ.get('QUEUE_OUT')
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

        filter_type = os.environ.get('FILTER_TYPE', 'temporal')  # temporal o amount
        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)

        data_type = os.environ.get('DATA_TYPE')
        if data_type not in config:
            raise ValueError(f"Unknown data type: {data_type}")

        columns_have = [c.strip() for c in config[data_type]['have'].split(',')]
        col_index = {c: i for i, c in enumerate(columns_have)}

        backoff_start = float(config['DEFAULT'].get('BACKOFF_START', 0.1))
        backoff_max = float(config['DEFAULT'].get('BACKOFF_MAX', 3.0))

        key = filter_type + '_' + data_type
        wsm_nodes = WSM_NODES[key]
        print("WSM NODES: ", wsm_nodes)

        if filter_type == 'temporal':
            temporal_config_path = os.path.join(os.path.dirname(__file__), 'temporal_filter_config.ini')
            temporal_config = configparser.ConfigParser()
            temporal_config.read(temporal_config_path)
            return TemporalFilter(queue_in, queue_out, rabbitmq_host, data_type, col_index, temporal_config, backoff_start, backoff_max, wsm_nodes)

        elif filter_type == 'amount':
            min_amount = os.environ.get('MIN_AMOUNT')
            return AmountFilter(queue_in, queue_out, rabbitmq_host, min_amount, col_index, backoff_start, backoff_max, wsm_nodes)

        else:
            raise ValueError(f"Unknown FILTER_TYPE: {filter_type}")

    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Filter.run_worker_main(create_filter, config_path)
