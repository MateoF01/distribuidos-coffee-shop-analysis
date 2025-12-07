import os
import socket
import time
import logging
import configparser
from shared import protocol
from shared.worker import StreamProcessingWorker
from WSM.wsm_client import WSMClient
from wsm_config import WSM_NODES


class Cleaner(StreamProcessingWorker):
    """
    Data cleaning worker that filters and transforms CSV rows.
    
    This worker processes incoming CSV data by:
    - Selecting specific columns from the input
    - Filtering out rows with empty required fields
    - Converting data types (e.g., user_id to integer)
    - Coordinating with Worker State Manager (WSM) for fault tolerance
    - Handling duplicate messages via position tracking
    
    The Cleaner integrates with WSM to ensure exactly-once processing semantics
    and coordinated END signal propagation across distributed replicas.
    
    Attributes:
        columns_have (list): Column names present in input data.
        columns_want (list): Column names to keep in output.
        keep_indices (list): Indices of columns to extract.
        keep_when_empty (list): Indices of columns that can be empty.
        wsm_client (WSMClient): Client for worker state coordination.
    
    Example:
        Basic usage:
        >>> cleaner = Cleaner(
        ...     queue_in='raw_data',
        ...     queue_out='clean_data',
        ...     columns_have=['id', 'name', 'age', 'city'],
        ...     columns_want=['id', 'name', 'age'],
        ...     rabbitmq_host='rabbitmq',
        ...     keep_when_empty=['city']
        ... )
        >>> cleaner.run()
        
        With WSM coordination:
        >>> cleaner = Cleaner(
        ...     queue_in='transactions',
        ...     queue_out='clean_transactions',
        ...     columns_have=['txn_id', 'user_id', 'amount'],
        ...     columns_want=['user_id', 'amount'],
        ...     rabbitmq_host='rabbitmq',
        ...     wsm_nodes=['wsm1:9000', 'wsm2:9000']
        ... )
    """
    def __init__(self, queue_in, queue_out, columns_have, columns_want, rabbitmq_host, keep_when_empty=None, backoff_start=0.1, backoff_max=3.0, wsm_nodes = None):
        """
        Initialize the Cleaner worker with column filtering configuration.
        
        Args:
            queue_in (str): Input RabbitMQ queue name.
            queue_out (str|list): Output queue name(s).
            columns_have (list): List of column names in the input data.
            columns_want (list): List of column names to keep in output.
            rabbitmq_host (str): RabbitMQ server hostname.
            keep_when_empty (list, optional): Column names that can be empty.
                Rows with empty values in other columns will be filtered out.
            backoff_start (float, optional): Initial backoff delay in seconds. Default: 0.1.
            backoff_max (float, optional): Maximum backoff delay in seconds. Default: 3.0.
            wsm_nodes (list, optional): List of WSM node addresses for coordination.
        
        Example:
            >>> cleaner = Cleaner(
            ...     queue_in='input',
            ...     queue_out='output',
            ...     columns_have=['id', 'name', 'email', 'city'],
            ...     columns_want=['id', 'email'],
            ...     rabbitmq_host='rabbitmq',
            ...     keep_when_empty=['city']
            ... )
        """
        service_name = f"cleaner_{os.environ.get('DATA_TYPE', '')}"
        super().__init__(queue_in, queue_out, rabbitmq_host, service_name=service_name)
        self.columns_have = columns_have
        self.columns_want = columns_want
        self.keep_indices = [self.columns_have.index(col) for col in self.columns_want]
        self.keep_when_empty = [self.columns_want.index(col) for col in keep_when_empty] if keep_when_empty else []
        self.backoff_start = backoff_start
        self.backoff_max = backoff_max

        replica_id = socket.gethostname()
        wsm_host = os.environ.get("WSM_HOST", "wsm")
        wsm_port = int(os.environ.get("WSM_PORT", "9000"))
        self.wsm_client = WSMClient(
            worker_type="cleaner",
            replica_id=replica_id,
            host=wsm_host,
            port=wsm_port,
            nodes=wsm_nodes,
        )

        logging.info(f"[Cleaner:{replica_id}] Inicializado - input: {queue_in}, output: {queue_out}")

    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
        """
        Process data messages by cleaning and filtering CSV rows.
        
        Coordinates with WSM to detect duplicates and track processing state.
        Only processes messages that haven't been processed before. Updates
        state to PROCESSING during execution and WAITING after completion.
        
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
            >>> # Message format: "id|name|age\nid2|name2|age2"
            >>> cleaner._process_message(
            ...     message=raw_msg,
            ...     msg_type=1,
            ...     data_type='users',
            ...     request_id='req-123',
            ...     position=5,
            ...     payload=b'1|John|30\n2|Jane|25'
            ... )
            [Processes rows, sends cleaned data to output queues]
        """

        #logging.info(f"INICIO procesado mensaje ({request_id}:{position})")

        if self.wsm_client.is_position_processed(request_id, position):
            logging.info(f"🔁 Mensaje duplicado detectado ({request_id}:{position}), descartando...")
            return

        self.wsm_client.update_state("PROCESSING", request_id, position)

        rows = payload.decode("utf-8").split("\n")
        cleaned_rows = self._process_rows(rows)

        if cleaned_rows:
            new_payload = "\n".join(cleaned_rows).encode("utf-8")
            new_msg = protocol.pack_message(msg_type, data_type, new_payload, request_id, position)
            for q in self.out_queues:
                q.send(new_msg)

        # TEST-CASE:
        # Se encolo en la salida pero no le avise al WSM
        # Se duplica el mensaje, pero el Coordinator lo detecta
        self.simulate_crash(queue_name, request_id)

        self.wsm_client.update_state("WAITING", request_id, position)
        #logging.info(f"FIN procesado mensaje ({request_id}:{position})")

    def _handle_end_signal(self, message, msg_type, data_type, request_id, position, queue_name=None):
        """
        Handle END signal with WSM coordination using exponential backoff.
        
        Extends the base END handling by first synchronizing with the Worker State
        Manager to ensure all replicas have processed their messages before forwarding
        the END signal. Uses exponential backoff to poll WSM for permission.
        
        The method:
        1. Registers END state with WSM
        2. Polls WSM with exponential backoff until permission granted
        3. Calls parent's END handling (forwards to output queues)
        4. Returns to WAITING state
        
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
            >>> cleaner._handle_end_signal(
            ...     message=end_msg,
            ...     msg_type=2,
            ...     data_type='users',
            ...     request_id='req-123',
            ...     position=100
            ... )
            [Cleaner] Recibido END para request req-123. Consultando WSM...
            [Cleaner] Esperando permiso para reenviar END de req-123... (backoff=0.100s)
            [Cleaner] ✅ Permiso otorgado para enviar END de req-123
            [END forwarded to output queues]
        """

        if data_type == protocol.DATA_END:
            logging.info(f"[Cleaner] Manejo de DATA_END para request {request_id}")
            self.wsm_client.cleanup_request(request_id)
            super()._handle_end_signal(message, msg_type, data_type, request_id, position, queue_name)
            return

        self.wsm_client.update_state("END", request_id, position)
        logging.info(f"[Cleaner] Recibido END para request {request_id}. Consultando WSM...")

        backoff = self.backoff_start
        total_wait = 0.0
        
        while not self.wsm_client.can_send_end(request_id, position):
            if total_wait >= self.backoff_max:
                error_msg = f"[Cleaner] Timeout esperando permiso WSM para END de {request_id} después de {total_wait:.2f}s"
                logging.error(error_msg)
                raise TimeoutError(error_msg)
            
            logging.info(f"[Cleaner] Esperando permiso para reenviar END de {request_id}... (backoff={backoff:.3f}s, total={total_wait:.2f}s)")
            time.sleep(backoff)
            total_wait += backoff
            backoff = min(backoff * 2, self.backoff_max - total_wait) if total_wait < self.backoff_max else 0

        logging.info(f"[Cleaner] ✅ Permiso otorgado para enviar END de {request_id}")

        super()._handle_end_signal(message, msg_type, data_type, request_id, position, queue_name)

        self.wsm_client.update_state("WAITING", request_id, position)

    def _process_rows(self, rows, queue_name=None):
        """
        Filter and clean multiple CSV rows.
        
        Processes each row by filtering columns, validating data, and removing
        rows that don't meet the criteria (empty required fields).
        
        Args:
            rows (list): List of CSV row strings.
            queue_name (str, optional): Source queue name (unused).
        
        Returns:
            list: List of cleaned row strings that passed validation.
        
        Example:
            >>> rows = [
            ...     'id|name|age|city',
            ...     '1|John|30|NYC',
            ...     '2||25|LA',  # Missing name
            ...     '3|Jane|28|'
            ... ]
            >>> cleaner._process_rows(rows)
            ['1|John|30', '3|Jane|28']
        """
        filtered_rows = []
        for row in rows:
            if row.strip():
                cleaned_row = self._filter_row(row)
                if cleaned_row:
                    filtered_rows.append(cleaned_row)
        return filtered_rows

    def _filter_row(self, row):
        """
        Filter and transform a single CSV row.
        
        Performs the following operations:
        1. Split row by delimiter ('|')
        2. Select only the configured columns
        3. Convert user_id to integer if present
        4. Filter out rows with empty required fields
        
        Args:
            row (str): CSV row string with '|' delimiter.
        
        Returns:
            str: Cleaned row string, or None if row should be filtered out.
        
        Example:
            Column selection:
            >>> cleaner.columns_have = ['id', 'name', 'age', 'city']
            >>> cleaner.columns_want = ['id', 'age']
            >>> cleaner._filter_row('1|John|30|NYC')
            '1|30'
            
            Empty field filtering:
            >>> cleaner.keep_when_empty = []  # No empty fields allowed
            >>> cleaner._filter_row('1||30|NYC')  # Missing name
            None
            
            User ID conversion:
            >>> cleaner.columns_want = ['user_id', 'amount']
            >>> cleaner._filter_row('123.0|50.00')
            '123|50.00'
        """
        items = row.split('|')

        if len(items) < max(self.keep_indices) + 1:
            logging.warning(f"Row has insufficient columns: {row}")
            return None

        try:
            selected = [items[i] for i in self.keep_indices]
        except IndexError as e:
            logging.error(f"Index error processing row: {row} - {e}")
            return None

        if 'user_id' in self.columns_want:
            user_id_idx = self.columns_want.index('user_id')
            if selected[user_id_idx] != '':
                try:
                    selected[user_id_idx] = str(int(float(selected[user_id_idx])))
                except Exception as e:
                    logging.warning(f"No se pudo convertir user_id '{selected[user_id_idx]}' a int: {e}")

        if any(selected[i] == '' and i not in self.keep_when_empty for i in range(len(selected))):
            return None

        return '|'.join(selected)


if __name__ == '__main__':
    def create_cleaner():
        """
        Factory function to create a Cleaner instance from environment variables and config.
        
        Reads configuration from:
        - Environment variables: QUEUE_IN, QUEUE_OUT, DATA_TYPE, RABBITMQ_HOST
        - config.ini file: Column mappings, keep_when_empty rules, backoff settings
        - wsm_config.py: WSM node addresses per data type
        
        Returns:
            Cleaner: Configured Cleaner instance ready to run.
        
        Raises:
            ValueError: If DATA_TYPE is not found in config.ini.
        
        Example:
            Environment setup:
            $ export QUEUE_IN=raw_transactions
            $ export QUEUE_OUT=clean_transactions
            $ export DATA_TYPE=transactions
            $ export RABBITMQ_HOST=rabbitmq
            
            Running:
            >>> cleaner = create_cleaner()
            >>> cleaner.run()
        """
        queue_in = os.environ.get('QUEUE_IN')
        queue_out = os.environ.get('QUEUE_OUT')
        data_type = os.environ.get('DATA_TYPE')
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)

        if data_type not in config:
            raise ValueError(f"Unknown data type: {data_type}")

        columns_have = [col.strip() for col in config[data_type]['have'].split(',')]
        columns_want = [col.strip() for col in config[data_type]['want'].split(',')]
        keep_when_empty_str = config[data_type].get('keep_when_empty', '').strip()
        keep_when_empty = [col.strip() for col in keep_when_empty_str.split(',')] if keep_when_empty_str else None
        
        backoff_start = float(config['DEFAULT'].get('BACKOFF_START', 0.1))
        backoff_max = float(config['DEFAULT'].get('BACKOFF_MAX', 3.0))

        wsm_nodes = WSM_NODES[data_type]

        return Cleaner(queue_in, queue_out, columns_have, columns_want, rabbitmq_host, keep_when_empty, backoff_start, backoff_max, wsm_nodes)

    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Cleaner.run_worker_main(create_cleaner, config_path)
