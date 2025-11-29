import os
import signal
import sys
import threading
import logging
import tempfile
from abc import ABC, abstractmethod
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue
from shared import protocol
from shared.logging_config import initialize_log


class Worker(ABC):
    """
    Abstract base class for all worker components in the distributed coffee shop analysis system.
    
    The Worker class provides a robust foundation for implementing data processing nodes that
    consume messages from RabbitMQ queues, process them, and forward results. It handles the
    complexities of message loop management, graceful shutdown, and queue lifecycle.
    
    Key Features:
        - Single or multiple input queue support
        - Multiple output queue routing
        - Automatic message validation and parsing
        - Graceful shutdown with signal handling (SIGTERM, SIGINT)
        - Thread-safe operations with shutdown events
        - Atomic file writing utilities
        - Logging configuration
    
    Subclass Implementation:
        Subclasses must implement the _process_message() method to define their
        specific data processing logic.
    
    Attributes:
        rabbitmq_host (str): RabbitMQ server hostname.
        queue_in (str): Name of the single input queue (if applicable).
        single_input_queue (CoffeeMessageMiddlewareQueue): Single input queue object.
        in_queues (dict): Dictionary of multiple input queues (for joiners).
        out_queues (list): List of output queue objects.
        multiple_input_queues (list): List of input queue names for multi-queue workers.
        worker_kwargs (dict): Additional parameters passed to subclasses.
    
    Example:
        Simple single-input, single-output worker:
        >>> class MyWorker(Worker):
        ...     def _process_message(self, message, msg_type, data_type, 
        ...                          request_id, position, payload, queue_name=None):
        ...         data = payload.decode('utf-8')
        ...         processed = data.upper()
        ...         new_msg = protocol.create_data_message(data_type, processed.encode(), 
        ...                                                request_id, position)
        ...         for q in self.out_queues:
        ...             q.send(new_msg)
        >>> worker = MyWorker('input_queue', 'output_queue', 'rabbitmq')
        >>> worker.run()
        MyWorker started - input queue: input_queue, output queues: ['output_queue']
        
        Multi-input worker (joiner pattern):
        >>> worker = MyWorker(
        ...     queue_in=None,
        ...     queue_out='joined_output',
        ...     rabbitmq_host='rabbitmq',
        ...     multiple_input_queues=['queue_a', 'queue_b']
        ... )
        >>> worker.run()
        MyWorker started - multiple input queues: ['queue_a', 'queue_b'], output queues: ['joined_output']
    """
    
    def __init__(self, queue_in, queue_out, rabbitmq_host, multiple_input_queues=None, **kwargs):
        """
        Initialize the worker with queue configuration and setup connections.
        
        Args:
            queue_in (str): Single input queue name, or None if using multiple_input_queues.
            queue_out (str|list): Output queue name(s). Can be:
                - Single queue name: 'output_queue'
                - Comma-separated string: 'queue1,queue2,queue3'
                - List of queue names: ['queue1', 'queue2']
            rabbitmq_host (str): RabbitMQ server hostname or IP address.
            multiple_input_queues (list, optional): List of input queue names for workers
                that consume from multiple sources (e.g., joiners).
            **kwargs: Additional worker-specific parameters stored in worker_kwargs.
        
        Example:
            Single input/output:
            >>> worker = MyWorker('input_q', 'output_q', 'rabbitmq')
            >>> worker.queue_in
            'input_q'
            >>> [q.queue_name for q in worker.out_queues]
            ['output_q']
            
            Multiple outputs:
            >>> worker = MyWorker('input_q', 'out1,out2,out3', 'rabbitmq')
            >>> [q.queue_name for q in worker.out_queues]
            ['out1', 'out2', 'out3']
            
            Multiple inputs:
            >>> worker = MyWorker(None, 'output_q', 'rabbitmq',
            ...                   multiple_input_queues=['in1', 'in2'])
            >>> list(worker.in_queues.keys())
            ['in1', 'in2']
        """
        self.rabbitmq_host = rabbitmq_host
        self._running = False
        self._shutdown_event = threading.Event()
        self._lock = threading.Lock()
        
        if multiple_input_queues:
            self.multiple_input_queues = multiple_input_queues
            self.in_queues = {
                queue_name: CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_name)
                for queue_name in multiple_input_queues
            }
            self.single_input_queue = None
            self.queue_in = None
        else:
            self.queue_in = queue_in
            self.single_input_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_in) if queue_in else None
            self.in_queues = {}
            self.multiple_input_queues = []
        
        self.out_queues = self._initialize_output_queues(queue_out)
        
        self.worker_kwargs = kwargs
    
    def _initialize_output_queues(self, queue_out):
        """
        Initialize output queue connections from flexible input formats.
        
        Args:
            queue_out (str|list|None): Output queue specification.
        
        Returns:
            list: List of CoffeeMessageMiddlewareQueue objects.
        
        Example:
            >>> worker._initialize_output_queues('single_queue')
            [<CoffeeMessageMiddlewareQueue: single_queue>]
            
            >>> worker._initialize_output_queues('q1,q2,q3')
            [<CoffeeMessageMiddlewareQueue: q1>, 
             <CoffeeMessageMiddlewareQueue: q2>,
             <CoffeeMessageMiddlewareQueue: q3>]
            
            >>> worker._initialize_output_queues(['queue_a', 'queue_b'])
            [<CoffeeMessageMiddlewareQueue: queue_a>,
             <CoffeeMessageMiddlewareQueue: queue_b>]
        """
        if not queue_out:
            return []
        
        if isinstance(queue_out, str):
            queue_names = [q.strip() for q in queue_out.split(',')]
        elif isinstance(queue_out, list):
            queue_names = queue_out
        else:
            queue_names = [str(queue_out)]
        
        return [CoffeeMessageMiddlewareQueue(host=self.rabbitmq_host, queue_name=q) 
                for q in queue_names if q]
    
    def run(self):
        """
        Start the worker's main execution loop.
        
        This method sets up message consumption from input queues and blocks the main
        thread until a shutdown signal is received. It handles both single and multiple
        input queue configurations.
        
        The method will run indefinitely until:
        - A shutdown signal (SIGTERM/SIGINT) is received
        - KeyboardInterrupt is raised
        - stop() is called from another thread
        
        Example:
            >>> worker = MyWorker('input_queue', 'output_queue', 'rabbitmq')
            >>> worker.run()
            MyWorker started - input queue: input_queue, output queues: ['output_queue']
            [Blocks here until shutdown signal]
            Received signal 15, shutting down MyWorker gracefully...
            Stopping MyWorker...
        """
        self._running = True
        
        if self.single_input_queue:
            self.single_input_queue.start_consuming(self._create_message_handler())
        else:
            for queue_name, queue in self.in_queues.items():
                handler = self._create_message_handler(queue_name)
                queue.start_consuming(handler)
        
        self._log_startup_info()
        
        try:
            self._shutdown_event.wait()
        except KeyboardInterrupt:
            logging.info("Keyboard interrupt received, shutting down...")
            self.stop()
    
    def _create_message_handler(self, queue_name=None):
        """
        Create a message handler callback for processing incoming queue messages.
        
        The handler validates messages, parses protocol fields, and routes to appropriate
        processing methods based on message type.
        
        Args:
            queue_name (str, optional): Source queue name for multi-queue workers.
        
        Returns:
            callable: Message handler function that accepts raw message bytes.
        
        Example:
            >>> handler = worker._create_message_handler('my_queue')
            >>> handler(b'\x02\x03\x00\x00\x00\x01test_payload')
            [Processes message and routes to _process_message or _handle_end_signal]
        """
        def on_message(message):
            if not self._running:
                return
            
            try:
                if not self._validate_message(message):
                    return
                
                msg_type, data_type, request_id, position, payload = protocol.unpack_message(message)
                
                if msg_type == protocol.MSG_TYPE_END:
                    self._handle_end_signal(message, msg_type, data_type, request_id, position, queue_name)
                    return
                
                self._process_message(message, msg_type, data_type, request_id, position, payload, queue_name)
                
            except Exception as e:
                logging.error(f"Error processing message: {e}")
        
        return on_message
    
    def _validate_message(self, message):
        """
        Validate that a message has the correct format and minimum size.
        
        Args:
            message (bytes): Raw message from queue.
        
        Returns:
            bool: True if message is valid, False otherwise.
        
        Example:
            >>> worker._validate_message(b'\x02\x03\x00\x00\x00\x01payload')
            True
            >>> worker._validate_message(b'\x02\x03')
            False
            >>> worker._validate_message('string')
            False
        """
        if not isinstance(message, bytes) or len(message) < 6:
            logging.warning(f"Invalid message format or too short: {message}")
            return False
        return True
    
    def _handle_end_signal(self, message, msg_type, data_type, request_id, position, queue_name=None):
        """
        Handle END signals by forwarding them to output queues.
        
        The default implementation forwards END signals to all output queues, allowing
        downstream workers to know when data streams complete. Subclasses can override
        this method for custom END signal handling.
        
        Args:
            message (bytes): Raw message bytes.
            msg_type (int): Message type constant (MSG_TYPE_END).
            data_type (int): Data type constant.
            request_id (int): Request identifier.
            position (int): Message position.
            queue_name (str, optional): Source queue name.
        
        Example:
            >>> worker._handle_end_signal(end_message, protocol.MSG_TYPE_END, 
            ...                           protocol.DATA_TRANSACTIONS, 1, 5, 'input_q')
            End-of-data signal received from queue: input_q
            [Forwards to all output queues]
        """
        
        for q in self.out_queues:
            q.send(message)
        
        if data_type == protocol.DATA_END:
            logging.info(f'End-of-data signal received from queue: {queue_name or self.queue_in}')
        else:
            queue_names = [q.queue_name for q in self.out_queues]
            logging.debug(f"Forwarded end signal (type:{msg_type}) to {queue_names}")
    
    def _log_startup_info(self):
        """
        Log worker startup information including queue configuration.
        
        Example:
            Single input:
            >>> worker._log_startup_info()
            MyWorker started - input queue: input_queue, output queues: ['output_queue']
            
            Multiple inputs:
            >>> worker._log_startup_info()
            MyWorker started - multiple input queues: ['q1', 'q2'], output queues: ['out']
        """
        if self.single_input_queue:
            input_info = f"input queue: {self.queue_in}"
        else:
            input_info = f"multiple input queues: {list(self.in_queues.keys())}"
        
        output_info = f"output queues: {[q.queue_name for q in self.out_queues]}"
        worker_type = self.__class__.__name__
        logging.info(f"{worker_type} started - {input_info}, {output_info}")
    
    @staticmethod
    def atomic_write(target_file, write_func):
        """
        Atomically write to a file using a temporary file and rename operation.
        
        This pattern ensures:
        1. File is written completely to a temporary file in the same directory
        2. Only if writing succeeds, the temp file is renamed to the final destination
        3. If writing fails, the temp file is cleaned up but the destination is not corrupted
        4. The rename operation is atomic on POSIX systems
        
        This prevents partial or corrupted files from being visible to other processes
        during crashes or interruptions.
        
        Args:
            target_file (str): The final destination file path.
            write_func (callable): A function that takes a file path and writes to it.
                Should handle opening/closing the file itself.
        
        Raises:
            Exception: Re-raises any exception from write_func after cleanup.
        
        Example:
            Simple CSV write:
            >>> def write_csv(path):
            ...     with open(path, 'w') as f:
            ...         f.write('col1,col2\n')
            ...         f.write('val1,val2\n')
            >>> Worker.atomic_write('/data/output.csv', write_csv)
            [atomic_write] Successfully wrote to /data/output.csv
            
            With error handling:
            >>> def write_with_error(path):
            ...     with open(path, 'w') as f:
            ...         f.write('data')
            ...         raise ValueError('Something went wrong')
            >>> Worker.atomic_write('/data/output.csv', write_with_error)
            [atomic_write] Error writing to /data/output.csv: Something went wrong
            ValueError: Something went wrong
        """
        target_dir = os.path.dirname(target_file) or '.'
        os.makedirs(target_dir, exist_ok=True)
        
        fd, temp_path = tempfile.mkstemp(dir=target_dir, prefix='.tmp_', suffix='.csv')
        try:
            os.close(fd)
            
            write_func(temp_path)
            
            os.rename(temp_path, target_file)
            
            logging.debug(f"[atomic_write] Successfully wrote to {target_file}")
            
        except Exception as e:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception as cleanup_err:
                logging.warning(f"[atomic_write] Failed to cleanup temp file {temp_path}: {cleanup_err}")
            
            logging.error(f"[atomic_write] Error writing to {target_file}: {e}")
            raise e
    
    @abstractmethod
    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
        """
        Process an incoming message. Must be implemented by subclasses.
        
        This is the core method where subclasses implement their specific data processing
        logic. It receives parsed message components and should process the payload,
        then forward results to output queues as needed.
        
        Args:
            message (bytes): Raw message bytes from the queue.
            msg_type (int): Message type constant from protocol.
            data_type (int): Data type constant from protocol.
            request_id (int): Request identifier for tracking.
            position (int): Message sequence position.
            payload (bytes): Message payload containing actual data.
            queue_name (str, optional): Source queue name (for multi-queue workers).
        
        Example:
            Simple forwarding implementation:
            >>> def _process_message(self, message, msg_type, data_type, 
            ...                      request_id, position, payload, queue_name=None):
            ...     data = payload.decode('utf-8')
            ...     processed = data.upper()
            ...     new_msg = protocol.create_data_message(data_type, 
            ...                                             processed.encode(),
            ...                                             request_id, position)
            ...     for q in self.out_queues:
            ...         q.send(new_msg)
            ...     logging.info(f"Processed message from request {request_id}")
        """
        pass
    
    def stop(self):
        """
        Stop the worker gracefully and clean up resources.
        
        This method stops consuming from queues, signals the shutdown event,
        and closes all queue connections. It can be called from signal handlers
        or externally to initiate shutdown.
        
        Example:
            >>> worker.stop()
            Stopping MyWorker...
            [Stops queue consumption]
            [Closes all queue connections]
        """
        logging.info(f"Stopping {self.__class__.__name__}...")
        self._running = False
        self._shutdown_event.set()
        
        try:
            if self.single_input_queue:
                self.single_input_queue.stop_consuming()
            else:
                for queue in self.in_queues.values():
                    queue.stop_consuming()
        except Exception as e:
            logging.error(f"Error stopping consumers: {e}")
        
        self.close()
    
    def close(self):
        """
        Close all RabbitMQ queue connections.
        
        Safely closes input and output queue connections, handling any exceptions
        that may occur during the closing process.
        
        Example:
            >>> worker.close()
            [Closes all queue connections]
        """
        try:
            if self.single_input_queue:
                self.single_input_queue.close()
            
            for queue in self.in_queues.values():
                queue.close()
            
            for queue in self.out_queues:
                queue.close()
        except Exception as e:
            logging.error(f"Error closing queues: {e}")
    
    @classmethod
    def setup_signal_handlers(cls, worker_instance):
        """
        Configure signal handlers for graceful worker shutdown.
        
        Registers handlers for SIGINT (Ctrl+C) and SIGTERM (kill) signals to
        allow clean shutdown of the worker process.
        
        Args:
            worker_instance (Worker): Worker instance to stop on signal reception.
        
        Example:
            >>> worker = MyWorker('in_q', 'out_q', 'rabbitmq')
            >>> Worker.setup_signal_handlers(worker)
            >>> # Press Ctrl+C
            Received signal 2, shutting down MyWorker gracefully...
            Stopping MyWorker...
        """
        def signal_handler(signum, frame):
            logging.info(f'Received signal {signum}, shutting down {cls.__name__} gracefully...')
            worker_instance.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    @classmethod
    def setup_logging(cls, config_path=None):
        """
        Configure logging for the worker process.
        
        Reads logging level from environment variable or config file and initializes
        the logging system.
        
        Args:
            config_path (str, optional): Path to configuration file containing logging settings.
        
        Example:
            From environment:
            >>> os.environ['LOGGING_LEVEL'] = 'DEBUG'
            >>> Worker.setup_logging()
            [Logging configured to DEBUG level]
            
            From config file:
            >>> Worker.setup_logging('config.ini')
            [Reads LOGGING_LEVEL from config.ini]
        """
        if config_path:
            import configparser
            config = configparser.ConfigParser()
            config.read(config_path)
            logging_level = os.environ.get('LOGGING_LEVEL', 
                                         config.get('DEFAULT', 'LOGGING_LEVEL', fallback='INFO'))
        else:
            logging_level = os.environ.get('LOGGING_LEVEL', 'INFO')
        
        initialize_log(logging_level)
    
    @classmethod
    def run_worker_main(cls, worker_factory, config_path=None):
        """
        Main entry point template for worker processes.
        
        This convenience method handles the complete worker lifecycle:
        - Logging setup
        - Worker instantiation
        - Signal handler registration
        - Worker execution
        - Error handling and cleanup
        
        Args:
            worker_factory (callable): Function that creates and returns a worker instance.
            config_path (str, optional): Path to config file for logging setup.
        
        Example:
            >>> def create_worker():
            ...     return MyWorker('input_q', 'output_q', 'rabbitmq')
            >>> Worker.run_worker_main(create_worker, 'config.ini')
            Starting MyWorker...
            MyWorker started - input queue: input_q, output queues: ['output_q']
            [Runs until shutdown signal]
            MyWorker shutdown complete.
        """
        try:
            # Setup logging
            cls.setup_logging(config_path)
            
            # Create worker instance
            worker = worker_factory()
            
            # Setup signal handlers
            cls.setup_signal_handlers(worker)
            
            # Start worker
            logging.info(f"Starting {cls.__name__}...")
            worker.run()
            
        except KeyboardInterrupt:
            logging.info(f'Keyboard interrupt received, shutting down {cls.__name__}.')
            if 'worker' in locals():
                worker.stop()
        except Exception as e:
            logging.error(f'Error in {cls.__name__}: {e}')
            if 'worker' in locals():
                worker.stop()
        finally:
            logging.info(f'{cls.__name__} shutdown complete.')


class StreamProcessingWorker(Worker):
    """
    Specialized worker for stream processing operations on row-based data.
    
    StreamProcessingWorker extends the base Worker class with functionality specific
    to processing data streams row-by-row. It handles common patterns for workers that
    filter, transform, or enrich rows of data before forwarding them to output queues.
    
    Key Features:
        - Automatic payload decoding and row splitting
        - Request ID tracking across messages
        - Simplified row processing interface
        - Automatic result forwarding to output queues
    
    Subclass Implementation:
        Subclasses must implement _process_rows() to define row transformation logic.
    
    Attributes:
        current_request_id (int): Currently processing request ID.
    
    Example:
        Filter worker that removes empty rows:
        >>> class FilterWorker(StreamProcessingWorker):
        ...     def _process_rows(self, rows, queue_name=None):
        ...         return [row for row in rows if row.strip()]
        >>> worker = FilterWorker('input_q', 'output_q', 'rabbitmq')
        >>> worker.run()
        FilterWorker started - input queue: input_q, output queues: ['output_q']
        
        Transform worker that uppercase data:
        >>> class UpperWorker(StreamProcessingWorker):
        ...     def _process_rows(self, rows, queue_name=None):
        ...         return [row.upper() for row in rows]
    """
    
    def __init__(self, queue_in, queue_out, rabbitmq_host, **kwargs):
        super().__init__(queue_in, queue_out, rabbitmq_host, **kwargs)
        self.current_request_id = 0
    
    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
        """
        Process incoming message by decoding, processing rows, and forwarding results.
        
        Args:
            message (bytes): Raw message bytes.
            msg_type (int): Message type constant.
            data_type (int): Data type constant.
            request_id (int): Request identifier.
            position (int): Message position.
            payload (bytes): Message payload.
            queue_name (str, optional): Source queue name.
        
        Example:
            >>> worker._process_message(msg, protocol.MSG_TYPE_DATA, 
            ...                         protocol.DATA_TRANSACTIONS, 1, 1, 
            ...                         b'row1\nrow2\nrow3', None)
            [Decodes payload, calls _process_rows, forwards results]
        """
        try:
            self.current_request_id = request_id
            
            payload_str = payload.decode('utf-8')
            rows = payload_str.split('\n')
            
            processed_rows = self._process_rows(rows, queue_name)
            
            self._send_processed_rows(processed_rows, msg_type, data_type, request_id, position)
            
        except Exception as e:
            logging.error(f"Failed to decode or process payload: {e}")
    
    @abstractmethod
    def _process_rows(self, rows, queue_name=None):
        """
        Process a list of data rows. Must be implemented by subclasses.
        
        Args:
            rows (list): List of row strings to process.
            queue_name (str, optional): Source queue name (for multi-queue workers).
            
        Returns:
            list: Processed rows as strings, or complex results for custom handling.
        
        Example:
            Filter implementation:
            >>> def _process_rows(self, rows, queue_name=None):
            ...     return [row for row in rows if 'valid' in row]
            
            Transform implementation:
            >>> def _process_rows(self, rows, queue_name=None):
            ...     result = []
            ...     for row in rows:
            ...         cols = row.split('|')
            ...         cols[0] = cols[0].upper()
            ...         result.append('|'.join(cols))
            ...     return result
        """
        pass
    
    def _send_processed_rows(self, processed_rows, msg_type, data_type, request_id, position):
        """
        Send processed rows to output queues.
        
        Handles both simple row lists and complex processing results. For simple string
        lists, forwards directly to all output queues. For complex results, delegates
        to _send_complex_results() for custom handling.
        
        Args:
            processed_rows: Results from _process_rows() - list of strings or complex data.
            msg_type (int): Original message type.
            data_type (int): Original data type.
            request_id (int): Request identifier.
            position (int): Message position.
        
        Example:
            >>> worker._send_processed_rows(['row1', 'row2', 'row3'],
            ...                             protocol.MSG_TYPE_DATA,
            ...                             protocol.DATA_TRANSACTIONS,
            ...                             1, 5)
            Sent processed message to ['output_queue'] (rows: 3, msg_type: 2, data_type: 1)
        """
        if not processed_rows:
            logging.debug("No rows after processing, nothing sent.")
            return
        
        if isinstance(processed_rows, list) and all(isinstance(row, str) for row in processed_rows):
            self._send_rows_to_all_queues(processed_rows, msg_type, data_type, request_id, position)
        else:
            self._send_complex_results(processed_rows, msg_type, data_type, request_id, position)

    def _send_rows_to_all_queues(self, rows, msg_type, data_type, request_id, position):
        """
        Send a list of row strings to all configured output queues.
        
        Args:
            rows (list): List of row strings.
            msg_type (int): Message type constant.
            data_type (int): Data type constant.
            request_id (int): Request identifier.
            position (int): Message position.
        
        Example:
            >>> worker._send_rows_to_all_queues(
            ...     ['row1', 'row2'], protocol.MSG_TYPE_DATA,
            ...     protocol.DATA_TRANSACTIONS, 1, 5)
            Sent processed message to ['output_q'] (rows: 2, msg_type: 2, data_type: 1)
        """
        new_payload_str = '\n'.join(rows)
        new_payload = new_payload_str.encode('utf-8')
        new_message = protocol.create_data_message(data_type, new_payload, request_id, position)
        
        for q in self.out_queues:
            q.send(new_message)
        
        queue_names = [q.queue_name for q in self.out_queues]
        logging.debug(f"Sent processed message to {queue_names} (rows: {len(rows)}, msg_type: {msg_type}, data_type: {data_type})")
    
    def _send_complex_results(self, processed_rows, msg_type, data_type, request_id, position):
        """
        Handle complex processing results. Override in subclasses as needed.
        
        Called when _process_rows() returns something other than a simple list of strings.
        Subclasses should override this method to handle their specific result formats.
        
        Args:
            processed_rows: Complex processing results from _process_rows().
            msg_type (int): Message type constant.
            data_type (int): Data type constant.
            request_id (int): Request identifier.
            position (int): Message position.
        
        Example:
            >>> def _send_complex_results(self, processed_rows, msg_type, 
            ...                           data_type, request_id, position):
            ...     for queue_name, rows in processed_rows.items():
            ...         target_queue = self.out_queues[queue_name]
            ...         payload = '\n'.join(rows).encode('utf-8')
            ...         msg = protocol.create_data_message(data_type, payload, 
            ...                                             request_id, position)
            ...         target_queue.send(msg)
        """
        logging.warning("Complex processing results not handled by default implementation")


class FileProcessingWorker(Worker):
    """
    Specialized worker for file-based batch processing operations.
    
    FileProcessingWorker is designed for workers that process entire files rather than
    streaming data. It responds to notification signals, processes files, and typically
    sends completion signals downstream.
    
    Common use cases include:
    - Sorting complete datasets
    - Aggregating results from temporary files
    - Merging multiple input files
    - Generating final output files
    
    Subclass Implementation:
        Subclasses must implement _process_file() to define file processing logic.
    
    Attributes:
        input_file (str): Path to input file to process.
        output_file (str): Path to output file to create.
        current_request_id (int): Request ID from the triggering notification.
    
    Example:
        Sort worker:
        >>> class SortWorker(FileProcessingWorker):
        ...     def _process_file(self):
        ...         with open(self.input_file) as f:
        ...             lines = sorted(f.readlines())
        ...         with open(self.output_file, 'w') as f:
        ...             f.writelines(lines)
        >>> worker = SortWorker('input_q', 'output_q', 'rabbitmq',
        ...                     input_file='/tmp/unsorted.csv',
        ...                     output_file='/tmp/sorted.csv')
        >>> worker.run()
        SortWorker started - input queue: input_q, output queues: ['output_q']
    """
    
    def __init__(self, queue_in, queue_out, rabbitmq_host, input_file=None, output_file=None, **kwargs):
        super().__init__(queue_in, queue_out, rabbitmq_host, **kwargs)
        self.input_file = input_file
        self.output_file = output_file
        self.current_request_id = 0

    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
        """
        Process incoming message - typically notification signals that trigger file processing.
        
        Args:
            message (bytes): Raw message bytes.
            msg_type (int): Message type constant.
            data_type (int): Data type constant.
            request_id (int): Request identifier.
            position (int): Message position.
            payload (bytes): Message payload.
            queue_name (str, optional): Source queue name.
        
        Example:
            >>> worker._process_message(noti_msg, protocol.MSG_TYPE_NOTI,
            ...                         protocol.Q1_RESULT, 1, 1, 
            ...                         b'process_complete', None)
            Completion signal received with request_id 1. Starting file processing: /data/input.csv
            [Calls _process_file()]
            File processing complete
        """
        if msg_type == protocol.MSG_TYPE_NOTI:
            self.current_request_id = request_id
            logging.info(f'Completion signal received with request_id {request_id}. Starting file processing: {self.input_file}')
            self._process_file()
            logging.info('File processing complete')
        else:
            logging.warning(f"Received unexpected message type: {msg_type}/{data_type}")
    
    @abstractmethod
    def _process_file(self):
        """
        Process the input file. Must be implemented by subclasses.
        
        This method should read from self.input_file, perform processing,
        and write results to self.output_file or send messages to output queues.
        
        Example:
            Sort implementation:
            >>> def _process_file(self):
            ...     with open(self.input_file) as f:
            ...         lines = sorted(f.readlines())
            ...     def write_sorted(path):
            ...         with open(path, 'w') as f:
            ...             f.writelines(lines)
            ...     Worker.atomic_write(self.output_file, write_sorted)
        """
        pass


class SignalProcessingWorker(Worker):
    """
    Generic worker that reacts to notification signals and propagates completion signals.
    
    SignalProcessingWorker is designed for coordination tasks that react to NOTI signals,
    perform processing, and then notify downstream workers of completion. This pattern
    is useful for orchestrating multi-stage pipelines where stages need to wait for
    upstream completion before starting.
    
    Common use cases:
    - Coordinating file merge operations
    - Triggering reducers after all mappers complete
    - Orchestrating multi-stage aggregations
    - Checkpoint management
    
    Subclass Implementation:
        Subclasses must implement _process_signal() to define processing logic.
    
    Example:
        Coordination worker:
        >>> class CoordinatorWorker(SignalProcessingWorker):
        ...     def _process_signal(self, request_id):
        ...         logging.info(f"Coordinating request {request_id}")
        ...         # Perform coordination tasks
        ...         self._notify_completion(protocol.Q1_RESULT, request_id)
        >>> worker = CoordinatorWorker('coord_in', 'coord_out', 'rabbitmq')
        >>> worker.run()
        CoordinatorWorker started - input queue: coord_in, output queues: ['coord_out']
    """

    def _process_message(self, message, msg_type, data_type, request_id, position, payload, queue_name=None):
        """
        Handle incoming messages, primarily notification signals.
        
        When a NOTI signal is received, triggers the processing logic defined in
        _process_signal() and handles any errors during processing.
        
        Args:
            message (bytes): Raw message bytes.
            msg_type (int): Message type constant.
            data_type (int): Data type constant.
            request_id (int): Request identifier.
            position (int): Message position.
            payload (bytes): Message payload.
            queue_name (str, optional): Source queue name.
        
        Example:
            >>> worker._process_message(noti_msg, protocol.MSG_TYPE_NOTI,
            ...                         protocol.DATA_END, 1, 1, 
            ...                         b'trigger', None)
            [CoordinatorWorker] Notification received — starting processing.
            [Calls _process_signal(1)]
            [CoordinatorWorker] Processing complete. Completion signal sent.
        """
        if msg_type == protocol.MSG_TYPE_NOTI:
            logging.info(f"[{self.__class__.__name__}] Notification received — starting processing.")
            try:
                self._process_signal(request_id)
                logging.info(f"[{self.__class__.__name__}] Processing complete. Completion signal sent.")
            except Exception as e:
                logging.error(f"[{self.__class__.__name__}] Error during processing: {e}")
        else:
            logging.warning(f"[{self.__class__.__name__}] Unexpected message type: {msg_type}/{data_type}")

    @abstractmethod
    def _process_signal(self, request_id):
        """
        Core processing logic triggered when a notification signal is received.
        
        Must be implemented by subclasses to define the specific processing that occurs
        in response to the notification. Typically calls _notify_completion() when done.
        
        Args:
            request_id (int): Request identifier from the notification message.
        
        Example:
            >>> def _process_signal(self, request_id):
            ...     logging.info(f"Processing request {request_id}")
            ...     # Perform file merge, aggregation, or coordination
            ...     result_data = self.aggregate_results()
            ...     self.write_output(result_data)
            ...     self._notify_completion(protocol.Q1_RESULT, request_id)
        """
        pass

    def _notify_completion(self, data_type, request_id=0):
        """
        Send a notification message to downstream workers indicating processing completion.
        
        Creates and sends a NOTI message to all configured output queues, allowing
        downstream workers to know that processing for this stage is complete.
        
        Args:
            data_type (int): Data type constant for the notification.
            request_id (int, optional): Request identifier. Defaults to 0.
        
        Example:
            >>> worker._notify_completion(protocol.Q1_RESULT, request_id=1)
            [CoordinatorWorker] Completion notification sent to ['next_stage_queue'] with request_id=1
        """
        noti_payload = f"{self.__class__.__name__.lower()}_completed".encode("utf-8")
        noti_message = protocol.create_notification_message(data_type, noti_payload, request_id)
        for q in self.out_queues:
            q.send(noti_message)
        logging.info(f"[{self.__class__.__name__}] Completion notification sent to {[q.queue_name for q in self.out_queues]} with request_id={request_id}")
