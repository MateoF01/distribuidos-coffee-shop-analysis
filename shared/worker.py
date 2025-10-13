import os
import signal
import sys
import threading
import logging
from abc import ABC, abstractmethod
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue
from shared import protocol
from shared.logging_config import initialize_log


class Worker(ABC):
    """Base class for all worker components in the coffee shop analysis system.
    
    Provides common functionality for:
    - Queue initialization and management (single/multiple input and output queues)
    - Message loop handling
    - Graceful shutdown with signal handling
    - Logging setup
    - Threading management
    """
    
    def __init__(self, queue_in, queue_out, rabbitmq_host, multiple_input_queues=None, **kwargs):
        """Initialize the worker with queue configuration.
        
        Args:
            queue_in: Single input queue name or None if using multiple_input_queues
            queue_out: Single output queue name, list of queue names, or comma-separated string
            rabbitmq_host: RabbitMQ host address
            multiple_input_queues: List of input queue names for workers that consume from multiple queues
            **kwargs: Additional worker-specific parameters
        """
        self.rabbitmq_host = rabbitmq_host
        self._running = False
        self._shutdown_event = threading.Event()
        self._lock = threading.Lock()
        
        # Handle multiple input queues (for joiners that need to consume from multiple sources)
        if multiple_input_queues:
            self.multiple_input_queues = multiple_input_queues
            self.in_queues = {
                queue_name: CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_name)
                for queue_name in multiple_input_queues
            }
            self.single_input_queue = None
            self.queue_in = None
        else:
            # Single input queue
            self.queue_in = queue_in
            self.single_input_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_in) if queue_in else None
            self.in_queues = {}
            self.multiple_input_queues = []
        
        # Handle multiple output queues
        self.out_queues = self._initialize_output_queues(queue_out)
        
        # Store kwargs for subclass-specific initialization
        self.worker_kwargs = kwargs
    
    def _initialize_output_queues(self, queue_out):
        """Initialize output queues from various input formats."""
        if not queue_out:
            return []
        
        # Handle comma-separated string
        if isinstance(queue_out, str):
            queue_names = [q.strip() for q in queue_out.split(',')]
        elif isinstance(queue_out, list):
            queue_names = queue_out
        else:
            queue_names = [str(queue_out)]
        
        return [CoffeeMessageMiddlewareQueue(host=self.rabbitmq_host, queue_name=q) 
                for q in queue_names if q]
    
    def run(self):
        """Main execution loop. Sets up message consumption and waits for shutdown."""
        self._running = True
        
        # Set up message consumers
        if self.single_input_queue:
            # Single input queue
            self.single_input_queue.start_consuming(self._create_message_handler())
        else:
            # Multiple input queues
            for queue_name, queue in self.in_queues.items():
                handler = self._create_message_handler(queue_name)
                queue.start_consuming(handler)
        
        # Log startup information
        self._log_startup_info()
        
        # Keep the main thread alive - wait indefinitely until shutdown is signaled
        try:
            self._shutdown_event.wait()
        except KeyboardInterrupt:
            logging.info("Keyboard interrupt received, shutting down...")
            self.stop()
    
    def _create_message_handler(self, queue_name=None):
        """Create a message handler function for the given queue."""
        def on_message(message):
            if not self._running:
                return
            
            try:
                # Validate message format
                if not self._validate_message(message):
                    return
                
                # Parse message using protocol helpers
                msg_type, data_type, timestamp, payload = protocol._unpack_message(message)
                
                # Handle end-of-data signals
                if msg_type == protocol.MSG_TYPE_END:
                    self._handle_end_signal(message, msg_type, data_type, queue_name)
                    return
                
                # Process the message content
                self._process_message(message, msg_type, data_type, timestamp, payload, queue_name)
                
            except Exception as e:
                logging.error(f"Error processing message: {e} - Message: {message}")
        
        return on_message
    
    def _validate_message(self, message):
        """Validate message format."""
        if not isinstance(message, bytes) or len(message) < 6:
            logging.warning(f"Invalid message format or too short: {message}")
            return False
        return True
    
    def _handle_end_signal(self, message, msg_type, data_type, queue_name=None):
        """Handle end-of-data signals. Default implementation forwards to all output queues."""
        for q in self.out_queues:
            q.send(message)
        
        if data_type == protocol.DATA_END:
            logging.info(f'End-of-data signal received from queue: {queue_name or self.queue_in}')
            # Don't stop automatically - let subclasses decide
        else:
            queue_names = [q.queue_name for q in self.out_queues]
            logging.debug(f"Forwarded end signal (type:{msg_type}) to {queue_names}")
    
    def _log_startup_info(self):
        """Log information about the worker startup."""
        if self.single_input_queue:
            input_info = f"input queue: {self.queue_in}"
        else:
            input_info = f"multiple input queues: {list(self.in_queues.keys())}"
        
        output_info = f"output queues: {[q.queue_name for q in self.out_queues]}"
        worker_type = self.__class__.__name__
        logging.info(f"{worker_type} started - {input_info}, {output_info}")
    
    @abstractmethod
    def _process_message(self, message, msg_type, data_type, timestamp, payload, queue_name=None):
        """Process a message. Must be implemented by subclasses.
        
        Args:
            message: Raw message bytes
            msg_type: Message type from protocol
            data_type: Data type from protocol
            timestamp: Message timestamp from protocol  
            payload: Message payload bytes
            queue_name: Name of the queue the message came from (for multi-queue workers)
        """
        pass
    
    def stop(self):
        """Stop the worker gracefully."""
        logging.info(f"Stopping {self.__class__.__name__}...")
        self._running = False
        self._shutdown_event.set()
        
        # Stop consuming from queues
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
        """Close all queue connections."""
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
        """Set up signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logging.info(f'Received signal {signum}, shutting down {cls.__name__} gracefully...')
            worker_instance.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    @classmethod
    def setup_logging(cls, config_path=None):
        """Set up logging configuration."""
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
        """Main entry point template for worker processes.
        
        Args:
            worker_factory: Function that creates and returns a worker instance
            config_path: Optional path to config file for logging setup
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
    """Specialized worker for stream processing (filtering, transforming rows).
    
    Handles common patterns for workers that process rows of data and forward them.
    """
    
    def _process_message(self, message, msg_type, data_type, timestamp, payload, queue_name=None):
        """Process message by extracting rows, processing them, and forwarding results."""
        try:
            # Decode payload
            payload_str = payload.decode('utf-8')
            rows = payload_str.split('\n')
            
            # Process rows (implemented by subclasses)
            processed_rows = self._process_rows(rows, queue_name)
            
            # Send processed rows
            self._send_processed_rows(processed_rows, msg_type, data_type, timestamp)
            
        except Exception as e:
            logging.error(f"Failed to decode or process payload: {e}")
    
    @abstractmethod
    def _process_rows(self, rows, queue_name=None):
        """Process a list of rows and return processed results.
        
        Args:
            rows: List of row strings
            queue_name: Source queue name (for multi-queue workers)
            
        Returns:
            Processed rows (format depends on implementation)
        """
        pass
    
    def _send_processed_rows(self, processed_rows, msg_type, data_type, timestamp):
        """Send processed rows to output queues.
        
        Args:
            processed_rows: Results from _process_rows()
            msg_type: Original message type
            data_type: Original data type
        """
        if not processed_rows:
            logging.debug("No rows after processing, nothing sent.")
            return
        
        # Default implementation: send to all output queues
        # Can be overridden for more complex routing
        if isinstance(processed_rows, list) and all(isinstance(row, str) for row in processed_rows):
            # Simple list of strings
            self._send_rows_to_all_queues(processed_rows, msg_type, data_type, timestamp)
        else:
            # Complex processing results - delegate to subclass
            self._send_complex_results(processed_rows, msg_type, data_type, timestamp)
    
    def _send_rows_to_all_queues(self, rows, msg_type, data_type, timestamp):
        """Send a list of row strings to all output queues."""
        new_payload_str = '\n'.join(rows)
        new_payload = new_payload_str.encode('utf-8')
        # Use per-hop timestamps: generate new timestamp for this forwarding step
        new_message = protocol.pack_message(msg_type, data_type, new_payload, None)
        
        for q in self.out_queues:
            q.send(new_message)
        
        queue_names = [q.queue_name for q in self.out_queues]
        logging.debug(f"Sent processed message to {queue_names} (rows: {len(rows)}, msg_type: {msg_type}, data_type: {data_type})")
    
    def _send_complex_results(self, processed_rows, msg_type, data_type, timestamp):
        """Handle complex processing results. Override in subclasses as needed."""
        logging.warning("Complex processing results not handled by default implementation")


class FileProcessingWorker(Worker):
    """Specialized worker for file-based processing.
    
    Handles workers that primarily work with files and send completion signals.
    """
    
    def __init__(self, queue_in, queue_out, rabbitmq_host, input_file=None, output_file=None, **kwargs):
        super().__init__(queue_in, queue_out, rabbitmq_host, **kwargs)
        self.input_file = input_file
        self.output_file = output_file
    
    def _process_message(self, message, msg_type, data_type, timestamp, payload, queue_name=None):
        """Process message - typically handles completion signals."""
        if msg_type == protocol.MSG_TYPE_NOTI:
            logging.info(f'Completion signal received. Starting file processing: {self.input_file}')
            self._process_file()
            logging.info('File processing complete')
        else:
            logging.warning(f"Received unexpected message type: {msg_type}/{data_type}")
    
    @abstractmethod
    def _process_file(self):
        """Process the input file. Must be implemented by subclasses."""
        pass


class SignalProcessingWorker(Worker):
    """
    Generic worker that reacts to a NOTI (notification) signal and sends another
    NOTI signal upon completion.
    """

    def _process_message(self, message, msg_type, data_type, timestamp, payload, queue_name=None):
        """
        Handle incoming messages. When a NOTI signal is received, perform the
        main processing and then notify downstream workers upon completion.
        """
        if msg_type == protocol.MSG_TYPE_NOTI:
            logging.info(f"[{self.__class__.__name__}] Notification received — starting processing.")
            try:
                self._process_signal()
                logging.info(f"[{self.__class__.__name__}] Processing complete. Completion signal sent.")
            except Exception as e:
                logging.error(f"[{self.__class__.__name__}] Error during processing: {e}")
        else:
            logging.warning(f"[{self.__class__.__name__}] Unexpected message type: {msg_type}/{data_type}")

    @abstractmethod
    def _process_signal(self):
        """
        Core logic triggered when a NOTI signal is received.
        Should be implemented by subclasses (e.g., file merge, reducer logic, etc.).
        """
        pass

    def _notify_completion(self, data_type):
        """
        Send a NOTI message to downstream workers indicating that the processing is complete.
        """
        noti_payload = f"{self.__class__.__name__.lower()}_completed".encode("utf-8")
        noti_message = protocol.pack_message(protocol.MSG_TYPE_NOTI, data_type, noti_payload)
        for q in self.out_queues:
            q.send(noti_message)
        logging.info(f"[{self.__class__.__name__}] Completion notification sent to {[q.queue_name for q in self.out_queues]}")
