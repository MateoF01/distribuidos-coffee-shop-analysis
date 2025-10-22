import socket
import threading
import time
import signal
import multiprocessing
from multiprocessing import Value, Manager
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue, CoffeeMessageMiddlewareExchange
import os
from shared import protocol
import logging
from shared.logging_config import initialize_log


HOST = os.environ.get('GATEWAY_HOST', '0.0.0.0')
PORT = int(os.environ.get('GATEWAY_PORT', 5000))
RESULTS_QUEUE = os.environ.get('QUEUE_IN', 'results')


data_type_names = {
    protocol.DATA_TRANSACTIONS: 'transactions',
    protocol.DATA_TRANSACTION_ITEMS: 'transaction_items',
    protocol.DATA_MENU_ITEMS: 'menu_items',
    protocol.DATA_USERS: 'users',
    protocol.DATA_STORES: 'stores',
    protocol.DATA_END: 'end'
}

queue_names = {
    protocol.DATA_TRANSACTIONS: 'transactions_queue',
    protocol.DATA_TRANSACTION_ITEMS: 'transaction_items_queue',
    protocol.DATA_MENU_ITEMS: 'menu_items_queue',
    protocol.DATA_USERS: 'users_queue',
    protocol.DATA_STORES: 'stores_queue'
}

class Server:
    def __init__(self, host, port, listen_backlog=1, shared_request_id=None, shared_requests=None):
        self.host = host
        self.port = port
        self.listen_backlog = listen_backlog
        self.shutdown_flag = False
        self._setup_socket()
        self._setup_signal_handlers()
        
        # Use shared requests dict if provided, else create a local one
        if shared_requests is not None:
            self.requests = shared_requests
        else:
            self.requests = {}
        
        # Use shared request id counter if provided, else create one
        if shared_request_id is not None:
            self.next_request_id = shared_request_id
        else:
            self.next_request_id = Value('i', 1)

    def _setup_rabbitmq(self):
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        self.queues = {}
        for q in queue_names.values():
            self.queues[q] = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=q)
        self.results_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=RESULTS_QUEUE)
        
        # Exchange publishers for END messages to cleaners
        self.transactions_end_exchange = CoffeeMessageMiddlewareExchange(
            host=rabbitmq_host, 
            exchange_name='transactions_end_exchange', 
            route_keys=[]
        )
        self.transaction_items_end_exchange = CoffeeMessageMiddlewareExchange(
            host=rabbitmq_host, 
            exchange_name='transaction_items_end_exchange', 
            route_keys=[]
        )

    def _setup_socket(self):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(self.listen_backlog)

    def _setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown"""
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logging.info(f"Received signal {signum}, shutting down gateway gracefully...")
        self.shutdown_flag = True

    def _get_next_request_id(self):
        with self.next_request_id.get_lock():
            rid = self.next_request_id.value
            self.next_request_id.value += 1
        return rid

    def _result_router(self):
        """
        Dedicated thread running in the main process that consumes ALL results
        from the results queue and routes them to the correct client sockets.
        This is the ONLY consumer of the results_queue, ensuring messages are
        processed in order and preventing load-balancing across multiple consumers.
        No buffering - just forward messages directly as they arrive.
        """
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        results_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=RESULTS_QUEUE)
        
        data_end_counts = {}  # Track DATA_END counts per request_id
        completed_requests = set()  # Track which requests have been finalized
        data_end_expected = 5  # Wait for 5 DATA_END signals before sending final DATA_END
        
        # Logging/tracking
        message_log = {}  # track all messages per request_id for debugging
        
        def on_result(message):
            try:
                msg_type, data_type, request_id, timestamp, payload = protocol.unpack_message(message)
                
                # Initialize logging for this request_id
                if request_id not in message_log:
                    message_log[request_id] = {
                        'data_messages': {},  # data_type -> {'count': int, 'rows': int, 'bytes': int}
                        'end_messages': {},   # data_type -> count
                        'total_payload_bytes': 0,
                        'first_seen': time.time()
                    }
                
                # Log incoming message details
                if msg_type == protocol.MSG_TYPE_DATA:
                    # Count rows in payload (split by newlines)
                    payload_str = payload.decode('utf-8', errors='ignore')
                    row_count = len([row for row in payload_str.split('\n') if row.strip()])
                    
                    if data_type not in message_log[request_id]['data_messages']:
                        message_log[request_id]['data_messages'][data_type] = {
                            'count': 0,
                            'rows': 0,
                            'bytes': 0
                        }
                    
                    message_log[request_id]['data_messages'][data_type]['count'] += 1
                    message_log[request_id]['data_messages'][data_type]['rows'] += row_count
                    message_log[request_id]['data_messages'][data_type]['bytes'] += len(payload)
                    message_log[request_id]['total_payload_bytes'] += len(payload)
                    
                    logging.info(f"[GATEWAY ROUTER INCOMING] DATA: data_type={data_type}, request_id={request_id}, rows={row_count}, payload_size={len(payload)}, msg_count={message_log[request_id]['data_messages'][data_type]['count']}, total_rows_for_type={message_log[request_id]['data_messages'][data_type]['rows']}")
                
                elif msg_type == protocol.MSG_TYPE_END:
                    if data_type not in message_log[request_id]['end_messages']:
                        message_log[request_id]['end_messages'][data_type] = 0
                    message_log[request_id]['end_messages'][data_type] += 1
                    logging.info(f"[GATEWAY ROUTER INCOMING] END: data_type={data_type}, request_id={request_id}, end_count_for_this_type={message_log[request_id]['end_messages'][data_type]}")
                
                # Skip messages for already-completed requests
                if request_id in completed_requests:
                    logging.debug(f"[GATEWAY ROUTER] Skipping message for completed request_id={request_id}")
                    return
                
                # Look up target connection
                target_conn = self.requests.get(request_id)
                if target_conn is None:
                    logging.warning(f"[GATEWAY ROUTER] No socket for request_id={request_id}, dropping message")
                    return

                # Handle final DATA_END (aggregated signal from all workers)
                if msg_type == protocol.MSG_TYPE_END and data_type == protocol.DATA_END:
                    if request_id not in data_end_counts:
                        data_end_counts[request_id] = 0
                    data_end_counts[request_id] += 1
                    
                    logging.info(f"[GATEWAY ROUTER] DATA_END for request_id={request_id} ({data_end_counts[request_id]}/{data_end_expected})")
                    
                    # Build detailed summary
                    summary = f"[GATEWAY ROUTER] Summary for request_id={request_id}:\n"
                    summary += f"  Total payload bytes: {message_log[request_id]['total_payload_bytes']}\n"
                    summary += f"  Data messages by type:\n"
                    for dtype, stats in message_log[request_id]['data_messages'].items():
                        dtype_name = {
                            protocol.Q1_RESULT: 'Q1(7)', 
                            protocol.Q2_RESULT_a: 'Q2_A(8)',
                            protocol.Q2_RESULT_b: 'Q2_B(9)',
                            protocol.Q3_RESULT: 'Q3(10)',
                            protocol.Q4_RESULT: 'Q4(11)'
                        }.get(dtype, f'type{dtype}')
                        summary += f"    {dtype_name}: {stats['count']} messages, {stats['rows']} total rows, {stats['bytes']} bytes\n"
                    summary += f"  END messages by type: {message_log[request_id]['end_messages']}"
                    logging.info(summary)

                    if data_end_counts[request_id] == data_end_expected:
                        # Send final DATA_END to client
                        try:
                            protocol.send_message(target_conn, protocol.MSG_TYPE_END, protocol.DATA_END, b"", request_id=request_id)
                            logging.info(f"[GATEWAY ROUTER] Sent final DATA_END to client for request_id={request_id}")
                        except Exception as e:
                            logging.warning(f"[GATEWAY ROUTER] Failed to send final DATA_END: {e}")
                        
                        # Mark as completed and remove from tracking
                        completed_requests.add(request_id)
                        if request_id in self.requests:
                            del self.requests[request_id]
                        if request_id in data_end_counts:
                            del data_end_counts[request_id]
                        
                        # Log cleanup
                        elapsed = time.time() - message_log[request_id]['first_seen']
                        logging.info(f"[GATEWAY ROUTER] Request completed: request_id={request_id}, elapsed_seconds={elapsed:.2f}")
                        
                        # NOTE: Connection lifecycle is managed by the client.
                        # Gateway keeps the connection open for subsequent requests or graceful client-side close.
                        # Do NOT close the connection here - let the client control when to close.
                else:
                    # Forward all other messages directly to client (no buffering)
                    try:
                        protocol.send_message(target_conn, msg_type, data_type, payload, request_id=request_id)
                        if msg_type == protocol.MSG_TYPE_END:
                            logging.info(f"[GATEWAY ROUTER] Forwarded END: data_type={data_type}, request_id={request_id}")
                        elif msg_type == protocol.MSG_TYPE_DATA:
                            logging.info(f"[GATEWAY ROUTER] Forwarded DATA: data_type={data_type}, request_id={request_id}, size={len(payload)}")
                    except Exception as e:
                        logging.warning(f"[GATEWAY ROUTER] Error forwarding message: {e}")

            except Exception as e:
                logging.error(f"[GATEWAY ROUTER] Exception: {e}")
                import traceback
                logging.debug(traceback.format_exc())

        logging.info("[GATEWAY ROUTER] Starting result consumer thread")
        results_queue.start_consuming(on_result)

    def run(self):
        logging.info(f'Server listening on {self.host}:{self.port}')
        self._setup_rabbitmq()
        
        # Start the result router thread FIRST (only consumer of results_queue)
        router_thread = threading.Thread(target=self._result_router, daemon=True)
        router_thread.start()
        time.sleep(1)  # Give router time to connect to RabbitMQ
        logging.info("Result router thread started")
        
        max_processes = int(os.environ.get('GATEWAY_MAX_PROCESSES', 4))
        processes = []
        while not self.shutdown_flag:
            # Clean up finished processes
            processes = [p for p in processes if p.is_alive()]
            if len(processes) < max_processes:
                try:
                    conn, addr = self._accept_new_connection()
                    if not self.shutdown_flag:
                        p = multiprocessing.Process(target=self._handle_client_connection, args=(conn, addr))
                        p.daemon = True
                        p.start()
                        processes.append(p)
                except OSError as e:
                    if self.shutdown_flag:
                        break
                    raise e
            else:
                time.sleep(0.1)  # Wait before checking again
            if self.shutdown_flag:
                logging.info("Shutdown flag is set. Exiting...")
                break
        for p in processes:
            p.join()

    def _accept_new_connection(self):
        conn, addr = self._server_socket.accept()
        logging.info(f'Connected by {addr}')
        return conn, addr

    def _handle_client_connection(self, conn, addr):
        # Setup RabbitMQ objects for this process (queues only, NOT results_queue)
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        queues = {q: CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=q) for q in queue_names.values()}
        transactions_end_exchange = CoffeeMessageMiddlewareExchange(
            host=rabbitmq_host, exchange_name='transactions_end_exchange', route_keys=[])
        transaction_items_end_exchange = CoffeeMessageMiddlewareExchange(
            host=rabbitmq_host, exchange_name='transaction_items_end_exchange', route_keys=[])

        # Main loop: receive client requests and track them by request_id
        try:
            # Track which data_types have received DATA_END for this connection
            data_end_received = set()
            current_request_id = self._get_next_request_id()
            self.requests[current_request_id] = conn
            logging.info(f"Client {addr} new request_id: {current_request_id}")

            while True:
                msg_type, data_type, request_id, timestamp, payload = protocol.receive_message(conn)
                if not msg_type:
                    break

                # Detect start of a new request: receiving DATA for a data_type that already received DATA_END
                if msg_type == protocol.MSG_TYPE_DATA and data_type in data_end_received:
                    # New request detected
                    current_request_id = self._get_next_request_id()
                    self.requests[current_request_id] = conn
                    data_end_received.clear()
                    logging.info(f"Client {addr} new request_id: {current_request_id} (new batch detected)")

                # Use per-hop timestamps: generate new timestamp for this forwarding step
                if msg_type == protocol.MSG_TYPE_DATA:
                    message = protocol.create_data_message(data_type, payload, current_request_id)
                elif msg_type == protocol.MSG_TYPE_END:
                    message = protocol.create_end_message(data_type, current_request_id)
                else:
                    message = protocol.create_notification_message(data_type, payload, current_request_id)

                if msg_type == protocol.MSG_TYPE_DATA:
                    if data_type in queue_names:
                        queues[queue_names[data_type]].send(message)
                        logging.debug(f"Sent {data_type_names.get(data_type, data_type)} data to queue with request_id={current_request_id}")
                    else:
                        logging.warning(f"Unknown data type: {data_type}")
                elif msg_type == protocol.MSG_TYPE_END:
                    if data_type == protocol.DATA_END:
                        # Special handling for DATA_END - send to all data queues and exchanges
                        logging.info("Received DATA_END from client, broadcasting to all queues and exchanges")
                        for queue_name in queue_names.values():
                            queues[queue_name].send(message)
                            logging.debug(f"Sent DATA_END to {queue_name} with request_id={current_request_id}")
                        # Also broadcast DATA_END to exchanges for cleaners
                        transactions_end_exchange.send(message)
                        transaction_items_end_exchange.send(message)
                        logging.debug("Sent DATA_END to cleaner exchanges")
                        # Mark all data_types as ended for this batch
                        data_end_received.update(queue_names.keys())
                    elif data_type in queue_names:
                        queues[queue_names[data_type]].send(message)
                        logging.debug(f"Sent END message for {data_type_names.get(data_type, data_type)} to queue with request_id={current_request_id}")

                        # Additionally send END messages to exchanges for transactions and transaction_items
                        if data_type == protocol.DATA_TRANSACTIONS:
                            transactions_end_exchange.send(message)
                            logging.debug(f"Sent END message for transactions to exchange")
                        elif data_type == protocol.DATA_TRANSACTION_ITEMS:
                            transaction_items_end_exchange.send(message)
                            logging.debug(f"Sent END message for transaction_items to exchange")

                        # Mark this data_type as ended for this batch
                        data_end_received.add(data_type)
                    else:
                        logging.warning(f"Unknown data type in END message: {data_type}")
                else:
                    logging.warning(f"Unknown message type: {msg_type}")
        except Exception as e:
            logging.error(f"Error in connection with {addr}: {type(e).__name__}: {e}")
            import traceback
            logging.debug(f"Traceback: {traceback.format_exc()}")
        finally:
            # Clean up all requests for this connection
            to_remove = [rid for rid, c in self.requests.items() if c == conn]
            for rid in to_remove:
                del self.requests[rid]
            logging.info(f"Cleaned up mappings for client {addr}")

    def close(self):
        for q in self.queues.values():
            q.close()
        self._server_socket.close()

if __name__ == '__main__':
    initialize_log(logging.INFO)
    
    # Create a manager for shared data structures
    manager = Manager()
    shared_requests = manager.dict()
    
    # Create a shared request id counter for all processes
    shared_request_id = Value('i', 1)
    
    server = Server(HOST, PORT, shared_request_id=shared_request_id, shared_requests=shared_requests)
    try:
        server.run()
    except KeyboardInterrupt:
        logging.info('Shutting down server.')
    except Exception as e:
        logging.error(f'Server error: {e}')
    finally:
        logging.info('Gateway shutting down...')
        server.close()
        logging.info('Gateway shutdown complete.')
