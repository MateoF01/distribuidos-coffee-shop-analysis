import socket
import threading
import time
import signal
import multiprocessing
from multiprocessing import Value
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
    def __init__(self, host, port, listen_backlog=1, shared_request_id=None):
        self.host = host
        self.port = port
        self.listen_backlog = listen_backlog
        self.shutdown_flag = False
        # self._setup_rabbitmq()  # Moved to per-client handler
        self._setup_socket()
        self._setup_signal_handlers()
        self.requests = {}  # {request_id: conn}
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

    def run(self):
        logging.info(f'Server listening on {self.host}:{self.port}')
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
        # Setup RabbitMQ objects for this process
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        queues = {q: CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=q) for q in queue_names.values()}
        results_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=RESULTS_QUEUE)
        transactions_end_exchange = CoffeeMessageMiddlewareExchange(
            host=rabbitmq_host, exchange_name='transactions_end_exchange', route_keys=[])
        transaction_items_end_exchange = CoffeeMessageMiddlewareExchange(
            host=rabbitmq_host, exchange_name='transaction_items_end_exchange', route_keys=[])

        data_end_count = 0
        data_end_expected = 5  # Wait for 5 DATA_END signals before sending final DATA_END
        final_end_sent = False

        def on_result(message):
            nonlocal data_end_count, final_end_sent
            try:
                msg_type, data_type, request_id, timestamp, payload = protocol.unpack_message(message)
                
                # Log all received messages with request_id for debugging
                if msg_type == protocol.MSG_TYPE_DATA:
                    logging.info(f"[GATEWAY] Received DATA message: data_type={data_type}, request_id={request_id}, payload_size={len(payload)}")
                elif msg_type == protocol.MSG_TYPE_END:
                    logging.info(f"[GATEWAY] Received END message: data_type={data_type}, request_id={request_id}")
                else:
                    logging.info(f"[GATEWAY] Received message: msg_type={msg_type}, data_type={data_type}, request_id={request_id}")
                
                target_conn = self.requests.get(request_id)
                if target_conn is None:
                    logging.warning(f"[GATEWAY] No connection found for request_id={request_id}, dropping message")
                    return

                if msg_type == protocol.MSG_TYPE_END and data_type == protocol.DATA_END:
                    # Count DATA_END signals but don't forward them yet
                    data_end_count += 1
                    logging.info(f"[GATEWAY] DATA_END received for request_id={request_id} ({data_end_count}/{data_end_expected})")

                    if data_end_count == data_end_expected and not final_end_sent:
                        # Send final DATA_END signal to client
                        protocol.send_message(target_conn, protocol.MSG_TYPE_END, protocol.DATA_END, b"")
                        final_end_sent = True
                        logging.info("All queries completed. Sent final DATA_END to client.")

                        # Clean up all requests for this connection
                        to_remove = [rid for rid, c in self.requests.items() if c == target_conn]
                        for rid in to_remove:
                            del self.requests[rid]

                        # Close connection after a short delay
                        def close_connection():
                            time.sleep(0.5)
                            try:
                                conn.shutdown(socket.SHUT_RDWR)
                            except OSError:
                                pass
                            conn.close()

                        threading.Thread(target=close_connection, daemon=True).start()
                else:
                    # Forward all other messages to client
                    protocol.send_message(target_conn, msg_type, data_type, payload)
                    if msg_type == protocol.MSG_TYPE_END:
                        logging.info(f"[GATEWAY] Forwarded END message with data_type={data_type} for request_id={request_id}")
                    elif msg_type == protocol.MSG_TYPE_DATA:
                        logging.info(f"[GATEWAY] Forwarded DATA message with data_type={data_type} for request_id={request_id}, payload_size={len(payload)}")

            except (BrokenPipeError, OSError) as e:
                logging.warning(f"Cliente desconectado: {e}")
                try:
                    conn.close()
                except:
                    pass

        # Start thread to consume results and route them
        threading.Thread(
            target=lambda: results_queue.start_consuming(on_result),
            daemon=True
        ).start()

        # Main loop: receive client requests and track them by request_id
        try:
            # Track which data_types have received DATA_END for this connection
            data_end_received = set()
            current_request_id = self._get_next_request_id()
            self.requests[current_request_id] = conn
            logging.info(f"Client {addr} new request_id: {current_request_id}")

            while True:
                msg_type, data_type, timestamp, payload = protocol.receive_message(conn)
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
    # Create a shared request id counter for all processes
    shared_request_id = Value('i', 1)
    server = Server(HOST, PORT, shared_request_id=shared_request_id)
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
