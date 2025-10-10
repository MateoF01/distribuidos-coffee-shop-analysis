import socket
import threading
import time
import signal
import sys
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue
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
    def __init__(self, host, port, listen_backlog=1):
        self.host = host
        self.port = port
        self.listen_backlog = listen_backlog
        self.shutdown_flag = False
        self._setup_rabbitmq()
        self._setup_socket()
        self._setup_signal_handlers()

    def _setup_rabbitmq(self):
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        self.queues = {}
        for q in queue_names.values():
            self.queues[q] = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=q)
        self.results_queue = CoffeeMessageMiddlewareQueue(host=rabbitmq_host, queue_name=RESULTS_QUEUE)

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

    def run(self):
        logging.info(f'Server listening on {self.host}:{self.port}')
        
        while not self.shutdown_flag:
            try:
                conn, addr = self._accept_new_connection()
                if not self.shutdown_flag:
                    self._handle_client_connection(conn, addr)
            except OSError as e:
                if self.shutdown_flag:
                    break
                raise e

            if self.shutdown_flag:
                logging.info("Shutdown flag is set. Exiting...")
                break

    def _accept_new_connection(self):
        conn, addr = self._server_socket.accept()
        logging.info(f'Connected by {addr}')
        return conn, addr

    def _handle_client_connection(self, conn, addr):
        data_end_count = 0
        data_end_expected = 5  # Wait for 5 DATA_END signals before sending final DATA_END
        final_end_sent = False

        def on_result(message):
            nonlocal data_end_count, final_end_sent
            try:
                msg_type, data_type, timestamp, _ = protocol._unpack_message(message)
                
                if msg_type == protocol.MSG_TYPE_END and data_type == protocol.DATA_END:
                    # Count DATA_END signals but don't forward them yet
                    data_end_count += 1
                    logging.debug(f"DATA_END received ({data_end_count}/{data_end_expected})")
                    
                    if data_end_count == data_end_expected and not final_end_sent:
                        # Send final DATA_END signal to client
                        final_message = protocol.pack_message(protocol.MSG_TYPE_END, protocol.DATA_END, b"", time.time())
                        conn.sendall(final_message)
                        final_end_sent = True
                        logging.info("All queries completed. Sent final DATA_END to client.")
                        
                        # Close connection after a short delay
                        def close_connection():
                            time.sleep(0.5)  # Small delay to ensure message is received
                            try:
                                conn.shutdown(socket.SHUT_RDWR)
                            except OSError:
                                pass
                            conn.close()
                        
                        threading.Thread(target=close_connection, daemon=True).start()
                else:
                    # Forward all other messages (including query-specific END messages)
                    conn.sendall(message)
                    if msg_type == protocol.MSG_TYPE_END:
                        logging.debug(f"Forwarded END message with data_type={data_type}")
                        
            except (BrokenPipeError, OSError) as e:
                logging.warning(f"Cliente desconectado: {e}")
                try:
                    conn.close()
                except:
                    pass

        # Lanza hilo que escucha resultados
        threading.Thread(
            target=lambda: self.results_queue.start_consuming(on_result),
            daemon=True
        ).start()

        # Mientras tanto recibe del cliente y encola
        try:
            while True:
                msg_type, data_type, timestamp, payload = protocol.receive_message(conn)
                if not msg_type:   # cliente cerró
                    break

                # Use per-hop timestamps: generate new timestamp for this forwarding step
                message = protocol.pack_message(msg_type, data_type, payload, None)

                if msg_type == protocol.MSG_TYPE_DATA:
                    if data_type in queue_names:
                        self.queues[queue_names[data_type]].send(message)
                        logging.debug(f"Sent {data_type_names.get(data_type, data_type)} data to queue")
                    else:
                        logging.warning(f"Unknown data type: {data_type}")
                elif msg_type == protocol.MSG_TYPE_END:
                    if data_type == protocol.DATA_END:
                        # Special handling for DATA_END - send to all data queues
                        logging.info("Received DATA_END from client, broadcasting to all queues")
                        for queue_name in queue_names.values():
                            self.queues[queue_name].send(message)
                            logging.debug(f"Sent DATA_END to {queue_name}")
                    elif data_type in queue_names:
                        self.queues[queue_names[data_type]].send(message)
                        logging.debug(f"Sent END message for {data_type_names.get(data_type, data_type)} to queue")
                    else:
                        logging.warning(f"Unknown data type in END message: {data_type}")
                else:
                    logging.warning(f"Unknown message type: {msg_type}")
        except Exception as e:
            logging.error(f"Error in connection with {addr}: {type(e).__name__}: {e}")
            import traceback
            logging.debug(f"Traceback: {traceback.format_exc()}")
        finally:
            # 👇 no cerramos conn acá
            pass

    def close(self):
        for q in self.queues.values():
            q.close()
        self._server_socket.close()

if __name__ == '__main__':
    initialize_log(logging.INFO)
    server = Server(HOST, PORT)
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
