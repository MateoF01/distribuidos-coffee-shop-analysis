import socket
import threading
import time
import signal
import sys
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue
import os
from shared import protocol


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
        print(f"Received signal {signum}, shutting down gateway gracefully...")
        self.shutdown_flag = True

    def run(self):
        print(f'Server listening on {self.host}:{self.port}')
        self._server_socket.settimeout(1.0)  # Add timeout to check shutdown flag
        
        while not self.shutdown_flag:
            try:
                conn, addr = self._accept_new_connection()
                if not self.shutdown_flag:
                    self._handle_client_connection(conn, addr)
            except socket.timeout:
                # Timeout allows us to check shutdown_flag periodically
                continue
            except OSError as e:
                if self.shutdown_flag:
                    break
                raise e

            if self.shutdown_flag:
                print("Shutdown flag is set. Exiting...")
                break

    def _accept_new_connection(self):
        conn, addr = self._server_socket.accept()
        print(f'Connected by {addr}')
        return conn, addr

    def _handle_client_connection(self, conn, addr):
        data_end_count = 0
        data_end_expected = 5  # Wait for 5 DATA_END signals before sending final DATA_END
        final_end_sent = False

        def on_result(message):
            nonlocal data_end_count, final_end_sent
            try:
                msg_type, data_type, _ = protocol._unpack_message(message)
                
                if msg_type == protocol.MSG_TYPE_END and data_type == protocol.DATA_END:
                    # Count DATA_END signals but don't forward them yet
                    data_end_count += 1
                    print(f"DATA_END received ({data_end_count}/{data_end_expected})")
                    
                    if data_end_count == data_end_expected and not final_end_sent:
                        # Send final DATA_END signal to client
                        final_message = protocol.pack_message(protocol.MSG_TYPE_END, protocol.DATA_END, b"")
                        conn.sendall(final_message)
                        final_end_sent = True
                        print("All queries completed. Sent final DATA_END to client.")
                        
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
                        print(f"Forwarded END message with data_type={data_type}")
                        
            except (BrokenPipeError, OSError) as e:
                print(f"[WARN] Cliente desconectado: {e}")
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
                msg_type, data_type, payload = protocol.receive_message(conn)
                if not msg_type:   # cliente cerró
                    break

                message = protocol.pack_message(msg_type, data_type, payload)

                if msg_type == protocol.MSG_TYPE_DATA:
                    self.queues[queue_names[data_type]].send(message)
                elif msg_type == protocol.MSG_TYPE_END:
                    self.queues[queue_names[data_type]].send(message)
                else:
                    print(f"Unknown message type: {msg_type}")
        except Exception as e:
            print(f"Error en la conexión con {addr}: {e}")
        finally:
            # 👇 no cerramos conn acá
            pass

    def close(self):
        for q in self.queues.values():
            q.close()
        self._server_socket.close()

if __name__ == '__main__':
    server = Server(HOST, PORT)
    try:
        server.run()
    except KeyboardInterrupt:
        print('Shutting down server.')
    except Exception as e:
        print(f'Server error: {e}')
    finally:
        print('Gateway shutting down...')
        server.close()
        print('Gateway shutdown complete.')
