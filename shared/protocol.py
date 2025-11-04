import struct
import time

# Message Types
MSG_TYPE_DATA = 1
MSG_TYPE_END = 2
MSG_TYPE_NOTI = 3

# Data Types
DATA_TRANSACTIONS = 1
DATA_TRANSACTION_ITEMS = 2
DATA_MENU_ITEMS = 3
DATA_USERS = 4
DATA_STORES = 5
DATA_END = 6

Q1_RESULT = 7
Q2_RESULT_a = 8
Q2_RESULT_b = 9
Q3_RESULT = 10
Q4_RESULT = 11



# === SOCKET FUNCTIONS (client <-> gateway) ===

def send_message(conn, msg_type: int, data_type: int, payload: bytes, position: int, request_id: int = 0):
    """
    Send messages over a socket connection
    - 1 byte: message type
    - 1 byte: data type
    - 1 byte: request id
    - 8 bytes: position (double)
    - 4 bytes: payload len
    - N bytes: payload
    """

    header = struct.pack(">BBBQI", msg_type, data_type, request_id, position, len(payload))
    conn.sendall(header + payload)

def receive_message(conn):
    """
    Receive messages over a socket connection
    - 1 byte: message type
    - 1 byte: data type
    - 1 byte: request id
    - 8 bytes: position (double)
    - 4 bytes: payload len
    - N bytes: payload
    """
    header = _read_full(conn, 15)  # 1 msg_type + 1 data_type + 1 request_id + 8 position + 4 len
    msg_type, data_type, request_id, position, length = struct.unpack(">BBBQI", header)
    payload = _read_full(conn, length) if length > 0 else b""
    return msg_type, data_type, request_id, position, payload

def _read_full(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket closed before receiving expected data")
        buf += chunk
    return buf


# === RABBITMQ FUNCTIONS (gateway <-> workers) ===

def pack_message(msg_type, data_type, payload, request_id, position):
    """
    Pack a message for RabbitMQ or internal gateway communication.
    Header: 1 byte msg_type, 1 byte data_type, 1 byte request_id, 8 bytes position, 4 bytes payload_len
    """
    header = struct.pack('>BBBQI', msg_type, data_type, request_id, position, len(payload))
    return header + payload

def unpack_message(msg_bytes):
    """
    Unpack a message from RabbitMQ or internal gateway communication.
    Header: 1 byte msg_type, 1 byte data_type, 1 byte request_id, 8 bytes position, 4 bytes payload_len
    """
    header = msg_bytes[:15]
    msg_type, data_type, request_id, position, payload_len = struct.unpack('>BBBQI', header)
    payload = msg_bytes[15:]
    return msg_type, data_type, request_id, position, payload


# === STANDARDIZED MESSAGE CREATION FUNCTIONS ===

def create_data_message(data_type: int, payload: bytes, request_id: int = 0, position: int = 1):
    """
    Create a standardized data message.
    
    Args:
        data_type: Type of data (DATA_TRANSACTIONS, DATA_TRANSACTION_ITEMS, etc.)
        payload: Message payload as bytes
        request_id: Request identifier for tracking (default: 0)
        position: Message position (default: 1)
    
    Returns:
        bytes: Packed message ready to send through middleware
    """
    return pack_message(MSG_TYPE_DATA, data_type, payload, request_id, position)

def create_end_message(data_type: int, request_id: int = 0, position: int = 1):
    """
    Create a standardized end message (signals end of data stream).
    
    Args:
        data_type: Type of data that ended (DATA_TRANSACTIONS, DATA_END, etc.)
        request_id: Request identifier for tracking (default: 0)
        position: Message position (default: 1)
    
    Returns:
        bytes: Packed message ready to send through middleware
    """
    return pack_message(MSG_TYPE_END, data_type, b"", request_id, position)

def create_notification_message(data_type: int, payload: bytes = b"", request_id: int = 0, position: int = 1):
    """
    Create a standardized notification message.
    
    Args:
        data_type: Type of notification (DATA_END, Q1_RESULT, etc.)
        payload: Notification payload as bytes (default: empty)
        request_id: Request identifier for tracking (default: 0)
        position: Message position (default: 1)
    
    Returns:
        bytes: Packed message ready to send through middleware
    """
    return pack_message(MSG_TYPE_NOTI, data_type, payload, request_id, position)
