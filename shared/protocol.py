# shared/protocol.py
import struct

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

def send_message(conn, msg_type: int, data_type: int, payload: bytes):
    """
    - 1 byte: message type
    - 1 byte: data type
    - 4 bytes: payload len
    - N bytes: payload
    """
    header = struct.pack(">BBI", msg_type, data_type, len(payload))
    conn.sendall(header + payload)

def send_notification(conn, msg_type = MSG_TYPE_NOTI):
    header = struct.pack(">BBI", msg_type)
    conn.sendall(header)

def receive_message(conn):
    header = _read_full(conn, 6)  # 1 type msg + 1 type dato + 4 len
    msg_type, data_type, length = struct.unpack(">BBI", header)
    payload = _read_full(conn, length) if length > 0 else b""
    return msg_type, data_type, payload


def _read_full(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket closed before receiving expected data")
        buf += chunk
    return buf

def pack_message(msg_type, data_type, payload):
    header = struct.pack('>BBI', msg_type, data_type, len(payload))
    return header + payload

def _unpack_message(msg_bytes):
    header = msg_bytes[:6]
    msg_type, data_type, payload_len = struct.unpack('>BBI', header)
    payload = msg_bytes[6:]
    return msg_type, data_type, payload
