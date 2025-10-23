import socket
import json
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class SharedHashmapClient:
  def __init__(self, replica_id, host="shm", port=9100, retry_interval=2):
    self.replica_id = replica_id
    self.host = host
    self.port = port
    self.retry_interval = retry_interval

  def _send(self, msg):
    for attempt in range(3):
      try:
        with socket.create_connection((self.host, self.port), timeout=5) as sock:
          sock.sendall(json.dumps(msg).encode())
          data = sock.recv(4096)
          if not data:
            raise ConnectionError("No response from SHM")
          return json.loads(data.decode()).get("response")
      except (ConnectionRefusedError, TimeoutError, OSError) as e:
        logging.warning(f"[SHMClient] Attempt {attempt+1}: {e}")
        time.sleep(self.retry_interval)
    return "ERROR"

  def lock(self, map_name):
    msg = {"action": "lock_hashmap", "map_name": map_name, "replica_id": self.replica_id}
    return self._send(msg)

  def unlock(self, map_name):
    msg = {"action": "unlock_hashmap", "map_name": map_name, "replica_id": self.replica_id}
    return self._send(msg)

  def get_version(self, map_name):
    msg = {"action": "get_version", "map_name": map_name}
    resp = self._send(msg)
    try:
      return int(resp)
    except (ValueError, TypeError):
      return -1
