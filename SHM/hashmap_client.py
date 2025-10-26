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
    self.registered_maps = {}

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

  def register(self, map_name, state="WAITING"):
    """
    Registers this replica_id under the given hashmap on the SHM server.
    Only sends once per map unless forced.
    """
    if self.registered_maps.get(map_name):
      return "ALREADY_REGISTERED"

    msg = {
      "action": "register",
      "map_name": map_name,
      "replica_id": self.replica_id,
      "state": state
    }
    resp = self._send(msg)
    if resp == "OK":
      self.registered_maps[map_name] = True
    return resp

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
  
  def put_ready(self, map_name):
    msg = {"action": "put_ready", "map_name": map_name, "replica_id": self.replica_id}
    return self._send(msg)
  
  def get_ready(self, map_name):
    msg = {"action": "get_ready", "map_name": map_name}
    return self._send(msg)

  def change_state(self, map_name, state):
    """
    Explicitly set the replica's state for a given hashmap.
    Example:
      client.change_state("user_map", "PROCESSING")
    """
    msg = {
      "action": "change_state",
      "map_name": map_name,
      "replica_id": self.replica_id,
      "state": state
    }
    return self._send(msg)

  def last_map_is_not_ready(self, last_map_name, maps_names):
    last_map_msg = {"action": "get_ready", "map_name": last_map_name}
    if self._send(last_map_msg) == "OK":
      return "ALREADY_READY"
    for name in maps_names:
      if last_map_name == name:
        continue
      msg = {"action": "get_ready", "map_name": name}
      if self._send(msg) != "OK":
        return "MAPS_NOT_READY"
    return "LAST_MAP_NOT_READY"
