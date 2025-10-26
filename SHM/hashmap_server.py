import socket
import threading
import json
import os
import time
import logging

HOST = os.environ.get("HM_HOST", "0.0.0.0")
PORT = int(os.environ.get("HM_PORT", "9100"))
STATE_FILE = os.environ.get("HM_STATE_PATH", "/app/output/hashmap_state.json")

os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class SharedHashmapManager:
  def __init__(self, state_file=STATE_FILE, lock_ttl=30):
    self.state_file = state_file
    self.lock_ttl = lock_ttl  # seconds before lock expires
    self.lock = threading.Lock()
    self.hashmaps = self._load_state()
    logging.info(f"[SHM] Archivo de estado: {self.state_file}")

  # -------------------------------
  # 🔄 Persistencia
  # -------------------------------
  def _load_state(self):
    if os.path.exists(self.state_file):
      try:
        with open(self.state_file, "r", encoding="utf-8") as f:
          return json.load(f)
      except Exception as e:
        logging.error(f"[SHM] Error cargando estado: {e}")
    return {}

  def _save_state(self):
    try:
      with open(self.state_file, "w", encoding="utf-8") as f:
        json.dump(self.hashmaps, f, indent=2, ensure_ascii=False)
    except Exception as e:
      logging.error(f"[SHM] Error guardando estado: {e}")

  # -------------------------------
  # 🧱 Helper
  # -------------------------------
  def _ensure_entry(self, name):
    if name not in self.hashmaps:
      self.hashmaps[name] = {
        "locked": False,
        "owner": None,
        "version": 0,
        "locked_at": 0,
        "ready": False,
        "replicas": {}
      }

  def _cleanup_expired_locks(self):
    now = time.time()
    expired = []
    for name, meta in self.hashmaps.items():
      if meta.get("locked") and now - meta.get("locked_at", 0) > self.lock_ttl:
        expired.append(name)
    for name in expired:
      logging.warning(f"[SHM] Liberando lock expirado en '{name}'")
      rid = self.hashmaps[name].get("owner")
      self.hashmaps[name]["locked"] = False
      self.hashmaps[name]["owner"] = None
      self.hashmaps[name]["locked_at"] = 0
      if rid and rid in self.hashmaps[name]["replicas"]:
        self.hashmaps[name]["replicas"][rid]["state"] = "WAITING"
      self._save_state()

  # -------------------------------
  # 🧱 Operaciones básicas
  # -------------------------------

  def register_replica(self, name, replica_id, state="WAITING"):
    """Ensure the replica_id is present in the hashmap's replicas dictionary."""
    with self.lock:
      self._ensure_entry(name)
      replicas = self.hashmaps[name]["replicas"]
      logging.info(f"[SHM] Registro de réplica {replica_id} en '{name}': {replicas}")
      if replica_id not in replicas:
        replicas[replica_id] = {"state": state}
        self._save_state()
        logging.info(f"[SHM] Registrada réplica {replica_id} en '{name}' como {state}")
      return "OK"

  def change_state(self, name, replica_id, state):
    """
    Explicitly update replica state (e.g., WAITING or PROCESSING)
    """
    with self.lock:
      self._ensure_entry(name)
      replicas = self.hashmaps[name]["replicas"]
      replicas[replica_id] = {"state": state}
      self._save_state()
      logging.info(f"[SHM] {name}:{replica_id} → {state}")
      return "OK"

  def any_processing(self, name):
    """Return True if any replica is still PROCESSING this hashmap."""
    self._ensure_entry(name)
    for rid, info in self.hashmaps[name]["replicas"].items():
      if info.get("state") == "PROCESSING":
        return True
    return False

  def lock_hashmap(self, name, replica_id):
    with self.lock:
      self._cleanup_expired_locks()
      self._ensure_entry(name)

      entry = self.hashmaps[name]
      if entry["locked"] and entry["owner"] != replica_id:
        logging.debug(f"[SHM] {replica_id} espera lock de '{name}'")
        return "WAIT"

      entry["locked"] = True
      entry["owner"] = replica_id
      entry["locked_at"] = time.time()
      self._save_state()
      logging.info(f"[SHM] {replica_id} obtuvo lock de '{name}'")
      return "OK"

  def unlock_hashmap(self, name, replica_id):
    with self.lock:
      self._ensure_entry(name)
      entry = self.hashmaps[name]
      if not entry or entry["owner"] != replica_id:
        return "ERROR"
      entry["locked"] = False
      entry["owner"] = None
      entry["version"] = entry.get("version", 0) + 1
      entry["locked_at"] = 0
      self._save_state()
      logging.info(f"[SHM] {replica_id} liberó lock de '{name}' (v{entry['version']})")
      return "OK"

  def put_ready(self, name, replica_id):
    with self.lock:
      self._cleanup_expired_locks()
      entry = self.hashmaps.get(name)
      if not entry:
        return "ERROR"
      if entry["locked"] or self.any_processing(name):
        return "WAIT"
      entry["ready"] = True
      self._save_state()
      logging.info(f"[SHM] {replica_id} marcó '{name}' como listo (v{entry['version']})")
      return "OK"
  
  def get_ready(self, name):
    return self.hashmaps.get(name, {}).get("ready", False)

  def get_version(self, name):
    return self.hashmaps.get(name, {}).get("version", 0)


# -------------------------------
# ⚡ Servidor TCP multicliente
# -------------------------------
class HashmapServer:
  def __init__(self, host=HOST, port=PORT):
    self.host = host
    self.port = port
    self.manager = SharedHashmapManager()

  def start(self):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((self.host, self.port))
    server_socket.listen(100)
    logging.info(f"[SHM] Servidor escuchando en {self.host}:{self.port}")

    while True:
      conn, addr = server_socket.accept()
      threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()

  def handle_client(self, conn, addr):
    try:
      data = conn.recv(4096)
      if not data:
        return
      msg = json.loads(data.decode())
      action = msg.get("action")
      response = self._handle_action(action, msg)
      conn.sendall(json.dumps({"response": response}).encode())
    except Exception as e:
      logging.error(f"[SHM] Error con cliente {addr}: {e}")
    finally:
      conn.close()

  def _handle_action(self, action, msg):
    name = msg.get("map_name")
    replica_id = msg.get("replica_id")

    if action == "register":
      return self.manager.register_replica(name, replica_id, msg["state"])
    elif action == "lock_hashmap":
      return self.manager.lock_hashmap(name, replica_id)
    elif action == "unlock_hashmap":
      return self.manager.unlock_hashmap(name, replica_id)
    elif action == "get_version":
      return str(self.manager.get_version(name))
    elif action == "put_ready":
      return self.manager.put_ready(name, replica_id)
    elif action == "get_ready":
      return "OK" if self.manager.get_ready(name) else "WAIT"
    elif action == "change_state":
      return self.manager.change_state(name, replica_id, msg["state"])
    else:
      return "ERROR: unknown action"


if __name__ == "__main__":
  HashmapServer().start()
