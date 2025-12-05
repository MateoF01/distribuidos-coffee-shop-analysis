import subprocess
import sys
import os
import re

PROJECT_NAME = "coffee-shop-22"

# Resolve paths relative to this script (tests_fault_tolerance/kill_worker.py)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
MAKEFILE_PATH = os.path.join(PROJECT_ROOT, "Makefile")
DOCKER_COMPOSE_PATH = os.path.join(PROJECT_ROOT, "docker-compose.yml")

def get_services_from_docker_compose():
    """Get list of all services defined in docker-compose."""
    try:
        result = subprocess.run(
            ["docker", "compose", "config", "--services"],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip().split('\n')
    except subprocess.CalledProcessError as e:
        print(f"Error getting services: {e}")
        sys.exit(1)

def parse_makefile_replicas():
    """
    Parse Makefile to get:
    1. Default replica counts (VAR ?= VAL)
    2. Service to variable mapping (--scale service=$(VAR))
    """
    replica_defaults = {}
    service_scale_map = {}
    
    if not os.path.exists(MAKEFILE_PATH):
        print(f"Makefile not found at {MAKEFILE_PATH}")
        return {}, {}

    with open(MAKEFILE_PATH, 'r') as f:
        for line in f:
            line = line.strip()
            
            # 1. Get default values: VAR ?= VAL
            if '?=' in line:
                parts = line.split('?=')
                if len(parts) == 2:
                    var_name = parts[0].strip()
                    try:
                        count = int(parts[1].strip())
                        replica_defaults[var_name] = count
                    except ValueError:
                        pass
            
            # 2. Get service mapping: --scale service=$(VAR)
            # This usually appears inside the 'up' target, possibly on multiple lines
            if '--scale' in line:
                # Remove --scale and whitespace
                clean_line = line.replace('--scale', '').strip()
                # Should be service=$(VAR) or service=$(VAR) \
                if clean_line.endswith('\\'):
                    clean_line = clean_line[:-1].strip()
                
                if '=' in clean_line:
                    parts = clean_line.split('=')
                    if len(parts) == 2:
                        service = parts[0].strip()
                        val_part = parts[1].strip()
                        # val_part should be $(VAR)
                        if val_part.startswith('$(') and val_part.endswith(')'):
                            var_name = val_part[2:-1]
                            service_scale_map[service] = var_name

    return replica_defaults, service_scale_map

def get_container_names_from_compose():
    """
    Parse docker-compose.yml to find explicit container_name definitions.
    Returns a dict: {service_name: container_name}
    """
    container_names = {}
    if not os.path.exists(DOCKER_COMPOSE_PATH):
        print(f"docker-compose.yml not found at {DOCKER_COMPOSE_PATH}")
        return {}

    current_service = None
    with open(DOCKER_COMPOSE_PATH, 'r') as f:
        for line in f:
            stripped = line.strip()
            # Simple heuristic parsing
            if line.startswith("  ") and not line.startswith("    ") and stripped.endswith(":"):
                current_service = stripped[:-1]
            elif current_service and "container_name:" in stripped:
                parts = stripped.split(":", 1)
                if len(parts) == 2:
                    container_names[current_service] = parts[1].strip()
    return container_names

def list_workers():
    services = get_services_from_docker_compose()
    replica_defaults, service_scale_map = parse_makefile_replicas()
    explicit_container_names = get_container_names_from_compose()
    
    workers = []
    
    for service in services:
        # Filter out non-worker services
        if service in ['rabbitmq', 'client']:
            continue
            
        # Special handling for client service to use INITIAL_CLIENT_COUNT
        if service == 'client':
            count = get_client_count_from_compose()
            for i in range(1, count + 1):
                workers.append(f"{PROJECT_NAME}-{service}-{i}")
            continue

        # Check if service is scaled
        if service in service_scale_map:
            var_name = service_scale_map[service]
            count = replica_defaults.get(var_name)
            
            if count is None:
                # Try with _REPLICAS suffix if not present
                if not var_name.endswith('_REPLICAS'):
                     count = replica_defaults.get(f"{var_name}_REPLICAS")
            
            if count is None:
                # Still not found? Default to 1 but warn
                # print(f"Warning: Could not find replica count for {service} (var: {var_name}). Defaulting to 1.")
                count = 1
            
            for i in range(1, count + 1):
                # Scaled services follow project-service-replica pattern
                container_name = f"{PROJECT_NAME}-{service}-{i}"
                workers.append(container_name)
        else:
            # Not scaled
            if service in explicit_container_names:
                workers.append(explicit_container_names[service])
            else:
                # Default docker compose naming for non-scaled services
                # Usually project-service-1
                workers.append(f"{PROJECT_NAME}-{service}-1")
                
    return sorted(workers)

def generate_crash_config(selected_workers, wait_count="5"):
    """
    Generate docker-compose.override.yml with crash settings for selected workers.
    """
    override_path = os.path.join(PROJECT_ROOT, "docker-compose.override.yml")
    
    services_config = {}
    
    explicit_names = get_container_names_from_compose()
    # Reverse map: container_name -> service_name
    container_to_service = {v: k for k, v in explicit_names.items()}
    
    for worker in selected_workers:
        service_name = None
        replica_id = "1"
        
        # Parse worker name to get service and replica
        # Format 1: PROJECT_NAME-service-replica
        prefix = f"{PROJECT_NAME}-"
        if worker.startswith(prefix):
            # Remove prefix
            suffix = worker[len(prefix):]
            # Split by last hyphen to get replica
            parts = suffix.rsplit('-', 1)
            if len(parts) == 2 and parts[1].isdigit():
                service_name = parts[0]
                replica_id = parts[1]
            else:
                # Fallback for non-standard names?
                pass
        
        # Format 2: Explicit container name (e.g. gateway, wsm_transaction_items_3)
        if not service_name:
            if worker in container_to_service:
                service_name = container_to_service[worker]
                
                # Try to extract replica ID from service name (e.g. wsm_..._3 -> 3)
                # Matches _N or -N at the end
                match = re.search(r'[_-](\d+)$', service_name)
                if match:
                    replica_id = match.group(1)
                else:
                    replica_id = "1" # Default for singletons
        
        if service_name:
            # Add to config
            # Note: If multiple replicas of same service are selected, 
            # the last one wins for CRASH_REPLICA_ID.
            services_config[service_name] = {
                "environment": {
                    "CRASH_PROBABILITY": "1.0",
                    "CRASH_REPLICA_ID": replica_id,
                    "CRASH_WAIT_COUNT": wait_count
                }
            }
        else:
            print(f"Warning: Could not map {worker} to a service. Skipping.")

    if not services_config:
        print("No valid services selected for crash.")
        return

    # Write YAML manually to avoid dependency
    with open(override_path, 'w') as f:
        f.write("services:\n")
        for service, env in services_config.items():
            f.write(f"  {service}:\n")
            f.write("    environment:\n")
            for k, v in env["environment"].items():
                f.write(f"      {k}: \"{v}\"\n")
    
    print(f"\nSuccessfully generated {override_path}")
    print("Crash configuration applied:")
    for service, env in services_config.items():
        print(f"  - {service}: Replica {env['environment']['CRASH_REPLICA_ID']}")
    print("\nRun 'make up' to start the system with this configuration.")

def reset_config():
    override_path = os.path.join(PROJECT_ROOT, "docker-compose.override.yml")
    if os.path.exists(override_path):
        os.remove(override_path)
        print(f"\nRemoved {override_path}")
        print("Crash configuration reset. System will run normally.")
    else:
        print("\nNo active crash configuration found.")

def get_all_replicas_for_service(service_name):
    """
    Get all container names for a specific service.
    """
    workers = list_workers()
    # Filter workers that belong to this service
    # Pattern: PROJECT_NAME-service_name-replica
    prefix = f"{PROJECT_NAME}-{service_name}-"
    
    replicas = []
    for w in workers:
        if w.startswith(prefix):
            replicas.append(w)
    
    # Also check explicit container names
    explicit_names = get_container_names_from_compose()
    if service_name in explicit_names:
        replicas.append(explicit_names[service_name])
        
    return replicas

def get_client_count_from_compose():
    """
    Parse docker-compose.yml to find INITIAL_CLIENT_COUNT in client service.
    """
    if not os.path.exists(DOCKER_COMPOSE_PATH):
        return 1
        
    try:
        with open(DOCKER_COMPOSE_PATH, 'r') as f:
            in_client_service = False
            for line in f:
                stripped = line.strip()
                # Check for client service definition
                if stripped == "client:":
                    in_client_service = True
                    continue
                
                # If we hit another service definition (no indentation), stop
                if in_client_service and line.startswith("  ") and not line.startswith("    ") and stripped.endswith(":"):
                    in_client_service = False
                
                if in_client_service and "INITIAL_CLIENT_COUNT:" in stripped:
                    parts = stripped.split(":", 1)
                    if len(parts) == 2:
                        return int(parts[1].strip())
    except Exception as e:
        print(f"Error parsing client count: {e}")
        
    return 1

def get_client_containers():
    """
    Get all client container names based on INITIAL_CLIENT_COUNT.
    """
    count = get_client_count_from_compose()
    clients = []
    for i in range(1, count + 1):
        clients.append(f"{PROJECT_NAME}-client-{i}")
    return clients

def open_stats_terminal(selected_workers):
    """
    Open a new terminal window with docker stats command typed but not executed.
    Order: rabbitmq, gateway, clients, selected_workers (all replicas).
    """
    import platform
    import shutil
    
    # 1. Fixed start
    ordered_names = ["rabbitmq", "gateway"]
    
    # 2. Clients
    clients = get_client_containers()
    ordered_names.extend(clients)
    
    # 3. Include all replicas of the services selected for crashing
    selected_services = set()
    explicit_names = get_container_names_from_compose()
    container_to_service = {v: k for k, v in explicit_names.items()}
    
    for worker in selected_workers:
        service_name = None
        prefix = f"{PROJECT_NAME}-"
        if worker.startswith(prefix):
            suffix = worker[len(prefix):]
            parts = suffix.rsplit('-', 1)
            if len(parts) == 2 and parts[1].isdigit():
                service_name = parts[0]
        
        if not service_name and worker in container_to_service:
            service_name = container_to_service[worker]
            
        if service_name:
            selected_services.add(service_name)
            
    # Get all replicas for these services
    worker_replicas = []
    for service in selected_services:
        worker_replicas.extend(get_all_replicas_for_service(service))
        
    # Sort
    worker_replicas = sorted(list(set(worker_replicas)))
    
    ordered_names.extend(worker_replicas)
    
    # Construct command parts
    cmd_parts = ["docker", "stats"] + ordered_names
    full_cmd_str = " ".join(cmd_parts)
    print(f"\nPreparing stats command: {full_cmd_str}")
    
    system = platform.system()
    
    if system == "Darwin":
        # macOS: Use AppleScript
        keystroke_lines = ""
        for i, part in enumerate(cmd_parts):
            keystroke_lines += f'        keystroke "{part}"\n'
            if i < len(cmd_parts) - 1:
                keystroke_lines += '        key code 49\n'

        osascript_cmd = f'''
        tell application "Terminal"
            activate
            do script ""
        end tell
        delay 0.5
        tell application "System Events"
{keystroke_lines}
        end tell
        '''
        
        try:
            subprocess.run(["osascript", "-e", osascript_cmd], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Failed to open stats terminal: {e}")
            
    elif system == "Linux":
        # Linux: Try different terminal emulators
        # Show the command and wait for Enter before executing
        bash_script = f'''echo "Command to run:"; echo ""; echo "{full_cmd_str}"; echo ""; read -p "Press Enter to execute..."; {full_cmd_str}'''
        
        terminal_cmds = [
            # gnome-terminal
            ["gnome-terminal", "--", "bash", "-c", bash_script],
            # xterm
            ["xterm", "-e", "bash", "-c", bash_script],
            # konsole (KDE)
            ["konsole", "-e", "bash", "-c", bash_script],
            # xfce4-terminal
            ["xfce4-terminal", "-e", f"bash -c '{bash_script}'"],
            # mate-terminal
            ["mate-terminal", "-e", f"bash -c '{bash_script}'"],
            # tilix
            ["tilix", "-e", f"bash -c '{bash_script}'"],
        ]
        
        terminal_opened = False
        for term_cmd in terminal_cmds:
            terminal_name = term_cmd[0]
            if shutil.which(terminal_name):
                try:
                    subprocess.Popen(term_cmd, start_new_session=True)
                    print(f"Opened stats terminal in {terminal_name}")
                    terminal_opened = True
                    break
                except Exception as e:
                    continue
        
        if not terminal_opened:
            print("\nCould not open a terminal automatically.")
            print("Please run this command manually in a new terminal:")
            print(f"  {full_cmd_str}")
    else:
        # Windows or other
        print("\nAutomatic terminal opening not supported on this OS.")
        print("Please run this command manually in a new terminal:")
        print(f"  {full_cmd_str}")

def main():
    workers = list_workers()
    
    print("\nAvailable Workers:")
    for i, worker in enumerate(workers):
        display_name = worker.replace(f"{PROJECT_NAME}-", "")
        print(f"{i + 1}. {display_name}")
        
    print("\nOptions:")
    print("  <numbers>: Comma-separated list of workers to crash (e.g. 1, 3)")
    print("  r:         Reset crash configuration")
    print("  q:         Quit")
    
    choice = input("\nSelect option > ").strip()
    
    if choice.lower() == 'q':
        return
    
    if choice.lower() == 'r':
        reset_config()
        return

    try:
        indices = [int(x.strip()) - 1 for x in choice.split(',')]
        selected = []
        for idx in indices:
            if 0 <= idx < len(workers):
                selected.append(workers[idx])
            else:
                print(f"Invalid selection: {idx + 1}")
        
        if selected:
            crash_wait_input = input("Enter messages to wait before crash (default 5): ").strip()
            crash_wait_count = crash_wait_input if crash_wait_input else "5"
            generate_crash_config(selected, crash_wait_count)
            open_stats_terminal(selected)
            
    except ValueError:
        print("Invalid input.")

if __name__ == "__main__":
    main()
