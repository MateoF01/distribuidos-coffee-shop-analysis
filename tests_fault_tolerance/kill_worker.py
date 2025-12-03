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
        if service in ['client', 'rabbitmq']:
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

def generate_crash_config(selected_workers):
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
        
        # Format 2: Explicit container name (e.g. gateway)
        if not service_name:
            if worker in container_to_service:
                service_name = container_to_service[worker]
                replica_id = "1" # Default for singletons
        
        if service_name:
            # Add to config
            # Note: If multiple replicas of same service are selected, 
            # the last one wins for CRASH_REPLICA_ID.
            services_config[service_name] = {
                "environment": {
                    "CRASH_PROBABILITY": "1.0",
                    "CRASH_REPLICA_ID": replica_id
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
            generate_crash_config(selected)
            
    except ValueError:
        print("Invalid input.")

if __name__ == "__main__":
    main()
