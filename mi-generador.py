import sys
import yaml
import copy

def main():

    if len(sys.argv) != 4:
        print("Uso: python3 mi-generador.py <cantidad_clientes> <requests_por_cliente> <max_clientes>")
        sys.exit(1)

    cantidad_clientes = int(sys.argv[1])
    requests_por_cliente = int(sys.argv[2])
    max_clientes = int(sys.argv[3])

    with open("docker-compose-base.yml", "r", encoding="utf-8") as base_file:
        compose = yaml.safe_load(base_file)

    services = compose["services"]

    # ============================================
    # 1. Expandir CLIENT
    # ============================================
    client_base = services["client"]
    if "container_name" in client_base:
        del client_base["container_name"]

    client_env = client_base.setdefault("environment", {})
    client_env["REQUESTS_PER_CLIENT"] = requests_por_cliente
    client_env["INITIAL_CLIENT_COUNT"] = cantidad_clientes

    # Gateway config
    if "gateway" in services:
        gw_env = services["gateway"].setdefault("environment", {})
        gw_env["REQUESTS_PER_CLIENT"] = requests_por_cliente
        gw_env["GATEWAY_MAX_PROCESSES"] = max_clientes

    # ============================================
    # 2. Expandir WSMs + puertos dinámicos
    # ============================================
    WSM_REPLICAS = 3

    wsm_services = {
        name: svc
        for name, svc in services.items()
        if name.startswith("wsm_") or name == "wsm"
        and not name == "wsm_base"
    }

    for base_name, base_service in wsm_services.items():

        base_env = base_service.setdefault("environment", {})
        container_port_base = int(base_env["PORT"])  # PORT BASE del compose original

        # Réplica 1
        base_env["WSM_ID"] = 1
        base_env["WSM_NAME"] = base_name
        base_env["WSM_REPLICAS"] = WSM_REPLICAS
        # Use base PORT if set, else assume offset
        base_env["PORT"] = container_port_base

        base_service["container_name"] = base_name
        base_service.pop("depends_on", None)

        previous = base_name

        # Réplica 2..N
        for i in range(2, WSM_REPLICAS + 1):

            replica_name = f"{base_name}_{i}"

            replica_service = copy.deepcopy(base_service)
            replica_env = replica_service.setdefault("environment", {})

            replica_env["WSM_ID"] = i
            replica_env["WSM_NAME"] = base_name
            # Port calculation: Base + (i-1). Example: 9000 + (2-1) = 9001
            new_port = container_port_base + (i - 1)
            replica_env["PORT"] = new_port
            replica_env["WSM_REPLICAS"] = WSM_REPLICAS

            # CRITICAL: Update the exposed ports mapping in docker-compose
            # Assumes format "HOST:CONTAINER", and since we use host networking or direct mapping
            # we want "NEW_PORT:NEW_PORT" to avoid conflict on host binding
            replica_service["ports"] = [f"{new_port}:{new_port}"]

            replica_service["container_name"] = replica_name
            replica_service["depends_on"] = [previous]

            services[replica_name] = replica_service
            previous = replica_name

    # ============================================
    # Guardar archivo final
    # ============================================
    with open("docker-compose.yml", "w", encoding="utf-8") as f:
        yaml.dump(compose, f, sort_keys=False, default_flow_style=False)

    print("✅ docker-compose.yml generado correctamente con puertos dinámicos para cada WSM.")


if __name__ == "__main__":
    main()
