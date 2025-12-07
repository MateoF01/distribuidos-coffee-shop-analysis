import sys
import yaml
import copy

def main():

    if len(sys.argv) != 4:
        print("Uso: python3 mi-generador.py <requests_por_cliente> <max_clientes> <cleanup_on_success>")
        sys.exit(1)

    requests_por_cliente = int(sys.argv[1])
    max_clientes = int(sys.argv[2])
    cleanup_on_success = sys.argv[3].lower() == "true"

    with open("docker-compose-base.yml", "r", encoding="utf-8") as base_file:
        compose = yaml.safe_load(base_file)

    services = compose["services"]

    # ============================================
    # 1. Configurar CLIENT y GATEWAY
    # ============================================
    client_base = services["client"]
    if "container_name" in client_base:
        del client_base["container_name"]

    client_env = client_base.setdefault("environment", {})
    client_env["REQUESTS_PER_CLIENT"] = requests_por_cliente

    # Gateway config
    if "gateway" in services:
        gw_env = services["gateway"].setdefault("environment", {})
        gw_env["REQUESTS_PER_CLIENT"] = requests_por_cliente
        gw_env["GATEWAY_MAX_PROCESSES"] = max_clientes
        gw_env["CLEANUP_ON_SUCCESS"] = str(cleanup_on_success).lower()

    # ============================================
    # 2. Expandir WSMs + puertos dinámicos
    # ============================================
    WSM_REPLICAS = 3

    wsm_services = {
        name: svc
        for name, svc in services.items()
        if name.startswith("wsm_")
        and not name == "wsm_base"
    }

    for base_name, base_service in wsm_services.items():

        base_env = base_service.setdefault("environment", {})
        container_port_base = int(base_env["PORT"])  # PORT BASE del compose original

        # Réplica 1
        base_env["WSM_ID"] = 1
        base_env["WSM_NAME"] = base_name
        base_env["PORT"] = container_port_base + 1
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
            replica_env["PORT"] = container_port_base + i

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
