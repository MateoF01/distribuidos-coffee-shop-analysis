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
    if "client" not in services:
        print("⚠️ No está el servicio 'client' en el YAML base.")
        sys.exit(1)

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
    # 2. Expandir WSMs automáticamente + depends_on
    # ============================================
    WSM_REPLICAS = 3   # Cantidad total de réplicas del WSM

    wsm_services = {
        name: svc
        for name, svc in services.items()
        if name.startswith("wsm_")
    }

    for base_name, base_service in wsm_services.items():

        # Replica 1 (el servicio base)
        base_env = base_service.setdefault("environment", {})
        base_env["WSM_ID"] = 1
        base_env["WSM_NAME"] = base_name
        base_service["container_name"] = base_name

        # La réplica base NO depende de ninguna
        base_service.pop("depends_on", None)

        # -------------------------------------------
        # Crear réplicas 2..N con depends_on en cascada
        # -------------------------------------------
        previous = base_name  # la réplica 2 depende de la original

        for i in range(2, WSM_REPLICAS + 1):
            replica_name = f"{base_name}_{i}"

            replica_service = copy.deepcopy(base_service)
            replica_env = replica_service.setdefault("environment", {})
            replica_env["WSM_ID"] = i
            replica_env["WSM_NAME"] = base_name
            replica_service["container_name"] = replica_name

            # AGREGAR depends_on en cascada
            replica_service["depends_on"] = [previous]

            services[replica_name] = replica_service

            # actualiza cadena
            previous = replica_name

    # ============================================
    # Guardar archivo final
    # ============================================
    with open("docker-compose.yml", "w", encoding="utf-8") as f:
        yaml.dump(compose, f, sort_keys=False, default_flow_style=False)

    print("✅ docker-compose.yml generado con réplicas WSM, depends_on en cascada y clientes escalables.")


if __name__ == "__main__":
    main()
