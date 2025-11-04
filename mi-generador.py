import sys
import yaml
import copy

def main():

    if len(sys.argv) != 4:
        print("Uso: python3 mi-generador.py <cantidad_clientes> <requests_por_cliente> <max_clientes>")
        sys.exit(1)

    try:
        cantidad_clientes = int(sys.argv[1])
        requests_por_cliente = int(sys.argv[2])
        max_clientes = int(sys.argv[3])
    except ValueError:
        print("Todos los parámetros deben ser enteros.")
        sys.exit(1)

    if cantidad_clientes > max_clientes:
        print(f"Error: cantidad_clientes ({cantidad_clientes}) no puede ser mayor que cantidad maxima de clientes ({max_clientes})")
        sys.exit(1)

    # Plantilla base del docker-compose
    with open("docker-compose-base.yml", "r", encoding="utf-8") as base_file:
        compose = yaml.safe_load(base_file)

    # Si ya existia una clave 'client' base, la usamos como plantilla
    if "client" not in compose["services"]:
        print("⚠️  No se encontró servicio base 'client' en el YAML base.")
        sys.exit(1)

    # Modificar el cliente base para que sea escalable
    base_client = compose["services"]["client"]
    
    # Actualizar environment del cliente base
    if "environment" not in base_client:
        base_client["environment"] = {}
    
    base_client["environment"]["REQUESTS_PER_CLIENT"] = requests_por_cliente
    base_client["environment"]["INITIAL_CLIENT_COUNT"] = cantidad_clientes
    
    # Remover container_name si existe para permitir scaling
    if "container_name" in base_client:
        del base_client["container_name"]

    # Añadir REQUESTS_PER_CLIENT y GATEWAY_MAX_PROCESSES al gateway
    if "gateway" in compose["services"]:
        gateway_env = compose["services"]["gateway"].get("environment", {})
        gateway_env["REQUESTS_PER_CLIENT"] = requests_por_cliente
        gateway_env["GATEWAY_MAX_PROCESSES"] = max_clientes
        compose["services"]["gateway"]["environment"] = gateway_env

    # Guardar nuevo archivo
    with open("docker-compose.yml", "w", encoding="utf-8") as f:
        yaml.dump(compose, f, sort_keys=False, default_flow_style=False)

    print(f"✅ docker-compose.yml generado con cliente escalable (inicial: {cantidad_clientes}), {requests_por_cliente} requests por cliente, máximo de {max_clientes} clientes.")
    print(f"💡 Para escalar dinámicamente: docker compose up -d --scale client=<N>")

if __name__ == "__main__":
    main()