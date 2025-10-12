import sys
import yaml
import copy

def main():
    if len(sys.argv) != 3:
        print("Uso: python3 mi-generador.py <cantidad_clientes> <requests_por_cliente>")
        sys.exit(1)

    try:
        cantidad_clientes = int(sys.argv[1])
        requests_por_cliente = int(sys.argv[2])
    except ValueError:
        print("Ambos parámetros deben ser enteros.")
        sys.exit(1)

    # Plantilla base del docker-compose
    with open("docker-compose-base.yml", "r", encoding="utf-8") as base_file:
        compose = yaml.safe_load(base_file)

    # Si ya existia una clave 'client' base, la usamos como plantilla
    if "client" not in compose["services"]:
        print("⚠️  No se encontró servicio base 'client' en el YAML base.")
        sys.exit(1)

    base_client = compose["services"].pop("client")

    # Añadir REQUESTS_PER_CLIENT al gateway
    if "gateway" in compose["services"]:
        gateway_env = compose["services"]["gateway"].get("environment", {})
        gateway_env["REQUESTS_PER_CLIENT"] = requests_por_cliente
        compose["services"]["gateway"]["environment"] = gateway_env

    # Generar clientes dinamicos
    services = compose["services"]
    new_services = {}
    inserted_clients = False
    for key in services:
        new_services[key] = services[key]
        if key == "gateway" and not inserted_clients:
            for i in range(1, cantidad_clientes + 1):
                client_name = f"client{i}"
                client = copy.deepcopy(base_client)
                client["container_name"] = client_name
                client["environment"]["CLIENT_ID"] = client_name
                client["environment"]["REQUESTS_PER_CLIENT"] = requests_por_cliente
                new_services[client_name] = client
            inserted_clients = True
    compose["services"] = new_services

    # Guardar nuevo archivo
    with open("docker-compose.yml", "w", encoding="utf-8") as f:
        yaml.dump(compose, f, sort_keys=False, default_flow_style=False)

    print(f"✅ docker-compose.yml generado con {cantidad_clientes} clientes y {requests_por_cliente} requests por cliente.")

if __name__ == "__main__":
    main()