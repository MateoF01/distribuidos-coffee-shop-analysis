#!/bin/bash
# -----------------------------------------------------------
# Genera docker-compose.yml a partir de docker-compose-base.yml
# El número de clientes se maneja con --scale en make up
# -----------------------------------------------------------

# Usar variables de entorno si existen, sino defaults
REQUESTS_PER_CLIENT=${REQUESTS_PER_CLIENT:-1}
GATEWAY_MAX_PROCESSES=${GATEWAY_MAX_PROCESSES:-5}

echo "📦 Generando docker-compose.yml..."
echo "   REQUESTS_PER_CLIENT:   $REQUESTS_PER_CLIENT"
echo "   GATEWAY_MAX_PROCESSES: $GATEWAY_MAX_PROCESSES"

python3 mi-generador.py "$REQUESTS_PER_CLIENT" "$GATEWAY_MAX_PROCESSES"

echo "✅ docker-compose.yml generado!"
