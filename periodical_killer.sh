#!/bin/bash

if [ -z "$1" ]; then
  echo "Uso: $0 <nombre_contenedor>"
  exit 1
fi

CONTAINER=$1

echo "Matando (docker kill) al contenedor '$CONTAINER' cada 8 segundos..."
echo "Presioná CTRL+C para salir."

while true; do
  docker kill "$CONTAINER"
  echo "Contenedor matado: $CONTAINER"
  sleep 8
done
