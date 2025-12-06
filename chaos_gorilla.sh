#!/bin/bash
# -----------------------------------------------------------
# 🦍 CHAOS GORILLA - RANDOM KILLER (NO RABBIT, NO CLIENTS ANYWHERE)
# -----------------------------------------------------------

INTERVAL=6
CHAOTIC=true

echo ""
echo "💥💥💥  CHAOS GORILLA ACTIVADO  💥💥💥"
echo "Cada $INTERVAL segundos se matará *un contenedor al azar*"
echo "Cortar con CTRL+C."
echo ""

while true; do
    # Obtener contenedores EXCLUYENDO rabbitmq y cualquier nombre que tenga 'client'
    CONTAINERS=($(docker ps --format '{{.Names}}' \
        | grep -v 'rabbitmq' \
        | grep -vi 'client'))

    if [ ${#CONTAINERS[@]} -eq 0 ]; then
        echo "⚠️  No hay contenedores elegibles para matar."
        sleep "$INTERVAL"
        continue
    fi

    # Elegir uno al azar
    RANDOM_INDEX=$((RANDOM % ${#CONTAINERS[@]}))
    VICTIM=${CONTAINERS[$RANDOM_INDEX]}

    echo "🧨 Matando contenedor al azar:  $VICTIM"

    if [ "$CHAOTIC" = true ]; then
        docker kill "$VICTIM" >/dev/null 2>&1 \
            && echo "   ☠️  $VICTIM murió (kill)" \
            || echo "   ⚠️  No se pudo matar $VICTIM"
    else
        docker stop "$VICTIM" --time 0 >/dev/null 2>&1 \
            && echo "   ☠️  $VICTIM murió (stop)" \
            || echo "   ⚠️  No se pudo matar $VICTIM"
    fi

    echo "⏳ Esperando $INTERVAL segundos..."
    echo ""
    sleep "$INTERVAL"
done
