#!/bin/bash
# -----------------------------------------------------------
# 🦍 CHAOS GORILLA - RANDOM KILLER (INFINITE LOOP EDITION)
# -----------------------------------------------------------

INTERVAL=5        # cada cuántos segundos matar un contenedor
CHAOTIC=true      # true → docker kill, false → docker stop --time 0

echo ""
echo "💥💥💥  CHAOS GORILLA ACTIVADO  💥💥💥"
echo "Cada $INTERVAL segundos se matará *un contenedor al azar*."
echo "Cortar con CTRL+C."
echo ""

while true; do
    # ---------------------------------------------
    # 1) Obtener contenedores en ejecución del compose
    # ---------------------------------------------
    CONTAINERS=($(docker ps --format '{{.Names}}'))

    if [ ${#CONTAINERS[@]} -eq 0 ]; then
        echo "⚠️  No hay contenedores corriendo. Reintentando..."
        sleep "$INTERVAL"
        continue
    fi

    # ---------------------------------------------
    # 2) Elegir uno al azar
    # ---------------------------------------------
    RANDOM_INDEX=$((RANDOM % ${#CONTAINERS[@]}))
    VICTIM=${CONTAINERS[$RANDOM_INDEX]}

    echo "🧨 Matando contenedor al azar:  $VICTIM"

    # ---------------------------------------------
    # 3) Ejecutar el caos
    # ---------------------------------------------
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
