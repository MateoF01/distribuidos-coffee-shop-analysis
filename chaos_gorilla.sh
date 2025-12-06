#!/bin/bash
# -----------------------------------------------------------
# 🦍 CHAOS GORILLA - KILL RANDOM COMPONENTS OF THE PIPELINE
# -----------------------------------------------------------

# ───────────────────────────────────────────────────────────
# Usage: ./chaos_gorilla.sh [INTERVAL_SECONDS]
#   INTERVAL_SECONDS: time between kills (default: 30)
#   Press Ctrl+C to stop
# ───────────────────────────────────────────────────────────

CHAOTIC=true          # true → docker kill, false → docker stop --time 0
KILL_COUNT=5          # cantidad de contenedores a matar por ronda
INTERVAL=${1:-30}     # intervalo entre matanzas (default 30s)

# Trap Ctrl+C and Ctrl+D to exit gracefully
trap 'echo ""; echo "🛑 Chaos Gorilla detenido."; exit 0' INT TERM

# ───────────────────────────────────────────────────────────
# Opción: Incluir WSMs (deshabilitado por defecto para que
# puedan revivir a los workers muertos)
# ───────────────────────────────────────────────────────────

INCLUDE_WSM=false  # Cambiar a true para matar WSMs también

echo ""
echo "🦍🦍🦍  CHAOS GORILLA INICIADO  🦍🦍🦍"
echo "Intervalo entre rondas: ${INTERVAL}s"
echo "Contenedores a matar por ronda: hasta $KILL_COUNT"
echo "Excluye: rabbitmq, clients"
echo "Presiona Ctrl+C para detener"
echo ""

# ───────────────────────────────────────────────────────────
# Main loop - runs until Ctrl+C
# ───────────────────────────────────────────────────────────
ROUND=1
while true; do

# ───────────────────────────────────────────────────────────
# Obtener contenedores dinámicamente cada ronda
# Excluye rabbitmq y clients
# ───────────────────────────────────────────────────────────

CONTAINERS=($(docker ps --format '{{.Names}}' \
    | grep -v 'rabbitmq' \
    | grep -vi 'client'))

# Opcionalmente excluir WSMs
if [ "$INCLUDE_WSM" = false ]; then
    CONTAINERS=($(printf '%s\n' "${CONTAINERS[@]}" | grep -v '^wsm_' || true))
fi

TOTAL=${#CONTAINERS[@]}
if [ "$TOTAL" -eq 0 ]; then
    echo "⚠️  No hay contenedores elegibles para matar."
    sleep "$INTERVAL"
    continue
fi

# Ajustar KILL_COUNT si hay menos contenedores disponibles
ACTUAL_KILL_COUNT=$KILL_COUNT
if [ "$ACTUAL_KILL_COUNT" -gt "$TOTAL" ]; then
    ACTUAL_KILL_COUNT=$TOTAL
fi

# ───────────────────────────────────────────────────────────
# Seleccionar víctimas aleatorias (sin repetir)
# ───────────────────────────────────────────────────────────

TO_KILL=()

while [ "${#TO_KILL[@]}" -lt "$ACTUAL_KILL_COUNT" ]; do
    IDX=$((RANDOM % TOTAL))
    CANDIDATE="${CONTAINERS[$IDX]}"
    
    if [[ ! " ${TO_KILL[*]} " =~ " ${CANDIDATE} " ]]; then
        TO_KILL+=("$CANDIDATE")
    fi
done

# ───────────────────────────────────────────────────────────
# Ejecutar el caos
# ───────────────────────────────────────────────────────────

echo ""
echo "��💥💥  RONDA $ROUND  💥💥💥"
echo "Matando $ACTUAL_KILL_COUNT de $TOTAL contenedores posibles:"
echo ""
((ROUND++))

for C in "${TO_KILL[@]}"; do
    echo "🧨 Eliminando:  $C"
    if [ "$CHAOTIC" = true ]; then
        docker kill "$C" >/dev/null 2>&1 \
            && echo "   ☠️  $C murió (kill)" \
            || echo "   ⚠️  No se pudo matar $C"
    else
        docker stop "$C" --time 0 >/dev/null 2>&1 \
            && echo "   ☠️  $C murió (stop)" \
            || echo "   ⚠️  No se pudo matar $C"
    fi
    echo ""
done

echo "✅ Ronda de caos completada."
echo "⏳ Esperando ${INTERVAL}s hasta la próxima ronda... (Ctrl+C para detener)"
echo ""
sleep "$INTERVAL"
done
