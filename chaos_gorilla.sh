#!/bin/bash
# ---------------------------------------------
# 🦍 Chaos Gorilla - destruye réplicas al azar
# ---------------------------------------------

MAKEFILE="Makefile"
CHAOTIC=true    # true → docker kill, false → docker stop --time 0
KILL_COUNT=2    # cantidad total de contenedores a matar

if [ ! -f "$MAKEFILE" ]; then
  echo "❌ No se encontró el Makefile en $(pwd)"
  exit 1
fi

# Extraer líneas reales de definición (ignora echos)
REPLICA_LINES=$(grep -E '^[A-Z0-9_]+_REPLICAS[[:space:]]*\?=' "$MAKEFILE")

CONTAINERS=()

while IFS=' ?= ' read -r VAR _ VALUE; do
  [ -z "$VAR" ] && continue
  [ -z "$VALUE" ] && continue

  # limpiar nombre
  NAME=$(echo "$VAR" | sed -E 's/_REPLICAS$//' | tr '[:upper:]' '[:lower:]')
  COUNT=$(echo "$VALUE" | tr -d ' ')

  for ((i=1; i<=COUNT; i++)); do
    CONTAINERS+=("distribuidos-coffee-shop-analysis-${NAME}-${i}")
  done
done <<< "$REPLICA_LINES"

TOTAL=${#CONTAINERS[@]}
if [ "$TOTAL" -eq 0 ]; then
  echo "⚠️ No se encontraron contenedores según el Makefile."
  exit 0
fi

# Elegir contenedores al azar
if [ "$KILL_COUNT" -gt "$TOTAL" ]; then
  KILL_COUNT=$TOTAL
fi

TO_KILL=()
while [ "${#TO_KILL[@]}" -lt "$KILL_COUNT" ]; do
  IDX=$((RANDOM % TOTAL))
  CANDIDATE="${CONTAINERS[$IDX]}"
  if [[ ! " ${TO_KILL[*]} " =~ " ${CANDIDATE} " ]]; then
    TO_KILL+=("$CANDIDATE")
  fi
done

# Ejecutar el caos
echo "💥 Chaos Gorilla: matando $KILL_COUNT de $TOTAL contenedores..."
for CONTAINER in "${TO_KILL[@]}"; do
  echo "🧨 Eliminando $CONTAINER"
  if [ "$CHAOTIC" = true ]; then
    docker kill "$CONTAINER" >/dev/null 2>&1 && echo "   ☠️  $CONTAINER MURIÓ" || echo "   ⚠️  No se pudo matar $CONTAINER"
  else
    docker stop "$CONTAINER" --time 0 >/dev/null 2>&1 && echo "   ☠️  $CONTAINER MURIÓ" || echo "   ⚠️  No se pudo matar $CONTAINER"
  fi
done

echo "✅ Fin del caos."
