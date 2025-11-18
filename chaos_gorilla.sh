#!/bin/bash
# -----------------------------------------------------------
# 🦍 CHAOS GORILLA - KILL RANDOM COMPONENTS OF THE PIPELINE
# -----------------------------------------------------------

MAKEFILE="Makefile"
CHAOTIC=true          # true → docker kill, false → docker stop --time 0
KILL_COUNT=5          # cantidad de contenedores a matar

# ───────────────────────────────────────────────────────────
# 1) Extraer contenedores según el Makefile (workers)
# ───────────────────────────────────────────────────────────

REPLICA_LINES=$(grep -E '^[A-Z0-9_]+_REPLICAS[[:space:]]*\?=' "$MAKEFILE")
CONTAINERS=()

while IFS=' ?= ' read -r VAR _ VALUE; do
  [ -z "$VAR" ] && continue
  [ -z "$VALUE" ] && continue

  NAME=$(echo "$VAR" | sed -E 's/_REPLICAS$//' | tr '[:upper:]' '[:lower:]')
  COUNT=$(echo "$VALUE" | tr -d ' ')

  for ((i=1; i<=COUNT; i++)); do
    CONTAINERS+=("distribuidos-coffee-shop-analysis-${NAME}-${i}")
  done
done <<< "$REPLICA_LINES"


# ───────────────────────────────────────────────────────────
# 2) Agregar *todos* los WSM (name real = wsm_*)
# ───────────────────────────────────────────────────────────

WSM_CONTAINERS=$(docker ps --format '{{.Names}}' | grep '^wsm_' || true)

while IFS= read -r WSM; do
  [ -z "$WSM" ] && continue
  CONTAINERS+=("$WSM")
done <<< "$WSM_CONTAINERS"


# ───────────────────────────────────────────────────────────
# 3) Validación
# ───────────────────────────────────────────────────────────

TOTAL=${#CONTAINERS[@]}
if [ "$TOTAL" -eq 0 ]; then
  echo "⚠️  No se encontraron contenedores para matar."
  exit 0
fi

if [ "$KILL_COUNT" -gt "$TOTAL" ]; then
  KILL_COUNT=$TOTAL
fi


# ───────────────────────────────────────────────────────────
# 4) Seleccionar víctimas aleatorias
# ───────────────────────────────────────────────────────────

TO_KILL=()

while [ "${#TO_KILL[@]}" -lt "$KILL_COUNT" ]; do
  IDX=$((RANDOM % TOTAL))
  CANDIDATE="${CONTAINERS[$IDX]}"
  
  if [[ ! " ${TO_KILL[*]} " =~ " ${CANDIDATE} " ]]; then
    TO_KILL+=("$CANDIDATE")
  fi
done


# ───────────────────────────────────────────────────────────
# 5) Ejecutar el caos
# ───────────────────────────────────────────────────────────

echo ""
echo "💥💥💥  CHAOS GORILLA ACTIVADO  💥💥💥"
echo "Matando $KILL_COUNT de $TOTAL contenedores posibles:"
echo ""

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

echo "✅ Fin del caos."
echo ""
