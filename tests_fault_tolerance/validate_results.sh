#!/bin/bash
# ─────────────────────────────────────────────────────────────────
# 🔍 VALIDATE RESULTS - Valida que los outputs coincidan con esperado
# ─────────────────────────────────────────────────────────────────
# Este script valida que los archivos CSV generados en output/
# coincidan con los archivos de referencia en results/
#
# Lógica de validación:
# - Q1, Q2_a, Q2_b, Q3: Archivos deben ser IDÉNTICOS (diff exacto)
# - Q4: Todas las filas deben estar en kaggle_q4.csv (subset check)
# - Ignora: carpetas temp, archivos unsorted
# ─────────────────────────────────────────────────────────────────

set -e

# Script dir
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Importar utilidades
source "$SCRIPT_DIR/common.sh"

# ─────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────

OUTPUT_DIR="$PROJECT_ROOT/output"
RESULTS_DIR="$PROJECT_ROOT/results"

# Archivos de referencia
REFERENCE_Q1="$RESULTS_DIR/kaggle_q1.csv"
REFERENCE_Q2_A="$RESULTS_DIR/kaggle_q2_a.csv"
REFERENCE_Q2_B="$RESULTS_DIR/kaggle_q2_b.csv"
REFERENCE_Q3="$RESULTS_DIR/kaggle_q3.csv"
REFERENCE_Q4="$RESULTS_DIR/kaggle_q4.csv"

# ─────────────────────────────────────────────────────────────────
# ESTADO GLOBAL
# ─────────────────────────────────────────────────────────────────

TOTAL_VALIDATIONS=0
TOTAL_PASSED=0
TOTAL_FAILED=0

# ─────────────────────────────────────────────────────────────────
# FUNCIONES DE VALIDACIÓN
# ─────────────────────────────────────────────────────────────────

validate_file_exact_match() {
    local query_name=$1
    local output_file=$2
    local reference_file=$3
    
    TOTAL_VALIDATIONS=$((TOTAL_VALIDATIONS + 1))
    
    if [ ! -f "$output_file" ]; then
        log_error "$query_name: Archivo no existe: $output_file"
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
        return 1
    fi
    
    if [ ! -f "$reference_file" ]; then
        log_error "$query_name: Archivo de referencia no existe: $reference_file"
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
        return 1
    fi
    
    # Crear archivos temporales con line endings normalizados (LF)
    local output_normalized=$(mktemp)
    local reference_normalized=$(mktemp)
    
    # Convertir CRLF a LF
    tr -d '\r' < "$output_file" > "$output_normalized"
    tr -d '\r' < "$reference_file" > "$reference_normalized"
    
    if diff "$output_normalized" "$reference_normalized" > /dev/null 2>&1; then
        log_success "$query_name: ✅ Coincide exactamente"
        TOTAL_PASSED=$((TOTAL_PASSED + 1))
        rm "$output_normalized" "$reference_normalized"
        return 0
    else
        log_error "$query_name: ❌ NO coincide con referencia"
        log_info "   Output: $output_file"
        log_info "   Referencia: $reference_file"
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
        rm "$output_normalized" "$reference_normalized"
        return 1
    fi
}

validate_q4_subset() {
    local request_id=$1
    local output_file=$2
    local reference_file=$3
    
    TOTAL_VALIDATIONS=$((TOTAL_VALIDATIONS + 1))
    
    if [ ! -f "$output_file" ]; then
        log_error "Q4 (Request $request_id): Archivo no existe: $output_file"
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
        return 1
    fi
    
    if [ ! -f "$reference_file" ]; then
        log_error "Q4 (Request $request_id): Archivo de referencia no existe: $reference_file"
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
        return 1
    fi
    
    # Crear archivo temporal sin headers para comparación (con line endings normalizados)
    local output_no_header=$(mktemp)
    local reference_truncated=$(mktemp)
    
    # Normalizar line endings (CRLF -> LF) y luego remover header
    tail -n +2 "$output_file" | tr -d '\r' | sort > "$output_no_header"
    
    # Para la referencia: remover header, quitar la columna purchases_qty (última columna), normalizar y sortear
    # La estructura es: store_name,birthdate,purchases_qty → queremos: store_name,birthdate
    tail -n +2 "$reference_file" | tr -d '\r' | cut -d',' -f1,2 | sort > "$reference_truncated"
    
    # Verificar que todas las filas del output están en la referencia (subset check)
    local diff_result=$(comm -23 "$output_no_header" "$reference_truncated")
    local diff_count=$(echo "$diff_result" | wc -l)
    
    if [ "$diff_count" -eq 1 ] && [ -z "$diff_result" ]; then
        log_success "Q4 (Request $request_id): ✅ Todas las filas están en referencia"
        TOTAL_PASSED=$((TOTAL_PASSED + 1))
        rm "$output_no_header" "$reference_truncated"
        return 0
    else
        log_error "Q4 (Request $request_id): ❌ Algunas filas NO están en referencia"
        log_info "   Output: $output_file"
        log_info "   Referencia: $reference_file (sin columna purchases_qty)"
        if [ -n "$diff_result" ]; then
            log_info "   Filas que falta encontrar:"
            echo "$diff_result" | head -3 | sed 's/^/     /'
        fi
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
        rm "$output_no_header" "$reference_truncated"
        return 1
    fi
}

# ─────────────────────────────────────────────────────────────────
# MAIN VALIDATION FLOW
# ─────────────────────────────────────────────────────────────────

main() {
    echo ""
    echo "╔═══════════════════════════════════════════════════════════╗"
    echo "║         🔍 VALIDATING QUERY RESULTS                       ║"
    echo "╚═══════════════════════════════════════════════════════════╝"
    echo ""
    
    # Verificar que directorios existen
    if [ ! -d "$OUTPUT_DIR" ]; then
        log_error "Directorio de output no existe: $OUTPUT_DIR"
        return 1
    fi
    
    if [ ! -d "$RESULTS_DIR" ]; then
        log_error "Directorio de resultados no existe: $RESULTS_DIR"
        return 1
    fi
    
    # Contar cuántos request hay
    local request_dirs=$(find "$OUTPUT_DIR" -maxdepth 1 -type d -name "[0-9]*" | sort -V)
    local request_count=$(echo "$request_dirs" | wc -l)
    
    if [ "$request_count" -eq 0 ]; then
        log_warning "No se encontraron carpetas de requests en $OUTPUT_DIR"
        return 1
    fi
    
    log_info "Encontrados $request_count request(s)"
    echo ""
    
    # Iterar sobre cada request
    while IFS= read -r request_dir; do
        local request_id=$(basename "$request_dir")
        
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "  Request #$request_id"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
        
        # Q1
        validate_file_exact_match \
            "  Q1 (Request $request_id)" \
            "$request_dir/q1.csv" \
            "$REFERENCE_Q1"
        
        # Q2_a
        validate_file_exact_match \
            "  Q2_a (Request $request_id)" \
            "$request_dir/q2_a.csv" \
            "$REFERENCE_Q2_A"
        
        # Q2_b
        validate_file_exact_match \
            "  Q2_b (Request $request_id)" \
            "$request_dir/q2_b.csv" \
            "$REFERENCE_Q2_B"
        
        # Q3
        validate_file_exact_match \
            "  Q3 (Request $request_id)" \
            "$request_dir/q3.csv" \
            "$REFERENCE_Q3"
        
        # Q4 (subset validation)
        validate_q4_subset \
            "$request_id" \
            "$request_dir/q4.csv" \
            "$REFERENCE_Q4"
        
        echo ""
    done <<< "$request_dirs"
    
    # ─────────────────────────────────────────────────────────────
    # RESUMEN FINAL
    # ─────────────────────────────────────────────────────────────
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  📊 RESUMEN DE VALIDACIÓN"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "  Total validaciones: $TOTAL_VALIDATIONS"
    echo "  Pasadas: $(echo -e "${GREEN}$TOTAL_PASSED${NC}")"
    echo "  Fallidas: $(echo -e "${RED}$TOTAL_FAILED${NC}")"
    echo ""
    
    if [ "$TOTAL_FAILED" -eq 0 ]; then
        log_success "✨ TODAS LAS VALIDACIONES PASARON"
        echo ""
        return 0
    else
        log_error "❌ ALGUNAS VALIDACIONES FALLARON"
        echo ""
        return 1
    fi
}

# ─────────────────────────────────────────────────────────────────
# EJECUTAR
# ─────────────────────────────────────────────────────────────────

main "$@"
