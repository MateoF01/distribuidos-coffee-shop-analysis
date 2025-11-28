#!/bin/bash

# Directorio desde donde se ejecuta el script
START_DIR="$(pwd)"

# Función para abrir terminal y dejar el comando escrito sin ejecutarlo
write_cmd() {
osascript <<EOF
tell application "Terminal"
    activate
    do script "cd \"$START_DIR\"" -- abre nueva ventana ubicada en el directorio correcto
end tell

delay 0.3

tell application "System Events"
    keystroke "$1"
end tell
EOF
}

# --- Ventana 1: make up (también en el mismo directorio) ---
osascript <<EOF
tell application "Terminal"
    activate
    do script "cd \"$START_DIR\"" -- ahora sí: la de make up también en el directory correcto
end tell

delay 0.3

tell application "System Events"
    keystroke "make up"
end tell
EOF

# --- Ventana 2 ---
write_cmd "docker logs -f distribuidos-coffee-shop-analysis-cleaner_transactions-2"

# --- Ventana 3 ---
write_cmd "docker kill wsm_transactions"

# --- Ventana 4 ---
write_cmd "docker logs -f wsm_transactions"

# --- Ventana 5 ---
write_cmd "docker logs -f wsm_transactions_2"

# --- Ventana 6 ---
write_cmd "docker logs -f wsm_transactions_3"
