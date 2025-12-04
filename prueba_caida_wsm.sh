#!/bin/bash

# Directorio desde donde se ejecuta el script
START_DIR="$(pwd)"

# Detectar el sistema operativo
OS_TYPE="$(uname)"

# Función para abrir terminal según el sistema operativo
open_terminal() {
    local title="$1"
    local cmd="$2"
    
    if [[ "$OS_TYPE" == "Darwin" ]]; then
        # macOS
        osascript <<EOF
tell application "Terminal"
    activate
    do script "cd \"$START_DIR\""
end tell

delay 0.3

tell application "System Events"
    keystroke "$cmd"
end tell
EOF
    elif [[ "$OS_TYPE" == "Linux" ]]; then
        # Linux - abrir ventanas separadas con comando pre-escrito
        if command -v gnome-terminal &> /dev/null; then
            gnome-terminal --title="$title" -- bash -c "cd '$START_DIR' && exec bash -c 'read -e -p \"\" -i \"$cmd\"; exec bash'" &
        elif command -v xterm &> /dev/null; then
            xterm -T "$title" -e bash -c "cd '$START_DIR' && exec bash -c 'read -e -p \"\" -i \"$cmd\"; exec bash'" &
        elif command -v konsole &> /dev/null; then
            konsole -p tabtitle="$title" -e bash -c "cd '$START_DIR' && exec bash -c 'read -e -p \"\" -i \"$cmd\"; exec bash'" &
        else
            echo "No se encontró un emulador de terminal compatible en Linux"
            exit 1
        fi
    else
        echo "Sistema operativo no soportado: $OS_TYPE"
        exit 1
    fi
}

# --- Ventana 1: make up ---
if [[ "$OS_TYPE" == "Darwin" ]]; then
    osascript <<EOF
tell application "Terminal"
    activate
    do script "cd \"$START_DIR\""
end tell

delay 0.3

tell application "System Events"
    keystroke "make up"
end tell
EOF
elif [[ "$OS_TYPE" == "Linux" ]]; then
    if command -v gnome-terminal &> /dev/null; then
        gnome-terminal --title="Make Up" -- bash -c "cd '$START_DIR' && exec bash -c 'read -e -p \"\" -i \"make up\"; exec bash'" &
    elif command -v xterm &> /dev/null; then
        xterm -T "Make Up" -e bash -c "cd '$START_DIR' && exec bash -c 'read -e -p \"\" -i \"make up\"; exec bash'" &
    elif command -v konsole &> /dev/null; then
        konsole -p tabtitle="Make Up" -e bash -c "cd '$START_DIR' && exec bash -c 'read -e -p \"\" -i \"make up\"; exec bash'" &
    fi
fi

sleep 2

# --- Ventana 2 ---
open_terminal "Cleaner Logs" "docker logs -f coffee-shop-22-cleaner_transactions-2"

# --- Ventana 3 ---
open_terminal "Kill WSM" "docker kill wsm_transactions"

# --- Ventana 4 ---
open_terminal "WSM Logs 1" "docker logs -f wsm_transactions"

# --- Ventana 5 ---
open_terminal "WSM Logs 2" "docker logs -f wsm_transactions_2"

# --- Ventana 6 ---
open_terminal "WSM Logs 3" "docker logs -f wsm_transactions_3"
