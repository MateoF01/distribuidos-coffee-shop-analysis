#!/bin/bash
echo "Cantidad de clientes: $1"
echo "Cantidad de requests por cliente: $2"
echo "Cantidad maxima de clientes (Gateway): $3"
python3 mi-generador.py $1 $2 $3