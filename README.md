# coffee-shop-22

## Como correr el test de WSM

 docker compose -f docker-compose.test.yml up --build --scale cleaner_transactions=5


## Como levantar el sistema? 

```bash
make up
```

## Como bajar el sistema?

```bash
make down    # Baja el sistema y limpia archivos automáticamente
```

## Comandos disponibles

- `make up` - Construye y levanta todos los servicios
- `make down` - Baja todos los servicios y limpia archivos automáticamente
- `make restart` - Reinicia todos los servicios con limpieza
- `make logs` - Muestra los logs de todos los servicios
- `make clean` - Solo limpia los archivos de output y temp
- `make build` - Solo construye las imágenes Docker
- `make status` - Muestra el estado de los servicios
- `make stop` - Solo detiene los servicios (sin limpieza)
- `make help` - Muestra todos los comandos disponibles

## Alternativas manuales

Si prefieres usar docker compose directamente:

```bash
# Levantar servicios
docker compose up --build -d

# Bajar servicios sin limpieza
docker compose down

# Limpieza manual de archivos
docker run --rm -v ./output:/tmp/output -v ./grouper/temp:/tmp/temp alpine:latest sh -c "rm -rf /tmp/output/* /tmp/temp/* 2>/dev/null || true"
``` 