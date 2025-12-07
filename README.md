# Arreglos tolerancia a fallos.

Se agregó la capacidad de revivir contenedores caídos. Para todos los grupos de contenedores que estaban conectados a un WSM, incorporamos un mecanismo de heartbeats mediante el cual los workers notifican periódicamente que siguen vivos. Si un WSM detecta un timeout, ejecuta un docker stop seguido de un reinicio del contenedor para que se reincorpore al sistema.

En el caso de los contenedores que originalmente no tenían WSM —ya que no requerían control de estados— se agregó un WSM dedicado que también mantiene el intercambio de heartbeats y garantiza la recuperación de workers caídos.

Además, en el conjunto de WSM Líder y Backups incorporamos un heartbeat bidireccional, que permite al líder detectar si alguno de los backups se cae y reiniciarlo automáticamente. A su vez, si el líder falla y un backup toma su lugar, este nuevo líder también se encargará de revivir cualquier réplica caída.

Una vez resueltos estos problemas, comenzamos a testear exhaustivamente usando una política al estilo Chaos Monkey. A lo largo de múltiples ejecuciones fuimos detectando y corrigiendo errores, reforzando la importancia de probar el sistema en un entorno altamente exigente. Muchos de estos fixes pueden verse reflejados en los commits, en varios casos junto con una descripción del problema solucionado.

Para correr el sistema se puede realizar make up, y paralelamente correr el script ./chaos_gorilla.sh

# Coffee Shop Analysis - Sistema Distribuido

Un **sistema distribuido de procesamiento de datos tolerante a fallas** construido con Python, RabbitMQ y Docker que analiza datos transaccionales de cafeterías usando una arquitectura de pipeline estilo MapReduce.

> **Documentación completa**: Ver [Documento de Arquitectura.pdf](./Documento%20de%20Arquitectura.pdf) para diagramas detallados, decisiones de diseño y especificaciones técnicas.

## Índice

- [Descripción General](#descripción-general)
- [Arquitectura](#arquitectura)
- [Consultas de Datos](#consultas-de-datos)
- [Componentes Principales](#componentes-principales)
- [Cómo Empezar](#cómo-empezar)
- [Comandos de Uso](#comandos-de-uso)
- [Escalado y Replicación](#escalado-y-replicación)
- [Tolerancia a Fallas](#tolerancia-a-fallas)
- [Testing y Validación](#testing-y-validación)
- [Estructura del Proyecto](#estructura-del-proyecto)

---

## Descripción General

Este sistema procesa datos a gran escala de cafeterías (transacciones, usuarios, sucursales, productos del menú) a través de un pipeline distribuido para responder consultas de inteligencia de negocios. Características principales:

- **Procesamiento Distribuido**: Workers paralelos usando colas de mensajes RabbitMQ
- **Escalado Horizontal**: Cantidad de réplicas configurable para cada etapa de procesamiento
- **Tolerancia a Fallas**: Worker State Manager (WSM) con elección de líder y recuperación ante crashes
- **Semántica Exactly-Once**: Seguimiento de posiciones y detección de duplicados
- **Soporte Multi-Cliente**: Manejo concurrente de requests con aislamiento por request

### Fuentes de Datos

| Dataset | Descripción | Tamaño de Muestra |
|---------|-------------|-------------------|
| **Transactions** | Registros de compras con montos y timestamps | ~620K filas/mes |
| **Transaction Items** | Items individuales por transacción | Variable |
| **Users** | Información de clientes con fechas de nacimiento | Múltiples archivos |
| **Stores** | 10 sucursales de cafeterías en Malasia | 10 filas |
| **Menu Items** | 8 productos de bebidas | 8 filas |

---

## Arquitectura

Para diagramas detallados de la arquitectura, consultar el [Documento de Arquitectura.pdf](./Documento%20de%20Arquitectura.pdf).

### Patrón de Comunicación

- **Cliente - Gateway**: Sockets TCP con protocolo binario
- **Gateway - Workers**: Colas de mensajes RabbitMQ
- **Workers - WSM**: Protocolo TCP JSON para coordinación de estado

---

## Consultas de Datos

El sistema responde 4 consultas de inteligencia de negocios sobre datos de 2024 y 2025:

### Q1: Transacciones de Alto Valor en Horario Comercial
> Transacciones (ID y monto) realizadas durante 2024 y 2025 entre las **06:00 AM y las 11:00 PM** con monto total **mayor o igual a $75**.

**Pipeline**: `Transactions → Cleaner → Temporal Filter → Amount Filter → Splitter → Sorter → Sender`

**Filtros aplicados**:
- Rango temporal: 2024-2025
- Horario: 06:00 - 23:00
- Monto mínimo: $75

**Salida**: Lista de transacciones ordenadas por ID con columnas `transaction_id, final_amount`

### Q2: Análisis de Productos por Mes

#### Q2-A: Productos Más Vendidos por Cantidad
> Para cada mes en 2024 y 2025, el producto más vendido (nombre y cantidad).

**Pipeline**: `Transaction Items → Cleaner → Temporal Filter → Grouper → Reducer → Topper → Joiner (con Menu Items) → Sender`

**Salida**: Una fila por mes con `mes, producto, cantidad`

#### Q2-B: Productos que Más Ganancias Generaron
> Para cada mes en 2024 y 2025, el producto que más ganancias generó (nombre y monto).

**Pipeline**: Similar a Q2-A pero agregando por monto en lugar de cantidad

**Salida**: Una fila por mes con `mes, producto, monto_total`

### Q3: TPV por Sucursal y Semestre
> Total Payment Value (TPV) por cada semestre en 2024 y 2025, para cada sucursal, considerando solo transacciones realizadas entre las **06:00 AM y las 11:00 PM**.

**Pipeline**: `Transactions → Cleaner → Temporal Filter → Grouper → Reducer → Topper → Joiner (con Stores) → Sorter → Sender`

**Filtros aplicados**:
- Rango temporal: 2024-2025
- Horario: 06:00 - 23:00
- Agrupación: Por sucursal y semestre

**Salida**: Rankings de facturación total por sucursal para cada semestre

### Q4: Top 3 Clientes por Sucursal
> Fecha de cumpleaños de los 3 clientes que han hecho más compras durante 2024 y 2025, para cada sucursal.

**Pipeline**: `Transactions → Cleaner → Temporal Filter → Grouper → Reducer → Topper → Joiner (con Stores + Users) → Sorter → Sender`

**Agrupación**: Por sucursal, contando cantidad de transacciones por cliente

**Salida**: Top 3 clientes por sucursal con `sucursal, cliente, cantidad_compras, fecha_nacimiento` (maneja empates)

---

## Componentes Principales

### Gateway (`gateway/`)
Punto de entrada del sistema distribuido. Maneja:
- Conexiones TCP multi-cliente
- Asignación y ruteo de request IDs
- Agregación y entrega de resultados
- Shutdown graceful y recuperación

### Workers (`cleaner/`, `filter/`, `grouper_v2/`, `joiner_v2/`, etc.)
Etapas de procesamiento que heredan de la clase base `Worker`:
- **Cleaners**: Validación y normalización de datos
- **Filters**: Filtros temporales (rangos de fechas) y por monto
- **Groupers**: Agregación por claves (sucursal, mes, usuario)
- **Reducers**: Merge de resultados parciales entre réplicas
- **Joiners**: Enriquecimiento de datos multi-fuente
- **Toppers**: Selección top-N con manejo de empates
- **Sorters**: Ordenamiento de resultados
- **Senders**: Entrega final de resultados

### Worker State Manager (`WSM/`)
Coordinador de tolerancia a fallas con:
- **Estado replicado**: 3 nodos WSM por tipo de worker
- **Elección de líder**: Algoritmo Bully con health checks
- **Tracking de posiciones**: Archivos append-only para recuperación ante crashes
- **Detección de duplicados**: Saltear mensajes ya procesados
- **Sincronización de END**: Coordinar finalización de streams entre réplicas

### Middleware (`middleware/`)
Capa de abstracción de RabbitMQ:
- `CoffeeMessageMiddlewareQueue`: Mensajería directa por cola
- `CoffeeMessageMiddlewareExchange`: Patrones pub/sub
- Manejo de conexiones thread-safe
- Publisher confirms para entrega garantizada

### Protocolo (`shared/protocol.py`)
Formato de mensajes binario:
```
Header (15 bytes):
- 1 byte:  tipo de mensaje (DATA/END/NOTI)
- 1 byte:  tipo de dato
- 1 byte:  request ID
- 8 bytes: posición (uint64)
- 4 bytes: longitud del payload
Payload (N bytes): datos CSV codificados en UTF-8
```

---

## Cómo Empezar

### Prerrequisitos
- Docker & Docker Compose
- Make (opcional, para comandos de conveniencia)
- Python 3.x (para scripts de validación)

### Inicio Rápido

```bash
# Clonar el repositorio
git clone https://github.com/MateoF01/distribuidos-coffee-shop-analysis.git
cd distribuidos-coffee-shop-analysis

# Levantar el sistema
make up

# Ver logs
make logs

# Bajar y limpiar
make down
```

---

## Comandos de Uso

| Comando | Descripción |
|---------|-------------|
| `make up` | Buildea y levanta todos los servicios con escalado default |
| `make down` | Baja los servicios y limpia todos los archivos de output/temp |
| `make restart` | Reinicio completo con limpieza |
| `make logs` | Seguir logs de todos los contenedores |
| `make status` | Mostrar estado de contenedores corriendo |
| `make clean` | Limpiar archivos de output sin bajar servicios |
| `make build` | Solo buildear imágenes Docker |
| `make help` | Mostrar todos los comandos disponibles |

### Docker Compose Manual

```bash
# Levantar servicios
docker compose up --build -d

# Bajar sin limpiar
docker compose down

# Ver logs de servicios específicos
docker compose logs -f gateway client
```

---

## Escalado y Replicación

Configurá la cantidad de réplicas mediante variables de entorno:

```bash
# Escalar workers específicos
CLEANER_TRANSACTIONS_REPLICAS=5 \
GROUPER_Q2_V2_REPLICAS=3 \
CLIENT_REPLICAS=2 \
make up
```

### Configuración de Réplicas por Defecto

| Componente | Réplicas Default |
|-----------|-----------------|
| Cleaners (por tipo de dato) | 2 |
| Temporal Filters | 2 |
| Amount Filters | 2 |
| Groupers (por query) | 2 |
| Reducers (por query) | 2 |
| Joiners (por query) | 2 |
| Splitters | 2 |
| Sorters | 2 |
| Clients | 1 |
| Nodos WSM (por grupo) | 3 |

### Escalado Dinámico

```bash
# Escalar clientes en runtime
make scale-clients CLIENT_REPLICAS=5

# Escalar cleaners
make scale-cleaners CLEANER_TRANSACTIONS_REPLICAS=10
```

---

## Tolerancia a Fallas

### Worker State Manager (WSM)

Cada grupo de workers está coordinado por 3 réplicas WSM:
- **Elección de Líder**: Failover automático cuando el líder crashea
- **Persistencia de Estado**: Archivos JSON de estado + logs de posiciones append-only
- **Recuperación ante Crashes**: Los workers reconectan y retoman desde el último checkpoint

### Procesamiento Exactly-Once

1. El worker se registra con el WSM antes de procesar
2. Se chequea la posición para detectar duplicados antes de procesar
3. Estado actualizado a `PROCESSING`
4. Se realiza el trabajo
5. Estado actualizado a `WAITING` (posición persistida)
6. Si crashea durante el procesamiento, el mensaje se reencola y se reprocesa

### Testeando Tolerancia a Fallas

```bash
# Chaos Gorilla - Matar workers random
./chaos_gorilla.sh

# Matar tipo de worker específico
docker kill coffee-shop-22-cleaner_transactions-1

# Los workers se recuperan automáticamente y retoman el procesamiento
```

---

## Testing y Validación

### Validar Resultados

```bash
# Validar un request específico de un cliente
./validate_results.sh client/results/client_xxxxx/request_1

# Validar todos los requests de un cliente
./validate_results.sh client/results/client_xxxxx

# Usar validación con dataset completo
./validate_results.sh client/results/client_xxxxx full
```

### Resultados Esperados

Los resultados se comparan contra archivos de referencia pre-calculados de Kaggle en `results/`:
- `kaggle_q1.csv` / `full_kaggle_q1.csv`
- `kaggle_q2_a.csv` / `full_kaggle_q2_a.csv`
- `kaggle_q2_b.csv` / `full_kaggle_q2_b.csv`
- `kaggle_q3.csv` / `full_kaggle_q3.csv`
- `kaggle_q4.csv` / `full_kaggle_q4.csv`

### Test de Integración WSM

```bash
docker compose -f docker-compose.test.yml up --build --scale cleaner_transactions=5
```

---

## Estructura del Proyecto

```
├── gateway/              # Servidor punto de entrada
├── client/               # Cliente que envía datos
├── cleaner/              # Workers de validación de datos
├── filter/               # Filtros temporales y por monto
├── grouper_v2/           # Workers de agregación
├── reducer/              # Merge de resultados
├── joiner_v2/            # Joins multi-fuente
├── topper/               # Selección top-N
├── splitter/             # División en chunks
├── sorter_v2/            # Ordenamiento de resultados
├── sender/               # Entrega de resultados
├── coordinator/          # Coordinación de señales END
├── WSM/                  # Worker State Manager
├── middleware/           # Abstracción de RabbitMQ
├── shared/               # Protocolo y utilidades
├── data/                 # Dataset de muestra
├── data_full/            # Dataset completo
├── results/              # Resultados esperados
├── tests/                # Tests unitarios
├── tests_fault_tolerance/# Tests de tolerancia a fallas
├── docker-compose.yml    # Orquestación principal
├── Makefile              # Comandos de conveniencia
└── README.md             # Este archivo
```

---

## Tecnologías

- **Python 3.x**: Lenguaje principal
- **RabbitMQ**: Message broker
- **Docker & Docker Compose**: Containerización
- **Pika**: Cliente Python de RabbitMQ
- **Alpine Linux**: Imagen base de contenedores

---

## Autores

- Universidad de Buenos Aires - Facultad de Ingeniería
- Sistemas Distribuidos I - 2024

---

## Licencia

Este proyecto es con fines educativos como parte de la materia Sistemas Distribuidos. 