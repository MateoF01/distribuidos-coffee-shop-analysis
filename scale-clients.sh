#!/bin/bash

# Script to dynamically add new client containers using docker run
# Usage: ./scale-clients.sh <number_of_clients> [REQUESTS_PER_CLIENT] [BATCH_MAX_AMOUNT]

if [ $# -eq 0 ]; then
    echo "Usage: $0 <number_of_clients> [REQUESTS_PER_CLIENT] [BATCH_MAX_AMOUNT]"
    echo "Example: $0 3                    # Add 3 clients with defaults"
    echo "Example: $0 5 2 10000           # Add 5 clients with custom settings"
    exit 1
fi

NUM_CLIENTS=$1
REQUESTS_PER_CLIENT=${2:-1}
BATCH_MAX_AMOUNT=${3:-5000}

# Validate input is a positive integer
if ! [[ "$NUM_CLIENTS" =~ ^[1-9][0-9]*$ ]]; then
    echo "❌ Error: Please provide a positive integer for client count"
    exit 1
fi

# Get the Docker network name
PROJECT_NAME=$(basename $(pwd) | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]//g')
NETWORK="${PROJECT_NAME}_coffee-net"

# Check if network exists
if ! docker network ls --format '{{.Name}}' | grep -q "^${NETWORK}$"; then
    # Try alternative network name
    NETWORK="coffee-shop-22_coffee-net"
    if ! docker network ls --format '{{.Name}}' | grep -q "^${NETWORK}$"; then
        echo "❌ Network not found. Make sure the system is running with 'make up'"
        echo "   Looking for: ${PROJECT_NAME}_coffee-net or coffee-shop-22_coffee-net"
        exit 1
    fi
fi

# Find the client image - try multiple naming patterns
CLIENT_IMAGE=""
POSSIBLE_IMAGES=(
    "coffee-shop-22-client:latest"
    "coffee-shop-22_client:latest"
    "${PROJECT_NAME}-client:latest"
    "${PROJECT_NAME}_client:latest"
)

for img in "${POSSIBLE_IMAGES[@]}"; do
    if docker image ls --format '{{.Repository}}:{{.Tag}}' | grep -q "^${img}$"; then
        CLIENT_IMAGE="$img"
        break
    fi
done

# If no image found, build it and find the actual name
if [ -z "$CLIENT_IMAGE" ]; then
    echo "❌ Client image not found. Building it now..."
    docker compose build client
    if [ $? -ne 0 ]; then
        echo "❌ Failed to build client image"
        exit 1
    fi
    
    # After building, try to find the image again
    for img in "${POSSIBLE_IMAGES[@]}"; do
        if docker image ls --format '{{.Repository}}:{{.Tag}}' | grep -q "^${img}$"; then
            CLIENT_IMAGE="$img"
            break
        fi
    done
    
    # If still not found, get any image with 'client' in the name
    if [ -z "$CLIENT_IMAGE" ]; then
        CLIENT_IMAGE=$(docker image ls --format '{{.Repository}}:{{.Tag}}' | grep -i 'client' | grep 'latest' | head -n1)
        if [ -z "$CLIENT_IMAGE" ]; then
            echo "❌ Could not find client image after building. Available images:"
            docker image ls | grep -i client
            exit 1
        fi
    fi
    
    echo "✅ Found client image: $CLIENT_IMAGE"
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🚀 Adding $NUM_CLIENTS new client container(s)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📡 Network:              $NETWORK"
echo "🏗️  Image:               $CLIENT_IMAGE"
echo "🔧 Requests per client:  $REQUESTS_PER_CLIENT"
echo "📦 Batch max amount:     $BATCH_MAX_AMOUNT"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

STARTED_CONTAINERS=()
FAILED_CONTAINERS=()

for i in $(seq 1 $NUM_CLIENTS); do
    # Generate unique container name with timestamp
    TIMESTAMP=$(date +%s%N | cut -b1-13)
    CONTAINER_NAME="client_${TIMESTAMP}_${i}"
    
    echo "  🚀 Starting: $CONTAINER_NAME"
    
    # Run the client container with all environment variables as parameters
    CONTAINER_ID=$(docker run -d \
        --name "$CONTAINER_NAME" \
        --network "$NETWORK" \
        -e PYTHONUNBUFFERED=1 \
        -e DATA_DIR=/app/.data \
        -e SERVER_ADDRESS=gateway:5000 \
        -e BATCH_MAX_AMOUNT=$BATCH_MAX_AMOUNT \
        -e REQUESTS_PER_CLIENT=$REQUESTS_PER_CLIENT \
        -v "$(pwd)/data:/app/.data" \
        -v "$(pwd)/client/results:/app/client/results" \
        "$CLIENT_IMAGE" 2>&1)
    
    if [ $? -eq 0 ]; then
        STARTED_CONTAINERS+=("$CONTAINER_NAME")
        echo "    ✅ Started (ID: ${CONTAINER_ID:0:12})"
    else
        FAILED_CONTAINERS+=("$CONTAINER_NAME")
        echo "    ❌ Failed: $CONTAINER_ID"
    fi
    
    # Small delay to avoid overwhelming the system
    sleep 0.5
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 Summary"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Successfully started: ${#STARTED_CONTAINERS[@]}/$NUM_CLIENTS"
if [ ${#FAILED_CONTAINERS[@]} -gt 0 ]; then
    echo "❌ Failed: ${#FAILED_CONTAINERS[@]}"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ ${#STARTED_CONTAINERS[@]} -gt 0 ]; then
    echo ""
    echo "📈 All running client containers:"
    docker ps --filter "name=.*client.*" --format "table {{.Names}}\t{{.Status}}\t{{.CreatedAt}}" | head -n 20
    
    TOTAL_CLIENTS=$(docker ps --filter "name=.*client.*" --format "{{.Names}}" | wc -l)
    if [ $TOTAL_CLIENTS -gt 19 ]; then
        echo "... and $((TOTAL_CLIENTS - 19)) more"
    fi
fi

echo ""
echo "💡 To view logs: docker logs -f <container_name>"
echo "💡 To stop all clients: docker stop \$(docker ps -q --filter 'name=.*client.*')"