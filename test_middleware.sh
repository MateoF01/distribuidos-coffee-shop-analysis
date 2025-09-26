#!/bin/bash

# Middleware Testing Script
# This script starts RabbitMQ, runs middleware tests, and cleans up

set -e  # Exit on any error

# Function to print simple output
print_status() {
    echo "[INFO] $1"
}

print_success() {
    echo "[SUCCESS] $1"
}

print_warning() {
    echo "[WARNING] $1"
}

print_error() {
    echo "[ERROR] $1"
}

# Function to cleanup on exit
cleanup() {
    print_status "Cleaning up..."
    docker compose down -v 2>/dev/null || true
    # Remove test container if it exists
    docker rm -f middleware-test-runner 2>/dev/null || true
    print_status "Cleanup completed"
}

# Set trap to cleanup on script exit
trap cleanup EXIT

# Check if docker compose is available
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed or not in PATH"
    exit 1
fi

if ! docker compose version &> /dev/null; then
    print_error "Docker Compose is not available"
    exit 1
fi

print_status "Starting RabbitMQ with Docker Compose..."

# Stop any existing containers
docker compose down -v 2>/dev/null || true

# Start RabbitMQ
if docker compose up -d rabbitmq; then
    print_success "RabbitMQ started successfully"
else
    print_error "Failed to start RabbitMQ"
    exit 1
fi

print_status "Waiting for RabbitMQ to be ready..."

# Wait for RabbitMQ to be ready (check if port 5672 is accepting connections)
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if docker compose exec rabbitmq rabbitmq-diagnostics ping 2>/dev/null; then
        print_success "RabbitMQ is ready!"
        break
    fi
    
    attempt=$((attempt + 1))
    if [ $attempt -eq $max_attempts ]; then
        print_error "RabbitMQ failed to start within expected time"
        docker compose logs rabbitmq
        exit 1
    fi
    
    print_status "Waiting for RabbitMQ... (attempt $attempt/$max_attempts)"
    sleep 2
done

# Give RabbitMQ a moment to fully initialize
sleep 5

print_status "Running middleware tests in Docker container..."

# Get the actual network name created by docker compose
NETWORK_NAME=$(docker network ls --format "{{.Name}}" | grep coffee-net)

# Create a test container with pytest and run the tests
# We'll use the Python image and mount the current directory
if docker run --rm \
    --name middleware-test-runner \
    --network "$NETWORK_NAME" \
    -v "$(pwd)":/app \
    -w /app \
    -e PYTHONPATH=/app \
    -e RABBITMQ_HOST=rabbitmq \
    python:3.9-slim \
    bash -c "pip install pytest pika && python -m pytest tests/test_middleware.py -v"; then
    print_success "All middleware tests passed!"
    test_result=0
else
    print_error "Some middleware tests failed!"
    test_result=1
fi

# Show RabbitMQ logs if tests failed
if [ $test_result -ne 0 ]; then
    print_warning "Showing RabbitMQ logs for debugging:"
    docker compose logs rabbitmq
fi

print_status "Stopping RabbitMQ..."
docker compose down -v

if [ $test_result -eq 0 ]; then
    print_success "Middleware testing completed successfully!"
else
    print_error "Middleware testing completed with failures!"
    exit 1
fi
