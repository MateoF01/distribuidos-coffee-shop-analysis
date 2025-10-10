SHELL := /bin/bash
PWD := $(shell pwd)

# Replica configuration - can be overridden via environment variables or command line
CLEANER_TRANSACTIONS_REPLICAS ?= 1
CLEANER_TRANSACTION_ITEMS_REPLICAS ?= 1

default: help

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  up      - Start all services"
	@echo "  down    - Stop all services and clean up files"
	@echo "  restart - Stop, clean up, and start services"
	@echo "  logs    - Show logs from all services"
	@echo "  clean   - Clean up output and temp files from all components"
	@echo "  build   - Build all Docker images"
	@echo ""
	@echo "Replica configuration (can be overridden):"
	@echo "  Current cleaner_transactions replicas: $(CLEANER_TRANSACTIONS_REPLICAS)"
	@echo "  Current cleaner_transaction_items replicas: $(CLEANER_TRANSACTION_ITEMS_REPLICAS)"
	@echo ""
	@echo "Examples:"
	@echo "  make up  # Use default replica counts"
	@echo "  CLEANER_TRANSACTIONS_REPLICAS=5 make up  # Override transactions replicas"
	@echo "  make up CLEANER_TRANSACTIONS_REPLICAS=2 CLEANER_TRANSACTION_ITEMS_REPLICAS=4"

.PHONY: build
build:
	docker compose build

.PHONY: up
up: build
	@echo "Starting services with cleaner_transactions replicas: $(CLEANER_TRANSACTIONS_REPLICAS), cleaner_transaction_items replicas: $(CLEANER_TRANSACTION_ITEMS_REPLICAS)"
	docker compose up -d --scale cleaner_transactions=$(CLEANER_TRANSACTIONS_REPLICAS) --scale cleaner_transaction_items=$(CLEANER_TRANSACTION_ITEMS_REPLICAS)

.PHONY: down
down:
	docker compose stop -t 5
	docker compose down
	@echo "Cleaning up output and temp directories..."
	@docker run --rm -v ./output:/tmp/output -v ./grouper/temp:/tmp/grouper_temp -v ./topper/temp:/tmp/topper_temp alpine:latest sh -c "rm -rf /tmp/output/* /tmp/grouper_temp/* /tmp/topper_temp/* 2>/dev/null || true"
	@echo "All services stopped and cleanup completed!"

.PHONY: restart
restart: down up
	@echo "Services restarted with cleanup!"

.PHONY: logs
logs:
	docker compose logs -f

.PHONY: clean
clean:
	@echo "Cleaning up output and temp directories..."
	@docker run --rm -v ./output:/tmp/output -v ./grouper/temp:/tmp/grouper_temp -v ./topper/temp:/tmp/topper_temp alpine:latest sh -c "rm -rf /tmp/output/* /tmp/grouper_temp/* /tmp/topper_temp/* 2>/dev/null || true"
	@echo "Cleanup completed!"

.PHONY: status
status:
	docker compose ps

.PHONY: stop
stop:
	docker compose stop

.PHONY: rm
rm: stop
	docker compose rm -f

# Replica management targets
.PHONY: scale-cleaners
scale-cleaners:
	@echo "Scaling cleaner services to transactions: $(CLEANER_TRANSACTIONS_REPLICAS), transaction_items: $(CLEANER_TRANSACTION_ITEMS_REPLICAS)"
	docker compose up -d --scale cleaner_transactions=$(CLEANER_TRANSACTIONS_REPLICAS) --scale cleaner_transaction_items=$(CLEANER_TRANSACTION_ITEMS_REPLICAS)

.PHONY: show-replicas
show-replicas:
	@echo "Current replica configuration:"
	@echo "  cleaner_transactions: $(CLEANER_TRANSACTIONS_REPLICAS)"
	@echo "  cleaner_transaction_items: $(CLEANER_TRANSACTION_ITEMS_REPLICAS)"
	@echo "Running containers:"
	@docker compose ps | grep -E "(cleaner_transactions|cleaner_transaction_items)" || echo "No cleaner containers running"

.PHONY: logs-cleaners
logs-cleaners:
	docker compose logs -f cleaner_transactions cleaner_transaction_items
