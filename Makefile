SHELL := /bin/bash
PWD := $(shell pwd)

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

.PHONY: build
build:
	docker compose build

.PHONY: up
up: build
	docker compose up -d

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
