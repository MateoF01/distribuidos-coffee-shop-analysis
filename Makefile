SHELL := /bin/bash
PWD := $(shell pwd)

# 🧩 Replica configuration (default values)
CLEANER_TRANSACTIONS_REPLICAS ?= 3
CLEANER_TRANSACTION_ITEMS_REPLICAS ?= 3
CLEANER_USERS_REPLICAS ?= 2
CLEANER_STORES_REPLICAS ?= 2
CLEANER_MENU_ITEMS_REPLICAS ?= 2
CLEANER_TRANSACTIONS_REPLICAS_Q4 ?= 3

GROUPER_Q2_V2_REPLICAS ?= 2
GROUPER_Q3_V2_REPLICAS ?= 2
GROUPER_Q4_V2_REPLICAS ?= 2

REDUCER_Q2_REPLICAS ?= 1
REDUCER_Q3_REPLICAS ?= 1
REDUCER_Q4_REPLICAS ?= 1

TOPPER_Q2_REPLICAS ?= 1
TOPPER_Q3_REPLICAS ?= 1
TOPPER_Q4_REPLICAS ?= 1

TEMPORAL_FILTER_TRANSACTIONS_REPLICAS ?= 2
TEMPORAL_FILTER_TRANSACTION_ITEMS_REPLICAS ?= 2
AMOUNT_FILTER_TRANSACTIONS_REPLICAS ?= 2

SPLITTER_Q1_REPLICAS ?= 2
SORTER_Q1_V2_REPLICAS ?= 1

JOINER_V2_Q2_REPLICAS ?= 2
JOINER_V2_Q3_REPLICAS ?= 2
JOINER_V2_Q4_REPLICAS ?= 2

CLIENT_REPLICAS ?= 1
REQUESTS_PER_CLIENT ?= 1
GATEWAY_MAX_PROCESSES ?= 5

default: help

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  up           - Build and start all services with scaling"
	@echo "  down         - Stop all services and clean up files"
	@echo "  restart      - Stop, clean, and start services"
	@echo "  logs         - Show logs from all services"
	@echo "  clean        - Remove output, temp, and client result files"
	@echo "  status       - Show running containers"
	@echo "  scale-clients- Scale client service dynamically (CLIENT_REPLICAS=N)"
	@echo ""
	@echo "Replica configuration (can be overridden):"
	@echo "  cleaner_transactions: $(CLEANER_TRANSACTIONS_REPLICAS)"
	@echo "  cleaner_transaction_items: $(CLEANER_TRANSACTION_ITEMS_REPLICAS)"
	@echo "  cleaner_users: $(CLEANER_USERS_REPLICAS)"
	@echo "  cleaner_stores: $(CLEANER_STORES_REPLICAS)"
	@echo "  cleaner_menu_items: $(CLEANER_MENU_ITEMS_REPLICAS)"
	@echo "  cleaner_transactions_q4: $(CLEANER_TRANSACTIONS_REPLICAS_Q4)"
	@echo "  grouper_q2_v2: $(GROUPER_Q2_V2_REPLICAS)"
	@echo "  grouper_q3_v2: $(GROUPER_Q3_V2_REPLICAS)"
	@echo "  grouper_q4_v2: $(GROUPER_Q4_V2_REPLICAS)"
	@echo "  reducer_q2: $(REDUCER_Q2_REPLICAS)"
	@echo "  reducer_q3: $(REDUCER_Q3_REPLICAS)"
	@echo "  reducer_q4: $(REDUCER_Q4_REPLICAS)"
	@echo "  temporal_filter_transactions: $(TEMPORAL_FILTER_TRANSACTIONS_REPLICAS)"
	@echo "  temporal_filter_transaction_items: $(TEMPORAL_FILTER_TRANSACTION_ITEMS_REPLICAS)"
	@echo "  amount_filter_transactions: $(AMOUNT_FILTER_TRANSACTIONS)"
	@echo "  splitter_q1: $(SPLITTER_Q1_REPLICAS)"
	@echo "  sorter_q1_v2: $(SORTER_Q1_V2_REPLICAS)"
	@echo "  topper_q2: $(TOPPER_Q2_REPLICAS)"
	@echo "  topper_q3: $(TOPPER_Q3_REPLICAS)"
	@echo "  topper_q4: $(TOPPER_Q4_REPLICAS)"
	@echo ""
	@echo "Client/Gateway configuration:"
	@echo "  CLIENT_REPLICAS:       $(CLIENT_REPLICAS)"
	@echo "  REQUESTS_PER_CLIENT:   $(REQUESTS_PER_CLIENT)"
	@echo "  GATEWAY_MAX_PROCESSES: $(GATEWAY_MAX_PROCESSES)"
	@echo ""
	@echo "Examples:"
	@echo "  make up  # Start with defaults"
	@echo "  CLIENT_REPLICAS=3 REQUESTS_PER_CLIENT=2 make up  # 3 clients, 2 requests each"
	@echo "  CLEANER_TRANSACTIONS_REPLICAS=5 make up  # Only scale transactions cleaners"
	@echo "  CLEANER_TRANSACTIONS_REPLICAS=3 CLEANER_USERS_REPLICAS=4 make up"

# 📦 Generate docker-compose.yml
.PHONY: generate
generate:
	@REQUESTS_PER_CLIENT=$(REQUESTS_PER_CLIENT) \
	 GATEWAY_MAX_PROCESSES=$(GATEWAY_MAX_PROCESSES) \
	 ./generar-compose.sh

# 🛠️ Build
.PHONY: build
build: generate
	docker compose build

# 🚀 Up
.PHONY: up
up: build
	@echo "Starting services with replicas:"
	@echo "  clients=$(CLIENT_REPLICAS), transactions=$(CLEANER_TRANSACTIONS_REPLICAS), transaction_items=$(CLEANER_TRANSACTION_ITEMS_REPLICAS), users=$(CLEANER_USERS_REPLICAS), stores=$(CLEANER_STORES_REPLICAS), menu_items=$(CLEANER_MENU_ITEMS_REPLICAS)"
	docker compose up -d \
		--scale client=$(CLIENT_REPLICAS) \
	  --scale cleaner_transactions=$(CLEANER_TRANSACTIONS_REPLICAS) \
	  --scale cleaner_transaction_items=$(CLEANER_TRANSACTION_ITEMS_REPLICAS) \
	  --scale cleaner_users=$(CLEANER_USERS_REPLICAS) \
	  --scale cleaner_stores=$(CLEANER_STORES_REPLICAS) \
	  --scale cleaner_menu_items=$(CLEANER_MENU_ITEMS_REPLICAS) \
	  --scale cleaner_transactions_q4=$(CLEANER_TRANSACTIONS_REPLICAS_Q4) \
	  --scale grouper_q2_v2=$(GROUPER_Q2_V2_REPLICAS) \
	  --scale grouper_q3_v2=$(GROUPER_Q3_V2_REPLICAS) \
	  --scale grouper_q4_v2=$(GROUPER_Q4_V2_REPLICAS) \
	  --scale reducer_q2=$(REDUCER_Q2_REPLICAS) \
	  --scale reducer_q3=$(REDUCER_Q3_REPLICAS) \
	  --scale reducer_q4=$(REDUCER_Q4_REPLICAS) \
	  --scale temporal_filter_transactions=$(TEMPORAL_FILTER_TRANSACTIONS_REPLICAS) \
	  --scale temporal_filter_transaction_items=$(TEMPORAL_FILTER_TRANSACTION_ITEMS_REPLICAS) \
    --scale amount_filter_transactions=$(AMOUNT_FILTER_TRANSACTIONS_REPLICAS) \
	  --scale splitter_q1=$(SPLITTER_Q1_REPLICAS) \
	  --scale sorter_q1_v2=$(SORTER_Q1_V2_REPLICAS) \
		--scale joiner_v2_q2=$(JOINER_V2_Q2_REPLICAS) \
		--scale joiner_v2_q3=$(JOINER_V2_Q3_REPLICAS) \
		--scale joiner_v2_q4=$(JOINER_V2_Q4_REPLICAS) \
		--scale topper_q2=$(TOPPER_Q2_REPLICAS) \
		--scale topper_q3=$(TOPPER_Q3_REPLICAS) \
		--scale topper_q4=$(TOPPER_Q4_REPLICAS) \



# 🧹 Down and cleanup
.PHONY: down
down:
	docker compose stop -t 5
	docker compose down
	@echo "🧹 Cleaning up output, temp, WSM, splitter, and client results..."
	@docker run --rm \
		-v $(PWD)/output:/tmp/output \
		-v $(PWD)/output_wsm:/tmp/output_wsm \
		-v $(PWD)/output_shm:/tmp/output_shm \
		-v $(PWD)/output_gateway:/tmp/output_gateway \
		-v $(PWD)/grouper_v2/temp/q2:/tmp/grouper_q2 \
		-v $(PWD)/grouper_v2/temp/q3:/tmp/grouper_q3 \
		-v $(PWD)/grouper_v2/temp/q4:/tmp/grouper_q4 \
		-v $(PWD)/splitter/temp:/tmp/splitter_temp \
		-v $(PWD)/reducer/temp:/tmp/reducer_temp \
		-v $(PWD)/topper/temp:/tmp/topper_temp \
		-v $(PWD)/client/results:/tmp/client_results \
		alpine:latest sh -c "rm -rf \
			/tmp/output/* \
			/tmp/output_wsm/* \
			/tmp/output_shm/* \
			/tmp/output_gateway/* \
			/tmp/grouper_q2/* \
			/tmp/grouper_q3/* \
			/tmp/grouper_q4/* \
			/tmp/splitter_temp/* \
			/tmp/reducer_temp/* \
			/tmp/topper_temp/* \
			/tmp/client_results/* 2>/dev/null || true"
	@echo "✅ Cleanup complete!"


# 🔁 Restart
.PHONY: restart
restart: down up
	@echo "🔁 Services restarted with cleanup!"

# 🧾 Logs
.PHONY: logs
logs:
	docker compose logs -t -f

# 🧽 Clean output/temp files
.PHONY: clean
clean:
	@echo "🧹 Cleaning up output, temp, WSM, splitter, and client result directories..."
	@docker run --rm \
		-v $(PWD)/output:/tmp/output \
		-v $(PWD)/output_wsm:/tmp/output_wsm \
		-v $(PWD)/output_shm:/tmp/output_shm \
		-v $(PWD)/output_gateway:/tmp/output_gateway \
		-v $(PWD)/grouper_v2/temp/q2:/tmp/grouper_q2 \
		-v $(PWD)/grouper_v2/temp/q3:/tmp/grouper_q3 \
		-v $(PWD)/grouper_v2/temp/q4:/tmp/grouper_q4 \
		-v $(PWD)/splitter/temp:/tmp/splitter_temp \
		-v $(PWD)/reducer/temp:/tmp/reducer_temp \
		-v $(PWD)/topper/temp:/tmp/topper_temp \
		-v $(PWD)/client/results:/tmp/client_results \
		alpine:latest sh -c "rm -rf \
			/tmp/output/* \
			/tmp/output_wsm/* \
			/tmp/output_shm/* \
			/tmp/output_gateway/* \
			/tmp/grouper_q2/* \
			/tmp/grouper_q3/* \
			/tmp/grouper_q4/* \
			/tmp/splitter_temp/* \
			/tmp/reducer_temp/* \
			/tmp/topper_temp/* \
			/tmp/client_results/* 2>/dev/null || true"
	@echo "✅ Cleanup complete!"

# 📊 Status
.PHONY: status
status:
	docker compose ps

# 🛑 Stop
.PHONY: stop
stop:
	docker compose stop

# ❌ Remove
.PHONY: rm
rm: stop
	docker compose rm -f

# 📈 Scale client replicas dynamically
.PHONY: scale-clients
scale-clients:
	@echo "Scaling client service to $(CLIENT_REPLICAS) replicas"
	docker compose up -d --scale client=$(CLIENT_REPLICAS)

# 📈 Scale cleaner replicas dynamically
.PHONY: scale-cleaners
scale-cleaners:
	@echo "Scaling cleaner services:"
	@echo "  transactions=$(CLEANER_TRANSACTIONS_REPLICAS)"
	@echo "  transaction_items=$(CLEANER_TRANSACTION_ITEMS_REPLICAS)"
	@echo "  users=$(CLEANER_USERS_REPLICAS)"
	@echo "  stores=$(CLEANER_STORES_REPLICAS)"
	@echo "  menu_items=$(CLEANER_MENU_ITEMS_REPLICAS)"
	docker compose up -d \
	  --scale cleaner_transactions=$(CLEANER_TRANSACTIONS_REPLICAS) \
	  --scale cleaner_transaction_items=$(CLEANER_TRANSACTION_ITEMS_REPLICAS) \
	  --scale cleaner_users=$(CLEANER_USERS_REPLICAS) \
	  --scale cleaner_stores=$(CLEANER_STORES_REPLICAS) \
	  --scale cleaner_menu_items=$(CLEANER_MENU_ITEMS_REPLICAS)

# 🔍 Show current replica setup
.PHONY: show-replicas
show-replicas:
	@echo "Current replica configuration:"
	@echo "  cleaner_transactions: $(CLEANER_TRANSACTIONS_REPLICAS)"
	@echo "  cleaner_transaction_items: $(CLEANER_TRANSACTION_ITEMS_REPLICAS)"
	@echo "  cleaner_users: $(CLEANER_USERS_REPLICAS)"
	@echo "  cleaner_stores: $(CLEANER_STORES_REPLICAS)"
	@echo "  cleaner_menu_items: $(CLEANER_MENU_ITEMS_REPLICAS)"
	@echo "  cleaner_transactions_q4: $(CLEANER_TRANSACTIONS_REPLICAS_Q4)"
	@echo "  grouper_q2_v2: $(GROUPER_Q2_V2_REPLICAS)"
	@echo "  grouper_q3_v2: $(GROUPER_Q3_V2_REPLICAS)"
	@echo "  grouper_q4_v2: $(GROUPER_Q4_V2_REPLICAS)"
	@echo "  reducer_q2: $(REDUCER_Q2_REPLICAS)"
	@echo "  reducer_q3: $(REDUCER_Q3_REPLICAS)"
	@echo "  reducer_q4: $(REDUCER_Q4_REPLICAS)"
	@echo "  temporal_filter_transactions: $(TEMPORAL_FILTER_TRANSACTIONS_REPLICAS)"
	@echo "  temporal_filter_transaction_items: $(TEMPORAL_FILTER_TRANSACTION_ITEMS_REPLICAS)"
	@echo "  amount_filter_transactions: $(AMOUNT_FILTER_TRANSACTIONS_REPLICAS)"
	@echo "  splitter_q1: $(SPLITTER_Q1_REPLICAS)"
	@echo "  sorter_q1_v2: $(SORTER_Q1_V2_REPLICAS)"
	@echo "  joiner_v2_q2: $(JOINER_V2_Q2_REPLICAS)"
	@echo "  joiner_v2_q3: $(JOINER_V2_Q3_REPLICAS)"
	@echo "  joiner_v2_q4: $(JOINER_V2_Q4_REPLICAS)"
	@echo "  topper_q2: $(TOPPER_Q2_REPLICAS)"
	@echo "  topper_q3: $(TOPPER_Q3_REPLICAS)"
	@echo "  topper_q4: $(TOPPER_Q4_REPLICAS)"
	@echo "  client: $(CLIENT_REPLICAS)"
	@echo ""
	@echo "Running containers:"
	@docker compose ps | grep -E "(cleaner_transactions|cleaner_transaction_items|cleaner_users|cleaner_stores|cleaner_menu_items)" || echo "No cleaner containers running"

# 🪵 Logs for cleaner services only
.PHONY: logs-cleaners
logs-cleaners:
	docker compose logs -f cleaner_transactions cleaner_transaction_items cleaner_users cleaner_stores cleaner_menu_items
