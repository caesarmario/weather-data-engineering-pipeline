####
## Makefile Weather Data Engineering Pipeline
## Mario Caesar // caesarmario87@gmail.com
####

# ---- Config ----
PROJECT           ?= weather-pipeline
COMPOSE_MAIN      ?= docker-compose.yml
COMPOSE_DATAHUB   ?= docker-compose.datahub.yml
COMPOSE_GRAFANA   ?= docker-compose.grafana.yml

ENV_FILE          ?= ./docker/.env

DCM = docker compose -p $(PROJECT) --env-file $(ENV_FILE) -f $(COMPOSE_MAIN)
DCD = docker compose -p $(PROJECT)-datahub --env-file $(ENV_FILE) -f $(COMPOSE_DATAHUB)
DCG = docker compose -p $(PROJECT)-grafana --env-file $(ENV_FILE) -f $(COMPOSE_GRAFANA)

.DEFAULT_GOAL := help

# ---- Help ----
.PHONY: help
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Main stack (Airflow, Postgres, MinIO, dbt, Adminer, Metabase, etc.)"
	@echo "  up                Start main stack (detached)"
	@echo "  up-build          Build & start main stack"
	@echo "  down              Stop main stack"
	@echo "  restart           Restart main stack"
	@echo "  logs              Tail main logs"
	@echo "  ps                List main services"
	@echo "  nuke              Stop & remove volumes (DATA LOSS)"
	@echo ""
	@echo "DataHub stack"
	@echo "  up-datahub        Start DataHub"
	@echo "  up-datahub-build  Build & start DataHub"
	@echo "  down-datahub      Stop DataHub"
	@echo "  logs-datahub      Tail DataHub logs"
	@echo "  ps-datahub        List DataHub services"
	@echo ""
	@echo "Grafana stack"
	@echo "  up-grafana        Start Grafana stack"
	@echo "  up-grafana-build  Build & start Grafana stack"
	@echo "  down-grafana      Stop Grafana stack"
	@echo "  logs-grafana      Tail Grafana logs"
	@echo "  ps-grafana        List Grafana services"
	@echo ""
	@echo "All stacks"
	@echo "  up-all            Start Main + DataHub + Grafana"
	@echo "  down-all          Stop  Main + DataHub + Grafana"
	@echo "  logs-all          Tail logs from all stacks"
	@echo ""
	@echo "Dev utilities"
	@echo "  airflow-cli ARGS=\"dags list\"     Run Airflow CLI in container"
	@echo "  dbt-pg CMD=\"run\"                 Run dbt against Postgres target"
	@echo "  dbt-ch CMD=\"run --target clickhouse\"  Run dbt against ClickHouse target"
	@echo "  metabase-logs                        Tail Metabase logs"
	@echo "  minio-logs                           Tail MinIO logs"

# ---- Main stack ----
.PHONY: up
up:
	$(DCM) up -d

.PHONY: up-build
up-build:
	$(DCM) up -d --build

.PHONY: down
down:
	$(DCM) down

.PHONY: restart
restart: down up

.PHONY: logs
logs:
	$(DCM) logs -f --tail=300

.PHONY: ps
ps:
	$(DCM) ps

.PHONY: nuke
nuke:
	$(DCM) down -v


# ---- DataHub ----
.PHONY: up-datahub
up-datahub:
	$(DCD) up -d

.PHONY: up-datahub-build
up-datahub-build:
	$(DCD) up -d --build

.PHONY: down-datahub
down-datahub:
	$(DCD) down

.PHONY: logs-datahub
logs-datahub:
	$(DCD) logs -f --tail=300

.PHONY: ps-datahub
ps-datahub:
	$(DCD) ps


# ---- Grafana ----
.PHONY: up-grafana
up-grafana:
	$(DCG) up -d

.PHONY: up-grafana-build
up-grafana-build:
	$(DCG) up -d --build

.PHONY: down-grafana
down-grafana:
	$(DCG) down

.PHONY: logs-grafana
logs-grafana:
	$(DCG) logs -f --tail=300

.PHONY: ps-grafana
ps-grafana:
	$(DCG) ps


# ---- All stacks ----
.PHONY: up-all
up-all: up up-datahub up-grafana

.PHONY: down-all
down-all: down down-datahub down-grafana

.PHONY: logs-all
logs-all:
	@echo ">>> Main stack logs";        $(DCM) logs -f --tail=50 & \
	 echo ">>> DataHub stack logs";    $(DCD) logs -f --tail=50 & \
	 echo ">>> Grafana stack logs";    $(DCG) logs -f --tail=50; wait


# ---- Dev utilities ----
# Airflow: run any CLI command inside the airflow container
.PHONY: airflow-cli
airflow-cli:
	$(DCM) exec airflow airflow $(ARGS)

# dbt (Postgres)
.PHONY: dbt-pg
dbt-pg:
	$(DCM) exec dbt dbt $(CMD)

# dbt (ClickHouse)
.PHONY: dbt-ch
dbt-ch:
	$(DCM) exec dbt dbt $(CMD)

# Tail specific service logs from the main stack
.PHONY: metabase-logs
metabase-logs:
	$(DCM) logs -f --tail=300 metabase_weather

.PHONY: minio-logs
minio-logs:
	$(DCM) logs -f --tail=300 minio