# One entry point for setup, run and test. `make help` lists the targets.
SHELL := /bin/bash
.DEFAULT_GOAL := help

-include .env
export

VENV := .venv
BIN := $(VENV)/bin
DBT_DIR := include/dbt
DBT := cd $(DBT_DIR) && DBT_PROFILES_DIR=. ../../$(BIN)/dbt
COMPOSE := docker compose
SCHEDULER := retail-gcp-pipeline-airflow-scheduler-1

.PHONY: help
help: ## List the available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

# --- Setup --------------------------------------------------------------------

.PHONY: setup
setup: .env ## Create the venv, install tools and git hooks, install dbt packages
	python3.12 -m venv $(VENV)
	$(BIN)/pip install -q -r requirements-dev.txt
	$(BIN)/pre-commit install
	$(DBT) deps

.env:
	cp .env.example .env
	@echo ">> .env created from .env.example: fill it in, then re-run make setup"
	@exit 1

.PHONY: fernet-key
fernet-key: ## Print a new Fernet key for AIRFLOW__CORE__FERNET_KEY
	@python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"

.PHONY: dataset
dataset: ## Download the full Online Retail II dataset into include/dataset/
	$(BIN)/python -I scripts/download_dataset.py

# --- Run ----------------------------------------------------------------------

.PHONY: up
up: ## Build and start Airflow + Metabase
	$(COMPOSE) up -d --build

.PHONY: down
down: ## Stop the stack
	$(COMPOSE) down

.PHONY: metabase
metabase: ## Create or update the Metabase dashboard (needs MB_ADMIN_* in .env)
	$(BIN)/python -I scripts/provision_metabase.py

.PHONY: run
run: ## Trigger the retail DAG
	docker exec $(SCHEDULER) airflow dags unpause retail
	docker exec $(SCHEDULER) airflow dags trigger retail

# --- dbt (dev target unless DBT_TARGET is set) ------------------------------

.PHONY: dbt-build
dbt-build: ## dbt build (seeds, models, tests) on the dev datasets
	$(DBT) build

.PHONY: dbt-docs
dbt-docs: ## Generate and serve the dbt docs site
	$(DBT) docs generate && $(DBT) docs serve

# --- Databricks (needs the databricks CLI, logged in) -------------------------

.PHONY: databricks-setup
databricks-setup: ## Create the retail catalog, schemas, landing volume and upload the CSVs
	./scripts/setup_databricks.sh

.PHONY: databricks-smoke
databricks-smoke: ## Run the Databricks pipeline SQL on a local Spark
	spark-submit databricks/tests/local_smoke_test.py \
		include/dataset/online_retail.csv include/dbt/seeds/non_merchandise_stock_codes.csv

.PHONY: databricks-deploy
databricks-deploy: ## Validate and deploy the bundle (pipeline + job)
	cd databricks && databricks bundle validate && databricks bundle deploy

.PHONY: databricks-run
databricks-run: ## Run the retail_weekly job and wait for it
	cd databricks && databricks bundle run retail_weekly

# --- Quality ------------------------------------------------------------------

.PHONY: lint
lint: ## sqlfluff + ruff + yaml checks (all pre-commit hooks)
	$(BIN)/pre-commit run --all-files

.PHONY: test-dags
test-dags: ## DAG integrity tests inside the Airflow image
	$(COMPOSE) build airflow-scheduler
	docker run --rm \
		-v "$(CURDIR)/dags:/opt/airflow/dags:ro" \
		-v "$(CURDIR)/include:/opt/airflow/include:ro" \
		-v "$(CURDIR)/tests:/opt/airflow/tests:ro" \
		-e PYTHONPATH=/opt/airflow -e GCP_PROJECT_ID=ci -e GCS_BUCKET=ci \
		-e AIRFLOW__CORE__LOAD_EXAMPLES=false -e AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=300 \
		--entrypoint bash retail-gcp-pipeline-airflow-scheduler \
		-c "pip install --user -q pytest==8.3.5 && airflow db migrate >/dev/null && cd /opt/airflow && python -m pytest tests -q -p no:cacheprovider"

.PHONY: test
test: lint dbt-build test-dags ## Everything CI runs: lint, dbt build, DAG tests
