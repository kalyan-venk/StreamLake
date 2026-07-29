# StreamLake: every hop of the pipeline, runnable one at a time or all at once.
#
#   make setup      one-time: virtualenv + dependencies
#   make batch      Layer 1: ingest -> bronze -> silver -> gold -> export -> warehouse -> dbt
#   make stream     Layer 2: Kafka up, produce events, consume them into Iceberg
#   make dashboard  Layer 3: render the BI dashboard from the marts
#   make all        the whole thing, in order

SHELL := /bin/bash
.DEFAULT_GOAL := help

VENV        := .venv
PY          := $(VENV)/bin/python
PIP         := uv pip
DBT         := $(VENV)/bin/dbt
AIRFLOW_VENV := .venv-airflow

# Spark 4 supports JDK 17 and 21. If JDK 25 is first on your PATH, Spark dies with an obscure
# reflection error, so the JDK is pinned here rather than left to chance.
JAVA_HOME ?= $(shell /usr/libexec/java_home -v 17 2>/dev/null)
export JAVA_HOME
export PYTHONPATH := src
export TZ := UTC

COMPOSE := docker compose -f docker/docker-compose.yml
STREAMLAKE := $(PY) -m streamlake

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# --- setup ---------------------------------------------------------------------------------

.PHONY: setup
setup: ## create the virtualenv and install dependencies (needs uv and a JDK 17)
	uv venv --python 3.12 $(VENV)
	VIRTUAL_ENV=$(VENV) $(PIP) install -e ".[dev]"
	@echo "JAVA_HOME resolved to: $(JAVA_HOME)"

.PHONY: setup-airflow
setup-airflow: ## install Airflow into its own venv (its pins conflict with the pipeline's)
	uv venv --python 3.12 $(AIRFLOW_VENV)
	VIRTUAL_ENV=$(AIRFLOW_VENV) $(PIP) install "apache-airflow==3.3.0" \
		--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.12.txt"

# --- layer 1: batch ------------------------------------------------------------------------

.PHONY: ingest bronze silver gold export warehouse dbt batch
ingest: ## download and checksum the NYC taxi source files
	$(STREAMLAKE) ingest

bronze: ## land the raw file into Iceberg, unchanged
	$(STREAMLAKE) bronze

silver: ## conform, quarantine, dedup
	$(STREAMLAKE) silver

gold: ## build the lake-side aggregates
	$(STREAMLAKE) gold

export: ## write the curated parquet the warehouse loads
	$(STREAMLAKE) export

warehouse: ## load the curated layer into DuckDB (or Snowflake)
	$(STREAMLAKE) warehouse-load

dbt: ## run every dbt model and test
	$(DBT) build --project-dir dbt/streamlake --profiles-dir dbt/streamlake

batch: ingest bronze silver gold export warehouse dbt ## the whole Layer 1 spine

# --- layer 2: streaming --------------------------------------------------------------------

.PHONY: kafka-up kafka-down kafka-ui produce consume stream
kafka-up: ## start the single-node Kafka broker
	$(COMPOSE) up -d kafka
	@echo "waiting for the broker to report healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' streamlake-kafka 2>/dev/null)" = "healthy" ]; do sleep 2; done
	@echo "kafka ready on localhost:9092"

kafka-ui: ## start the Kafka web UI on http://localhost:8085
	$(COMPOSE) --profile ui up -d kafka-ui

kafka-down: ## stop the local infrastructure
	$(COMPOSE) --profile ui --profile s3 down

minio-up: ## start MinIO, to run the lakehouse against real S3 object storage
	$(COMPOSE) --profile s3 up -d minio minio-init

produce: ## replay curated trips onto the Kafka topic
	$(STREAMLAKE) produce

consume: ## run the streaming consumer for a bounded window
	$(STREAMLAKE) consume

stream: kafka-up produce consume ## Layer 2 end to end

# --- layer 3: serving ----------------------------------------------------------------------

.PHONY: dashboard contracts summary
dashboard: ## render the static BI dashboard
	$(STREAMLAKE) dashboard

contracts: ## list every contract and the assertions it makes
	$(STREAMLAKE) contracts

summary: ## roll the latest contract reports into one summary
	$(PY) -m streamlake.contracts.summary

# --- infrastructure ------------------------------------------------------------------------

.PHONY: image kind-up kind-load k8s-apply k8s-status kind-down tf-init tf-plan tf-apply tf-destroy
image: ## build the streaming consumer container image
	docker build -f docker/Dockerfile.stream -t streamlake/stream:local .

kind-up: ## create the local Kubernetes cluster
	kind create cluster --config infra/k8s/kind-cluster.yaml

kind-load: image ## push the local image into the kind node (no registry needed)
	kind load docker-image streamlake/stream:local --name streamlake

k8s-apply: ## apply the manifests with kustomize
	kubectl apply -k infra/k8s

k8s-status: ## what the cluster thinks is running
	kubectl -n streamlake get pods,svc,pvc

kind-down: ## delete the local cluster
	kind delete cluster --name streamlake

tf-init: ## initialise the Terraform module
	cd infra/terraform && tofu init

tf-plan: ## show what Terraform would change
	cd infra/terraform && tofu plan

tf-apply: ## deploy the streaming consumer via Terraform
	cd infra/terraform && tofu apply -auto-approve

tf-destroy: ## tear it down
	cd infra/terraform && tofu destroy -auto-approve

# --- localstack + terraform (lakehouse storage as code) ------------------------------------
# Separate from the K8s module above. The LocalStack target uses a repo-scoped compose project
# name so it does not collide with the other local stacks running on this machine.

COMPOSE_LS := docker compose -p streamlake-ext -f docker/docker-compose.yml --profile localstack
TF_LS      := cd infra/terraform-localstack && tofu

.PHONY: localstack-up localstack-down tf-ls-init tf-ls-plan tf-ls-apply tf-ls-destroy tf-ls-output batch-localstack
localstack-up: ## start LocalStack (S3, Glue, IAM, STS) on localhost:4566
	$(COMPOSE_LS) up -d localstack
	@echo "waiting for LocalStack to report healthy..."
	@until curl -sf http://localhost:4566/_localstack/health >/dev/null 2>&1; do sleep 2; done
	@echo "localstack ready on localhost:4566"

localstack-down: ## stop LocalStack and free port 4566
	$(COMPOSE_LS) down

tf-ls-init: ## initialise the lakehouse-storage Terraform module
	$(TF_LS) init -input=false

tf-ls-plan: ## show what the lakehouse-storage module would provision
	$(TF_LS) plan -input=false

tf-ls-apply: ## provision the lakehouse S3 bucket and IAM against LocalStack (Glue is Pro-only)
	$(TF_LS) apply -auto-approve -var enable_glue=false

tf-ls-destroy: ## tear down the lakehouse-storage resources
	$(TF_LS) destroy -auto-approve

tf-ls-output: ## print the module outputs (warehouse_bucket, warehouse_uri, ...)
	$(TF_LS) output

batch-localstack: ## run the batch spine against the TF-provisioned S3 bucket
	./scripts/localstack_env.sh $(MAKE) batch

# --- orchestration -------------------------------------------------------------------------

.PHONY: airflow-test airflow-up
airflow-test: ## parse and dry-run the batch DAG without a scheduler
	AIRFLOW_HOME=$(PWD)/airflow/home \
	AIRFLOW__CORE__DAGS_FOLDER=$(PWD)/airflow/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	$(AIRFLOW_VENV)/bin/airflow dags list

airflow-up: ## run Airflow locally (webserver + scheduler in one process)
	AIRFLOW_HOME=$(PWD)/airflow/home \
	AIRFLOW__CORE__DAGS_FOLDER=$(PWD)/airflow/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	$(AIRFLOW_VENV)/bin/airflow standalone

# --- quality -------------------------------------------------------------------------------

.PHONY: test lint fmt check clean all
test: ## run the unit tests
	$(VENV)/bin/pytest -q

lint: ## ruff, and the dbt project's own parse check
	$(VENV)/bin/ruff check src tests
	$(DBT) parse --project-dir dbt/streamlake --profiles-dir dbt/streamlake --quiet

fmt: ## autofix what ruff can
	$(VENV)/bin/ruff check --fix src tests
	$(VENV)/bin/ruff format src tests

check: lint test ## everything CI runs

clean: ## remove generated data, keeping the source files
	rm -rf warehouse _reports checkpoints data/curated data/warehouse \
	       dbt/streamlake/target dbt/streamlake/logs

all: batch stream export warehouse dbt dashboard ## the entire pipeline, in order
