# Recipes stay single-line uv calls so they run the same under sh (Linux/CI)
# and cmd.exe (GNU make on Windows without Git Bash on PATH).

# The CLI writes to $BRONZE_URI when it is set, else to lake/bronze; silver to
# $SILVER_URI, else lake/silver. dbt reads silver from there and builds gold into
# $GOLD_DB, else lake/gold.duckdb: here a local folder (dbt's `local` target); s3:// silver
# is read by the `minio` target, which the Airflow stack (make airflow) uses.
SINCE ?= 2015-01-01
export DBT_PROJECT_DIR := transform
export DBT_PROFILES_DIR := transform

.PHONY: help setup lint format typecheck test run backfill silver gold freshness docs dashboard minio minio-run airflow docker infra-check plan clean

help:
	@echo "setup      install deps (+ pre-commit hooks once .pre-commit-config.yaml exists)"
	@echo "lint       ruff check + format check"
	@echo "format     apply ruff fixes and formatting"
	@echo "typecheck  mypy (strict) on src/"
	@echo "test       pytest with coverage (offline: recorded fixtures)"
	@echo "run        a BMKG snapshot + the last 7 days of USGS into BRONZE_URI, then silver and gold"
	@echo "backfill   fetch USGS month by month from SINCE=2015-01-01 into BRONZE_URI"
	@echo "silver     rebuild silver (SILVER_URI, default lake/silver) from all of bronze"
	@echo "gold       dbt build: seeds, models and tests into GOLD_DB (default lake/gold.duckdb)"
	@echo "freshness  dbt source freshness: is the newest event in silver recent?"
	@echo "docs       dbt docs generate, then open transform/target/static_index.html"
	@echo "dashboard  Streamlit on gold (GOLD_DB) at http://127.0.0.1:8501"
	@echo "minio      start MinIO (127.0.0.1:9000) and create the 'lake' bucket"
	@echo "minio-run  bronze and silver as in run, inside a container, into s3://lake/ on MinIO"
	@echo "airflow    MinIO + Airflow (http://127.0.0.1:8080); the daily DAG runs the whole pipeline"
	@echo "docker     build the CLI image"
	@echo "infra-check  Terraform in infra/: fmt, validate, tflint, terraform test (offline)"
	@echo "plan       terraform plan of the AWS deployment, with no AWS account (never applied)"

setup:
	uv sync
	$(if $(wildcard .pre-commit-config.yaml),uv run pre-commit install)

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run mypy

test:
	uv run pytest --cov --cov-report=term-missing

run:
	uv run quake-lake bmkg
	uv run quake-lake usgs
	uv run quake-lake silver
	uv run dbt build

backfill:
	uv run quake-lake usgs --since $(SINCE)

silver:
	uv run quake-lake silver

gold:
	uv run dbt build

freshness:
	uv run dbt source freshness

docs:
	uv run dbt docs generate --static

dashboard:
	uv run streamlit run src/quake_lake/dashboard.py

minio:
	docker compose up -d --wait minio
	docker compose run --rm createbucket

minio-run:
	docker compose run --rm --build ingest bmkg
	docker compose run --rm ingest usgs
	docker compose run --rm ingest silver

airflow:
	docker compose up -d --wait --build

docker:
	docker build -t quake-lake:dev .

# Terraform and tflint must be installed. Nothing here needs AWS credentials or applies.
# tflint --recursive reads a .tflint.hcl per folder; an absolute --config gives the
# modules the AWS ruleset too.
infra-check:
	terraform -chdir=infra init -input=false
	terraform -chdir=infra fmt -check -recursive
	terraform -chdir=infra validate
	tflint --chdir=infra --init
	tflint --chdir=infra --recursive --config="$(CURDIR)/infra/.tflint.hcl"
	terraform -chdir=infra test

plan:
	terraform -chdir=infra init -input=false
	terraform -chdir=infra plan -input=false -var plan_without_credentials=true

clean:
	uv run python -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in (\".pytest_cache\", \".mypy_cache\", \".ruff_cache\", \"htmlcov\", \"dist\")]"
