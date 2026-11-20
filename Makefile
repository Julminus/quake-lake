# Recipes stay single-line uv calls so they run the same under sh (Linux/CI)
# and cmd.exe (GNU make on Windows without Git Bash on PATH).

# The CLI writes to $BRONZE_URI when it is set, else to lake/bronze.
SINCE ?= 2015-01-01

.PHONY: help setup lint format typecheck test run backfill minio minio-run docker clean

help:
	@echo "setup      install deps (+ pre-commit hooks once .pre-commit-config.yaml exists)"
	@echo "lint       ruff check + format check"
	@echo "format     apply ruff fixes and formatting"
	@echo "typecheck  mypy (strict) on src/"
	@echo "test       pytest with coverage (offline: recorded fixtures)"
	@echo "run        a BMKG snapshot + the last 7 days of USGS into BRONZE_URI (default lake/bronze)"
	@echo "backfill   fetch USGS month by month from SINCE=2015-01-01 into BRONZE_URI"
	@echo "minio      start MinIO (127.0.0.1:9000) and create the 'lake' bucket"
	@echo "minio-run  the same as run, inside a container, into s3://lake/bronze on MinIO"
	@echo "docker     build the CLI image"

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

backfill:
	uv run quake-lake usgs --since $(SINCE)

minio:
	docker compose up -d --wait minio
	docker compose run --rm createbucket

minio-run:
	docker compose run --rm --build ingest bmkg
	docker compose run --rm ingest usgs

docker:
	docker build -t quake-lake:dev .

clean:
	uv run python -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in (\".pytest_cache\", \".mypy_cache\", \".ruff_cache\", \"htmlcov\", \"dist\")]"
