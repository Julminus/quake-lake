# Two images from one file:
#   docker build .                  the CLI image (the last stage): bronze, silver and gold
#   docker build --target airflow . Airflow with the same pipeline, for compose.yaml
#
# The CLI: build the wheel and its dependencies with uv, then copy only the
# virtualenv into a slim runtime image (no compiler, no uv, no source tree).
FROM python:3.12-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.13.8 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

# Airflow (the slim image: core and the five providers it requires, standard among them)
# and the pipeline in a virtualenv of its own, /app/.venv, built against this image's
# Python, so Airflow's pinned dependencies and dbt's never have to agree. Tasks call
# /app/.venv/bin/... .
FROM apache/airflow:slim-3.3.2-python3.12@sha256:a54e7bd6c4d7a17e55cd14b0ead257877b8f8111680d2692cbdba2ff32bd3ff3 AS airflow
# The metadata database is Postgres; the slim image has no drivers for it (Airflow 3
# uses psycopg2 and asyncpg). These are the versions Airflow 3.3.2's constraints pin.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "psycopg2-binary==2.9.13" "asyncpg==0.31.0"
USER root
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE ./
# uv is mounted for the build, not copied into the image, and so is its download cache
# (the base image puts UV_CACHE_DIR in /tmp/.cache/uv: 689 MB that would ship otherwise).
RUN --mount=from=ghcr.io/astral-sh/uv:0.13.8,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/tmp/.cache/uv \
    uv sync --locked --no-dev --no-install-project --python /usr/python/bin/python3.12
COPY src ./src
RUN --mount=from=ghcr.io/astral-sh/uv:0.13.8,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/tmp/.cache/uv \
    uv sync --locked --no-dev --no-editable --python /usr/python/bin/python3.12
# dbt runs from /app, so the relative paths in transform/ (src/quake_lake/provinces.csv,
# lake/gold.duckdb) resolve as they do in a checkout; it writes target/ and logs/ in
# transform/.
COPY --chown=airflow:root transform ./transform
RUN install -d -o airflow -g root /app/lake
USER airflow
# DuckDB reads silver on S3 through its httpfs extension; install it now, not at run time.
RUN /app/.venv/bin/python -c "import duckdb; duckdb.sql('INSTALL httpfs')"
ENV DBT_PROJECT_DIR=/app/transform DBT_PROFILES_DIR=/app/transform
WORKDIR /opt/airflow

FROM python:3.12-slim
# /work is where the default bronze (lake/bronze) lands, so the user must own it.
RUN useradd --create-home --uid 1000 quake && install -d -o quake -g quake /work
COPY --from=build /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
WORKDIR /work
USER quake
ENTRYPOINT ["quake-lake"]
CMD ["--help"]
