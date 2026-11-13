# Build context is the repository root (see docker-compose.yml), so that the
# dbt project can be copied into the image. The compose `dbt` service still
# bind-mounts ./dbt over it for local iteration, while the Airflow DAG runs
# the image as it is, with the project baked in.
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 DBT_PROFILES_DIR=/app/dbt

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg ca-certificates git  \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
    -o /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 unixodbc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app/dbt

COPY docker/dbt/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY dbt/ /app/dbt/
