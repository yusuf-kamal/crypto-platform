from airflow.sdk import Asset

# --- Source 1: Binance public archive (batch files) ---
BRONZE_AGGTRADES = Asset(
    name="bronze_binance_aggtrades", uri="s3://bronze/binance/aggTrades"
)

SILVER_AGGTRADES = Asset(
    name="silver_binance_aggtrades", uri="s3://silver/aggtrades"
)

# --- Source 2: public REST APIs (daily grain) ---
BRONZE_API_DAILY = Asset(
    name="bronze_api_daily", uri="s3://bronze/api_daily"
)

SILVER_API_DAILY = Asset(
    name="silver_api_daily", uri="s3://silver/api_daily"
)

# --- Warehouse ---
# Published by whichever DAG finished loading a staging table. Both the
# Binance Gold DAG and the API DAG update it, so dbt_build runs after either
# one; a single shared Asset keeps that an OR instead of an AND.
WAREHOUSE_STAGING = Asset(
    name="warehouse_staging", uri="mssql://CryptoDWH/staging"
)
