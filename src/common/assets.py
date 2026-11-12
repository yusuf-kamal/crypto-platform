from airflow.sdk import Asset

BRONZE_AGGTRADES = Asset(
    name="bronze_binance_aggtrades", uri="s3://bronze/binance/aggTrades"
)

SILVER_AGGTRADES = Asset(
    name="silver_binance_aggtrades", uri="s3://silver/aggtrades"
)
