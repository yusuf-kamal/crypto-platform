from airflow.sdk import Asset

BRONZE_AGGTRADES = Asset(
    name="bronze_binance_aggtrades", uri="s3://bronze/binance/aggTrades"
)
