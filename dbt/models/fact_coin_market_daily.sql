-- Daily market context per symbol from CoinGecko (source type 2).
-- Grain: one row per symbol per day. Joins the same dim_symbol and dim_date
-- as fact_candles_1m, so a single query can put minute-level trading activity
-- next to that day's market cap.
{{ config(
    materialized='incremental',
    unique_key=['date_key', 'symbol_key']
) }}

select
    d.date_key,
    s.symbol_key,
    m.price_usd,
    m.market_cap_usd,
    m.total_volume_usd,
    sysutcdatetime() as loaded_at
from {{ source('staging', 'coin_market_daily') }} m
join {{ ref('dim_symbol') }} s on s.symbol = m.symbol
join {{ source('gold_seed', 'dim_date') }} d on d.full_date = m.snapshot_date
