-- Daily crypto Fear & Greed index from alternative.me (source type 2).
-- Grain: one row per day, market-wide (not per symbol), which is why it is a
-- separate fact instead of a column on fact_coin_market_daily.
{{ config(
    materialized='incremental',
    unique_key=['date_key']
) }}

select
    d.date_key,
    f.fng_value,
    f.fng_classification,
    sysutcdatetime() as loaded_at
from {{ source('staging', 'fear_greed_daily') }} f
join {{ source('gold_seed', 'dim_date') }} d on d.full_date = f.snapshot_date
