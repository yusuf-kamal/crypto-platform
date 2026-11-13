-- Daily crypto Fear & Greed index from alternative.me (source type 2).
-- Grain: one row per day, market-wide (not per symbol), which is why it is a
-- separate fact instead of a column on fact_coin_market_daily.
{{ config(
    materialized='incremental',
    unique_key=['date_key'],
    post_hook=[
        "{{ not_null_columns([{'name': 'date_key', 'type': 'int'}]) }}",
        "{{ add_primary_key('PK_fact_fear_greed_daily', 'date_key') }}",
        "{{ add_foreign_key('FK_fear_greed_date', 'date_key', 'gold.dim_date(date_key)') }}",
        "{{ add_check('CK_fear_greed_range', 'fng_value between 0 and 100') }}"
    ]
) }}

select
    d.date_key,
    f.fng_value,
    f.fng_classification,
    sysutcdatetime() as loaded_at
from {{ source('staging', 'fear_greed_daily') }} f
join {{ source('gold_seed', 'dim_date') }} d on d.full_date = f.snapshot_date
