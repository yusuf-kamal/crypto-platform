-- Daily market context per symbol from CoinGecko (source type 2).
-- Grain: one row per symbol per day. Joins the same dim_symbol and dim_date
-- as fact_candles_1m, so a single query can put minute-level trading
-- activity next to that day's market cap.
{{ config(
    materialized='incremental',
    unique_key=['date_key', 'symbol_key'],
    post_hook=[
        "{{ not_null_columns([
             {'name': 'date_key', 'type': 'int'},
             {'name': 'symbol_key', 'type': 'int'}
           ]) }}",
        "{{ add_primary_key('PK_fact_coin_market_daily', 'date_key, symbol_key') }}",
        "{{ add_foreign_key('FK_coin_market_date', 'date_key', 'gold.dim_date(date_key)') }}",
        "{{ add_foreign_key('FK_coin_market_symbol', 'symbol_key', 'gold.dim_symbol(symbol_key)') }}",
        "{{ add_check('CK_coin_market_price', 'price_usd is null or price_usd > 0') }}"
    ]
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
