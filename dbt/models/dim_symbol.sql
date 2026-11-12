{{ config(materialized='incremental', unique_key='symbol') }}

with new_symbols as (
    select distinct symbol
    from {{ source('staging', 'candles_1m') }}
    {% if is_incremental() %}
    where symbol not in (select symbol from {{ this }})
    {% endif %}
)

select
    {% if is_incremental() %}
    (select coalesce(max(symbol_key), 0) from {{ this }}) +
    {% endif %}
    row_number() over (order by symbol) as symbol_key,
    symbol,
    replace(symbol, 'USDT', '') as base_asset,
    'USDT' as quote_asset
from new_symbols