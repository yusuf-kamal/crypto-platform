{{ config(
    materialized='incremental',
    unique_key='symbol',
    pre_hook=["{{ drop_foreign_keys_referencing('gold', 'dim_symbol') }}"],
    post_hook=[
        "{{ not_null_columns([
             {'name': 'symbol_key', 'type': 'int'},
             {'name': 'symbol', 'type': 'varchar(20)'}
           ]) }}",
        "{{ add_primary_key('PK_dim_symbol', 'symbol_key') }}"
    ]
) }}

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
