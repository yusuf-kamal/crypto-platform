{{ config(
    materialized='incremental',
    unique_key=['date_key', 'time_key', 'symbol_key'],
    post_hook=[
        "{{ not_null_columns([
             {'name': 'date_key', 'type': 'int'},
             {'name': 'time_key', 'type': 'smallint'},
             {'name': 'symbol_key', 'type': 'int'}
           ]) }}",
        "{{ add_primary_key('PK_fact_candles_1m', 'date_key, time_key, symbol_key') }}",
        "{{ add_foreign_key('FK_candles_date', 'date_key', 'gold.dim_date(date_key)') }}",
        "{{ add_foreign_key('FK_candles_time', 'time_key', 'gold.dim_time(time_key)') }}",
        "{{ add_foreign_key('FK_candles_symbol', 'symbol_key', 'gold.dim_symbol(symbol_key)') }}",
        "{{ add_check('CK_candles_high_low', 'high_price >= low_price') }}",
        "{{ add_check('CK_candles_positive', 'volume >= 0 and trade_count > 0') }}"
    ]
) }}

select
    d.date_key,
    t.time_key,
    s.symbol_key,
    c.open_price,
    c.high_price,
    c.low_price,
    c.close_price,
    c.volume,
    c.quote_volume,
    c.trade_count,
    c.buy_volume,
    c.sell_volume,
    sysutcdatetime() as loaded_at
from {{ source('staging', 'candles_1m') }} c
join {{ ref('dim_symbol') }} s on s.symbol = c.symbol
join {{ source('gold_seed', 'dim_date') }} d on d.full_date = cast(c.candle_ts as date)
join {{ source('gold_seed', 'dim_time') }} t
    on t.time_key = datepart(hour, c.candle_ts) * 100 + datepart(minute, c.candle_ts)
