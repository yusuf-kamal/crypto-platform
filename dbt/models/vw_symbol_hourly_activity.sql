-- Business question 1: which symbols trade the most, and at which hours?
--
-- Views, not tables: the fact is 1.1 M rows, a BI tool aggregates that in
-- well under a second, and a materialised copy would be one more thing that
-- can go stale. The hour of day comes from dim_time, so "at which hours" is
-- a group-by rather than a date function in the dashboard.
{{ config(materialized='view') }}

select
    s.symbol,
    d.full_date,
    d.day_name,
    d.is_weekend,
    t.[hour]                                   as hour_utc,
    sum(f.volume)                              as volume,
    sum(f.quote_volume)                        as quote_volume,
    sum(f.trade_count)                         as trade_count,
    sum(f.buy_volume)                          as buy_volume,
    sum(f.sell_volume)                         as sell_volume,
    max(f.high_price)                          as high_price,
    min(f.low_price)                           as low_price,
    count(*)                                   as minutes_traded
from {{ ref('fact_candles_1m') }} f
join {{ ref('dim_symbol') }} s             on s.symbol_key = f.symbol_key
join {{ source('gold_seed', 'dim_date') }} d on d.date_key = f.date_key
join {{ source('gold_seed', 'dim_time') }} t on t.time_key = f.time_key
group by s.symbol, d.full_date, d.day_name, d.is_weekend, t.[hour]
