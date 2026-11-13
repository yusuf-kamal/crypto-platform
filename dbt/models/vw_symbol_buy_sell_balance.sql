-- Business question 3: how does buy volume compare with sell volume?
--
-- "Buy" means the buyer was the aggressor (is_buyer_maker = false in the raw
-- trade). buy_share above 0.5 is net buying pressure for that day.
{{ config(materialized='view') }}

with daily as (
    select
        s.symbol,
        d.full_date,
        sum(f.buy_volume)   as buy_volume,
        sum(f.sell_volume)  as sell_volume,
        sum(f.volume)       as volume,
        sum(f.trade_count)  as trade_count
    from {{ ref('fact_candles_1m') }} f
    join {{ ref('dim_symbol') }} s             on s.symbol_key = f.symbol_key
    join {{ source('gold_seed', 'dim_date') }} d on d.date_key = f.date_key
    group by s.symbol, d.full_date
)

select
    symbol,
    full_date,
    buy_volume,
    sell_volume,
    volume,
    trade_count,
    buy_volume - sell_volume as net_flow,
    -- nullif guards a day with no volume at all; the share is then null
    -- rather than a division error.
    cast(buy_volume as float) / nullif(cast(volume as float), 0) as buy_share
from daily
