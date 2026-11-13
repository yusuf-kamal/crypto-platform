-- Business question 2: how volatile is each symbol over time?
--
-- This is also where the two sources meet. The candle fact gives the
-- intraday movement, and the daily API facts give the market context for the
-- same day: market cap, total market volume, and whether the market was
-- fearful or greedy. Both are LEFT JOINs, because the batch scope and the
-- API lookback do not cover exactly the same days.
{{ config(materialized='view') }}

with minutes as (
    select
        f.symbol_key,
        f.date_key,
        f.time_key,
        f.open_price,
        f.close_price,
        f.high_price,
        f.low_price,
        f.volume,
        f.trade_count
    from {{ ref('fact_candles_1m') }} f
),

aggregated as (
    select
        symbol_key,
        date_key,
        max(high_price)                               as day_high,
        min(low_price)                                as day_low,
        sum(volume)                                   as volume,
        sum(trade_count)                              as trade_count,
        max(high_price - low_price)                   as max_minute_range,
        avg(cast(high_price - low_price as float))    as avg_minute_range,
        count(*)                                      as minutes_traded
    from minutes
    group by symbol_key, date_key
),

edges as (
    -- The day's open and close are the first and last *minute* of the day,
    -- found by time_key. A min()/max() over prices would give the cheapest
    -- and dearest minute instead, which is a different thing entirely.
    select
        symbol_key,
        date_key,
        max(case when first_minute = 1 then open_price  end) as day_open,
        max(case when last_minute  = 1 then close_price end) as day_close
    from (
        select
            symbol_key,
            date_key,
            open_price,
            close_price,
            row_number() over (
                partition by symbol_key, date_key order by time_key
            ) as first_minute,
            row_number() over (
                partition by symbol_key, date_key order by time_key desc
            ) as last_minute
        from minutes
    ) ranked
    where first_minute = 1 or last_minute = 1
    group by symbol_key, date_key
)

select
    s.symbol,
    d.full_date,
    d.day_name,
    d.is_weekend,
    e.day_open,
    a.day_high,
    a.day_low,
    e.day_close,
    a.volume,
    a.trade_count,
    a.minutes_traded,
    a.max_minute_range,
    a.avg_minute_range,
    -- The classic daily range as a percentage of the low. nullif keeps a
    -- zero low (which should never happen) from blowing up the view.
    100.0 * cast(a.day_high - a.day_low as float)
          / nullif(cast(a.day_low as float), 0)      as day_range_pct,
    100.0 * cast(e.day_close - e.day_open as float)
          / nullif(cast(e.day_open as float), 0)     as day_change_pct,
    -- Market context from source type 2.
    m.market_cap_usd,
    m.total_volume_usd                                as market_volume_usd,
    fg.fng_value,
    fg.fng_classification
from aggregated a
join edges e                                 on e.symbol_key = a.symbol_key
                                            and e.date_key   = a.date_key
join {{ ref('dim_symbol') }} s               on s.symbol_key = a.symbol_key
join {{ source('gold_seed', 'dim_date') }} d on d.date_key   = a.date_key
left join {{ ref('fact_coin_market_daily') }} m on m.symbol_key = a.symbol_key
                                               and m.date_key   = a.date_key
left join {{ ref('fact_fear_greed_daily') }} fg on fg.date_key  = a.date_key
