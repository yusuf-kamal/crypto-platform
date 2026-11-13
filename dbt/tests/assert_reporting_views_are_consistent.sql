-- The views are what the dashboard reads, so a mistake in a join or a
-- group-by there is as damaging as one in a fact. Three invariants:
--
--   1. buy_share stays inside [0, 1]
--   2. the daily volatility view never reports a high below its own low
--   3. the hourly view never reports more than 60 minutes in an hour
--
-- Each branch returns the offending rows, so dbt fails and names them.
select 'buy_share_out_of_range' as problem, symbol, full_date
from {{ ref('vw_symbol_buy_sell_balance') }}
where buy_share is not null and (buy_share < 0 or buy_share > 1)

union all

select 'day_high_below_day_low', symbol, full_date
from {{ ref('vw_symbol_daily_volatility') }}
where day_high < day_low

union all

select 'more_than_60_minutes_in_an_hour', symbol, full_date
from {{ ref('vw_symbol_hourly_activity') }}
where minutes_traded > 60
