select date_key, time_key, symbol_key, count(*) as n
from {{ ref('fact_candles_1m') }}
group by date_key, time_key, symbol_key
having count(*) > 1