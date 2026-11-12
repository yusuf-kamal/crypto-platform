select date_key, time_key, symbol_key, count(*) as n
from "CryptoDWH"."gold"."fact_candles_1m"
group by date_key, time_key, symbol_key
having count(*) > 1