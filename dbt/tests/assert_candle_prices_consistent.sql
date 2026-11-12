select *
from {{ ref('fact_candles_1m') }}
where high_price < low_price
   or open_price  not between low_price and high_price
   or close_price not between low_price and high_price
   or volume < 0
   or trade_count <= 0