

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
from "CryptoDWH"."staging"."candles_1m" c
join "CryptoDWH"."gold"."dim_symbol" s on s.symbol = c.symbol
join "CryptoDWH"."gold"."dim_date" d on d.full_date = cast(c.candle_ts as date)
join "CryptoDWH"."gold"."dim_time" t
    on t.time_key = datepart(hour, c.candle_ts) * 100 + datepart(minute, c.candle_ts)