

with new_symbols as (
    select distinct symbol
    from "CryptoDWH"."staging"."candles_1m"
    
)

select
    
    row_number() over (order by symbol) as symbol_key,
    symbol,
    replace(symbol, 'USDT', '') as base_asset,
    'USDT' as quote_asset
from new_symbols