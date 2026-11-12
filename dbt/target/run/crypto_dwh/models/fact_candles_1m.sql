USE "CryptoDWH";
    USE "CryptoDWH";
    
    

    

    USE "CryptoDWH";
    EXEC('
        CREATE OR ALTER VIEW "gold"."fact_candles_1m__dbt_tmp__dbt_tmp_vw" AS 

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
    on t.time_key = datepart(hour, c.candle_ts) * 100 + datepart(minute, c.candle_ts);
    ')

EXEC('IF OBJECT_ID(''"gold"."fact_candles_1m__dbt_tmp"'', ''U'') IS NOT NULL
            EXEC(''DROP TABLE "CryptoDWH"."gold"."fact_candles_1m__dbt_tmp"'');SELECT TOP 0 * INTO "CryptoDWH"."gold"."fact_candles_1m__dbt_tmp" FROM "CryptoDWH"."gold"."fact_candles_1m__dbt_tmp__dbt_tmp_vw"
        UNION ALL
        SELECT TOP 0 * FROM "CryptoDWH"."gold"."fact_candles_1m__dbt_tmp__dbt_tmp_vw"')
EXEC('INSERT INTO "CryptoDWH"."gold"."fact_candles_1m__dbt_tmp" WITH (TABLOCK)
        SELECT * FROM "CryptoDWH"."gold"."fact_candles_1m__dbt_tmp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');
')

    

    
    USE "CryptoDWH";
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'gold_fact_candles_1m_cci'
        AND object_id=object_id('"gold"."fact_candles_1m__dbt_tmp"')
    )
    DROP index "gold"."fact_candles_1m__dbt_tmp"."gold_fact_candles_1m_cci"
    CREATE CLUSTERED COLUMNSTORE INDEX "gold_fact_candles_1m_cci"
    ON "gold"."fact_candles_1m__dbt_tmp"

   