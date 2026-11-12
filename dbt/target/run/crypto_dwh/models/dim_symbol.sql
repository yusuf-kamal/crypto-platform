USE "CryptoDWH";
    USE "CryptoDWH";
    
    

    

    USE "CryptoDWH";
    EXEC('
        CREATE OR ALTER VIEW "gold"."dim_symbol__dbt_tmp__dbt_tmp_vw" AS 

with new_symbols as (
    select distinct symbol
    from "CryptoDWH"."staging"."candles_1m"
    
)

select
    
    row_number() over (order by symbol) as symbol_key,
    symbol,
    replace(symbol, ''USDT'', '''') as base_asset,
    ''USDT'' as quote_asset
from new_symbols;
    ')

EXEC('IF OBJECT_ID(''"gold"."dim_symbol__dbt_tmp"'', ''U'') IS NOT NULL
            EXEC(''DROP TABLE "CryptoDWH"."gold"."dim_symbol__dbt_tmp"'');SELECT TOP 0 * INTO "CryptoDWH"."gold"."dim_symbol__dbt_tmp" FROM "CryptoDWH"."gold"."dim_symbol__dbt_tmp__dbt_tmp_vw"
        UNION ALL
        SELECT TOP 0 * FROM "CryptoDWH"."gold"."dim_symbol__dbt_tmp__dbt_tmp_vw"')
EXEC('INSERT INTO "CryptoDWH"."gold"."dim_symbol__dbt_tmp" WITH (TABLOCK)
        SELECT * FROM "CryptoDWH"."gold"."dim_symbol__dbt_tmp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');
')

    

    
    USE "CryptoDWH";
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'gold_dim_symbol_cci'
        AND object_id=object_id('"gold"."dim_symbol__dbt_tmp"')
    )
    DROP index "gold"."dim_symbol__dbt_tmp"."gold_dim_symbol_cci"
    CREATE CLUSTERED COLUMNSTORE INDEX "gold_dim_symbol_cci"
    ON "gold"."dim_symbol__dbt_tmp"

   