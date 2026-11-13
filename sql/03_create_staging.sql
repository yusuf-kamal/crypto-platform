USE CryptoDWH;
GO
IF SCHEMA_ID('staging') IS NULL EXEC('CREATE SCHEMA staging');
GO
IF OBJECT_ID('staging.candles_1m') IS NULL
CREATE TABLE staging.candles_1m (
    symbol       VARCHAR(20)   NOT NULL,
    candle_ts    DATETIME2(0)  NOT NULL,
    open_price   DECIMAL(18,8) NOT NULL,
    high_price   DECIMAL(18,8) NOT NULL,
    low_price    DECIMAL(18,8) NOT NULL,
    close_price  DECIMAL(18,8) NOT NULL,
    volume       DECIMAL(28,8) NOT NULL,
    quote_volume DECIMAL(28,8) NOT NULL,
    trade_count  INT           NOT NULL,
    buy_volume   DECIMAL(28,8) NOT NULL,
    sell_volume  DECIMAL(28,8) NOT NULL
);
GO
-- The dbt models join this table on symbol + candle_ts (to dim_symbol, dim_date
-- and dim_time). Without an index SQL Server scans and hash-joins 1.1 M rows,
-- which is what exhausted the memory grant (error 8645) during `dbt run`.
-- The table is rebuilt with truncate + bulk load on every Gold run, so a
-- clustered index also keeps the rows physically ordered for those joins.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('staging.candles_1m')
      AND name = 'CIX_candles_1m_symbol_ts'
)
CREATE CLUSTERED INDEX CIX_candles_1m_symbol_ts
    ON staging.candles_1m (symbol, candle_ts);
GO
GRANT SELECT, INSERT, DELETE, ALTER ON SCHEMA::staging TO crypto_app;
GO
