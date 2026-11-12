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
GRANT SELECT, INSERT, DELETE, ALTER ON SCHEMA::staging TO crypto_app;
GO