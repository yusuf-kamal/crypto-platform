--Create the database and schema 

IF DB_ID('CryptoDWH') IS NULL CREATE DATABASE CryptoDWH;
GO
USE CryptoDWH;
GO
IF SCHEMA_ID('gold') IS NULL EXEC('CREATE SCHEMA gold');
GO

-- 2) dim_symbol
IF OBJECT_ID('gold.dim_symbol') IS NULL
CREATE TABLE gold.dim_symbol (
    symbol_key  INT IDENTITY(1,1) CONSTRAINT PK_dim_symbol PRIMARY KEY,
    symbol      VARCHAR(20) NOT NULL CONSTRAINT UQ_dim_symbol_symbol UNIQUE,
    base_asset  VARCHAR(10) NOT NULL,
    quote_asset VARCHAR(10) NOT NULL
);
GO

-- 3) dim_date
IF OBJECT_ID('gold.dim_date') IS NULL
CREATE TABLE gold.dim_date (
    date_key    INT         NOT NULL CONSTRAINT PK_dim_date PRIMARY KEY,  -- 20261010
    full_date   DATE        NOT NULL,
    [year]      SMALLINT    NOT NULL,
    [month]     TINYINT     NOT NULL,
    month_name  VARCHAR(10) NOT NULL,
    [day]       TINYINT     NOT NULL,
    day_of_week TINYINT     NOT NULL,   
    day_name    VARCHAR(10) NOT NULL,
    is_weekend  BIT         NOT NULL
);
GO

-- 4) dim_time 

IF OBJECT_ID('gold.dim_time') IS NULL
CREATE TABLE gold.dim_time (
    time_key   SMALLINT   NOT NULL CONSTRAINT PK_dim_time PRIMARY KEY,   
    [hour]     TINYINT    NOT NULL,
    [minute]   TINYINT    NOT NULL,
    hour_label CHAR(5)    NOT NULL      
);
GO

 -- 5) fact_candles_1m
IF OBJECT_ID('gold.fact_candles_1m') IS NULL
CREATE TABLE gold.fact_candles_1m (
    date_key     INT           NOT NULL CONSTRAINT FK_fact_date   FOREIGN KEY REFERENCES gold.dim_date(date_key),
    time_key     SMALLINT      NOT NULL CONSTRAINT FK_fact_time   FOREIGN KEY REFERENCES gold.dim_time(time_key),
    symbol_key   INT           NOT NULL CONSTRAINT FK_fact_symbol FOREIGN KEY REFERENCES gold.dim_symbol(symbol_key),
    open_price   DECIMAL(18,8) NOT NULL,
    high_price   DECIMAL(18,8) NOT NULL,
    low_price    DECIMAL(18,8) NOT NULL,
    close_price  DECIMAL(18,8) NOT NULL,
    volume       DECIMAL(28,8) NOT NULL,
    quote_volume DECIMAL(28,8) NOT NULL,
    trade_count  INT           NOT NULL,
    buy_volume   DECIMAL(28,8) NOT NULL,
    sell_volume  DECIMAL(28,8) NOT NULL,
    loaded_at    DATETIME2     NOT NULL CONSTRAINT DF_fact_loaded_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_fact_candles_1m PRIMARY KEY (date_key, time_key, symbol_key),
    CONSTRAINT CK_fact_high_low CHECK (high_price >= low_price),
    CONSTRAINT CK_fact_positive CHECK (volume >= 0 AND trade_count > 0)
);
GO