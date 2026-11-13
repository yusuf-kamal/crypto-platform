-- Database and the gold schema, plus the two dimensions that are *generated*
-- rather than derived from source data.
--
-- dim_symbol and the fact tables are deliberately NOT created here: dbt owns
-- those names and recreates them on `--full-refresh`, which would silently
-- drop any key or check constraint defined in this file. Their constraints
-- live in dbt/macros/warehouse_constraints.sql and are re-applied as hooks
-- after every build. One owner per table.

IF DB_ID('CryptoDWH') IS NULL CREATE DATABASE CryptoDWH;
GO
USE CryptoDWH;
GO
IF SCHEMA_ID('gold') IS NULL EXEC('CREATE SCHEMA gold');
GO

-- dim_date: one row per calendar day. Seeded by 02_seed_dims.sql and topped
-- up by 04_create_api_staging.sql, never touched by dbt (it is a dbt source).
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

-- A unique index on full_date: every fact model joins on it, and the foreign
-- keys the dbt hooks create point at date_key.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('gold.dim_date') AND name = 'UX_dim_date_full_date'
)
CREATE UNIQUE INDEX UX_dim_date_full_date ON gold.dim_date (full_date);
GO

-- dim_time: a constant 1,440 rows, one per minute of the day.
IF OBJECT_ID('gold.dim_time') IS NULL
CREATE TABLE gold.dim_time (
    time_key   SMALLINT   NOT NULL CONSTRAINT PK_dim_time PRIMARY KEY,
    [hour]     TINYINT    NOT NULL,
    [minute]   TINYINT    NOT NULL,
    hour_label CHAR(5)    NOT NULL
);
GO
