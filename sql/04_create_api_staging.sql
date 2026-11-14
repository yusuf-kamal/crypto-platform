USE CryptoDWH;
GO

-- ---------------------------------------------------------------------------
-- 1) Extend dim_date backwards.
--
-- sql/02 seeds 2026-2027, but both API sources reach further back, and a date
-- missing from dim_date is dropped silently by the inner join in the fact
-- model -- the rows simply are not there, with no error anywhere.
--
-- How far back is set by the longer of the two: CoinGecko's free tier returns
-- 365 days, but the Fear & Greed index goes back to its first publication on
-- 2018-02-01. Measured on a real load: with the dimension starting in 2025,
-- 2,522 of 3,205 index days were being discarded. assert_api_facts_cover_staging
-- is what caught it.
--
-- 2018-2027 is 3,653 rows. A date dimension is cheap; losing history is not.
-- The top-up runs per missing date rather than on an empty table, so it also
-- extends an installation that is already seeded.
-- ---------------------------------------------------------------------------
;WITH d AS (
    SELECT CAST('2018-01-01' AS DATE) AS dt
    UNION ALL
    SELECT DATEADD(DAY, 1, dt) FROM d WHERE dt < '2027-12-31'
)
INSERT INTO gold.dim_date (date_key, full_date, [year], [month], month_name, [day],
                           day_of_week, day_name, is_weekend)
SELECT YEAR(dt) * 10000 + MONTH(dt) * 100 + DAY(dt),
       dt, YEAR(dt), MONTH(dt),
       CHOOSE(MONTH(dt), 'January','February','March','April','May','June',
              'July','August','September','October','November','December'),
       DAY(dt),
       (DATEDIFF(DAY, '19000101', dt) % 7) + 1,
       CHOOSE((DATEDIFF(DAY, '19000101', dt) % 7) + 1,
              'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'),
       CASE WHEN (DATEDIFF(DAY, '19000101', dt) % 7) + 1 IN (6, 7) THEN 1 ELSE 0 END
FROM d
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_date x WHERE x.full_date = d.dt)
OPTION (MAXRECURSION 0);
GO

-- ---------------------------------------------------------------------------
-- 2) Staging tables for the REST API source.
--
-- Both are rebuilt with truncate + load on every run, like staging.candles_1m.
-- The amounts arrive as JSON floats but are stored as DECIMAL so that
-- arithmetic in the warehouse matches the Binance fact table.
-- ---------------------------------------------------------------------------
IF SCHEMA_ID('staging') IS NULL EXEC('CREATE SCHEMA staging');
GO

IF OBJECT_ID('staging.coin_market_daily') IS NULL
CREATE TABLE staging.coin_market_daily (
    symbol           VARCHAR(20)   NOT NULL,
    coin_id          VARCHAR(50)   NOT NULL,
    snapshot_date    DATE          NOT NULL,
    price_usd        DECIMAL(28,8)     NULL,
    market_cap_usd   DECIMAL(28,2)     NULL,
    total_volume_usd DECIMAL(28,2)     NULL
);
GO

IF OBJECT_ID('staging.fear_greed_daily') IS NULL
CREATE TABLE staging.fear_greed_daily (
    snapshot_date      DATE        NOT NULL,
    fng_value          INT             NULL,
    fng_classification VARCHAR(30)     NULL
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('staging.coin_market_daily')
      AND name = 'CIX_coin_market_daily'
)
CREATE CLUSTERED INDEX CIX_coin_market_daily
    ON staging.coin_market_daily (symbol, snapshot_date);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('staging.fear_greed_daily')
      AND name = 'CIX_fear_greed_daily'
)
CREATE CLUSTERED INDEX CIX_fear_greed_daily
    ON staging.fear_greed_daily (snapshot_date);
GO

GRANT SELECT, INSERT, DELETE, ALTER ON SCHEMA::staging TO crypto_app;
GO
