USE CryptoDWH;
GO

--- 2) dim_symbol
IF NOT EXISTS (SELECT 1 FROM gold.dim_date)
BEGIN
    ;WITH d AS (
        SELECT CAST('2026-01-01' AS DATE) AS dt
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
    OPTION (MAXRECURSION 0);
END
GO

-- dim_time
IF NOT EXISTS (SELECT 1 FROM gold.dim_time)
BEGIN
    ;WITH n AS (
        SELECT 0 AS m
        UNION ALL
        SELECT m + 1 FROM n WHERE m < 1439
    )
    INSERT INTO gold.dim_time (time_key, [hour], [minute], hour_label)
    SELECT (m / 60) * 100 + (m % 60),
           m / 60, m % 60,
           RIGHT('0' + CAST(m / 60 AS VARCHAR(2)), 2) + ':00'
    FROM n
    OPTION (MAXRECURSION 0);
END
GO