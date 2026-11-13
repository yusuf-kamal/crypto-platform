USE CryptoDWH;
GO

-- ---------------------------------------------------------------------------
-- Run metrics. Every job appends one row per entity it processed, so run
-- history becomes queryable instead of living only in Airflow task logs:
--
--   SELECT entity, AVG(seconds), MAX(rows_rejected * 1.0 / NULLIF(rows_in,0))
--   FROM ops.pipeline_runs WHERE layer = 'silver' GROUP BY entity;
-- ---------------------------------------------------------------------------
IF SCHEMA_ID('ops') IS NULL EXEC('CREATE SCHEMA ops');
GO

IF OBJECT_ID('ops.pipeline_runs') IS NULL
CREATE TABLE ops.pipeline_runs (
    run_pk        BIGINT IDENTITY(1,1) CONSTRAINT PK_pipeline_runs PRIMARY KEY,
    layer         VARCHAR(20)    NOT NULL,   -- bronze / silver / gold / api
    entity        VARCHAR(100)   NOT NULL,   -- e.g. BTCUSDT|2026-06
    status        VARCHAR(20)    NOT NULL,   -- processed / skipped / failed
    rows_in       BIGINT             NULL,
    rows_valid    BIGINT             NULL,
    rows_rejected BIGINT             NULL,
    seconds       FLOAT              NULL,
    details       NVARCHAR(MAX)      NULL,   -- the job's own JSON metrics
    recorded_at   DATETIME2      NOT NULL
        CONSTRAINT DF_pipeline_runs_recorded_at DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('ops.pipeline_runs')
      AND name = 'IX_pipeline_runs_layer_time'
)
CREATE INDEX IX_pipeline_runs_layer_time
    ON ops.pipeline_runs (layer, recorded_at DESC);
GO

-- SELECT + INSERT for the jobs writing metrics, ALTER so that dbt can create
-- and replace the health view in this schema.
GRANT SELECT, INSERT, ALTER ON SCHEMA::ops TO crypto_app;
GO
