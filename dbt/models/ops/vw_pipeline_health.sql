-- One row per (layer, entity): how its last run went, and how it compares
-- with its own history.
--
-- ops.pipeline_runs has been collecting metrics since the observability
-- commit, and nothing read it. A table nobody queries is not observability,
-- so this view answers the questions it was built for: what ran last, how
-- long it took, how much it rejected, and whether anything has gone quiet.
{{ config(materialized='view', schema='ops') }}

with runs as (
    select
        layer,
        entity,
        status,
        rows_in,
        rows_valid,
        rows_rejected,
        seconds,
        recorded_at,
        run_pk
    from {{ source('ops', 'pipeline_runs') }}
),

latest as (
    select *
    from (
        select
            runs.*,
            row_number() over (
                partition by layer, entity
                order by recorded_at desc, run_pk desc
            ) as recency
        from runs
    ) ranked
    where recency = 1
),

history as (
    select
        layer,
        entity,
        count(*)              as total_runs,
        avg(seconds)          as avg_seconds,
        max(seconds)          as max_seconds,
        sum(rows_valid)       as rows_valid_total,
        sum(rows_rejected)    as rows_rejected_total,
        min(recorded_at)      as first_run_at
    from runs
    group by layer, entity
)

select
    l.layer,
    l.entity,
    l.status                                       as last_status,
    l.recorded_at                                  as last_run_at,
    datediff(hour, l.recorded_at, sysutcdatetime()) as hours_since_last_run,
    l.rows_in                                      as last_rows_in,
    l.rows_valid                                   as last_rows_valid,
    l.rows_rejected                                as last_rows_rejected,
    l.seconds                                      as last_seconds,
    -- nullif keeps a run that reported no input (the API and Gold jobs do
    -- not count input rows) from dividing by zero.
    cast(l.rows_rejected as float)
        / nullif(cast(l.rows_in as float), 0)      as last_error_rate,
    h.total_runs,
    h.first_run_at,
    h.avg_seconds,
    h.max_seconds,
    h.rows_valid_total,
    h.rows_rejected_total,
    -- A single column a dashboard or an alert query can filter on. The
    -- order matters: a failed run is worth more attention than a stale one,
    -- and staleness more than a reject rate that may be expected.
    case
        when l.status <> 'processed'                                then 'attention'
        when datediff(hour, l.recorded_at, sysutcdatetime()) > 48   then 'stale'
        when l.rows_in > 0
             and cast(l.rows_rejected as float) / l.rows_in > 0.01  then 'quality'
        else 'ok'
    end                                            as health
from latest l
join history h
    on h.layer = l.layer
   and h.entity = l.entity
