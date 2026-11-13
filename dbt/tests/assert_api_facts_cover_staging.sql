-- Guard against silent row loss: the fact models use inner joins to dim_date,
-- so any staging day that is missing from the dimension would disappear
-- without an error. This test fails instead.
select m.snapshot_date, count(*) as staging_rows
from {{ source('staging', 'coin_market_daily') }} m
where not exists (
    select 1 from {{ source('gold_seed', 'dim_date') }} d
    where d.full_date = m.snapshot_date
)
group by m.snapshot_date

union all

select f.snapshot_date, count(*) as staging_rows
from {{ source('staging', 'fear_greed_daily') }} f
where not exists (
    select 1 from {{ source('gold_seed', 'dim_date') }} d
    where d.full_date = f.snapshot_date
)
group by f.snapshot_date
