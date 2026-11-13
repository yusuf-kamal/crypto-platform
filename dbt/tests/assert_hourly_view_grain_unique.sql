-- The reporting views have a grain too, and a wrong group-by would double
-- every measure in the dashboard without any other symptom.
select symbol, full_date, hour_utc, count(*) as n
from {{ ref('vw_symbol_hourly_activity') }}
group by symbol, full_date, hour_utc
having count(*) > 1
