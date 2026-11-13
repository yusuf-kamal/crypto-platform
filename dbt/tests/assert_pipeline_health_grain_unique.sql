-- The health view must have exactly one row per (layer, entity), otherwise
-- an operator reading it sees the same job twice with different verdicts.
select layer, entity, count(*) as n
from {{ ref('vw_pipeline_health') }}
group by layer, entity
having count(*) > 1
