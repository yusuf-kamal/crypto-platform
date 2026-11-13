-- The index is defined on a 0-100 scale; anything outside means the API
-- changed its contract or a row was parsed wrongly.
select date_key, fng_value
from {{ ref('fact_fear_greed_daily') }}
where fng_value is null
   or fng_value < 0
   or fng_value > 100
