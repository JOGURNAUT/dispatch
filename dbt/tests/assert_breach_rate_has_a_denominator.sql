-- A rate reported without the trips it was computed over is uninterpretable,
-- and a rate reported over zero trips is a division nobody noticed.

select store_day_key, trips_measured, breach_rate
from {{ ref('mart_store_daily') }}
where breach_rate is not null
  and (trips_measured is null or trips_measured = 0)
