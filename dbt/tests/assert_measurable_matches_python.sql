-- The two definitions of "measurable" must not drift.
--
-- TripFact.measurable in Python decides what the quality gate counts, and
-- stg_trip.is_measurable decides what the marts average. They were written
-- separately because they serve different callers, and two copies of one rule
-- is exactly the thing that quietly disagrees after someone edits one of them.
--
-- Returns rows only on disagreement, which is a dbt test failure.

select
    trip_id,
    completeness,
    tat_minutes,
    is_measurable
from {{ ref('stg_trip') }}
where is_measurable
  and (completeness != 'complete' or tat_minutes is null)
