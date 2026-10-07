-- A trip may appear in exactly one store-day bucket.
--
-- The guard against the late-arrival bug: an event that lands after its day's
-- run, appended to today's partition instead of merged into its own, puts the
-- same trip in two days and counts it in both. The totals still look plausible,
-- which is why this needs a test rather than a glance.

with per_trip as (

    select trip_id, count(distinct date_key) as day_count
    from {{ ref('stg_trip') }}
    group by trip_id

)

select * from per_trip where day_count > 1
