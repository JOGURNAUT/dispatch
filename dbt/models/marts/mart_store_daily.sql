-- Per store, per day, over measurable trips only.
--
-- trips_total and trips_measured are both carried deliberately. A breach rate
-- computed over 40 of 300 trips and one computed over 290 of 300 are different
-- claims, and a table that reports only the rate makes them look identical.

with trips as (

    select * from {{ ref('stg_trip') }}

),

aggregated as (

    select
        store_id || '|' || date_key                       as store_day_key,
        store_id,
        date_key,

        count(*)                                          as trips_total,
        sum(case when is_measurable then 1 else 0 end)    as trips_measured,
        sum(case when completeness = 'open' then 1 else 0 end)    as trips_open,
        sum(case when completeness = 'stalled' then 1 else 0 end) as trips_stalled,
        sum(case when completeness = 'broken' then 1 else 0 end)  as trips_broken,

        -- Every measure below filters to measurable inside the aggregate rather
        -- than in a WHERE. A WHERE would drop the open and stalled trips from
        -- the counts above too, and then the denominator that makes the rate
        -- interpretable would be gone from the row that reports the rate.
        avg(case when is_measurable then tat_minutes end)          as mean_tat_minutes,
        avg(case when is_measurable then pickup_minutes end)       as mean_pickup_minutes,
        avg(case when is_measurable then distance_m end)           as mean_distance_m,
        sum(case when is_measurable and is_breach then 1 else 0 end) as breaches

    from trips
    group by store_id, date_key

)

select
    store_day_key,
    store_id,
    date_key,
    trips_total,
    trips_measured,
    trips_open,
    trips_stalled,
    trips_broken,
    round(mean_tat_minutes, 2)      as mean_tat_minutes,
    round(mean_pickup_minutes, 2)   as mean_pickup_minutes,
    round(mean_distance_m, 0)       as mean_distance_m,
    breaches,

    -- Guarded. A day whose trips are all still open divides by zero otherwise,
    -- and in SQLite that is null rather than an error, so the pipeline would
    -- succeed and the dashboard would show a blank where a number belongs.
    case when trips_measured > 0
         then round(cast(breaches as real) / trips_measured, 4) end as breach_rate,
    case when trips_total > 0
         then round(cast(trips_measured as real) / trips_total, 4) end as measured_share

from aggregated
