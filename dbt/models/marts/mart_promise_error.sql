-- Where the promise formula is wrong, and in which direction, per store.
--
-- The formula is store-blind: one global speed, one flat buffer. The stores are
-- not, so the error has a sign that depends on the store. Pooling them reports
-- a small average error and hides that one store is systematically
-- over-promised and the other under-promised -- two problems whose fixes point
-- in opposite directions, cancelling into the appearance of a formula that is
-- roughly right.
--
-- p50 and p90 are carried next to the mean because a mean alone cannot tell a
-- formula that is uniformly off from one that is fine for most trips and badly
-- wrong for the long tail. Those need different fixes.

with trips as (

    select * from {{ ref('stg_trip') }}
    where is_measurable
      and promise_error_minutes is not null

),

ranked as (

    select
        store_id,
        promise_error_minutes,
        tat_minutes,
        promised_minutes,
        -- Percentiles by row-number rather than a window percentile function,
        -- which SQLite does not have. Portable across both targets, and the
        -- arithmetic is visible rather than hidden in a dialect-specific call.
        row_number() over (partition by store_id order by promise_error_minutes) as rn,
        count(*) over (partition by store_id)                                    as n
    from trips

)

select
    store_id,
    max(n)                                                   as trips_measured,
    round(avg(promise_error_minutes), 2)                     as mean_signed_error_minutes,
    round(avg(abs(promise_error_minutes)), 2)                as mean_absolute_error_minutes,
    round(max(case when rn = cast(n * 0.5 as integer) then promise_error_minutes end), 2) as p50_error_minutes,
    round(max(case when rn = cast(n * 0.9 as integer) then promise_error_minutes end), 2) as p90_error_minutes,
    round(avg(tat_minutes), 2)                               as mean_tat_minutes,
    round(avg(promised_minutes), 2)                          as mean_promised_minutes,
    sum(case when promise_error_minutes > 0 then 1 else 0 end) as trips_late,
    round(cast(sum(case when promise_error_minutes > 0 then 1 else 0 end) as real)
          / max(n), 4)                                       as breach_rate

from ranked
group by store_id
