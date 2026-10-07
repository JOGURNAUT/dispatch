-- The one place "which trips count" is decided.
--
-- Every mart reads this model rather than fct_trip directly, so the rule lives
-- once. Repeating `WHERE completeness = 'complete'` across four marts is how
-- three of them end up agreeing and the fourth quietly does not.

with source as (

    select * from {{ source('gold', 'fct_trip') }}

),

typed as (

    select
        trip_id,
        order_id,
        driver_id,
        store_id,
        date_key,
        assigned_at,
        picked_up_at,
        delivered_at,
        terminal_event,
        completeness,
        event_count,
        distance_m,
        promised_minutes,
        tat_minutes,
        pickup_minutes,
        case when is_breach = 1 then true
             when is_breach = 0 then false
             else null end                                as is_breach,
        issues,

        -- Mirrors TripFact.measurable in the Python layer. Both exist because
        -- the loader needs it to gate and SQL readers need it to filter; the
        -- contract test below asserts they have not drifted apart.
        case
            when completeness = 'complete'
             and tat_minutes is not null
             and tat_minutes >= {{ var('min_plausible_tat') }}
            then true
            else false
        end                                               as is_measurable,

        -- Signed, with the sign meaning "late". Positive is a trip that took
        -- longer than promised. An absolute value here would make the two
        -- stores' opposite biases look like the same problem.
        case
            when tat_minutes is not null and promised_minutes is not null
            then tat_minutes - promised_minutes
        end                                               as promise_error_minutes

    from source

)

select * from typed
