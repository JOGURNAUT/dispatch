"""Event stream to one fact row per trip.

The whole difficulty is that a trip is not finished because the batch is
finished. At any cut-off some trips are mid-flight, and the question "is this
trip missing its delivered event because it has not been delivered, or because
the event has not arrived yet?" has two answers that look identical in the data
and must not be treated the same way.

This is not hypothetical. On a real last-mile dataset a rider-side table lagged
the order table, and 187 of 269 orders came back with a null delivered_at. Every
downstream reader took the null at face value and the orders were classified as
something they were not. A re-pull three days later brought that 187 down to 20.
Nothing in the pipeline had failed; the pipeline had simply believed a null.

So a trip gets a `completeness` label rather than being silently included:

    complete    terminal event present -- safe to measure
    open        no terminal event, last event inside the lag window -- still in
                flight, excluded from metrics, not an error
    stalled     no terminal event, last event older than the lag window -- this
                one is a real problem and should be visible, not averaged away
    broken      lifecycle violated (delivered before picked_up, say)

Only `complete` rows carry a TAT. An `open` trip with a null TAT is correct; an
`open` trip with a TAT invented from `now()` is a number that changes every time
the job runs.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from .contracts import EVENT_RANK, TERMINAL_EVENTS, NormalisedEvent

# How long after its last event a trip may stay silent before it stops counting
# as in-flight. Chosen to exceed the worst observed producer lag with room to
# spare: too short and healthy trips get flagged, too long and genuinely stuck
# trips stay invisible for a day.
DEFAULT_LAG_WINDOW = timedelta(hours=6)


@dataclass
class TripFact:
    trip_id: str
    order_id: str | None
    driver_id: str | None
    store_id: str | None
    assigned_at: datetime | None
    picked_up_at: datetime | None
    delivered_at: datetime | None
    terminal_event: str | None
    completeness: str
    event_count: int
    distance_m: int | None
    promised_minutes: int | None
    tat_minutes: float | None
    pickup_minutes: float | None
    is_breach: bool | None
    issues: list[str]

    @property
    def measurable(self) -> bool:
        """Whether this row may enter an average.

        The guard that keeps in-flight trips out of the denominator. A metric
        built over `measurable` rows answers a stable question; one built over
        every row answers a different question every hour.
        """
        return self.completeness == "complete" and self.tat_minutes is not None


def _first_ts(events: Sequence[NormalisedEvent], event_type: str) -> datetime | None:
    hits = [e.event_ts for e in events if e.event_type == event_type]
    return min(hits) if hits else None


def _lifecycle_violations(events: Sequence[NormalisedEvent]) -> list[str]:
    """Events that occur out of their defined order.

    Compares each ranked event against the one before it in the lifecycle rather
    than against all predecessors, so one misplaced event reports once instead of
    cascading into a violation per later stage.
    """
    ranked = sorted(
        ((EVENT_RANK[e.event_type], e) for e in events if e.event_type in EVENT_RANK),
        key=lambda pair: pair[0],
    )
    problems = []
    # strict=False is the intent: the two sequences differ in length by one by
    # construction, and pairing each event with its successor is the point.
    for (_, earlier), (_, later) in zip(ranked, ranked[1:], strict=False):
        if later.event_ts < earlier.event_ts:
            problems.append(f"{later.event_type}_before_{earlier.event_type}")
    return problems


def build_trip_fact(events: Sequence[NormalisedEvent],
                    as_of: datetime,
                    lag_window: timedelta = DEFAULT_LAG_WINDOW) -> TripFact:
    """Collapse one trip's events into its fact row.

    `as_of` is passed in rather than read from the clock so a re-run over the
    same window reproduces the same table. A job that calls datetime.utcnow()
    internally is a job whose output silently depends on when it ran, and a
    backfill then disagrees with the original run for no visible reason.
    """
    if not events:
        raise ValueError("build_trip_fact needs at least one event")

    ordered = sorted(events, key=lambda e: (e.event_ts, EVENT_RANK.get(e.event_type, 99)))
    issues = sorted({issue for e in ordered for issue in e.issues})

    terminal = next((e for e in reversed(ordered) if e.event_type in TERMINAL_EVENTS), None)
    violations = _lifecycle_violations(ordered)
    issues.extend(violations)

    assigned_at = _first_ts(ordered, "assigned")
    picked_up_at = _first_ts(ordered, "picked_up")
    delivered_at = _first_ts(ordered, "delivered")

    if violations:
        completeness = "broken"
    elif terminal is not None:
        completeness = "complete"
    elif ordered[-1].event_ts >= as_of - lag_window:
        completeness = "open"
    else:
        completeness = "stalled"

    # TAT exists only where both ends are real observations. Substituting `as_of`
    # for a missing delivered_at would make every open trip look like a slow one
    # and would grow its "duration" on every run.
    tat = None
    if completeness == "complete" and delivered_at and assigned_at:
        tat = round((delivered_at - assigned_at).total_seconds() / 60.0, 3)
    pickup = None
    if picked_up_at and assigned_at and picked_up_at >= assigned_at:
        pickup = round((picked_up_at - assigned_at).total_seconds() / 60.0, 3)

    # Dimension attributes taken from the first event that carries one. Later
    # events can arrive with a null driver_id after a reassignment, and reading
    # the last non-null would quietly change a trip's driver between runs.
    def carried(attr: str):
        return next((getattr(e, attr) for e in ordered if getattr(e, attr) is not None), None)

    promised = carried("promised_minutes")
    is_breach = None
    if tat is not None and promised is not None:
        is_breach = tat > promised

    return TripFact(
        trip_id=ordered[0].trip_id,
        order_id=carried("order_id"),
        driver_id=carried("driver_id"),
        store_id=carried("store_id"),
        assigned_at=assigned_at,
        picked_up_at=picked_up_at,
        delivered_at=delivered_at,
        terminal_event=terminal.event_type if terminal else None,
        completeness=completeness,
        event_count=len(ordered),
        distance_m=carried("distance_m"),
        promised_minutes=promised,
        tat_minutes=tat,
        pickup_minutes=pickup,
        is_breach=is_breach,
        issues=sorted(set(issues)),
    )


def sessionize(events: Iterable[NormalisedEvent],
               as_of: datetime,
               lag_window: timedelta = DEFAULT_LAG_WINDOW) -> list[TripFact]:
    """Group a deduped event set into trip facts, one row per trip_id."""
    grouped: dict[str, list[NormalisedEvent]] = {}
    for event in events:
        grouped.setdefault(event.trip_id, []).append(event)
    facts = [build_trip_fact(rows, as_of, lag_window) for rows in grouped.values()]
    return sorted(facts, key=lambda f: f.trip_id)
