"""Making a re-read of the same Kafka offsets produce the same silver table.

Kafka gives at-least-once. A consumer that crashes between writing a batch and
committing its offset will replay that batch on restart, and a backfill replays
on purpose. Neither is an error, so neither may change the answer.

The naive fix -- `DISTINCT` on the whole row -- fails on the case that actually
occurs: the same logical event re-sent with a corrected field. Two rows that
differ in `distance_m` are not duplicates to `DISTINCT`, and both survive, so the
trip now has two "delivered" events and its TAT depends on which one a later
aggregate happens to pick.

So identity is declared, not inferred:

    natural key   (trip_id, event_type)  -- a trip is delivered once
    tie-break     latest ingested_at, then latest event_ts, then event_id

Last write wins on the natural key, which is what makes a correcting re-publish
*correct* the row rather than duplicate it. `event_id` is the final tie-break
only so the result is deterministic when a replay carries identical timestamps;
sorting on a value that can tie makes the output depend on input order, and then
two runs over the same data disagree.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from .contracts import NormalisedEvent


def natural_key(event: NormalisedEvent) -> tuple[str, str]:
    """What makes two records the same event rather than two events.

    Deliberately NOT event_id: a producer retry generates a fresh uuid for a
    re-publish of the same fact, so event_id identifies a *message*, and the
    pipeline needs to identify a *fact*.
    """
    return (event.trip_id, event.event_type)


def _precedence(event: NormalisedEvent) -> tuple:
    # Later ingestion wins, because a re-publish is a correction of what came
    # before. event_ts breaks a tie within one batch; event_id makes the result
    # independent of the order rows happened to arrive in.
    return (event.ingested_at, event.event_ts, event.event_id)


@dataclass
class DedupeResult:
    rows: list[NormalisedEvent]
    duplicates_removed: int
    corrected: int  # duplicates whose winner differed from the earlier record

    @property
    def kept(self) -> int:
        return len(self.rows)


def _differs(a: NormalisedEvent, b: NormalisedEvent) -> bool:
    fields = ("order_id", "driver_id", "store_id", "event_ts", "lat", "lon",
              "distance_m", "promised_minutes")
    return any(getattr(a, f) != getattr(b, f) for f in fields)


def dedupe(events: Iterable[NormalisedEvent]) -> DedupeResult:
    """Collapse to one row per natural key, newest ingestion winning.

    Deterministic: the output depends only on the set of inputs, never on the
    order they arrived in. That property is the whole reason a replay is safe,
    and `test_dedupe_is_order_independent` holds it.
    """
    best: dict[tuple[str, str], NormalisedEvent] = {}
    seen = Counter()
    corrected = 0

    for event in events:
        key = natural_key(event)
        seen[key] += 1
        incumbent = best.get(key)
        if incumbent is None:
            best[key] = event
            continue
        winner, loser = ((event, incumbent) if _precedence(event) > _precedence(incumbent)
                         else (incumbent, event))
        if _differs(winner, loser):
            corrected += 1
        best[key] = winner

    duplicates = sum(count - 1 for count in seen.values())
    # Sorted output so two runs produce byte-identical files, which is what lets
    # a reviewer diff a backfill against the run it replaced.
    rows = sorted(best.values(), key=lambda e: (e.trip_id, e.event_ts, e.event_type))
    return DedupeResult(rows=rows, duplicates_removed=duplicates, corrected=corrected)


def merge_with_existing(existing: Sequence[NormalisedEvent],
                        incoming: Sequence[NormalisedEvent]) -> DedupeResult:
    """Fold a new batch into a partition already on disk.

    The late-arrival path. A `delivered` event that lands two days after its
    trip's other events belongs in the trip's partition, not today's, so the job
    rewrites the affected partition rather than appending to the current one.
    Appending is how a trip ends up split across two days and counted twice.
    """
    return dedupe(list(existing) + list(incoming))
