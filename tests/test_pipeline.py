"""Tests for the parts that can be wrong without anything raising.

Each test here corresponds to a failure that produces a plausible-looking,
wrong answer rather than an error: a replay that double-counts, an inner join
wearing a LEFT, a null read as a fact, a metric whose value depends on when the
job ran. Those are the ones worth a test, because a crash announces itself and
these do not.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

import pytest

from dispatch.contracts import (
    SILVER_COLUMNS, ContractViolation, normalise_event, parse_timestamp,
)
from dispatch.dedupe import dedupe, merge_with_existing
from dispatch.quality import (
    BLOCK, GateReport, QualityGateFailed, check_freshness, check_measurable_share,
    check_not_null, check_referential_integrity, check_reconciliation,
    check_row_conservation, check_unique_key, check_volume_band,
)
from dispatch.sessionize import sessionize

T0 = datetime(2026, 10, 1, 9, 0, 0)


def ev(trip, etype, minutes, *, eid=None, ingested=0, **over):
    raw = {
        "event_id": eid or f"{trip}-{etype}-{minutes}",
        "trip_id": trip,
        "order_id": f"O-{trip[2:]}",
        "driver_id": "D-001",
        "store_id": "FC002",
        "event_type": etype,
        "event_ts": T0 + timedelta(minutes=minutes),
        "distance_m": 4200,
        "promised_minutes": 45,
        "producer_version": "v1",
    }
    raw.update(over)
    return normalise_event(raw, ingested_at=T0 + timedelta(minutes=ingested))


def full_trip(trip, start=0, tat=30, **over):
    return [
        ev(trip, "assigned", start, **over),
        ev(trip, "picked_up", start + 8, **over),
        ev(trip, "delivered", start + tat, **over),
    ]


# ------------------------------------------------------- timestamp drift

@pytest.mark.parametrize("raw,expected", [
    ("2026-07-24 03:16:02", datetime(2026, 7, 24, 3, 16, 2)),
    ("2026-08-25T17:01:06.860000", datetime(2026, 8, 25, 17, 1, 6, 860000)),
    ("2026-08-25T17:01:06", datetime(2026, 8, 25, 17, 1, 6)),
    ("2026-08-25T17:01:06Z", datetime(2026, 8, 25, 17, 1, 6)),
    ("2026-08-25T22:31:06+05:30", datetime(2026, 8, 25, 17, 1, 6)),
    (1787562285, datetime.fromtimestamp(1787562285, timezone.utc).replace(tzinfo=None)),
    ("1787562285000", datetime.fromtimestamp(1787562285, timezone.utc).replace(tzinfo=None)),
])
def test_every_producer_spelling_lands_on_one_instant(raw, expected):
    """Two export paths serialised the same field differently and a parser that
    knew one shape dropped the other's rows without raising."""
    assert parse_timestamp(raw) == expected


def test_offset_and_z_forms_agree():
    assert parse_timestamp("2026-08-25T17:01:06Z") == parse_timestamp("2026-08-25T22:31:06+05:30")


def test_unparseable_event_ts_is_fatal_not_silent():
    with pytest.raises(ContractViolation):
        normalise_event({"event_id": "e1", "trip_id": "T-1",
                         "event_type": "assigned", "event_ts": "last tuesday"})


def test_silver_column_order_is_the_contract():
    row = ev("T-1", "assigned", 0).as_row()
    assert tuple(row) == SILVER_COLUMNS


# ------------------------------------------------------- null join keys

def test_missing_dimension_key_is_flagged_not_dropped():
    """The row an inner join eats silently. Keeping it is what lets the gold
    layer decide to lose it on purpose rather than by accident."""
    event = ev("T-1", "assigned", 0, driver_id=None)
    assert "null_driver_id" in event.issues
    assert event.trip_id == "T-1"


def test_out_of_range_coordinate_is_nulled_not_kept():
    event = ev("T-1", "assigned", 0, lat=91.5)
    assert event.lat is None and "lat_out_of_range" in event.issues


# ------------------------------------------------------------- dedupe

def test_replayed_batch_does_not_double_count():
    """Kafka is at-least-once. A consumer that dies before committing its offset
    replays, and a replay must not change the answer."""
    batch = full_trip("T-1")
    once = dedupe(batch)
    twice = dedupe(batch + batch)
    assert once.kept == twice.kept == 3
    assert twice.duplicates_removed == 3


def test_correcting_republish_replaces_rather_than_duplicates():
    """The case DISTINCT gets wrong: the same fact re-sent with a fixed field is
    not a duplicate row, so both survive and the trip has two delivered events."""
    original = ev("T-1", "delivered", 30, distance_m=4200, ingested=0)
    corrected = ev("T-1", "delivered", 30, eid="different-uuid",
                   distance_m=5100, ingested=120)
    result = dedupe([original, corrected])
    assert result.kept == 1
    assert result.rows[0].distance_m == 5100
    assert result.corrected == 1


def test_dedupe_is_order_independent():
    """Determinism is the property that makes a replay safe. If output depended
    on arrival order, two runs over the same data would disagree."""
    events = full_trip("T-1") + full_trip("T-2") + full_trip("T-1")
    baseline = [e.event_id for e in dedupe(events).rows]
    rng = random.Random(7)
    for _ in range(25):
        shuffled = events[:]
        rng.shuffle(shuffled)
        assert [e.event_id for e in dedupe(shuffled).rows] == baseline


def test_late_event_merges_into_its_own_partition():
    """A delivered event arriving two days late belongs to its trip's partition,
    not to today's. Appending is how one trip ends up counted in two days."""
    on_disk = [ev("T-1", "assigned", 0), ev("T-1", "picked_up", 8)]
    late = [ev("T-1", "delivered", 30, ingested=2880)]
    merged = merge_with_existing(on_disk, late)
    assert merged.kept == 3
    assert {e.event_type for e in merged.rows} == {"assigned", "picked_up", "delivered"}


# -------------------------------------------------------- sessionization

def test_complete_trip_gets_a_tat():
    fact = sessionize(full_trip("T-1", tat=30), as_of=T0 + timedelta(hours=2))[0]
    assert fact.completeness == "complete"
    assert fact.tat_minutes == 30.0
    assert fact.is_breach is False
    assert fact.measurable


def test_open_trip_is_excluded_rather_than_given_an_invented_tat():
    """The 187-of-269 failure. An in-flight trip has no duration yet; filling it
    from now() makes every open trip look slow and grow on each run."""
    fact = sessionize([ev("T-1", "assigned", 0), ev("T-1", "picked_up", 8)],
                      as_of=T0 + timedelta(hours=1))[0]
    assert fact.completeness == "open"
    assert fact.tat_minutes is None
    assert not fact.measurable


def test_silent_trip_past_the_lag_window_is_stalled_not_open():
    """Separating these is the point: 'open' is normal and 'stalled' is a real
    problem, and a pipeline that calls both 'missing' hides the second."""
    fact = sessionize([ev("T-1", "assigned", 0)], as_of=T0 + timedelta(hours=12))[0]
    assert fact.completeness == "stalled"


def test_tat_does_not_move_when_the_job_runs_later():
    """A metric whose value depends on run time cannot be backfilled, because a
    re-run disagrees with the original for no visible reason."""
    events = full_trip("T-1", tat=30)
    early = sessionize(events, as_of=T0 + timedelta(hours=2))[0]
    late = sessionize(events, as_of=T0 + timedelta(days=9))[0]
    assert early.tat_minutes == late.tat_minutes == 30.0


def test_delivered_before_picked_up_is_broken_not_fast():
    """Without the lifecycle check this trip reports a negative or absurdly
    small TAT and drags the average down looking like good news."""
    events = [ev("T-1", "assigned", 0), ev("T-1", "picked_up", 40),
              ev("T-1", "delivered", 20)]
    fact = sessionize(events, as_of=T0 + timedelta(hours=2))[0]
    assert fact.completeness == "broken"
    assert fact.tat_minutes is None
    assert any("before" in issue for issue in fact.issues)


def test_breach_is_measured_against_the_carried_promise():
    fact = sessionize(full_trip("T-1", tat=60, promised_minutes=45),
                      as_of=T0 + timedelta(hours=3))[0]
    assert fact.is_breach is True


def test_driver_is_taken_from_first_non_null_not_last():
    """A reassignment can land a null driver_id on a later event; reading the
    last value would change a trip's driver between runs."""
    events = [ev("T-1", "assigned", 0, driver_id="D-009"),
              ev("T-1", "picked_up", 8, driver_id=None),
              ev("T-1", "delivered", 30, driver_id=None)]
    assert sessionize(events, as_of=T0 + timedelta(hours=2))[0].driver_id == "D-009"


# ------------------------------------------------------------- gates

def test_inner_join_wearing_a_left_is_blocked():
    """1,647 of ~32,000 orders disappeared this way on a real dataset. Nothing
    raised; the numbers were just wrong."""
    report = GateReport("silver_to_gold")
    check_row_conservation(report, left_rows=32380, joined_rows=30733,
                           join_name="trip_x_driver")
    assert not report.ok
    with pytest.raises(QualityGateFailed, match="INNER"):
        report.enforce()


def test_fan_out_from_a_non_unique_dimension_is_blocked():
    report = GateReport("silver_to_gold")
    check_row_conservation(report, left_rows=100, joined_rows=137, join_name="trip_x_store")
    assert not report.ok
    assert "extra rows" in report.verdicts[0].detail


def test_row_conservation_passes_when_the_join_behaves():
    report = GateReport("silver_to_gold")
    check_row_conservation(report, left_rows=500, joined_rows=500, join_name="trip_x_driver")
    assert report.enforce().ok


def test_unaccounted_records_block_the_load():
    report = GateReport("bronze_to_silver")
    check_reconciliation(report, incoming=1000, accepted=940, quarantined=12)
    assert not report.ok
    assert "48 record(s) unaccounted" in report.verdicts[0].detail


def test_duplicate_grain_is_blocked():
    facts = sessionize(full_trip("T-1") + full_trip("T-2"), as_of=T0 + timedelta(hours=2))
    report = GateReport("gold")
    check_unique_key(report, rows=facts + facts[:1], key=lambda f: f.trip_id,
                     key_name="trip_id")
    assert not report.ok


def test_orphan_foreign_key_is_blocked_but_null_is_not():
    facts = sessionize(full_trip("T-1", driver_id="D-404"), as_of=T0 + timedelta(hours=2))
    report = GateReport("gold")
    check_referential_integrity(report, rows=facts, column="driver_id",
                                known={"D-001"}, dim_name="dim_driver")
    assert not report.ok

    nulled = sessionize(full_trip("T-2", driver_id=None), as_of=T0 + timedelta(hours=2))
    clean = GateReport("gold")
    check_referential_integrity(clean, rows=nulled, column="driver_id",
                               known={"D-001"}, dim_name="dim_driver")
    assert clean.ok


def test_stopped_producer_fails_rather_than_staying_green():
    report = GateReport("bronze")
    check_freshness(report, latest_event=T0, as_of=T0 + timedelta(hours=9),
                    max_lag=timedelta(hours=2))
    assert not report.ok
    assert "producer may have stopped" in report.verdicts[0].detail


def test_partial_export_falls_outside_the_volume_band():
    report = GateReport("bronze")
    check_volume_band(report, observed=3100, baseline=[9900, 10100, 10000, 9950])
    assert not report.ok


def test_volume_band_is_not_evaluated_without_history():
    """A gate that fires on day one of a backfill trains people to ignore it."""
    report = GateReport("bronze")
    check_volume_band(report, observed=3100, baseline=[10000])
    assert report.ok


def test_lagging_upstream_blocks_metrics_instead_of_biasing_them():
    """82 of 269 complete is the shape of the real incident: an average over the
    few that closed is a biased sample of the fast ones."""
    facts = (sessionize(sum((full_trip(f"T-c{i}") for i in range(82)), []),
                        as_of=T0 + timedelta(hours=2))
             + sessionize([ev(f"T-o{i}", "assigned", 0) for i in range(187)],
                          as_of=T0 + timedelta(hours=2)))
    report = GateReport("gold")
    check_measurable_share(report, facts=facts, min_share=0.5)
    assert not report.ok
    assert "upstream is probably lagging" in report.verdicts[0].detail


def test_healthy_batch_passes_every_gate():
    """The gates have to be quiet on good data or nobody keeps them switched on."""
    events = sum((full_trip(f"T-{i}", start=i) for i in range(40)), [])
    deduped = dedupe(events)
    as_of = T0 + timedelta(hours=3)
    facts = sessionize(deduped.rows, as_of=as_of)

    report = GateReport("full_run")
    check_reconciliation(report, incoming=len(events),
                         accepted=deduped.kept,
                         quarantined=len(events) - deduped.kept)
    check_unique_key(report, rows=facts, key=lambda f: f.trip_id, key_name="trip_id")
    check_not_null(report, rows=facts, column="trip_id")
    check_row_conservation(report, left_rows=len(facts), joined_rows=len(facts),
                           join_name="trip_x_driver")
    check_referential_integrity(report, rows=facts, column="driver_id",
                                known={"D-001"}, dim_name="dim_driver")
    check_freshness(report, latest_event=max(e.event_ts for e in deduped.rows),
                    as_of=as_of, max_lag=timedelta(hours=6))
    check_measurable_share(report, facts=facts)
    assert report.enforce().ok
    assert len(report.verdicts) == 7
