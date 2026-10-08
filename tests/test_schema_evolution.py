"""Tests for a producer changing its schema under a consumer that did not change.

The failure this guards against is the quietest one in the file. A producer adds
a field, the consumer ignores the unknown key, every run succeeds, and the field
is absent for every row from the day it was added. Nobody finds out until
someone asks for a report on it, and by then the only fix is to start collecting
and wait.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from dispatch.contracts import SILVER_COLUMNS, normalise_event
from dispatch.schema_registry import (
    BREAKING,
    FULL,
    FieldSpec,
    IncompatibleSchema,
    SchemaVersion,
    apply_defaults,
    check_compatibility,
    get,
    split_known,
    validate_types,
)

T0 = datetime(2026, 9, 1, 10, 0, 0)


def payload(**over):
    base = {
        "event_id": "e-1", "trip_id": "T-1", "event_type": "assigned",
        "event_ts": "2026-09-01 10:00:00", "order_id": "O-1",
        "driver_id": "D-1", "store_id": "NORTHGATE", "distance_m": 4200,
        "promised_minutes": 45, "producer_version": "v1",
    }
    base.update(over)
    return base


# ------------------------------------------------- the forward-compatible case

def test_a_field_the_consumer_has_never_heard_of_is_kept_not_dropped():
    """The whole point. Ignoring an unknown key is free to write and loses the
    field for every row from the day the producer started sending it."""
    event = normalise_event(payload(weather_code="heavy_rain"), ingested_at=T0)

    assert event.extra["weather_code"] == "heavy_rain"
    assert "undeclared_fields" in event.issues


def test_an_undeclared_field_survives_the_silver_round_trip():
    """Kept in memory but lost on write is the same as dropped."""
    event = normalise_event(payload(weather_code="rain"), ingested_at=T0)
    row = event.as_row()

    assert tuple(row) == SILVER_COLUMNS, "extra must be a declared column"
    assert json.loads(row["extra"])["weather_code"] == "rain"


def test_a_newer_producers_declared_fields_land_in_extra():
    """A v1.1.0 producer against a consumer that still treats 1.0.0 as current.
    Both are in the topic at once, because a fleet does not upgrade atomically."""
    event = normalise_event(
        payload(schema_version="1.1.0", vehicle_type="ev_bike", battery_pct=63),
        ingested_at=T0)

    assert event.get("vehicle_type") == "ev_bike"
    assert event.get("battery_pct") == 63
    assert event.schema_version == "1.1.0"
    # Declared by 1.1.0, so not a surprise -- the flag is for genuinely unknown
    # fields and firing it here would make it meaningless.
    assert "undeclared_fields" not in event.issues


def test_an_older_row_reads_under_the_newer_schema_via_defaults():
    """A v1.0.0 row has no vehicle_type. The answer is the declared default, not
    a KeyError and not a bare null a reader cannot interpret."""
    event = normalise_event(payload(schema_version="1.1.0"), ingested_at=T0)
    assert event.get("vehicle_type") == "unknown"
    assert event.get("battery_pct") is None


def test_get_reads_real_columns_and_extras_alike():
    """A caller should not have to know which schema version produced a row."""
    event = normalise_event(payload(schema_version="1.1.0", vehicle_type="bike"),
                            ingested_at=T0)
    assert event.get("trip_id") == "T-1"
    assert event.get("vehicle_type") == "bike"
    assert event.get("nothing_like_this", "fallback") == "fallback"


def test_pipeline_internal_keys_are_not_mistaken_for_producer_fields():
    """Ingestion timestamps and generator markers are ours, not the producer's.
    Counting them as undeclared fields would make the flag fire on every row."""
    known, extra = split_known(
        payload(_ingested_at="2026-09-01T10:00:00", _delay_hours=4.0))
    assert extra == {}
    assert "_ingested_at" not in known


# --------------------------------------------------- compatibility checking

def test_adding_optional_fields_is_fully_compatible():
    report = check_compatibility("1.0.0", "1.1.0")
    assert report.verdict == FULL
    assert sorted(report.added) == ["battery_pct", "vehicle_type"]
    assert report.ok


def test_making_an_existing_field_required_is_breaking():
    """It breaks every reader of data written before the rule existed, because
    that data legitimately does not have the field."""
    stricter = SchemaVersion(
        version="2.0.0-test",
        fields=tuple(
            FieldSpec(f.name, f.kind, required=True) if f.name == "driver_id" else f
            for f in get("1.0.0").fields),
    )
    from dispatch import schema_registry
    schema_registry.REGISTRY[stricter.version] = stricter
    try:
        report = check_compatibility("1.0.0", stricter.version)
        assert report.verdict == BREAKING
        assert "driver_id" in report.newly_required
        with pytest.raises(IncompatibleSchema, match="driver_id"):
            report.raise_if_breaking()
    finally:
        schema_registry.REGISTRY.pop(stricter.version)


def test_changing_a_fields_type_is_breaking():
    """No consumer can absorb a field whose meaning moved under it."""
    retyped = SchemaVersion(
        version="2.1.0-test",
        fields=tuple(
            FieldSpec(f.name, "str") if f.name == "distance_m" else f
            for f in get("1.0.0").fields),
    )
    from dispatch import schema_registry
    schema_registry.REGISTRY[retyped.version] = retyped
    try:
        report = check_compatibility("1.0.0", retyped.version)
        assert report.verdict == BREAKING
        assert "distance_m" in report.retyped
    finally:
        schema_registry.REGISTRY.pop(retyped.version)


def test_an_unknown_version_falls_back_to_the_oldest_not_the_newest():
    """An uncatalogued version is far more often an old producer than a future
    one, and assuming the latest would read its absent new fields as missing
    rather than as not-yet-existing."""
    assert get("0.9.0-nonexistent").version == "1.0.0"
    assert get(None).version == "1.0.0"


# -------------------------------------------------------- type validation

def test_a_mistyped_field_flags_the_row_rather_than_failing_the_batch():
    event = normalise_event(payload(distance_m="four thousand"), ingested_at=T0)
    assert any(i.startswith("mistyped:") for i in event.issues)
    # Still a usable row: one bad optional field is not a reason to lose a trip.
    assert event.trip_id == "T-1"


def test_validate_types_accepts_a_well_formed_payload():
    assert validate_types(split_known(payload())[0]) == []


def test_apply_defaults_does_not_overwrite_a_value_that_is_present():
    filled = apply_defaults({"vehicle_type": "scooter"}, "1.1.0")
    assert filled["vehicle_type"] == "scooter"
    assert filled["battery_pct"] is None
