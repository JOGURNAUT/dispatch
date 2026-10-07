"""Tests for the Debezium change stream.

Every case here is a wrong answer rather than a crash: a deleted row that keeps
appearing in reports, a value that reverts because two updates arrived out of
order, a connector restart that rewinds the warehouse. None of them raise, and
none are visible without looking.
"""

from __future__ import annotations

import json
import random

import pytest

from dispatch.cdc import (
    CdcViolation,
    collapse,
    flatten,
    parse_debezium,
)

KEYS = ("driver_id",)


def envelope(op, *, driver_id="D-001", lsn=100, ts_ms=1_700_000_000_000,
             snapshot=False, after=None, before=None, table="drivers", **fields):
    body = {"driver_id": driver_id, "name": "A Rider", "vehicle": "bike", **fields}
    return {
        "op": op,
        "before": before if before is not None else (body if op == "d" else None),
        "after": after if after is not None else (None if op == "d" else body),
        "source": {"table": table, "lsn": lsn, "snapshot": str(snapshot).lower()},
        "ts_ms": ts_ms,
    }


# ------------------------------------------------------------- flattening

def test_nested_payload_flattens_to_columns():
    out = flatten({"id": 1, "addr": {"city": "Goa", "geo": {"lat": 15.4}}})
    assert out == {"id": 1, "addr_city": "Goa", "addr_geo_lat": 15.4}


def test_flatten_is_depth_bounded():
    """A curated table whose columns depend on its contents is not a table."""
    deep = {"a": {"b": {"c": {"d": {"e": {"f": 1}}}}}}
    out = flatten(deep, max_depth=3)
    assert any(isinstance(v, str) and "f" in v for v in out.values())


def test_lists_are_serialised_not_exploded():
    """Exploding a list turns one row into many and silently changes the grain,
    which is exactly what the gold layer asserts has not happened."""
    out = flatten({"id": 1, "tags": ["a", "b"]})
    assert json.loads(out["tags"]) == ["a", "b"]


# --------------------------------------------------------------- parsing

def test_tombstone_is_not_a_record():
    """Debezium emits a null-value message after a delete so Kafka compaction
    can drop the key. Parsed as a record it becomes a row of nulls."""
    assert parse_debezium(None, KEYS) is None
    assert parse_debezium("null", KEYS) is None
    assert parse_debezium("", KEYS) is None
    assert parse_debezium({"key": {"driver_id": "D-001"}, "value": None}, KEYS) is None


def test_delete_takes_its_key_from_the_before_image():
    """A delete has no `after`. Code written against `after` alone either skips
    it -- leaving the row in the warehouse forever -- or writes an empty row."""
    record = parse_debezium(envelope("d"), KEYS)
    assert record.is_delete
    assert record.key == ("D-001",)
    assert record.before["name"] == "A Rider"


def test_a_delete_without_a_before_image_is_rejected():
    bad = envelope("d")
    bad["before"] = None
    with pytest.raises(CdcViolation, match="no before image"):
        parse_debezium(bad, KEYS)


def test_schema_wrapped_envelope_is_unwrapped():
    """Connectors with schemas enabled nest everything under `payload`. One
    place knows about the setting instead of every caller."""
    record = parse_debezium({"schema": {"...": 1}, "payload": envelope("c")}, KEYS)
    assert record.op == "c" and record.key == ("D-001",)


def test_a_null_primary_key_is_rejected():
    with pytest.raises(CdcViolation, match="primary key"):
        parse_debezium(envelope("c", driver_id=None), KEYS)


def test_unknown_op_is_rejected():
    with pytest.raises(CdcViolation, match="unknown op"):
        parse_debezium(envelope("x"), KEYS)


def test_snapshot_reads_are_marked():
    assert parse_debezium(envelope("r", lsn=0, snapshot=True), KEYS).snapshot
    assert parse_debezium(envelope("r", lsn=0), KEYS).snapshot  # op "r" alone


# -------------------------------------------------------------- collapse

def test_delete_produces_a_tombstone_not_an_absent_key():
    """The difference between 'this row was removed upstream' and 'this batch
    did not mention it'. The first must remove the warehouse row; the second
    must leave it alone."""
    result = collapse([envelope("c", lsn=10), envelope("d", lsn=20)], KEYS)

    assert ("D-001",) in result.states
    assert result.states[("D-001",)].deleted is True
    assert result.live == []
    assert len(result.deleted) == 1


def test_latest_lsn_wins_within_a_row():
    result = collapse([
        envelope("c", lsn=10, vehicle="bike"),
        envelope("u", lsn=20, vehicle="ev_bike"),
    ], KEYS)
    assert result.states[("D-001",)].row["vehicle"] == "ev_bike"


def test_an_out_of_order_update_does_not_revert_the_row():
    """ts_ms is a commit timestamp and ties at millisecond resolution; the LSN
    is monotonic and does not. Ordering by time lets the older update win and
    the row silently reverts."""
    result = collapse([
        envelope("u", lsn=20, vehicle="ev_bike", ts_ms=1_700_000_000_000),
        envelope("u", lsn=10, vehicle="bike", ts_ms=1_700_000_000_000),
    ], KEYS)

    assert result.states[("D-001",)].row["vehicle"] == "ev_bike"
    assert result.out_of_order == 1


def test_collapse_is_order_independent():
    """A replayed partition or a restarted connector must converge on the same
    table, not on whichever record happened to arrive last."""
    messages = [
        envelope("c", lsn=10, vehicle="bike"),
        envelope("u", lsn=20, vehicle="scooter"),
        envelope("u", lsn=30, vehicle="ev_bike"),
        envelope("c", driver_id="D-002", lsn=15),
        envelope("d", driver_id="D-002", lsn=40),
    ]
    baseline = collapse(messages, KEYS)
    rng = random.Random(3)
    for _ in range(25):
        shuffled = messages[:]
        rng.shuffle(shuffled)
        other = collapse(shuffled, KEYS)
        assert {k: (v.row, v.deleted) for k, v in other.states.items()} == \
               {k: (v.row, v.deleted) for k, v in baseline.states.items()}


def test_a_snapshot_read_never_beats_a_streamed_change():
    """A connector restart re-reads the table. If the snapshot can win, the
    warehouse rewinds to whatever the table looked like at restart."""
    result = collapse([
        envelope("u", lsn=50, vehicle="ev_bike"),
        envelope("r", lsn=0, snapshot=True, vehicle="bike"),
    ], KEYS)
    assert result.states[("D-001",)].row["vehicle"] == "ev_bike"


def test_a_snapshot_still_seeds_a_row_nothing_else_touched():
    result = collapse([envelope("r", lsn=0, snapshot=True, driver_id="D-009")], KEYS)
    assert result.states[("D-009",)].row["driver_id"] == "D-009"
    assert not result.states[("D-009",)].deleted


def test_a_delete_can_be_followed_by_a_reinsert():
    """A key removed and created again is live, not deleted."""
    result = collapse([
        envelope("c", lsn=10),
        envelope("d", lsn=20),
        envelope("c", lsn=30, vehicle="scooter"),
    ], KEYS)
    state = result.states[("D-001",)]
    assert state.deleted is False
    assert state.row["vehicle"] == "scooter"


def test_tombstones_are_counted_not_applied():
    result = collapse([envelope("c", lsn=10), None, "null"], KEYS)
    assert result.tombstones_skipped == 2
    assert result.live and not result.deleted


def test_truncate_marks_every_row_deleted():
    """Dropping the keys instead would make a truncate look like a quiet batch,
    and the warehouse would keep every row."""
    result = collapse([
        envelope("c", driver_id="D-001", lsn=10),
        envelope("c", driver_id="D-002", lsn=11),
        {"op": "t", "source": {"table": "drivers", "lsn": 12}, "ts_ms": 1},
    ], KEYS)
    assert result.truncates == 1
    assert result.live == []
    assert len(result.deleted) == 2


def test_row_carries_the_delete_marker_to_the_loader():
    result = collapse([envelope("c", lsn=10), envelope("d", lsn=20)], KEYS)
    row = result.states[("D-001",)].as_row(KEYS)
    assert row["_deleted"] is True
    assert row["_lsn"] == 20
    assert row["driver_id"] == "D-001"


def test_a_composite_key_is_supported():
    keys = ("store_id", "driver_id")
    result = collapse([
        envelope("c", lsn=10, store_id="FC002"),
        envelope("c", lsn=11, store_id="FC004"),
    ], keys)
    assert set(result.states) == {("FC002", "D-001"), ("FC004", "D-001")}


# ------------------------------------------------- dimension merge semantics

def test_a_quiet_batch_does_not_empty_the_dimension(tmp_path, monkeypatch):
    """The mistake that looks like the fact path working.

    A dimension loaded with `DELETE partition; INSERT batch` empties itself the
    moment an hour produces no changes, and every fact loaded afterwards fails
    its referential-integrity gate for a reason nowhere near the real cause.
    """
    from transforms.load_dimension import load_driver_dimension
    from transforms.warehouse import Warehouse

    wh = Warehouse(str(tmp_path / "wh.db"))
    load_driver_dimension([envelope("c", driver_id="D-001", lsn=10)], warehouse=wh)
    assert wh.count("dim_driver") == 1

    load_driver_dimension([], warehouse=wh)
    assert wh.count("dim_driver") == 1, "a batch with no changes removed rows"


def test_a_deleted_row_leaves_the_dimension(tmp_path):
    from transforms.load_dimension import load_driver_dimension
    from transforms.warehouse import Warehouse

    wh = Warehouse(str(tmp_path / "wh.db"))
    load_driver_dimension([
        envelope("c", driver_id="D-001", lsn=10),
        envelope("c", driver_id="D-002", lsn=11),
    ], warehouse=wh)
    load_driver_dimension([envelope("d", driver_id="D-002", lsn=20)], warehouse=wh)

    rows = {r[0] for r in wh.query("SELECT driver_id FROM dim_driver")}
    assert rows == {"D-001"}


def test_replaying_a_change_batch_leaves_the_dimension_unchanged(tmp_path):
    from transforms.load_dimension import demo_stream, load_driver_dimension
    from transforms.warehouse import Warehouse

    wh = Warehouse(str(tmp_path / "wh.db"))
    stream = demo_stream(seed=3, drivers=30)

    first = load_driver_dimension(stream, warehouse=wh)
    before = wh.query("SELECT driver_id, trips_total FROM dim_driver ORDER BY driver_id")
    load_driver_dimension(stream, warehouse=wh)
    after = wh.query("SELECT driver_id, trips_total FROM dim_driver ORDER BY driver_id")

    assert before == after
    assert first["removed"] > 0 and first["out_of_order"] > 0, "stream was too clean to test"
