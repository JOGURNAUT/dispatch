"""The pipeline against its own generated data, including the properties that
only exist end to end.

The unit tests prove each rule in isolation. These prove the things that are
properties of the whole run and cannot be checked a layer at a time: that a
replay does not change the warehouse, that a late event lands in its own day,
and that the gates fire on data that is broken in a way the layers individually
accept.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from datetime import datetime

import pytest

from dispatch.quality import QualityGateFailed
from dispatch.storage import open_store
from generator.produce import generate
from transforms import run_pipeline
from transforms.warehouse import Warehouse

# The generator lays trips over `days` from 2026-09-01, so the newest event is
# early on the 8th. AS_OF sits just past that: far enough to have closed the
# window, close enough that the freshness gate does not (correctly) refuse a
# batch whose data is a fortnight stale.
AS_OF = datetime(2026, 9, 9)


def gen_args(**over):
    base = {"trips": 400, "days": 7, "seed": 11, "dup_rate": 0.05,
            "correction_rate": 0.02, "late_rate": 0.03, "v2_rate": 0.3,
            "null_driver_rate": 0.01, "broken_rate": 0.004, "cancel_rate": 0.03,
            "v110_rate": 0.2, "rogue_field_rate": 0.03}
    base.update(over)
    return argparse.Namespace(**base)


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    """Each test gets its own bronze, silver and warehouse.

    Sharing them would make the idempotency test pass for the wrong reason --
    because a previous test had already written the rows.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(run_pipeline, "BRONZE", tmp_path / "bronze")
    monkeypatch.setattr(run_pipeline, "SILVER", tmp_path / "silver")
    monkeypatch.setenv("DISPATCH_DSN", str(tmp_path / "wh.db"))
    return tmp_path


def write_source(path, events):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(e) for e in events), encoding="utf-8")
    return path


def fingerprint(dsn) -> tuple[int, str]:
    conn = sqlite3.connect(str(dsn))
    rows = conn.execute(
        "SELECT trip_id, tat_minutes, is_breach, completeness, store_id, driver_id "
        "FROM fct_trip ORDER BY trip_id").fetchall()
    conn.close()
    return len(rows), hashlib.sha256(repr(rows).encode()).hexdigest()


def test_pipeline_runs_end_to_end(workspace):
    source = write_source(workspace / "raw.jsonl", generate(gen_args()))
    result = run_pipeline.run_all(str(source), as_of=AS_OF)

    assert result["bronze"]["received"] > 0
    assert result["silver"]["duplicates_removed"] > 0, "generator injected no redeliveries"
    assert result["gold"]["trips"] == 400
    assert result["gold"]["loaded"] == 400


def test_replay_does_not_change_the_warehouse(workspace):
    """The property every retry depends on.

    Airflow retries a failed task, and a consumer that dies before committing
    replays its batch. Both re-run this pipeline over data it has already seen.
    If the second run disagreed with the first, no retry would ever be safe and
    the gates would be measuring a moving target.
    """
    source = write_source(workspace / "raw.jsonl", generate(gen_args()))

    run_pipeline.run_all(str(source), as_of=AS_OF)
    first = fingerprint(os.environ["DISPATCH_DSN"])

    run_pipeline.run_all(str(source), as_of=AS_OF)
    second = fingerprint(os.environ["DISPATCH_DSN"])

    assert first == second, "a replay changed the warehouse"
    # Bronze really did grow -- otherwise this passes because nothing was
    # re-ingested, which proves nothing about the dedupe.
    bronze = open_store(workspace / "bronze")
    bronze_rows = sum(len(bronze.read_partition(day)) for day in bronze.partitions())
    assert bronze_rows > first[0], "bronze did not accumulate, replay was not exercised"


def test_backfill_reproduces_the_original_run(workspace):
    """A re-run for an old window must reproduce that window's answer.

    Only possible because every stage takes `as_of` as a parameter. A stage that
    read the clock would make the backfill disagree with the original for no
    reason visible in the data.
    """
    source = write_source(workspace / "raw.jsonl", generate(gen_args()))
    run_pipeline.run_all(str(source), as_of=AS_OF)
    original = fingerprint(os.environ["DISPATCH_DSN"])

    # The same window, re-run as though from much later. Nothing may move.
    run_pipeline.build_gold(batch_id="backfill", as_of=AS_OF,
                            warehouse=Warehouse(os.environ["DISPATCH_DSN"]))
    assert fingerprint(os.environ["DISPATCH_DSN"]) == original


def test_late_event_lands_in_its_own_day_not_todays(workspace):
    """The bug that makes one trip appear in two days and be counted in both."""
    events = generate(gen_args(trips=120, late_rate=0.25))
    source = write_source(workspace / "raw.jsonl", events)
    run_pipeline.run_all(str(source), as_of=AS_OF)

    conn = sqlite3.connect(os.environ["DISPATCH_DSN"])
    spread = conn.execute(
        "SELECT trip_id, COUNT(DISTINCT date_key) d FROM fct_trip "
        "GROUP BY trip_id HAVING d > 1").fetchall()
    conn.close()
    assert spread == [], f"{len(spread)} trip(s) split across days"


def test_lifecycle_violations_are_caught_not_averaged(workspace):
    """Delivered-before-picked-up produces a flatteringly short TAT.

    Without the lifecycle check these rows improve the average, which is the
    worst kind of data defect: it moves the number in the direction everyone
    wants it to move.
    """
    source = write_source(workspace / "raw.jsonl",
                          generate(gen_args(trips=600, broken_rate=0.05)))
    result = run_pipeline.run_all(str(source), as_of=AS_OF)
    assert result["gold"]["completeness"].get("broken", 0) > 0

    conn = sqlite3.connect(os.environ["DISPATCH_DSN"])
    leaked = conn.execute(
        "SELECT COUNT(*) FROM fct_trip WHERE completeness = 'broken' "
        "AND tat_minutes IS NOT NULL").fetchone()[0]
    conn.close()
    assert leaked == 0, "a broken trip carried a TAT into the fact table"


def test_lagging_upstream_fails_the_run_instead_of_loading_a_biased_table(workspace):
    """A batch that is mostly still in flight must not load.

    This is the shape of the real incident the measurable-share gate was written
    for: an upstream table lagging turned most rows into nulls, and every
    downstream reader took the null at face value.
    """
    # Keep only the opening event of each trip, so almost nothing is complete.
    events = [e for e in generate(gen_args(trips=300))
              if e["event_type"] in ("assigned", "reached_pickup")]
    source = write_source(workspace / "raw.jsonl", events)

    with pytest.raises(QualityGateFailed, match="measurable"):
        run_pipeline.run_all(str(source), as_of=AS_OF)


def test_a_failed_gate_leaves_the_warehouse_untouched(workspace):
    """Gating before the load, not after.

    A validate task placed downstream of a load means the bad rows are already
    being read by the time anything objects. Here the gate is tripped on a batch
    the warehouse has already been loaded from, so the only thing under test is
    whether a raised gate can still move rows.

    Note the second run is NOT "re-ingest worse data": bronze is cumulative by
    design, so a later bad batch is silvered alongside every good record already
    landed and the batch-level gates correctly pass. Tripping the gate directly
    is the honest way to test the load boundary.
    """
    source = write_source(workspace / "raw.jsonl", generate(gen_args(trips=300)))
    run_pipeline.run_all(str(source), as_of=AS_OF)
    before = fingerprint(os.environ["DISPATCH_DSN"])

    # A threshold no real batch can clear. The gate fires inside build_gold,
    # after the dimensions are written and before fct_trip is touched.
    with pytest.raises(QualityGateFailed, match="measurable"):
        run_pipeline.build_gold(
            batch_id="gated", as_of=AS_OF, min_measurable=1.01,
            warehouse=Warehouse(os.environ["DISPATCH_DSN"]))

    assert fingerprint(os.environ["DISPATCH_DSN"]) == before


def test_every_gate_verdict_is_persisted_pass_or_fail(workspace):
    """An audit trail that records only failures cannot distinguish a check that
    passed from one that never ran."""
    source = write_source(workspace / "raw.jsonl", generate(gen_args()))
    result = run_pipeline.run_all(str(source), as_of=AS_OF)

    conn = sqlite3.connect(os.environ["DISPATCH_DSN"])
    rows = conn.execute(
        "SELECT stage, check_name, passed FROM run_audit WHERE batch_id = ?",
        (result["gold"]["batch_id"],)).fetchall()
    conn.close()

    stages = {r[0] for r in rows}
    assert stages == {"bronze_to_silver", "silver_to_gold"}
    assert all(r[2] == 1 for r in rows)
    assert len(rows) == 9


def test_both_producer_serialisations_survive_the_whole_run(workspace):
    """A v2 producer's rows must not quietly vanish between Kafka and the mart."""
    events = generate(gen_args(trips=300, v2_rate=0.5))
    v2_trips = {e["trip_id"] for e in events if e["producer_version"] == "v2"}
    source = write_source(workspace / "raw.jsonl", events)
    run_pipeline.run_all(str(source), as_of=AS_OF)

    conn = sqlite3.connect(os.environ["DISPATCH_DSN"])
    loaded = {r[0] for r in conn.execute("SELECT trip_id FROM fct_trip")}
    conn.close()
    assert v2_trips and v2_trips <= loaded, "v2-serialised trips were lost"


def test_an_undeclared_field_reaches_silver_instead_of_vanishing(workspace):
    """The quietest data loss there is.

    A producer adds a field, the consumer ignores the unknown key, every run is
    green, and the field is absent for every row from the day it appeared.
    Nobody finds out until a report is asked for, and the only remedy then is to
    start collecting and wait.
    """
    events = generate(gen_args(trips=300, v110_rate=0.4, rogue_field_rate=0.3))
    source = write_source(workspace / "raw.jsonl", events)
    run_pipeline.run_all(str(source), as_of=AS_OF)

    silver = open_store(workspace / "silver")
    preserved = set()
    for day in silver.partitions():
        for row in silver.read_partition(day):
            if row.get("extra"):
                preserved.update(json.loads(row["extra"]))

    # vehicle_type is declared by 1.1.0 but is not a promoted column;
    # weather_code is declared by no version at all. Both must survive.
    assert "vehicle_type" in preserved
    assert "weather_code" in preserved


def test_mixed_schema_versions_coexist_in_one_batch(workspace):
    """A fleet does not upgrade atomically, so both versions are in the topic at
    once and neither may be lost for being the other one."""
    events = generate(gen_args(trips=300, v110_rate=0.5))
    source = write_source(workspace / "raw.jsonl", events)
    result = run_pipeline.run_all(str(source), as_of=AS_OF)

    silver = open_store(workspace / "silver")
    versions = {row["schema_version"] for day in silver.partitions()
                for row in silver.read_partition(day)}

    assert versions == {"1.0.0", "1.1.0"}
    assert result["gold"]["trips"] == 300
