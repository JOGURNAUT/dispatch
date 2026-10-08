"""The medallion run: raw -> bronze -> silver -> gold, with a gate between layers.

Each stage is a separate entry point because Airflow calls them as separate
tasks, and a stage that fails has to be retryable without re-running the ones
before it. They are also importable and callable directly, which is what lets
the whole pipeline be exercised by pytest without a scheduler.

    python -m transforms.run_pipeline all --source data/raw/events.jsonl

The layer boundaries are drawn by what each one is allowed to lose:

  bronze   nothing. Exactly what arrived, plus when it arrived. A bronze that
           drops malformed records has destroyed the only copy of the evidence
           needed to work out what the producer did wrong.
  silver   only what violates the contract, and that goes to quarantine rather
           than to nowhere. One row per fact, deduped, conformed.
  gold     nothing further. Joins here are asserted to preserve cardinality, so
           a row that disappears is a failed run, not a smaller table.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import uuid
from datetime import UTC, datetime, timedelta

from dispatch import notify
from dispatch.contracts import ContractViolation, normalise_event
from dispatch.dedupe import dedupe
from dispatch.quality import (
    GateReport,
    check_freshness,
    check_measurable_share,
    check_not_null,
    check_reconciliation,
    check_referential_integrity,
    check_row_conservation,
    check_unique_key,
    check_volume_band,
)
from dispatch.sessionize import DEFAULT_LAG_WINDOW, sessionize
from dispatch.storage import open_store

from .warehouse import FCT_TRIP_COLUMNS, Warehouse, fact_row

# URIs, not paths: "data/bronze" is a directory and "gs://bucket/bronze" is a
# bucket, and nothing below this line knows which it got. Env-overridable so a
# scheduled run points at a bucket without a code change.
BRONZE = os.environ.get("DISPATCH_BRONZE", "data/bronze")
SILVER = os.environ.get("DISPATCH_SILVER", "data/silver")
MAX_SOURCE_LAG = timedelta(days=3)


def _now() -> datetime:
    """Naive UTC, matching what contracts.parse_timestamp returns.

    One helper rather than scattered utcnow() calls, so there is a single place
    the pipeline's notion of "now" can be pinned in a test.
    """
    return datetime.now(UTC).replace(tzinfo=None)


def _batch_id() -> str:
    return f"{_now():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


def _partition(ts: datetime) -> str:
    return f"{ts:%Y-%m-%d}"


def _alert_then_raise(report, batch_id: str, context: dict | None = None) -> None:
    """Send on a blocking failure, then raise unchanged.

    The order matters. Raising first means the alert is never built; alerting
    instead of raising means the load is not stopped and the message is advice
    nobody has to take. `rows_loaded=0` is the honest figure because every gate
    here runs before its load.
    """
    alert = notify.from_report(report, batch_id, rows_loaded=0, context=context)
    if alert is not None:
        notify.send(alert)
    report.enforce()


# ------------------------------------------------------------------ bronze

def ingest_bronze(source: str, batch_id: str) -> dict:
    """Land raw events untouched, partitioned by the day they describe.

    Partitioning by event date rather than arrival date is what makes the late
    path work: an event that turns up three days late belongs in its own day's
    partition, and the silver job then rewrites that day. Partition by arrival
    and the same trip is spread over two days and counted in both.

    `_delay_hours`, written by the generator to simulate a producer sitting on a
    message, is applied here as ingestion time -- the event's own timestamp is
    untouched, because a delayed message does not change when the thing happened.
    """
    src = pathlib.Path(source)
    if not src.exists():
        raise FileNotFoundError(f"no source at {src} - run generator.produce first")

    partitions: dict[str, list[dict]] = {}
    received = 0
    for line in src.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        received += 1
        event = json.loads(line)
        event["_ingested_at"] = _now().isoformat()
        day = str(event.get("event_ts", ""))[:10] or "unknown"
        partitions.setdefault(day, []).append(event)

    # One object per batch per partition, never an append. Object storage has no
    # append at all, and writing a whole object is the safer shape on a
    # filesystem too: a crash leaves an identifiable partial file rather than
    # half a line glued onto good data.
    #
    # Bronze still accumulates -- a second delivery of the same event is a fact
    # about the stream and belongs in the record. Silver is where identity is
    # decided.
    store = open_store(BRONZE)
    for day, events in partitions.items():
        store.write_batch(day, events, batch_id)

    return {"batch_id": batch_id, "received": received,
            "partitions": sorted(partitions), "written": received}


# ------------------------------------------------------------------ silver

def build_silver(batch_id: str, partitions: list[str] | None = None,
                 warehouse: Warehouse | None = None,
                 as_of: datetime | None = None) -> dict:
    """Conform, dedupe and quarantine. One row per fact.

    Rewrites whole partitions rather than appending, which is what makes the
    stage idempotent: running it twice over the same bronze produces the same
    silver, and a late event re-triggers only the days it touches.
    """
    wh = warehouse or Warehouse()
    # Freshness is judged against the window the run is FOR, not against the
    # wall clock. Reading the clock here makes every backfill fail for the only
    # reason a backfill exists -- that its data is older than today.
    as_of = as_of or _now()
    bronze, silver = open_store(BRONZE), open_store(SILVER)
    days = partitions or bronze.partitions()

    report = GateReport("bronze_to_silver")
    totals = {"read": 0, "accepted": 0, "quarantined": 0,
              "duplicates_removed": 0, "corrected": 0}
    quarantined_rows: list[tuple] = []
    newest: datetime | None = None

    for day in days:
        raw_events = bronze.read_partition(day)
        if not raw_events:
            continue
        totals["read"] += len(raw_events)

        normalised = []
        for raw in raw_events:
            ingested = datetime.fromisoformat(raw["_ingested_at"])
            # A producer that held a message adds to ingestion time only. The
            # event's own timestamp is when the thing happened and is not ours
            # to move.
            if raw.get("_delay_hours"):
                ingested += timedelta(hours=float(raw["_delay_hours"]))
            try:
                normalised.append(normalise_event(raw, ingested_at=ingested))
            except ContractViolation as exc:
                quarantined_rows.append(
                    (batch_id, "bronze_to_silver", str(exc),
                     json.dumps(raw)[:4000], _now()))

        result = dedupe(normalised)
        totals["accepted"] += result.kept
        totals["duplicates_removed"] += result.duplicates_removed
        totals["corrected"] += result.corrected
        if result.rows:
            day_newest = max(e.event_ts for e in result.rows)
            newest = day_newest if newest is None else max(newest, day_newest)

        # Replaced, not accumulated: silver is derived, so recomputing a
        # partition has to produce that partition rather than add to it.
        rows = []
        for event in result.rows:
            row = event.as_row()
            row["event_ts"] = row["event_ts"].isoformat()
            row["ingested_at"] = row["ingested_at"].isoformat()
            rows.append(row)
        silver.replace_partition(day, rows)

    totals["quarantined"] = len(quarantined_rows)
    if quarantined_rows:
        wh.load_replace("quarantine",
                        ("batch_id", "stage", "reason", "payload", "quarantined_at"),
                        quarantined_rows, partition_column="batch_id",
                        partition_values=[batch_id])

    # Reconciliation counts deduplication as accounted-for, not as loss: a
    # redelivery that collapsed into an existing row has not gone missing.
    check_reconciliation(
        report,
        incoming=totals["read"],
        accepted=totals["accepted"] + totals["duplicates_removed"],
        quarantined=totals["quarantined"])
    check_freshness(report, latest_event=newest, as_of=as_of,
                    max_lag=MAX_SOURCE_LAG)

    wh.record_gates(batch_id, report)
    _alert_then_raise(report, batch_id, context={"partitions": len(days),
                                                 "read": totals["read"]})
    return {"batch_id": batch_id, "partitions": days, **totals,
            "gates": report.summary()}


# -------------------------------------------------------------------- gold

def _read_silver(days: list[str]):
    from dispatch.contracts import NormalisedEvent
    events = []
    silver = open_store(SILVER)
    for day in days:
        for row in silver.read_partition(day):
            row["event_ts"] = datetime.fromisoformat(row["event_ts"])
            row["ingested_at"] = datetime.fromisoformat(row["ingested_at"])
            # `extra` is written as a JSON string so a silver record stays one
            # flat line. Parsed back rather than left as text: a caller asking
            # for an undeclared field should get the value, not a string that
            # happens to contain it.
            row["extra"] = json.loads(row["extra"]) if row.get("extra") else {}
            events.append(NormalisedEvent(**row))
    return events


def build_gold(batch_id: str, partitions: list[str] | None = None,
               as_of: datetime | None = None,
               warehouse: Warehouse | None = None,
               min_measurable: float = 0.5) -> dict:
    """Sessionize to one fact row per trip and load the star schema.

    `as_of` is a parameter, not a clock read, so a backfill of last Tuesday
    reproduces last Tuesday's answer. A job that reads the clock internally
    produces a different table on every run and cannot be backfilled at all.
    """
    wh = warehouse or Warehouse()
    wh.migrate()
    as_of = as_of or _now()
    days = partitions or open_store(SILVER).partitions()

    events = _read_silver(days)
    if not events:
        raise RuntimeError(f"no silver rows for partitions {days}")
    facts = sessionize(events, as_of=as_of, lag_window=DEFAULT_LAG_WINDOW)

    # ---- dimensions, built from what the facts actually reference
    stores = sorted({f.store_id for f in facts if f.store_id})
    drivers: dict[str, dict] = {}
    for fact in facts:
        if not fact.driver_id:
            continue
        seen = drivers.setdefault(fact.driver_id,
                                  {"first_seen_at": fact.assigned_at, "trips_total": 0})
        seen["trips_total"] += 1
        if fact.assigned_at and (seen["first_seen_at"] is None
                                 or fact.assigned_at < seen["first_seen_at"]):
            seen["first_seen_at"] = fact.assigned_at

    date_keys = sorted({_partition(f.assigned_at) for f in facts if f.assigned_at})

    wh.load_replace("dim_store", ("store_id", "store_name", "city"),
                    [(s, s.title() + " Depot", "Mirefield") for s in stores],
                    partition_column=None)
    wh.load_replace("dim_driver", ("driver_id", "first_seen_at", "trips_total"),
                    [(d, v["first_seen_at"], v["trips_total"])
                     for d, v in sorted(drivers.items())],
                    partition_column=None)
    wh.load_replace("dim_date", ("date_key", "day_of_week", "is_weekend"),
                    [(k, datetime.fromisoformat(k).weekday(),
                      int(datetime.fromisoformat(k).weekday() >= 5)) for k in date_keys],
                    partition_column=None)

    # ---- gates before the fact load, not after
    report = GateReport("silver_to_gold")
    check_unique_key(report, rows=facts, key=lambda f: f.trip_id, key_name="trip_id")
    check_not_null(report, rows=facts, column="trip_id")

    # The join that has to be asserted. A fact whose driver_id is present but
    # absent from dim_driver would vanish on an inner join and leave a smaller,
    # entirely reasonable-looking table behind.
    joinable = [f for f in facts if f.driver_id is not None]
    resolved = [f for f in joinable if f.driver_id in drivers]
    check_row_conservation(report, left_rows=len(joinable), joined_rows=len(resolved),
                           join_name="fct_trip_x_dim_driver")
    check_referential_integrity(report, rows=facts, column="driver_id",
                                known=set(drivers), dim_name="dim_driver")
    check_referential_integrity(report, rows=facts, column="store_id",
                                known=set(stores), dim_name="dim_store")

    # Volume is compared per day against days this batch is NOT rewriting.
    #
    # Two mistakes are easy here and both make the gate useless rather than
    # noisy. Comparing the whole multi-day batch against per-day history is an
    # apples-to-oranges check that fires on every backfill. Including the days
    # being rewritten in the baseline is circular: on a replay the batch is
    # compared against the rows it wrote last time, so it always agrees with
    # itself and the gate can never fail.
    per_day: dict[str, int] = {}
    for fact in facts:
        key = _partition(fact.assigned_at) if fact.assigned_at else "unknown"
        per_day[key] = per_day.get(key, 0) + 1
    rewriting = set(per_day)
    baseline = [row[1] for row in wh.query(
        "SELECT date_key, COUNT(*) FROM fct_trip GROUP BY date_key")
        if row[0] not in rewriting]
    observed = sorted(per_day.values())[len(per_day) // 2] if per_day else 0
    check_volume_band(report, observed=observed, baseline=baseline)
    check_measurable_share(report, facts=facts, min_share=min_measurable)

    wh.record_gates(batch_id, report)
    _alert_then_raise(report, batch_id, context={"trips": len(facts),
                                                 "partitions": len(days)})

    loaded = wh.load_replace(
        "fct_trip", FCT_TRIP_COLUMNS,
        [fact_row(f, _partition(f.assigned_at) if f.assigned_at else "unknown")
         for f in facts],
        partition_column="date_key",
        partition_values=date_keys + ["unknown"])

    by_state: dict[str, int] = {}
    for fact in facts:
        by_state[fact.completeness] = by_state.get(fact.completeness, 0) + 1

    return {"batch_id": batch_id, "trips": len(facts), "loaded": loaded,
            "completeness": by_state, "gates": report.summary(),
            "dim_driver": len(drivers), "dim_store": len(stores)}


# -------------------------------------------------------------------- cli

def run_all(source: str, as_of: datetime | None = None) -> dict:
    batch_id = _batch_id()
    wh = Warehouse()
    wh.migrate()
    bronze = ingest_bronze(source, batch_id)
    silver = build_silver(batch_id, bronze["partitions"], warehouse=wh, as_of=as_of)
    gold = build_gold(batch_id, bronze["partitions"], as_of=as_of, warehouse=wh)
    return {"bronze": bronze, "silver": silver, "gold": gold}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("stage", choices=["bronze", "silver", "gold", "all"])
    parser.add_argument("--source", default="data/raw/events.jsonl")
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--as-of", default=None,
                        help="ISO timestamp the run reasons as-of; defaults to now")
    args = parser.parse_args(argv)

    as_of = datetime.fromisoformat(args.as_of) if args.as_of else None
    batch_id = args.batch_id or _batch_id()

    if args.stage == "all":
        result = run_all(args.source, as_of)
    elif args.stage == "bronze":
        result = ingest_bronze(args.source, batch_id)
    elif args.stage == "silver":
        result = build_silver(batch_id, as_of=as_of)
    else:
        result = build_gold(batch_id, as_of=as_of)

    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
