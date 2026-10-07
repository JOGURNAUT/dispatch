"""Synthetic trip telemetry, with the defects the pipeline exists to survive.

A generator that emits clean data tests nothing. Every gate downstream was
written for a specific failure, so this producer injects those failures at
configurable rates and the run is only interesting if the pipeline catches them.

Injected, each independently tunable:

  --dup-rate          the same event published twice (Kafka at-least-once, or a
                      consumer that died before committing its offset)
  --correction-rate   a re-publish of an event with a changed field, which
                      DISTINCT would keep as a second row
  --late-rate         an event whose producer sat on it for hours, so it lands
                      long after its trip's other events
  --v2-rate           a producer on the other serialisation: T separator and
                      microseconds instead of a space
  --null-driver-rate  an event with no driver_id, the row an inner join eats
  --broken-rate       a lifecycle violation: delivered before picked_up
  --v110-rate         a producer already on schema 1.1.0, sending vehicle_type
                      and battery_pct, which an un-updated consumer has never
                      heard of. The fields must survive to the warehouse rather
                      than being ignored on the way through.
  --rogue-field-rate  a field in no schema version at all -- the real case,
                      where a producer team ships first and tells the data team
                      afterwards

Writes to Kafka when confluent_kafka is importable and a broker answers,
otherwise to newline-delimited JSON. The fallback is not a convenience: it means
the pipeline's logic can be exercised end to end on a laptop with no cluster,
and the Kafka path is then only responsible for transport.

    python -m generator.produce --trips 5000 --out data/raw/events.jsonl
    python -m generator.produce --trips 5000 --bootstrap localhost:9092
"""

from __future__ import annotations

import argparse
import json
import pathlib
import random
import sys
import uuid
from datetime import datetime, timedelta

TOPIC = "trip_events"

STORES = [
    # store_id, share of volume, mean km/h -- the speed difference is real: two
    # stores on the same promise formula delivered at measurably different
    # speeds, and a single global constant over-allowed one and under-allowed
    # the other. Keeping them apart here gives the marts something true to find.
    ("FC002", 0.58, 17.0),
    ("FC004", 0.42, 25.0),
]
DRIVERS = [f"D-{i:04d}" for i in range(1, 61)]
LIFECYCLE = ("assigned", "reached_pickup", "picked_up", "out_for_delivery",
             "reached_delivery", "delivered")

# Minutes between consecutive lifecycle stages, as (mean, sigma). Lognormal, not
# normal: delivery durations have a long right tail and a symmetric distribution
# produces a tidy world where no promise is ever badly missed.
STAGE_GAPS = {
    "reached_pickup": (4.0, 0.45),
    "picked_up": (6.0, 0.55),
    "out_for_delivery": (1.5, 0.35),
    "reached_delivery": (14.0, 0.60),
    "delivered": (2.5, 0.40),
}


def _fmt(ts: datetime, v2: bool) -> str:
    """The two serialisations that actually turned up from one logical source."""
    return ts.isoformat() if v2 else ts.strftime("%Y-%m-%d %H:%M:%S")


def _pick_store(rng: random.Random):
    roll = rng.random()
    cumulative = 0.0
    for store in STORES:
        cumulative += store[1]
        if roll <= cumulative:
            return store
    return STORES[-1]


def _promise(distance_m: int) -> int:
    """The promise as a spec would write it: one global speed, one flat buffer.

    Deliberately store-blind. That is the defect the gold marts are meant to
    surface -- a single constant cannot fit two stores that move at different
    speeds, and the per-store error is the finding.
    """
    return int(round(12 + (distance_m / 1000.0) / 20.0 * 60.0))


def trip_events(trip_no: int, day: datetime, rng: random.Random, args) -> list[dict]:
    store_id, _, speed_kmh = _pick_store(rng)
    trip_id = f"T-{trip_no:06d}"
    driver_id = rng.choice(DRIVERS)
    distance_m = int(rng.lognormvariate(1.1, 0.5) * 1000)
    distance_m = max(400, min(distance_m, 18000))

    start = day + timedelta(hours=rng.uniform(8, 21), minutes=rng.uniform(0, 59))
    v2 = rng.random() < args.v2_rate
    # A producer that has moved to 1.1.0 while this consumer still declares
    # 1.0.0 as current. Both have to coexist in one topic, because they do.
    v110 = rng.random() < getattr(args, "v110_rate", 0.0)
    base = {
        "trip_id": trip_id,
        "order_id": f"O-{trip_no:06d}",
        "store_id": store_id,
        "distance_m": distance_m,
        "promised_minutes": _promise(distance_m),
        "producer_version": "v2" if v2 else "v1",
        "schema_version": "1.1.0" if v110 else "1.0.0",
    }
    if v110:
        base["vehicle_type"] = rng.choice(["bike", "ev_bike", "scooter"])
        base["battery_pct"] = rng.randrange(5, 100)

    # Travel time scales with the store's real speed, which is what makes the
    # one-size promise above wrong in a direction that depends on the store.
    travel_scale = 20.0 / speed_kmh
    out: list[dict] = []
    ts = start
    for stage in LIFECYCLE:
        if stage != "assigned":
            mean, sigma = STAGE_GAPS[stage]
            gap = rng.lognormvariate(0.0, sigma) * mean
            if stage in ("reached_delivery", "out_for_delivery"):
                gap *= travel_scale * (distance_m / 4000.0)
            ts += timedelta(minutes=max(0.3, gap))

        event = dict(base)
        event.update({
            "event_id": str(uuid.uuid4()),
            "event_type": stage,
            "event_ts": _fmt(ts, v2),
            "driver_id": None if rng.random() < args.null_driver_rate else driver_id,
            "lat": round(15.49 + rng.uniform(-0.05, 0.05), 6),
            "lon": round(73.82 + rng.uniform(-0.05, 0.05), 6),
        })
        if rng.random() < getattr(args, "rogue_field_rate", 0.0):
            # Undeclared by every version. The pipeline must keep it, not drop
            # it: ignoring the key is free to write and loses the field for
            # every row from the day it appeared.
            event["weather_code"] = rng.choice(["clear", "rain", "heavy_rain"])
        out.append(event)

        # A trip that is cancelled mid-flight stops here. Its later stages never
        # existed, which is different from their not having arrived yet.
        if stage == "out_for_delivery" and rng.random() < args.cancel_rate:
            cancelled = dict(event)
            cancelled.update({"event_id": str(uuid.uuid4()), "event_type": "cancelled",
                              "event_ts": _fmt(ts + timedelta(minutes=2), v2)})
            out.append(cancelled)
            return out

    if rng.random() < args.broken_rate:
        # Delivered stamped before picked_up. Without the lifecycle gate this
        # trip reports an absurdly short TAT and improves the average.
        delivered = out[-1]
        picked = next(e for e in out if e["event_type"] == "picked_up")
        delivered["event_ts"] = _fmt(
            datetime.fromisoformat(picked["event_ts"]) - timedelta(minutes=9), v2)

    return out


def apply_transport_faults(events: list[dict], rng: random.Random, args) -> list[dict]:
    """What the broker and the consumer do to a clean stream.

    Separate from generation on purpose: these are not properties of the
    business event, they are properties of the delivery of it, and the pipeline
    has to be correct under them without the producer cooperating.
    """
    out: list[dict] = []
    for event in events:
        out.append(event)
        if rng.random() < args.dup_rate:
            out.append(dict(event))  # same event_id: a true redelivery
        elif rng.random() < args.correction_rate:
            fixed = dict(event)
            fixed["event_id"] = str(uuid.uuid4())  # a re-publish is a new message
            fixed["distance_m"] = int(event["distance_m"] * rng.uniform(1.05, 1.4))
            fixed["_corrected"] = True
            out.append(fixed)
    if args.late_rate:
        for event in out:
            if rng.random() < args.late_rate:
                event["_delay_hours"] = round(rng.uniform(2, 50), 2)
    return out


def generate(args) -> list[dict]:
    rng = random.Random(args.seed)
    day_zero = datetime(2026, 9, 1)
    events: list[dict] = []
    for trip_no in range(1, args.trips + 1):
        day = day_zero + timedelta(days=rng.randrange(args.days))
        events.extend(trip_events(trip_no, day, rng, args))
    events = apply_transport_faults(events, rng, args)
    # Shuffled because a partition is not ordered across keys and code that
    # relies on arrival order is code that breaks the first time it is replayed.
    rng.shuffle(events)
    return events


def write_jsonl(events: list[dict], path: pathlib.Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event) + "\n")


def write_kafka(events: list[dict], bootstrap: str, topic: str) -> None:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap,
                         "linger.ms": 50, "compression.type": "snappy"})
    failures: list[str] = []

    def on_delivery(err, _msg):
        if err is not None:
            failures.append(str(err))

    for i, event in enumerate(events):
        # Keying by trip_id puts one trip's events on one partition, so a
        # consumer sees them in order without a global sort. Keying by event_id
        # would scatter them and make any per-trip stateful logic wrong.
        producer.produce(topic, key=event["trip_id"].encode(),
                         value=json.dumps(event).encode(), on_delivery=on_delivery)
        if i % 10000 == 0:
            producer.poll(0)
    producer.flush(30)
    if failures:
        raise RuntimeError(f"{len(failures)} message(s) failed to deliver: {failures[:3]}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--trips", type=int, default=5000)
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--bootstrap", help="Kafka brokers; omit to write JSONL")
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--out", default="data/raw/events.jsonl")
    parser.add_argument("--dup-rate", type=float, default=0.04)
    parser.add_argument("--correction-rate", type=float, default=0.01)
    parser.add_argument("--late-rate", type=float, default=0.02)
    parser.add_argument("--v2-rate", type=float, default=0.25)
    parser.add_argument("--null-driver-rate", type=float, default=0.01)
    parser.add_argument("--broken-rate", type=float, default=0.005)
    parser.add_argument("--cancel-rate", type=float, default=0.03)
    parser.add_argument("--v110-rate", type=float, default=0.15)
    parser.add_argument("--rogue-field-rate", type=float, default=0.02)
    args = parser.parse_args(argv)

    events = generate(args)
    if args.bootstrap:
        write_kafka(events, args.bootstrap, args.topic)
        where = f"kafka://{args.bootstrap}/{args.topic}"
    else:
        path = pathlib.Path(args.out)
        write_jsonl(events, path)
        where = str(path)

    dups = len(events) - len({e["event_id"] for e in events})
    print(f"{len(events)} events for {args.trips} trips -> {where}")
    print(f"  redeliveries {dups}  corrections "
          f"{sum(1 for e in events if e.get('_corrected'))}  "
          f"late {sum(1 for e in events if e.get('_delay_hours'))}")
    print(f"  schema 1.1.0 {sum(1 for e in events if e.get('schema_version') == '1.1.0')}"
          f"  undeclared field {sum(1 for e in events if 'weather_code' in e)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
