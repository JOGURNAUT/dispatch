"""Loading a dimension from a Debezium change stream.

The second source, and the one that makes the warehouse's two halves honest.
Facts come from the trip event stream, which is append-only; dimensions come
from CDC of the application's Postgres, which is not. A driver row is edited and
deleted in the operational database, and the warehouse has to follow.

The load is a MERGE rather than a replace, which is the whole difference between
this and the fact path:

  present and live     upsert
  present and deleted  remove from the dimension
  absent from batch    leave alone -- a CDC batch is a set of CHANGES, and a row
                       nobody edited is not a row that went away

That last line is the one that gets written wrong. A dimension loaded with the
fact path's `DELETE partition; INSERT batch` empties itself the moment a quiet
hour produces no changes, and every fact loaded afterwards fails its
referential-integrity gate for a reason nowhere near the real cause.

    python -m transforms.load_dimension --demo
"""

from __future__ import annotations

import argparse
import json
import pathlib
import random
import sys

from dispatch.cdc import collapse

from .warehouse import Warehouse

DIM_DRIVER_COLUMNS = ("driver_id", "first_seen_at", "trips_total")


def load_driver_dimension(messages, warehouse: Warehouse | None = None,
                          key_columns=("driver_id",)) -> dict:
    """Merge a change batch into dim_driver.

    Returns counts rather than printing them so the Airflow task can put them in
    its XCom and the gates can assert on them.
    """
    wh = warehouse or Warehouse()
    wh.migrate()
    result = collapse(messages, key_columns)

    live = result.live
    gone = result.deleted
    placeholder = wh.placeholder

    with wh.connect() as conn:
        cur = conn.cursor()
        try:
            for state in gone:
                cur.execute(
                    f"DELETE FROM dim_driver WHERE driver_id = {placeholder}",
                    (state.key[0],))
            for state in live:
                driver_id = state.key[0]
                # Upsert by hand rather than with ON CONFLICT: the two targets
                # spell it differently (SQLite needs ON CONFLICT DO UPDATE,
                # older Postgres wants the constraint named), and a dimension
                # load is small enough that one round trip per row costs
                # nothing worth a dialect fork.
                cur.execute(
                    f"DELETE FROM dim_driver WHERE driver_id = {placeholder}",
                    (driver_id,))
                cur.execute(
                    f"INSERT INTO dim_driver (driver_id, first_seen_at, trips_total) "
                    f"VALUES ({placeholder}, {placeholder}, {placeholder})",
                    (driver_id, state.row.get("created_at"),
                     state.row.get("trips_total") or 0))
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return {
        "changes": len(result.states),
        "upserted": len(live),
        "removed": len(gone),
        "tombstones_skipped": result.tombstones_skipped,
        "out_of_order": result.out_of_order,
        "rows_now": wh.count("dim_driver"),
    }


def demo_stream(seed: int = 7, drivers: int = 40) -> list:
    """A Debezium-shaped stream with the awkward cases actually in it.

    A clean stream of inserts proves nothing: the handling that matters is for
    deletes, their trailing tombstones, out-of-order updates and a connector
    restart's snapshot.
    """
    rng = random.Random(seed)
    lsn = 1000
    out: list = []

    for i in range(1, drivers + 1):
        lsn += 1
        did = f"D-{i:04d}"
        out.append({
            "op": "c",
            "after": {"driver_id": did, "created_at": "2026-09-01 08:00:00",
                      "trips_total": 0, "vehicle": rng.choice(["bike", "ev_bike"])},
            "source": {"table": "drivers", "lsn": lsn, "snapshot": "false"},
            "ts_ms": 1_700_000_000_000 + lsn,
        })

    # Updates, a third of them published out of order.
    for i in rng.sample(range(1, drivers + 1), k=drivers // 2):
        did = f"D-{i:04d}"
        first, second = lsn + 1, lsn + 2
        lsn += 2
        pair = [
            {"op": "u",
             "after": {"driver_id": did, "created_at": "2026-09-01 08:00:00",
                       "trips_total": rng.randrange(1, 40), "vehicle": "bike"},
             "source": {"table": "drivers", "lsn": first, "snapshot": "false"},
             "ts_ms": 1_700_000_000_000 + first},
            {"op": "u",
             "after": {"driver_id": did, "created_at": "2026-09-01 08:00:00",
                       "trips_total": rng.randrange(40, 90), "vehicle": "ev_bike"},
             "source": {"table": "drivers", "lsn": second, "snapshot": "false"},
             "ts_ms": 1_700_000_000_000 + second},
        ]
        if rng.random() < 0.33:
            pair.reverse()
        out.extend(pair)

    # Deletes, each followed by the tombstone Kafka compaction needs.
    for i in rng.sample(range(1, drivers + 1), k=max(2, drivers // 10)):
        lsn += 1
        did = f"D-{i:04d}"
        out.append({
            "op": "d",
            "before": {"driver_id": did, "created_at": "2026-09-01 08:00:00",
                       "trips_total": 12, "vehicle": "bike"},
            "after": None,
            "source": {"table": "drivers", "lsn": lsn, "snapshot": "false"},
            "ts_ms": 1_700_000_000_000 + lsn,
        })
        out.append(None)

    # A connector restart re-reads the table. These must not win.
    for i in range(1, min(6, drivers) + 1):
        did = f"D-{i:04d}"
        out.append({
            "op": "r",
            "after": {"driver_id": did, "created_at": "2026-09-01 08:00:00",
                      "trips_total": 0, "vehicle": "bike"},
            "source": {"table": "drivers", "lsn": 0, "snapshot": "true"},
            "ts_ms": 1_700_000_000_000,
        })

    rng.shuffle(out)
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", help="Debezium JSONL; omit with --demo")
    parser.add_argument("--demo", action="store_true",
                        help="generate a change stream instead of reading one")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args(argv)

    if args.demo:
        messages = demo_stream(args.seed)
    elif args.source:
        messages = [json.loads(line) if line.strip() and line.strip() != "null" else None
                    for line in pathlib.Path(args.source).read_text(
                        encoding="utf-8").splitlines()]
    else:
        parser.error("give --source or --demo")

    print(json.dumps(load_driver_dimension(messages), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
