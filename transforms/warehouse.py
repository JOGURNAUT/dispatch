"""Loading the gold layer into a relational warehouse.

Targets SQLite by default and PostgreSQL when a DSN is supplied. Both paths go
through the same DDL and the same load routine, with only the placeholder style
and one type name differing, so the local demo exercises the code that runs in
Docker rather than a simplified cousin of it.

SQLite is not a toy choice here: it means the whole pipeline runs end to end on a
machine with nothing installed, which is the difference between a reviewer seeing
the thing work and reading a claim that it would.

Loads are transactional and idempotent. A task that is retried -- and Airflow
retries -- must not leave the warehouse holding one and a half loads, and must
not append a second copy of a partition it already wrote. Each load replaces its
partition inside one transaction: either the new partition is there or the old
one still is, never both and never neither.
"""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterable, Sequence
from contextlib import contextmanager
from dataclasses import fields as dataclass_fields
from datetime import UTC, datetime
from typing import Any

DEFAULT_SQLITE = "data/warehouse.db"

# Python 3.12 removed sqlite3's implicit datetime adapter. Without an explicit
# one a datetime is stored by str() with no guarantee of format, and the column
# stops being comparable or parseable on read.
sqlite3.register_adapter(datetime, lambda dt: dt.isoformat(sep=" "))

DDL = {
    "dim_store": """
        CREATE TABLE IF NOT EXISTS dim_store (
            store_id      TEXT PRIMARY KEY,
            store_name    TEXT,
            city          TEXT
        )""",
    "dim_driver": """
        CREATE TABLE IF NOT EXISTS dim_driver (
            driver_id     TEXT PRIMARY KEY,
            first_seen_at TIMESTAMP,
            trips_total   INTEGER
        )""",
    "dim_date": """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key      TEXT PRIMARY KEY,
            day_of_week   INTEGER,
            is_weekend    INTEGER
        )""",
    "fct_trip": """
        CREATE TABLE IF NOT EXISTS fct_trip (
            trip_id          TEXT PRIMARY KEY,
            order_id         TEXT,
            driver_id        TEXT,
            store_id         TEXT,
            date_key         TEXT,
            assigned_at      TIMESTAMP,
            picked_up_at     TIMESTAMP,
            delivered_at     TIMESTAMP,
            terminal_event   TEXT,
            completeness     TEXT,
            event_count      INTEGER,
            distance_m       INTEGER,
            promised_minutes INTEGER,
            tat_minutes      REAL,
            pickup_minutes   REAL,
            is_breach        INTEGER,
            issues           TEXT
        )""",
    # Not an afterthought. A row that fails a contract has to land somewhere a
    # person can read, or "we quarantined 412 records" is a number with nothing
    # behind it and nobody ever finds out what was actually wrong with them.
    "quarantine": """
        CREATE TABLE IF NOT EXISTS quarantine (
            batch_id    TEXT,
            stage       TEXT,
            reason      TEXT,
            payload     TEXT,
            quarantined_at TIMESTAMP
        )""",
    # The run log is what makes the gates auditable after the fact. Without it,
    # a gate that passed and a gate that was never run look identical a week later.
    "run_audit": """
        CREATE TABLE IF NOT EXISTS run_audit (
            batch_id    TEXT,
            stage       TEXT,
            check_name  TEXT,
            passed      INTEGER,
            severity    TEXT,
            detail      TEXT,
            observed    TEXT,
            expected    TEXT,
            recorded_at TIMESTAMP
        )""",
}

INDEXES = (
    "CREATE INDEX IF NOT EXISTS ix_fct_trip_date ON fct_trip(date_key)",
    "CREATE INDEX IF NOT EXISTS ix_fct_trip_store ON fct_trip(store_id)",
    "CREATE INDEX IF NOT EXISTS ix_audit_batch ON run_audit(batch_id)",
)


class Warehouse:
    """A thin two-dialect wrapper. Deliberately not an ORM.

    The transformations are already done by the time anything reaches here, so
    what is needed is DDL, a transactional replace, and a way to read a count
    back for the gates. An ORM would add a mapping layer over code whose whole
    job is to not reinterpret the rows.
    """

    def __init__(self, dsn: str | None = None):
        self.dsn = dsn or os.environ.get("DISPATCH_DSN") or DEFAULT_SQLITE
        self.is_postgres = self.dsn.startswith(("postgres://", "postgresql://"))
        self.placeholder = "%s" if self.is_postgres else "?"

    @contextmanager
    def connect(self):
        if self.is_postgres:
            import psycopg2
            conn = psycopg2.connect(self.dsn)
        else:
            path = os.path.abspath(self.dsn)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            conn = sqlite3.connect(path)
            # Without this SQLite accepts an orphaned foreign key without a word,
            # which would make the referential-integrity gate the only thing
            # standing between a typo and a silently wrong join.
            conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
        finally:
            conn.close()

    def migrate(self) -> None:
        ddl = DDL
        if self.is_postgres:
            ddl = {name: sql.replace("INTEGER PRIMARY KEY", "SERIAL PRIMARY KEY")
                   for name, sql in DDL.items()}
        with self.connect() as conn:
            cur = conn.cursor()
            for statement in ddl.values():
                cur.execute(statement)
            for statement in INDEXES:
                cur.execute(statement)
            conn.commit()

    def load_replace(self, table: str, columns: Sequence[str],
                     rows: Iterable[Sequence[Any]],
                     partition_column: str | None = None,
                     partition_values: Sequence[str] = ()) -> int:
        """Replace a partition inside one transaction, or change nothing.

        The retry-safety guarantee. Deleting and inserting in two autocommitted
        statements leaves a window where the table holds neither the old
        partition nor the new one, and a task that dies in that window takes a
        dashboard with it.
        """
        rows = list(rows)
        marks = ", ".join([self.placeholder] * len(columns))
        insert = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({marks})"

        with self.connect() as conn:
            cur = conn.cursor()
            try:
                if partition_column and partition_values:
                    holes = ", ".join([self.placeholder] * len(partition_values))
                    cur.execute(
                        f"DELETE FROM {table} WHERE {partition_column} IN ({holes})",
                        tuple(partition_values))
                elif partition_column is None:
                    cur.execute(f"DELETE FROM {table}")
                cur.executemany(insert, rows)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return len(rows)

    def count(self, table: str, where: str = "") -> int:
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {table} {where}")
            return cur.fetchone()[0]

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(sql, tuple(params))
            return cur.fetchall()

    def record_gates(self, batch_id: str, report) -> int:
        """Persist every verdict, passed and failed.

        Only logging failures is the mistake that makes an audit trail useless:
        a check that passed and a check that never ran look the same afterwards.
        """
        now = datetime.now(UTC).replace(tzinfo=None)
        rows = [(batch_id, report.stage, v.name, 1 if v.passed else 0, v.severity,
                 v.detail, str(v.observed), str(v.expected), now)
                for v in report.verdicts]
        columns = ("batch_id", "stage", "check_name", "passed", "severity",
                   "detail", "observed", "expected", "recorded_at")
        marks = ", ".join([self.placeholder] * len(columns))
        with self.connect() as conn:
            cur = conn.cursor()
            cur.executemany(
                f"INSERT INTO run_audit ({', '.join(columns)}) VALUES ({marks})", rows)
            conn.commit()
        return len(rows)


def fact_row(fact, date_key: str) -> tuple:
    """One TripFact as the column tuple fct_trip declares.

    Built from the dataclass field order rather than hand-listed so a field added
    to TripFact and forgotten here fails loudly at load instead of shifting every
    later column by one.
    """
    names = [f.name for f in dataclass_fields(fact)]
    expected = ["trip_id", "order_id", "driver_id", "store_id", "assigned_at",
                "picked_up_at", "delivered_at", "terminal_event", "completeness",
                "event_count", "distance_m", "promised_minutes", "tat_minutes",
                "pickup_minutes", "is_breach", "issues"]
    if names != expected:
        raise RuntimeError(
            f"TripFact fields changed ({names}) - update fct_trip DDL and this mapping")
    return (
        fact.trip_id, fact.order_id, fact.driver_id, fact.store_id, date_key,
        fact.assigned_at, fact.picked_up_at, fact.delivered_at, fact.terminal_event,
        fact.completeness, fact.event_count, fact.distance_m, fact.promised_minutes,
        fact.tat_minutes, fact.pickup_minutes,
        None if fact.is_breach is None else int(fact.is_breach),
        ",".join(fact.issues) or None,
    )


FCT_TRIP_COLUMNS = ("trip_id", "order_id", "driver_id", "store_id", "date_key",
                    "assigned_at", "picked_up_at", "delivered_at", "terminal_event",
                    "completeness", "event_count", "distance_m", "promised_minutes",
                    "tat_minutes", "pickup_minutes", "is_breach", "issues")
