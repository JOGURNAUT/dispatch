"""Change data capture: turning a Debezium change stream into current state.

The second source. Trip events are things that happened and are never amended;
a driver row in the application's Postgres is a thing that *is*, and it changes.
Facts come from the event stream, dimensions come from CDC of the operational
database, and the two need different handling because an event is immutable and
a row is not.

Debezium wraps every change in an envelope:

    {"op": "c|u|d|r", "before": {...}|null, "after": {...}|null,
     "source": {"lsn": 42, "table": "drivers", "snapshot": "false"}, "ts_ms": ...}

Four things in that stream break a naive consumer, and each is a quiet wrong
answer rather than a crash.

A DELETE HAS NO `after`. Code written against `after` alone sees null and either
skips the record or writes an empty row. Skipping is worse: the row stays in the
warehouse, so a driver deleted in the application is still being joined against
and still appears in reports. A delete has to produce a *tombstone* -- a row
marked deleted -- not an absent key.

TOMBSTONE MESSAGES ARE NOT DELETES. After a delete Debezium emits a second
message with a null VALUE, so Kafka log compaction can drop the key. It carries
no payload. Parsed as a record it becomes a row of nulls with no primary key.

ORDERING IS BY LSN, NOT BY CLOCK. Two updates to one row can arrive out of
order; `ts_ms` is the commit timestamp and ties at millisecond resolution, while
the Postgres LSN is monotonic and does not. Ordering by time means the older
update sometimes wins and the row silently reverts.

A SNAPSHOT READ IS NOT A CHANGE. When a connector starts it reads the table as
it stands and emits `op: "r"` for every row, with no meaningful LSN. If a
snapshot row is allowed to beat a streamed change, restarting a connector
rewinds the warehouse to whatever the table looked like at restart.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

OP_CREATE = "c"
OP_UPDATE = "u"
OP_DELETE = "d"
OP_READ = "r"          # snapshot
OP_TRUNCATE = "t"
VALID_OPS = frozenset({OP_CREATE, OP_UPDATE, OP_DELETE, OP_READ, OP_TRUNCATE})


class CdcViolation(ValueError):
    """A message that cannot be interpreted as a change at all."""


def flatten(payload: dict[str, Any], prefix: str = "", sep: str = "_",
            max_depth: int = 4) -> dict[str, Any]:
    """Nested CDC JSON to one flat row, as the curated layer wants it.

    Depth-bounded. An unbounded flatten over a document with a deep or cyclic
    structure produces hundreds of columns whose names depend on the data, and a
    curated table whose schema changes with its contents is not a table. Beyond
    the limit the subtree is kept as JSON text so nothing is lost.

    Lists are serialised rather than exploded: exploding turns one row into many
    and silently changes the grain, which is the thing the gold layer asserts
    has not happened.
    """
    out: dict[str, Any] = {}
    for key, value in payload.items():
        name = f"{prefix}{sep}{key}" if prefix else key
        if isinstance(value, dict):
            if max_depth <= 1:
                out[name] = json.dumps(value, default=str)
            else:
                out.update(flatten(value, name, sep, max_depth - 1))
        elif isinstance(value, (list, tuple)):
            out[name] = json.dumps(value, default=str)
        else:
            out[name] = value
    return out


@dataclass
class CdcRecord:
    op: str
    table: str
    key: tuple
    after: dict[str, Any] | None
    before: dict[str, Any] | None
    lsn: int
    ts_ms: int
    snapshot: bool = False

    @property
    def is_delete(self) -> bool:
        return self.op == OP_DELETE

    def precedence(self) -> tuple:
        """What decides which of two changes to one row is the later one.

        A streamed change always outranks a snapshot read, whatever their LSNs
        say, because a snapshot carries no meaningful position. Within streamed
        changes the LSN decides; `ts_ms` only breaks a tie between two records
        that genuinely share a position.
        """
        return (0 if self.snapshot else 1, self.lsn, self.ts_ms)


@dataclass
class RowState:
    """Where a primary key ended up after every change that touched it."""

    key: tuple
    row: dict[str, Any]
    deleted: bool
    lsn: int
    op: str
    changes: int = 1
    # The winning record's precedence, kept rather than recomputed. Rebuilding
    # it from the stored fields means reconstructing whether that record was a
    # snapshot, and getting that wrong silently lets a snapshot beat a change.
    rank: tuple = (1, 0, 0)

    def as_row(self, key_columns: Iterable[str]) -> dict[str, Any]:
        """The flat row for the curated table, including the delete marker.

        `_deleted` travels with the data rather than being filtered out here.
        A loader that drops deleted keys cannot distinguish "this row was
        removed upstream" from "this batch did not mention it", and the first
        has to remove the warehouse row while the second must leave it alone.
        """
        out = dict(self.row)
        out["_deleted"] = self.deleted
        out["_lsn"] = self.lsn
        for column in key_columns:
            out.setdefault(column, None)
        return out


def parse_debezium(message: Any, key_columns: Iterable[str] = ("id",),
                   ) -> CdcRecord | None:
    """One Debezium envelope to a CdcRecord, or None for a tombstone.

    Returns None rather than raising for a tombstone because it is a normal,
    expected message with nothing in it -- it exists so Kafka compaction can
    drop the key, and the delete it follows was already a real record.
    """
    if message is None:
        return None
    if isinstance(message, (str, bytes)):
        text = message.decode() if isinstance(message, bytes) else message
        if not text.strip() or text.strip() == "null":
            return None
        try:
            message = json.loads(text)
        except json.JSONDecodeError as exc:
            raise CdcViolation(f"not JSON: {exc}") from exc
    if not isinstance(message, dict):
        raise CdcViolation(f"envelope is {type(message).__name__}, not an object")

    # Connectors configured with schemas enabled wrap everything one level
    # deeper. Unwrapping here rather than in each caller means one place knows
    # about the setting.
    if "payload" in message and isinstance(message["payload"], dict):
        message = message["payload"]

    if message.get("value", "absent") is None and "op" not in message:
        return None  # tombstone shaped as {"key": ..., "value": null}

    op = (message.get("op") or "").strip().lower()
    if not op:
        return None  # no operation and no payload: a tombstone
    if op not in VALID_OPS:
        raise CdcViolation(f"unknown op {op!r}")

    source = message.get("source") or {}
    after = message.get("after")
    before = message.get("before")

    if op == OP_TRUNCATE:
        # A truncate is about the table, not a row: it carries no before or
        # after image and therefore no primary key. Demanding one here rejected
        # the only message that means "every row in this table is now gone".
        return CdcRecord(
            op=op,
            table=str(source.get("table") or message.get("table") or "unknown"),
            key=(),
            after=None,
            before=None,
            lsn=int(source.get("lsn") or 0),
            ts_ms=int(message.get("ts_ms") or source.get("ts_ms") or 0),
        )

    if op == OP_DELETE and before is None:
        raise CdcViolation("delete carries no before image, so it has no key")
    if op in (OP_CREATE, OP_UPDATE, OP_READ) and after is None:
        raise CdcViolation(f"op {op!r} carries no after image")

    # A delete's identity lives in `before`; everything else's in `after`.
    identity = before if op == OP_DELETE else after
    flat = flatten(identity or {})
    missing = [c for c in key_columns if flat.get(c) is None]
    if missing:
        raise CdcViolation(f"primary key column(s) {missing} absent or null")

    snapshot = str(source.get("snapshot", "false")).lower() in ("true", "last", "first")
    return CdcRecord(
        op=op,
        table=str(source.get("table") or message.get("table") or "unknown"),
        key=tuple(flat[c] for c in key_columns),
        after=flatten(after) if after else None,
        before=flatten(before) if before else None,
        # A snapshot read legitimately has no LSN; 0 is correct for it and the
        # snapshot flag in precedence() is what actually keeps it from winning.
        lsn=int(source.get("lsn") or source.get("scn") or 0),
        ts_ms=int(message.get("ts_ms") or source.get("ts_ms") or 0),
        snapshot=snapshot or op == OP_READ,
    )


@dataclass
class CollapseResult:
    states: dict[tuple, RowState] = field(default_factory=dict)
    tombstones_skipped: int = 0
    out_of_order: int = 0
    truncates: int = 0

    @property
    def live(self) -> list[RowState]:
        return [s for s in self.states.values() if not s.deleted]

    @property
    def deleted(self) -> list[RowState]:
        return [s for s in self.states.values() if s.deleted]


def collapse(messages: Iterable[Any], key_columns: Iterable[str] = ("id",),
             ) -> CollapseResult:
    """Every change in a batch, reduced to the current state of each row.

    Order-independent by construction: the result depends on the set of
    messages, not on the order they were read in. That is what makes a replayed
    Kafka partition or a re-run connector converge on the same table instead of
    on whichever record happened to arrive last.
    """
    key_columns = tuple(key_columns)
    result = CollapseResult()

    for message in messages:
        record = parse_debezium(message, key_columns)
        if record is None:
            result.tombstones_skipped += 1
            continue
        if record.op == OP_TRUNCATE:
            # A truncate removes every row in the table. Marking them deleted
            # rather than forgetting them keeps the loader able to remove the
            # warehouse rows; dropping the keys here would make a truncate look
            # exactly like a quiet batch.
            result.truncates += 1
            for state in result.states.values():
                state.deleted = True
            continue

        incumbent = result.states.get(record.key)
        seen = (incumbent.changes + 1) if incumbent else 1

        if incumbent is not None and record.precedence() <= incumbent.rank:
            # An older change for a row already at a later position. Counted,
            # not applied: this is the out-of-order case, and applying it would
            # revert the row to a value the database has already moved past.
            result.out_of_order += 1
            incumbent.changes = seen
            continue

        result.states[record.key] = RowState(
            key=record.key,
            row=dict(record.before or {}) if record.is_delete else dict(record.after or {}),
            deleted=record.is_delete,
            lsn=record.lsn,
            op=record.op,
            changes=seen,
            rank=record.precedence(),
        )

    return result
