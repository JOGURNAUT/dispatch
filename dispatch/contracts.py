"""The boundary where an event stops being whatever the producer sent and becomes
a row this pipeline is allowed to reason about.

Everything downstream reads the names and types declared here. That is the whole
point of a contract: a producer can change its serialisation without a report
three layers away silently changing meaning.

Two things this file exists to absorb, both of which have happened in production
rather than in a tutorial:

TIMESTAMP DRIFT. The same logical field arrives as "2026-07-24 03:16:02" from one
export path and "2026-08-25T17:01:06.860000" from another -- a T separator and
microseconds, same query, different client. A parser that handles only one shape
does not fail loudly; it drops the rows it cannot read, and the loss shows up
weeks later as a dip nobody can explain.

NULL JOIN KEYS. An event with no driver_id is not an error worth discarding, but
it will vanish on an inner join against the driver dimension. It is tagged here
so the join can be made deliberately rather than accidentally.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from . import schema_registry

SCHEMA_VERSION = "1.0.0"

# The lifecycle a trip is allowed to walk. Ordering is what makes an out-of-order
# or impossible event detectable: "delivered" arriving before "picked_up" is not
# a late event, it is a broken one, and the two need different handling.
EVENT_SEQUENCE = (
    "assigned",
    "reached_pickup",
    "picked_up",
    "out_for_delivery",
    "reached_delivery",
    "delivered",
)
TERMINAL_EVENTS = frozenset({"delivered", "cancelled", "returned"})
VALID_EVENTS = frozenset(EVENT_SEQUENCE) | TERMINAL_EVENTS
EVENT_RANK = {name: i for i, name in enumerate(EVENT_SEQUENCE)}

# Columns every downstream job may rely on existing, in a fixed order. A job that
# writes a subset or reorders them is a bug the contract test catches.
SILVER_COLUMNS = (
    "event_id",
    "trip_id",
    "order_id",
    "driver_id",
    "store_id",
    "event_type",
    "event_ts",
    "ingested_at",
    "lat",
    "lon",
    "distance_m",
    "promised_minutes",
    "producer_version",
    "schema_version",
    # Fields the producer sent that the registry does not declare yet, as JSON.
    # Carried rather than dropped so promoting one to a real column later is a
    # backfill over data that exists, instead of a wait for new data to
    # accumulate from the day someone noticed.
    "extra",
)

# Accepted serialisations of a timestamp, most specific first. ISO-8601 with a
# "T" and microseconds is tried before the space-separated form because
# fromisoformat accepts both and the space form does not round-trip the "T".
_TS_PATTERNS = (
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
)

_EPOCH_RE = re.compile(r"^\d{10}(\.\d+)?$")
_EPOCH_MS_RE = re.compile(r"^\d{13}$")


def _from_epoch(seconds: float) -> datetime:
    """Epoch to naive UTC, matching how every other branch returns."""
    return datetime.fromtimestamp(seconds, UTC).replace(tzinfo=None)


class ContractViolation(ValueError):
    """Raised when a record cannot be made to satisfy the contract at all.

    Distinct from a quarantine: a violation means the record is unreadable, not
    that it is readable and wrong.
    """


def parse_timestamp(value: Any) -> datetime | None:
    """Every accepted spelling of an instant, reduced to one naive UTC datetime.

    Returns None rather than raising, because a single unparseable timestamp in a
    batch of a million should quarantine one row, not fail the run. The caller
    decides whether None is tolerable; `normalise_event` treats it as fatal for
    `event_ts` and benign elsewhere.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo else value

    text = str(value).strip()
    if not text:
        return None

    # Epoch seconds and milliseconds. Producers emit these when someone reaches
    # for time.time() instead of the shared helper, and they are unambiguous
    # enough to accept rather than quarantine.
    if _EPOCH_MS_RE.match(text):
        return _from_epoch(int(text) / 1000.0)
    if _EPOCH_RE.match(text):
        return _from_epoch(float(text))

    # A trailing Z or a numeric offset: resolve to UTC, then drop the tzinfo so
    # the whole pipeline compares naive-UTC against naive-UTC. Mixing aware and
    # naive datetimes raises at comparison time, in whichever job happens to
    # compare them first, which is a long way from where the mistake was made.
    iso = text.replace("Z", "+00:00") if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(iso)
        return parsed.astimezone(UTC).replace(tzinfo=None) if parsed.tzinfo else parsed
    except ValueError:
        pass

    for pattern in _TS_PATTERNS:
        try:
            return datetime.strptime(text, pattern)
        except ValueError:
            continue
    return None


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return None if out != out else out  # NaN is not a coordinate


def _as_int(value: Any) -> int | None:
    out = _as_float(value)
    return None if out is None else int(out)


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


@dataclass
class NormalisedEvent:
    """One event, in the only shape downstream code is allowed to see."""

    event_id: str
    trip_id: str
    order_id: str | None
    driver_id: str | None
    store_id: str | None
    event_type: str
    event_ts: datetime
    ingested_at: datetime
    lat: float | None = None
    lon: float | None = None
    distance_m: int | None = None
    promised_minutes: int | None = None
    producer_version: str = "unknown"
    schema_version: str = SCHEMA_VERSION
    # Declared fields this payload did not carry, plus fields it carried that
    # the registry does not declare. Both are kept: the first so a reader can
    # tell a defaulted value from a real one, the second so nothing a producer
    # sent is lost before anyone has decided whether it matters.
    extra: dict[str, Any] = field(default_factory=dict)
    # Not persisted. Carries why a record was held back, for the quarantine sink.
    issues: list[str] = field(default_factory=list)

    def as_row(self) -> dict[str, Any]:
        row = {name: getattr(self, name) for name in SILVER_COLUMNS}
        # Serialised at the boundary rather than by each sink, so the JSONL
        # writer and the Parquet writer cannot disagree about the encoding.
        row["extra"] = json.dumps(self.extra, default=str) if self.extra else None
        return row

    def get(self, name: str, default: Any = None) -> Any:
        """A field by name, whether it is a real column or an undeclared extra.

        Lets a caller read `vehicle_type` without first knowing which schema
        version produced the row, which is the point of keeping the extras.
        """
        if name in SILVER_COLUMNS:
            return getattr(self, name)
        return self.extra.get(name, default)


def normalise_event(raw: dict[str, Any], ingested_at: datetime | None = None) -> NormalisedEvent:
    """Raw producer payload to contract shape, or ContractViolation.

    Fatal (raises): no trip_id, no event_id, unparseable or missing event_ts, an
    event_type outside the vocabulary. Each of those makes the record impossible
    to place in time, in a trip, or in the lifecycle -- there is nothing to
    quarantine it *as*.

    Non-fatal (recorded in `issues`): a missing dimension key, an out-of-range
    coordinate, a negative distance. The row survives, flagged, because a trip
    with no driver_id is still a real trip and discarding it understates volume.

    Schema evolution: fields the registry does not declare are moved to `extra`
    rather than ignored, and declared fields the payload omits are filled from
    their declared defaults. Ignoring an unknown key costs nothing to write and
    loses the field for every row from the day the producer added it.
    """
    issues: list[str] = []

    declared_version = _as_text(raw.get("schema_version"))
    known, extra = schema_registry.split_known(raw, declared_version)
    mistyped = schema_registry.validate_types(known, declared_version)
    if mistyped:
        issues.extend(f"mistyped:{name}" for name in mistyped)
    if extra:
        # Not an error. A producer ahead of the registry is the normal case, and
        # the flag is what makes it visible that the registry owes an update.
        issues.append("undeclared_fields")

    event_id = _as_text(raw.get("event_id"))
    trip_id = _as_text(raw.get("trip_id"))
    if not event_id:
        raise ContractViolation("event_id is required and was empty")
    if not trip_id:
        raise ContractViolation("trip_id is required and was empty")

    event_type = (_as_text(raw.get("event_type")) or "").lower()
    if event_type not in VALID_EVENTS:
        raise ContractViolation(f"unknown event_type {event_type!r}")

    event_ts = parse_timestamp(raw.get("event_ts"))
    if event_ts is None:
        raise ContractViolation(f"event_ts {raw.get('event_ts')!r} is not a timestamp")

    driver_id = _as_text(raw.get("driver_id"))
    store_id = _as_text(raw.get("store_id"))
    # Flagged, not dropped. These are exactly the rows an inner join would eat
    # silently, so the gold layer has to opt into losing them.
    if not driver_id:
        issues.append("null_driver_id")
    if not store_id:
        issues.append("null_store_id")

    lat, lon = _as_float(raw.get("lat")), _as_float(raw.get("lon"))
    if lat is not None and not -90.0 <= lat <= 90.0:
        issues.append("lat_out_of_range")
        lat = None
    if lon is not None and not -180.0 <= lon <= 180.0:
        issues.append("lon_out_of_range")
        lon = None

    distance_m = _as_int(raw.get("distance_m"))
    if distance_m is not None and distance_m < 0:
        issues.append("negative_distance")
        distance_m = None

    promised_minutes = _as_int(raw.get("promised_minutes"))
    if promised_minutes is not None and promised_minutes <= 0:
        issues.append("non_positive_promise")
        promised_minutes = None

    return NormalisedEvent(
        event_id=event_id,
        trip_id=trip_id,
        order_id=_as_text(raw.get("order_id")),
        driver_id=driver_id,
        store_id=store_id,
        event_type=event_type,
        event_ts=event_ts,
        ingested_at=ingested_at or datetime.now(UTC).replace(tzinfo=None),
        lat=lat,
        lon=lon,
        distance_m=distance_m,
        promised_minutes=promised_minutes,
        producer_version=_as_text(raw.get("producer_version")) or "unknown",
        schema_version=declared_version or SCHEMA_VERSION,
        # Declared-but-absent fields are filled from their defaults and kept
        # alongside the undeclared ones, so a reader can ask for a v1.1 field on
        # a v1.0 row and get the declared answer instead of a KeyError.
        # Three sources, in increasing priority: declared fields this version
        # has that are not promoted columns (filled from their defaults), the
        # values the payload actually carried for them, and fields no version
        # declares at all. Dropping any of the three is a silent data loss --
        # the second was, until a test caught vehicle_type arriving and
        # vanishing between the contract and the row.
        extra={k: v for k, v in schema_registry.apply_defaults(
                   {n: val for n, val in known.items() if n not in SILVER_COLUMNS},
                   declared_version).items()
               if k not in SILVER_COLUMNS} | extra,
        issues=issues,
    )
