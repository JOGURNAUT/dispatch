"""Letting a producer change its schema without breaking the pipeline, and
refusing the changes that cannot be absorbed.

The contract in `contracts.py` says what a record must contain. This says how
that set is allowed to move over time, which is a different question and the one
that actually comes up: a producer team adds a field on Tuesday and nobody tells
the data team.

Three things can happen and they need three different answers.

A PRODUCER ADDS A FIELD. The consumer does not know it exists. The wrong answer
is the one that costs nothing to write, which is to ignore the key -- the data
arrives, the pipeline succeeds, and the field is gone. Six weeks later someone
asks for a report on it and the answer is that it was never stored, for every
row, since the day it was added. Unknown fields are captured in `_extra` here,
so promoting one to a real column later is a backfill rather than a wait.

A PRODUCER STOPS SENDING A FIELD. Fine if the field is optional, fatal if it is
required. The registry knows which, so this is decided rather than discovered.

A PRODUCER CHANGES WHAT A FIELD MEANS. Renames it, changes its type, narrows an
enum. No consumer can absorb this, and the only safe response is to refuse the
version and say what broke. `check_compatibility` is what the producer team runs
in their own CI before shipping, which is the point at which this is cheap.

Compatibility is named from the reader's perspective, as it is in Avro and the
Confluent registry, because that is the side that breaks:

    BACKWARD   a new reader can read data written by the old writer
    FORWARD    an old reader can read data written by the new writer
    FULL       both
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Envelope, not payload: present on the record, owned by the pipeline, and not
# a field any schema version declares.
RESERVED_KEYS = frozenset({"schema_version"})

BACKWARD = "backward"
FORWARD = "forward"
FULL = "full"
BREAKING = "breaking"


class IncompatibleSchema(ValueError):
    """A proposed version no consumer can be expected to absorb."""


@dataclass(frozen=True)
class FieldSpec:
    name: str
    kind: str                 # "str" | "int" | "float" | "timestamp"
    required: bool = False
    # Carried so a field added in v1.1 can be read from a v1.0 row without the
    # caller having to know which version produced it.
    default: Any = None
    since: str = "1.0.0"
    # Set when a field is retired. Kept in the registry rather than deleted:
    # removing the entry would make a row that still carries the field look like
    # it has an unknown one, and the reason it was dropped would be nowhere.
    until: str | None = None

    def accepts(self, value: Any) -> bool:
        if value is None:
            return not self.required
        if self.kind == "str":
            return isinstance(value, str)
        if self.kind == "int":
            return isinstance(value, int) and not isinstance(value, bool)
        if self.kind == "float":
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        if self.kind == "timestamp":
            from .contracts import parse_timestamp
            return parse_timestamp(value) is not None
        return True


@dataclass
class SchemaVersion:
    version: str
    fields: tuple[FieldSpec, ...]
    note: str = ""

    @property
    def by_name(self) -> dict[str, FieldSpec]:
        return {spec.name: spec for spec in self.fields}

    @property
    def required_names(self) -> set[str]:
        return {spec.name for spec in self.fields if spec.required}

    @property
    def live_names(self) -> set[str]:
        return {spec.name for spec in self.fields if spec.until is None}


# The history of the event schema. Appended to, never edited: a past version
# rewritten to match the present is a version that no longer describes the rows
# written under it, and every claim about compatibility becomes unfalsifiable.
_V1_0_0 = SchemaVersion(
    version="1.0.0",
    note="initial trip telemetry contract",
    fields=(
        FieldSpec("event_id", "str", required=True),
        FieldSpec("trip_id", "str", required=True),
        FieldSpec("event_type", "str", required=True),
        FieldSpec("event_ts", "timestamp", required=True),
        FieldSpec("order_id", "str"),
        FieldSpec("driver_id", "str"),
        FieldSpec("store_id", "str"),
        FieldSpec("lat", "float"),
        FieldSpec("lon", "float"),
        FieldSpec("distance_m", "int"),
        FieldSpec("promised_minutes", "int"),
        FieldSpec("producer_version", "str"),
    ),
)

# A real additive change: the rider app started reporting vehicle type and
# battery level. Both optional, both defaulted, so a v1.0.0 row reads under
# v1.1.0 and a v1.1.0 row reads under v1.0.0 with the extras parked in `_extra`.
_V1_1_0 = SchemaVersion(
    version="1.1.0",
    note="rider app adds vehicle_type and battery_pct; both optional",
    fields=_V1_0_0.fields + (
        FieldSpec("vehicle_type", "str", default="unknown", since="1.1.0"),
        FieldSpec("battery_pct", "int", default=None, since="1.1.0"),
    ),
)

REGISTRY: dict[str, SchemaVersion] = {
    _V1_0_0.version: _V1_0_0,
    _V1_1_0.version: _V1_1_0,
}
LATEST = _V1_1_0.version


def get(version: str | None) -> SchemaVersion:
    """The schema for a version, falling back to the oldest for an unknown one.

    Falling back to the OLDEST rather than the latest is deliberate. An unknown
    version is more likely an old producer nobody catalogued than a future one,
    and assuming the latest would mean treating its absent new fields as missing
    rather than as not-yet-existing.
    """
    if version and version in REGISTRY:
        return REGISTRY[version]
    return _V1_0_0


@dataclass
class CompatibilityReport:
    old: str
    new: str
    verdict: str
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    retyped: list[str] = field(default_factory=list)
    newly_required: list[str] = field(default_factory=list)
    added_without_default: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.verdict != BREAKING

    def __str__(self) -> str:
        bits = [f"{self.old} -> {self.new}: {self.verdict}"]
        for label, names in (("added", self.added), ("removed", self.removed),
                             ("retyped", self.retyped),
                             ("newly required", self.newly_required),
                             ("added without default", self.added_without_default)):
            if names:
                bits.append(f"  {label}: {', '.join(sorted(names))}")
        return "\n".join(bits)

    def raise_if_breaking(self) -> CompatibilityReport:
        if not self.ok:
            raise IncompatibleSchema(str(self))
        return self


def check_compatibility(old_version: str, new_version: str) -> CompatibilityReport:
    """What a reader of one version can do with data written under the other.

    Run this in the producer's CI, before the change ships. Finding out at
    ingestion time means finding out from a partition that has already landed.
    """
    old, new = get(old_version), get(new_version)
    old_fields, new_fields = old.by_name, new.by_name

    added = [n for n in new_fields if n not in old_fields]
    removed = [n for n in old_fields if n not in new_fields]
    retyped = [n for n in new_fields
               if n in old_fields and new_fields[n].kind != old_fields[n].kind]
    # A field that becomes required breaks every reader of data written before
    # it existed, because that data legitimately does not have it.
    newly_required = [n for n in new.required_names
                      if n in old_fields and not old_fields[n].required]
    newly_required += [n for n in added if new_fields[n].required]
    # An added optional field with no default cannot be read out of an old row
    # at all -- there is nothing to return. That is an additive change that is
    # nonetheless not forward compatible.
    added_without_default = [n for n in added
                             if not new_fields[n].required
                             and new_fields[n].default is None
                             and new_fields[n].kind != "str"]

    report = CompatibilityReport(
        old=old.version, new=new.version, verdict=FULL,
        added=added, removed=removed, retyped=retyped,
        newly_required=sorted(set(newly_required)),
        added_without_default=added_without_default,
    )

    if retyped or report.newly_required:
        # Nothing a consumer can do with a field whose meaning moved under it.
        report.verdict = BREAKING
    elif removed and added:
        report.verdict = BREAKING if retyped else BACKWARD
    elif removed:
        # A new reader copes (the field is simply gone); an old reader expecting
        # it does not.
        report.verdict = BACKWARD
    elif added:
        report.verdict = FULL
    return report


def split_known(raw: dict[str, Any], version: str | None = None
                ) -> tuple[dict[str, Any], dict[str, Any]]:
    """Separate a payload into fields the schema declares and everything else.

    The extras are returned rather than discarded. That is the whole mechanism:
    a field a producer started sending before anyone updated the registry is
    stored, so promoting it to a real column later is a backfill over data that
    exists rather than a wait for new data to accumulate.

    Keys beginning with "_" are pipeline-internal (ingestion timestamps, the
    generator's fault markers) and are neither schema fields nor producer
    extras. `schema_version` is the envelope rather than the payload -- counting
    it as an undeclared field would make that flag fire on every well-behaved
    record, which is the fastest way to make a flag worthless.
    """
    schema = get(version)
    known_names = schema.by_name
    known: dict[str, Any] = {}
    extra: dict[str, Any] = {}
    for key, value in raw.items():
        if key.startswith("_") or key in RESERVED_KEYS:
            continue
        (known if key in known_names else extra)[key] = value
    return known, extra


def apply_defaults(known: dict[str, Any], version: str | None = None) -> dict[str, Any]:
    """Fill fields the schema declares that this payload does not carry.

    What makes an old row readable under a newer schema: a v1.0.0 event has no
    `vehicle_type`, and the answer is the declared default, not a KeyError and
    not a silent null that a reader cannot distinguish from a real missing value.
    """
    schema = get(version)
    out = dict(known)
    for spec in schema.fields:
        if spec.until is None and spec.name not in out:
            out[spec.name] = spec.default
    return out


def validate_types(known: dict[str, Any], version: str | None = None) -> list[str]:
    """Fields present but of the wrong type, by name.

    Returned rather than raised: one mistyped optional field should flag a row,
    not fail a batch, and the caller knows which of its fields are load-bearing.
    """
    schema = get(version)
    bad = []
    for name, value in known.items():
        spec = schema.by_name.get(name)
        if spec is not None and not spec.accepts(value):
            bad.append(name)
    return sorted(bad)
