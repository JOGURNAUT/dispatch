"""Dispatch: a last-mile delivery telemetry pipeline.

The transformation logic lives here as pure Python with no Spark, Kafka or
Airflow import anywhere in it. That is deliberate. Those three are wiring --
where the bytes come from and what schedules the job -- and none of them are the
part that can be wrong in a way that matters. Keeping the logic separable means
the correctness of a dedupe or a late-arrival rule is provable by `pytest` in
under a second, instead of needing a cluster up to find out.
"""

from .contracts import ContractViolation, NormalisedEvent, normalise_event, parse_timestamp
from .dedupe import DedupeResult, dedupe, merge_with_existing
from .quality import GateReport, QualityGateFailed
from .sessionize import TripFact, sessionize

__version__ = "0.1.0"

__all__ = [
    "ContractViolation", "NormalisedEvent", "normalise_event", "parse_timestamp",
    "DedupeResult", "dedupe", "merge_with_existing",
    "GateReport", "QualityGateFailed",
    "TripFact", "sessionize",
]
