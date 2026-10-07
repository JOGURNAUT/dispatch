"""Gates that stop a bad load instead of describing it afterwards.

A check that logs a warning and lets the run continue is a check nobody reads.
Every gate here returns a verdict with a severity, and the Airflow task raises on
any BLOCK, so a failed gate is a red task rather than a line in a log file that
turns up in a post-mortem three weeks later.

The gates are chosen from failures that have actually happened on real pipelines,
not from a generic list:

ROW CONSERVATION. A LEFT JOIN written correctly and then made inner by a WHERE
clause on the right-hand table drops rows silently and produces a smaller, very
reasonable-looking table. This exact defect dropped 1,647 orders out of ~32,000
on a delivery dataset and biased every figure computed before it was found.
Nothing failed, no error was raised, the numbers were simply wrong. A join that
must preserve its left side is asserted to preserve it.

RECONCILIATION. Everything that enters a layer must leave it as either a row or a
quarantined row. A stage that can silently lose records is a stage where loss is
invisible until someone counts.

FRESHNESS. A pipeline that keeps succeeding on yesterday's data is worse than one
that fails, because the dashboard stays green.

DISTRIBUTION DRIFT. Volume inside a plausible band against recent history. Catches
a partial upstream export -- the failure mode where the job succeeds, the schema
validates, every row is individually correct, and a third of the data is missing.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

BLOCK = "block"
WARN = "warn"


class QualityGateFailed(RuntimeError):
    """Raised by `enforce` when any BLOCK-severity gate fails."""


@dataclass
class Verdict:
    name: str
    passed: bool
    severity: str
    detail: str
    observed: Any = None
    expected: Any = None

    def __str__(self) -> str:
        mark = "PASS" if self.passed else ("FAIL" if self.severity == BLOCK else "WARN")
        return f"[{mark}] {self.name}: {self.detail}"


@dataclass
class GateReport:
    stage: str
    verdicts: list[Verdict] = field(default_factory=list)

    def add(self, verdict: Verdict) -> Verdict:
        self.verdicts.append(verdict)
        return verdict

    @property
    def blocking_failures(self) -> list[Verdict]:
        return [v for v in self.verdicts if not v.passed and v.severity == BLOCK]

    @property
    def ok(self) -> bool:
        return not self.blocking_failures

    def enforce(self) -> GateReport:
        """Raise unless every blocking gate passed. Called by the Airflow task."""
        if self.blocking_failures:
            lines = "\n".join(f"  - {v}" for v in self.blocking_failures)
            raise QualityGateFailed(
                f"{len(self.blocking_failures)} blocking check(s) failed in "
                f"stage {self.stage!r}:\n{lines}")
        return self

    def summary(self) -> str:
        passed = sum(1 for v in self.verdicts if v.passed)
        return f"{self.stage}: {passed}/{len(self.verdicts)} checks passed"


# ------------------------------------------------------------------ gates

def check_row_conservation(report: GateReport, *, left_rows: int, joined_rows: int,
                           join_name: str) -> Verdict:
    """A left join must not change the cardinality of its left side.

    Fewer rows means the join became inner somewhere -- a null key, or a filter
    on the right-hand table that runs before the join is materialised. More rows
    means the right side is not unique on the join key and the left side has been
    fanned out, which silently double-counts every measure downstream.
    """
    passed = joined_rows == left_rows
    if joined_rows < left_rows:
        detail = (f"{join_name} dropped {left_rows - joined_rows} of {left_rows} left rows "
                  f"- the join is behaving as INNER")
    elif joined_rows > left_rows:
        detail = (f"{join_name} produced {joined_rows - left_rows} extra rows from "
                  f"{left_rows} - the right side is not unique on the join key")
    else:
        detail = f"{join_name} preserved all {left_rows} left rows"
    return report.add(Verdict("row_conservation", passed, BLOCK, detail,
                              observed=joined_rows, expected=left_rows))


def check_reconciliation(report: GateReport, *, incoming: int, accepted: int,
                         quarantined: int) -> Verdict:
    """Nothing enters a stage and quietly fails to leave it."""
    total = accepted + quarantined
    passed = total == incoming
    detail = (f"{incoming} in = {accepted} accepted + {quarantined} quarantined"
              if passed else
              f"{incoming} in but {total} out ({accepted} accepted + {quarantined} "
              f"quarantined) - {incoming - total} record(s) unaccounted for")
    return report.add(Verdict("reconciliation", passed, BLOCK, detail,
                              observed=total, expected=incoming))


def check_unique_key(report: GateReport, *, rows: Sequence[Any],
                     key: Callable[[Any], Any], key_name: str) -> Verdict:
    """The declared grain really is the grain.

    A fact table with two rows per trip does not raise anything; it just makes
    every count twice what it should be.
    """
    seen, dupes = set(), set()
    for row in rows:
        k = key(row)
        (dupes if k in seen else seen).add(k)
    passed = not dupes
    detail = (f"{key_name} unique across {len(rows)} rows" if passed else
              f"{len(dupes)} duplicate {key_name} value(s), e.g. "
              f"{sorted(map(str, dupes))[:3]}")
    return report.add(Verdict("unique_key", passed, BLOCK, detail,
                              observed=len(dupes), expected=0))


def check_not_null(report: GateReport, *, rows: Sequence[Any], column: str,
                   severity: str = BLOCK) -> Verdict:
    nulls = sum(1 for row in rows if getattr(row, column, None) is None)
    passed = nulls == 0
    detail = (f"{column} has no nulls across {len(rows)} rows" if passed else
              f"{nulls} of {len(rows)} rows have a null {column}")
    return report.add(Verdict(f"not_null:{column}", passed, severity, detail,
                              observed=nulls, expected=0))


def check_referential_integrity(report: GateReport, *, rows: Sequence[Any],
                                column: str, known: set, dim_name: str,
                                severity: str = BLOCK) -> Verdict:
    """Every non-null foreign key resolves in its dimension.

    Nulls are excluded deliberately: a trip with no driver yet is a real trip,
    and the not-null gate is where that is judged. What this catches is a key
    that is present and wrong, which is the one an outer join hides.
    """
    orphans = {getattr(row, column) for row in rows
               if getattr(row, column, None) is not None
               and getattr(row, column) not in known}
    passed = not orphans
    detail = (f"all {column} values resolve in {dim_name}" if passed else
              f"{len(orphans)} {column} value(s) missing from {dim_name}, e.g. "
              f"{sorted(map(str, orphans))[:3]}")
    return report.add(Verdict(f"fk:{column}", passed, severity, detail,
                              observed=len(orphans), expected=0))


def check_freshness(report: GateReport, *, latest_event: datetime | None,
                    as_of: datetime, max_lag: timedelta) -> Verdict:
    """The data is as new as the run claims it is.

    Without this a stopped producer looks exactly like a quiet hour, and the
    pipeline keeps succeeding over a table that stopped moving.
    """
    if latest_event is None:
        return report.add(Verdict("freshness", False, BLOCK,
                                  "no events in the batch at all"))
    lag = as_of - latest_event
    passed = lag <= max_lag
    detail = (f"newest event is {lag} old (limit {max_lag})" if passed else
              f"newest event is {lag} old, over the {max_lag} limit "
              f"- the producer may have stopped")
    return report.add(Verdict("freshness", passed, BLOCK, detail,
                              observed=str(lag), expected=str(max_lag)))


def check_volume_band(report: GateReport, *, observed: int, baseline: Sequence[int],
                      tolerance: float = 0.5, severity: str = BLOCK) -> Verdict:
    """Today's volume sits within a band around recent history.

    The gate for a partial export: schema valid, every row correct, a third of
    the data absent. Skipped when there is not enough history to have a baseline,
    because a gate that fires on day one of a backfill trains people to ignore it.
    """
    if len(baseline) < 3:
        return report.add(Verdict("volume_band", True, WARN,
                                  f"only {len(baseline)} baseline day(s) - band not evaluated",
                                  observed=observed))
    expected = sorted(baseline)[len(baseline) // 2]  # median resists one bad day
    low, high = expected * (1 - tolerance), expected * (1 + tolerance)
    passed = low <= observed <= high
    detail = (f"{observed} rows within [{low:.0f}, {high:.0f}] of median {expected}"
              if passed else
              f"{observed} rows outside [{low:.0f}, {high:.0f}] (median {expected}) "
              f"- suspect a partial or duplicated load")
    return report.add(Verdict("volume_band", passed, severity, detail,
                              observed=observed, expected=expected))


def check_measurable_share(report: GateReport, *, facts: Sequence[Any],
                           min_share: float = 0.5) -> Verdict:
    """Enough trips are complete for an average over them to mean anything.

    A batch where most trips are still open is not wrong, but a metric computed
    over the few that closed is a biased sample of the fast ones. This is the
    gate that would have caught a lagging upstream table turning 187 of 269
    orders into nulls that downstream code read as fact.
    """
    if not facts:
        return report.add(Verdict("measurable_share", False, BLOCK, "no trips in batch"))
    measurable = sum(1 for f in facts if f.measurable)
    share = measurable / len(facts)
    passed = share >= min_share
    detail = (f"{measurable}/{len(facts)} trips measurable ({share:.1%})" if passed else
              f"only {measurable}/{len(facts)} trips measurable ({share:.1%}, floor "
              f"{min_share:.0%}) - upstream is probably lagging, metrics would be biased")
    return report.add(Verdict("measurable_share", passed, BLOCK, detail,
                              observed=round(share, 4), expected=min_share))
