"""Telling someone a gate failed.

A red task in a scheduler nobody has open is a failure that is discovered the
next morning, by the person reading the dashboard it corrupted. The gates
already stop the load; this is the part that makes the stop visible.

What a useful alert has to contain, and what most do not:

WHICH CHECK AND WHAT IT SAW. "dispatch_medallion failed" sends the reader to the
Airflow UI to find out anything at all. The verdict text already says `fct_trip
dropped 1,647 of 32,380 left rows - the join is behaving as INNER`, and that
sentence is the alert.

WHETHER ANYTHING MOVED. The first question on being paged is whether the
warehouse is currently wrong. Because the gates run before the load, the answer
is almost always "no, nothing moved", and saying so turns a page into a ticket.

NOT EVERY RUN. An alert on success trains people to filter the channel, and a
filtered channel is the same as no alerting with extra steps.

Delivery is a webhook when `DISPATCH_ALERT_WEBHOOK` is set and stdout otherwise.
Stdout is not a placeholder: in Airflow it lands in the task log next to the
traceback, which is where someone reading a failed task is already looking.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone

WEBHOOK_ENV = "DISPATCH_ALERT_WEBHOOK"
TIMEOUT_SECONDS = 10


@dataclass
class Alert:
    stage: str
    batch_id: str
    failures: list[str]
    rows_loaded: int | None = None
    context: dict | None = None

    @property
    def title(self) -> str:
        count = len(self.failures)
        return (f"dispatch: {count} quality gate{'s' if count != 1 else ''} "
                f"failed in {self.stage}")

    def to_text(self) -> str:
        lines = [
            self.title,
            f"batch   {self.batch_id}",
            # Stated explicitly rather than implied. The reader's first question
            # is whether the warehouse is wrong right now, and "the load did not
            # run" is the difference between a page and a ticket.
            f"loaded  {'nothing - the gate ran before the load' if not self.rows_loaded else self.rows_loaded}",
            "",
        ]
        lines += [f"  - {failure}" for failure in self.failures]
        for key, value in (self.context or {}).items():
            lines.append(f"  {key}: {value}")
        return "\n".join(lines)

    def to_payload(self) -> dict:
        return {
            "text": self.to_text(),
            "stage": self.stage,
            "batch_id": self.batch_id,
            "failure_count": len(self.failures),
            "failures": self.failures,
            "sent_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        }


def send(alert: Alert, webhook: str | None = None) -> bool:
    """Deliver, returning whether it reached a webhook.

    Never raises. An alerter that can fail a task has made the pipeline less
    reliable than it was without alerting, and the failure it would mask is the
    one it was called to report. A webhook that will not answer falls back to
    stdout, which in Airflow is the task log the reader is already in.
    """
    text = alert.to_text()
    print(text)

    url = webhook or os.environ.get(WEBHOOK_ENV)
    if not url:
        return False

    try:
        # Constructed inside the try, not above it. Request() parses the URL
        # eagerly and raises ValueError on a malformed one, so building it
        # outside let a typo'd webhook crash the alerter -- the one component
        # that must never fail the task it was called to report on.
        request = urllib.request.Request(
            url,
            data=json.dumps(alert.to_payload()).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, OSError, ValueError) as exc:
        print(f"[notify] webhook delivery failed, alert is above: {exc}")
        return False


def from_report(report, batch_id: str, rows_loaded: int | None = None,
                context: dict | None = None) -> Alert | None:
    """An alert from a GateReport, or None when nothing blocking failed.

    Returning None on success is the policy, not an omission: a channel that
    also carries every green run gets muted, and then the red ones are muted
    too.
    """
    failures = report.blocking_failures
    if not failures:
        return None
    return Alert(
        stage=report.stage,
        batch_id=batch_id,
        failures=[str(verdict) for verdict in failures],
        rows_loaded=rows_loaded,
        context=context,
    )


def airflow_failure_callback(context) -> None:
    """`on_failure_callback` for the DAG's tasks.

    The catch-all. `from_report` covers a gate that failed, which is the case
    with something to say; this covers everything else that can kill a task --
    an unreachable broker, an OOM, a bad deploy -- so a failure never passes
    silently just because it was not a gate.
    """
    task = context.get("task_instance")
    exception = context.get("exception")
    alert = Alert(
        stage=getattr(task, "task_id", "unknown"),
        batch_id=context.get("run_id", "unknown"),
        failures=[str(exception) if exception else "task failed without an exception"],
        rows_loaded=None,
        context={"dag": getattr(task, "dag_id", "dispatch_medallion"),
                 "try": getattr(task, "try_number", "?"),
                 "log": getattr(task, "log_url", "")},
    )
    send(alert)
