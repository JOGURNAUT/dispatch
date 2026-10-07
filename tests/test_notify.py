"""Tests for the alerting path.

Two properties matter more than the formatting. An alerter must never fail the
task that called it, because the failure it would mask is the one it was called
to report. And it must say nothing on a successful run, because a channel that
also carries every green run gets muted, and then the red ones are muted too.
"""

from __future__ import annotations

import json
from datetime import timedelta

import pytest

from dispatch import notify
from dispatch.quality import (
    GateReport,
    check_measurable_share,
    check_row_conservation,
    check_unique_key,
)


def failing_report() -> GateReport:
    report = GateReport("silver_to_gold")
    check_row_conservation(report, left_rows=32380, joined_rows=30733,
                           join_name="fct_trip_x_dim_driver")
    return report


def passing_report() -> GateReport:
    report = GateReport("silver_to_gold")
    check_row_conservation(report, left_rows=500, joined_rows=500, join_name="ok")
    return report


def test_a_passing_report_produces_no_alert():
    """Alerting on success is how a channel gets filtered, and a filtered
    channel is no alerting with extra steps."""
    assert notify.from_report(passing_report(), batch_id="b1") is None


def test_an_alert_carries_the_verdict_not_just_the_task_name():
    """'dispatch_medallion failed' sends the reader to the UI to learn anything
    at all. The verdict already says what it saw."""
    alert = notify.from_report(failing_report(), batch_id="b1", rows_loaded=0)
    text = alert.to_text()

    assert "1647" in text or "1,647" in text.replace(" ", "")
    assert "INNER" in text
    assert "b1" in text


def test_an_alert_says_whether_anything_moved():
    """The first question on being paged. Because the gates run before the load
    the answer is almost always 'nothing', and saying so turns a page into a
    ticket."""
    alert = notify.from_report(failing_report(), batch_id="b1", rows_loaded=0)
    assert "nothing" in alert.to_text().lower()


def test_several_failures_are_all_reported():
    report = failing_report()
    check_unique_key(report, rows=[1, 1, 2], key=lambda r: r, key_name="trip_id")
    alert = notify.from_report(report, batch_id="b1")
    assert len(alert.failures) == 2
    assert "2 quality gates failed" in alert.title


def test_a_dead_webhook_does_not_raise():
    """The property that matters most. An alerter that can fail a task has made
    the pipeline less reliable than it was with no alerting at all."""
    alert = notify.from_report(failing_report(), batch_id="b1")
    assert notify.send(alert, webhook="http://127.0.0.1:1/nothing-here") is False


def test_a_malformed_webhook_url_does_not_raise():
    alert = notify.from_report(failing_report(), batch_id="b1")
    assert notify.send(alert, webhook="not-a-url") is False


def test_no_webhook_configured_falls_back_to_stdout(capsys, monkeypatch):
    """In Airflow stdout is the task log, which is where someone reading a
    failed task already is."""
    monkeypatch.delenv(notify.WEBHOOK_ENV, raising=False)
    alert = notify.from_report(failing_report(), batch_id="b1")

    assert notify.send(alert) is False
    assert "INNER" in capsys.readouterr().out


def test_webhook_payload_is_json_with_the_failures_listed():
    sent = {}

    class FakeResponse:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def fake_urlopen(request, timeout=None):
        sent["url"] = request.full_url
        sent["body"] = json.loads(request.data.decode())
        return FakeResponse()

    alert = notify.from_report(failing_report(), batch_id="b1", rows_loaded=0)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(notify.urllib.request, "urlopen", fake_urlopen)
        assert notify.send(alert, webhook="https://hooks.example/abc") is True

    assert sent["url"] == "https://hooks.example/abc"
    assert sent["body"]["failure_count"] == 1
    assert "INNER" in sent["body"]["failures"][0]
    assert sent["body"]["stage"] == "silver_to_gold"


def test_a_non_2xx_webhook_response_is_reported_as_undelivered():
    class FakeResponse:
        status = 500
        def __enter__(self): return self
        def __exit__(self, *a): return False

    alert = notify.from_report(failing_report(), batch_id="b1")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(notify.urllib.request, "urlopen",
                   lambda *a, **k: FakeResponse())
        assert notify.send(alert, webhook="https://hooks.example/abc") is False


def test_airflow_callback_handles_a_failure_that_is_not_a_gate(capsys):
    """The catch-all: an unreachable broker or an OOM must not pass silently
    merely for not being a quality gate."""
    class FakeTask:
        task_id = "silver"
        dag_id = "dispatch_medallion"
        try_number = 2
        log_url = "http://airflow/log/1"

    notify.airflow_failure_callback({
        "task_instance": FakeTask(),
        "exception": ConnectionError("kafka:9092 unreachable"),
        "run_id": "manual__2026-09-16",
    })
    out = capsys.readouterr().out
    assert "kafka:9092 unreachable" in out
    assert "manual__2026-09-16" in out


def test_airflow_callback_survives_a_context_missing_everything():
    """Airflow's callback context is not guaranteed to carry a task instance,
    and a crash inside a failure handler replaces the real error with its own."""
    notify.airflow_failure_callback({})


def test_pipeline_alerts_before_it_raises(capsys, tmp_path, monkeypatch):
    """Ordering. Raising first means the alert is never built; alerting instead
    of raising means the load is not stopped."""
    from dispatch.sessionize import sessionize
    from transforms import run_pipeline

    report = GateReport("silver_to_gold")
    check_measurable_share(report, facts=[], min_share=0.5)

    from dispatch.quality import QualityGateFailed
    with pytest.raises(QualityGateFailed):
        run_pipeline._alert_then_raise(report, "b1", context={"trips": 0})

    assert "no trips in batch" in capsys.readouterr().out
    assert sessionize and timedelta  # imports exercised, keeps linters honest
