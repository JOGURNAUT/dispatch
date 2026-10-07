"""Airflow DAG for the dispatch medallion pipeline.

One task per layer, with the quality gate inside the task that produces the
layer rather than in a task after it. A separate "validate" task downstream of
"load" means the bad data is already in the warehouse by the time anything
objects, and someone is reading it while the alert is being written. Gating
before the load is what makes a failure mean "nothing moved" instead of "it
moved and we noticed".

Scheduling choices worth defending in a review:

  catchup=True              a missed window is a window that still has to run.
                            Backfill is the normal case for a data pipeline, not
                            an exception, and it only works because every stage
                            takes `as_of` as a parameter.
  max_active_runs=1         the stages rewrite shared partitions. Two runs over
                            overlapping days would interleave their writes and
                            the loser's rows would survive in the winner's table.
  depends_on_past=False     a failed Tuesday must not block Wednesday. The
                            partitions are independent and the load is a replace,
                            so Tuesday can be re-run afterwards without redoing
                            anything else.
  retries=2, exponential    the failures worth retrying here are transport ones.
                            Every stage is idempotent, which is the precondition
                            that makes an automatic retry safe rather than a way
                            to load the same day twice.

The late-arrival reprocess task is the one that is easy to leave out and
expensive to leave out. Without it an event that lands after its day's run is
never picked up at all: its partition has already been written and nothing will
visit it again. It rewrites a trailing window on every run, which costs a few
seconds and is the difference between a trip being counted and not.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from dispatch.quality import QualityGateFailed
from transforms.run_pipeline import build_gold, build_silver, ingest_bronze
from transforms.warehouse import Warehouse

# How far back each run revisits. Must exceed the worst producer lag the
# freshness gate tolerates, or an event can arrive inside the tolerated window
# and still land on a partition no later run will ever rewrite.
REPROCESS_DAYS = 3

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=30),
    "execution_timeout": pendulum.duration(hours=2),
}


@dag(
    dag_id="dispatch_medallion",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dispatch", "medallion", "last-mile"],
    doc_md=__doc__,
)
def dispatch_medallion():

    @task
    def ingest(**context) -> dict:
        """Raw -> bronze. Appends; never rewrites. Bronze is the evidence."""
        batch_id = context["run_id"]
        source = Variable.get("dispatch_source", default_var="data/raw/events.jsonl")
        return ingest_bronze(source, batch_id)

    @task
    def silver(bronze: dict, **context) -> dict:
        """Bronze -> silver, with the contract and dedupe gates.

        The reprocess window is unioned into the partitions this run would
        otherwise touch, so a late event lands in its own day and that day is
        rebuilt rather than left stale.
        """
        data_interval_end = context["data_interval_end"]
        recent = {
            (data_interval_end - pendulum.duration(days=offset)).format("YYYY-MM-DD")
            for offset in range(REPROCESS_DAYS + 1)
        }
        partitions = sorted(set(bronze["partitions"]) | recent)
        try:
            return build_silver(
                batch_id=context["run_id"],
                partitions=partitions,
                as_of=data_interval_end.naive(),
            )
        except QualityGateFailed:
            # Re-raised unchanged so the task goes red and the detail lands in
            # the task log. Swallowing it to "let the run finish" is how a bad
            # partition reaches the warehouse with a green DAG above it.
            raise

    @task
    def gold(silver_result: dict, **context) -> dict:
        """Silver -> star schema, gated before the load, not after."""
        return build_gold(
            batch_id=context["run_id"],
            partitions=silver_result["partitions"],
            as_of=context["data_interval_end"].naive(),
        )

    @task
    def publish_audit(gold_result: dict, **context) -> str:
        """Surface the run's gate verdicts where an on-call person will see them.

        Reads back from run_audit rather than from the in-memory report, so what
        is reported is what was actually persisted. A summary built from a
        variable that was never written is a summary of a load that may not have
        happened.
        """
        wh = Warehouse()
        rows = wh.query(
            "SELECT stage, check_name, passed, detail FROM run_audit "
            "WHERE batch_id = ? ORDER BY stage, check_name".replace("?", wh.placeholder),
            (context["run_id"],))
        failed = [r for r in rows if not r[2]]
        lines = [f"{gold_result['trips']} trips loaded, {len(rows)} checks recorded"]
        lines += [f"  WARN {r[0]}/{r[1]}: {r[3]}" for r in failed]
        summary = "\n".join(lines)
        print(summary)
        return summary

    publish_audit(gold(silver(ingest())))


dispatch_medallion()
