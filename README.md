# Dispatch

A last-mile delivery telemetry pipeline: Kafka → Spark → medallion lake → star
schema in Postgres, orchestrated by Airflow, with dbt marts on top.

It is built around a claim that is easy to state and hard to hold: **a data
pipeline's dangerous failures are the ones that do not raise.** A job that
crashes gets fixed the same morning. A job that succeeds while a left join
quietly behaves as an inner one produces a smaller, entirely reasonable-looking
table, and the wrong numbers are in a deck before anyone counts the rows.

Every quality gate here was written for a specific failure of that kind, most of
them taken from defects found on a real delivery dataset rather than from a
generic checklist.

```
┌──────────┐   ┌───────┐   ┌─────────────────────────────────────┐   ┌──────────┐
│ producer │──▶│ Kafka │──▶│  bronze  ──▶  silver  ──▶  gold     │──▶│   dbt    │
│  (trip   │   │  3    │   │   raw        conformed    star      │   │  marts   │
│  events) │   │ parts │   │  append       dedupe      schema    │   │ + tests  │
└──────────┘   └───────┘   └─────────────────────────────────────┘   └──────────┘
                    │              ▲           ▲          ▲
               Spark Structured    │      2 gates    7 gates
                  Streaming        └──────── Airflow ─────┘
```

## Run it

Nothing to install. The transformation logic is standard-library Python, so the
whole pipeline runs on a laptop in about two seconds.

```bash
make demo
```

```
125352 events for 20000 trips -> data/raw/events.jsonl
  redeliveries 4793  corrections 1155  late 2545

bronze   received 125352  partitions 21
silver   accepted 119404  dupes removed 5948  corrections 1155  quarantined 0
         bronze_to_silver: 2/2 checks passed
gold     trips 20000  {'complete': 19905, 'broken': 95}
         silver_to_gold: 7/7 checks passed  dims: 60 drivers, 2 stores
```

```bash
make test     # 43 tests, no cluster needed
make up       # Kafka, Spark, Postgres and Airflow in Docker
```

## Why the logic has no Spark in it

`dispatch/` is pure Python and imports nothing from Spark, Kafka or Airflow. The
Spark job and the local batch runner both call the same functions; what differs
is who executes them and over how much data.

This is not portability for its own sake. A dedupe rule written in Spark SQL can
only be tested by starting Spark, so in practice it is tested by running the
pipeline and looking at the output — which is how a rule that is subtly wrong
survives for months. Keeping the rules separable means `make test` proves them in
six seconds, and CI runs them on every push with no containers at all.

Spark is then responsible only for distribution, and the engine is a deployment
decision rather than a rewrite.

## The failure modes this is built around

Each has a test named after it.

### A replay must not change the answer

Kafka is at-least-once. A consumer that dies between writing a batch and
committing its offset replays that batch; Airflow retries a failed task; a
backfill replays on purpose. None of these are errors, so none may move a number.

The naive defence — `DISTINCT` on the row — fails on the case that actually
happens: the same logical event re-published with a corrected field. Two rows
differing in `distance_m` are not duplicates, both survive, and the trip now has
two `delivered` events. Which one a later aggregate picks is arbitrary.

So identity is declared rather than inferred: the natural key is
`(trip_id, event_type)`, and the latest ingestion wins. A correcting re-publish
*corrects* the row. In the run above that collapsed 5,948 redeliveries and
applied 1,155 corrections, and the warehouse is byte-identical after a second
run.

### A null is not a fact

At any cut-off some trips are mid-flight. "No `delivered` event" can mean the
trip has not been delivered, or that the event has not arrived yet, and those
look identical in the data.

On a real dataset a rider-side table lagged the order table and 187 of 269 orders
came back with a null `delivered_at`. Every downstream reader took the null at
face value. A re-pull three days later brought the 187 down to 20. Nothing had
failed; the pipeline had believed a null.

So a trip carries a `completeness` label instead of a silent inclusion:

| label      | meaning                                            | enters metrics |
|------------|----------------------------------------------------|----------------|
| `complete` | terminal event present                             | yes            |
| `open`     | still in flight, last event inside the lag window  | no             |
| `stalled`  | silent past the lag window — a real problem         | no, and visible|
| `broken`   | lifecycle violated                                 | no             |

Only `complete` rows carry a TAT. Substituting `now()` for a missing
`delivered_at` would make every open trip look slow and grow its duration on
every run.

### A LEFT JOIN that behaves as INNER

A left join made inner by a filter on the right-hand table, or by a null key,
drops rows and raises nothing. This exact defect removed 1,647 of ~32,000 orders
from a real analysis and biased every figure computed before it was found.

A join that must preserve its left side is asserted to preserve it, in both
directions — fewer rows means it went inner, more means the right side is not
unique on the key and every measure downstream is double-counted.

### A metric whose value depends on when the job ran

Every stage takes `as_of` as a parameter. No stage reads the clock. A job that
calls `utcnow()` internally produces a different table on each run and cannot be
backfilled, because the re-run disagrees with the original for a reason that is
nowhere in the data.

This rule caught its own violation during development: the freshness gate was
reading the wall clock, which made every backfill fail for the only reason a
backfill exists — that its data is older than today.

### Timestamp drift between producers

The same logical field arrives as `2026-07-24 03:16:02` from one export path and
`2026-08-25T17:01:06.860000` from another: same query, different client. A parser
that knows one shape does not fail loudly, it drops the rows it cannot read, and
the loss surfaces weeks later as a dip nobody can explain. Seven spellings,
including epoch seconds and milliseconds, are normalised at the boundary.

## The gates

Nine, all persisted to `run_audit` whether they pass or fail — a log that records
only failures cannot distinguish a check that passed from one that never ran.

| gate | catches |
|---|---|
| `reconciliation` | records that entered a stage and never left it |
| `freshness` | a stopped producer, which otherwise looks like a quiet hour |
| `unique_key` | a fact table at the wrong grain, doubling every count |
| `not_null` | a missing key on the declared grain |
| `row_conservation` | an inner join wearing a LEFT, and fan-out from a non-unique dimension |
| `fk:driver_id`, `fk:store_id` | a key that is present and wrong, which an outer join hides |
| `volume_band` | a partial load: schema valid, every row correct, a third of the data absent |
| `measurable_share` | a lagging upstream turning most rows into nulls |

They run **before** the load, not after. A validate task placed downstream of a
load means the bad rows are already being read by the time anything objects.

Two choices that keep them useful rather than noisy:

- `volume_band` compares **per day**, against days the batch is not rewriting.
  Comparing a multi-day batch against per-day history fires on every backfill;
  including the days being rewritten is circular, so a replay always agrees with
  itself and the gate can never fail.
- `volume_band` is skipped below three days of history, because a gate that fires
  on day one of a backfill trains people to ignore it.

## What the marts find

`mart_promise_error` splits the promise error by store. The promise formula is
store-blind — one global speed, one flat buffer — and the stores are not.

| store | trips | mean signed error | MAE | p50 | p90 | breach rate |
|---|---|---|---|---|---|---|
| FC002 | 11,245 | **+10.24 min** | 11.26 | 7.10 | 25.27 | 82.8% |
| FC004 | 8,064 | **+4.18 min** | 6.84 | 2.53 | 14.85 | 65.4% |
| *pooled* | 19,309 | *+7.71* | *9.41* | | | |

The pooled figure is the point. One number for both stores reports a formula
that is "about 8 minutes optimistic" and hides that it is more than twice as
wrong at one store as the other — two problems whose fixes point in different
directions, partly cancelling into the appearance of a formula that is roughly
right.

The error is signed deliberately. `abs()` would report both biases as "wrong by
N minutes" and lose the only information that says what to do about it.

`p50` and `p90` sit next to the mean because a mean alone cannot separate a
formula that is uniformly off from one that is fine for most trips and badly
wrong in the tail. Those need different fixes.

## Layout

```
dispatch/            pure-Python transformation logic — no engine imports
  contracts.py         schema boundary, timestamp normalisation
  dedupe.py            idempotent replay, natural keys, correcting re-publishes
  sessionize.py        events → trip facts, completeness, late arrival
  quality.py           the nine gates
generator/produce.py   synthetic telemetry with injected defects, Kafka or JSONL
streaming/ingest.py    Spark Structured Streaming: Kafka → bronze Parquet
transforms/            batch runner and the warehouse loader
dags/dispatch_dag.py   Airflow: one task per layer, gated before each load
dbt/                   staging + two marts, 3 singular tests, 11 schema tests
tests/                 43 tests
```

## The generator is adversarial on purpose

A producer that emits clean data tests nothing. Every defect the gates exist for
is injected at a tunable rate: redeliveries, correcting re-publishes, delayed
messages, the second serialisation, null join keys, lifecycle violations and
mid-flight cancellations. Defaults produce roughly 4.8% redeliveries and 95
lifecycle violations per 20,000 trips, and the run is only interesting if the
pipeline catches them.

```bash
python -m generator.produce --trips 20000 --dup-rate 0.1 --broken-rate 0.05
```

## Known limits

- **The per-store finding is a demonstration, not a validated model.** The speed
  constants that generate the data are the same ones the marts recover, so the
  mart is confirming an arrangement it was handed. On real data this needs a
  holdout window. What it does show is that the pooled average hides the split —
  that part does not depend on knowing the true constants.
- **`volume_band` cannot fire on a first load**, by design. Three days of history
  are needed before the band means anything.
- **The dbt marts are tested by executing their compiled SQL against the SQLite
  warehouse**, not by a `dbt build` in CI, because dbt is not among the
  dependencies CI installs. The three singular tests and the model SQL run; the
  schema tests are declared but exercised only under `make marts`.
- **Exactly-once holds for the Spark path through checkpoint-plus-output commit.**
  The batch runner's guarantee is weaker and different: it is idempotent, so a
  replay converges, which is sufficient here and is what the test asserts.
