"""Build the dbt models against the local SQLite warehouse.

dbt is not among the dependencies CI installs, and the local target is SQLite
rather than Postgres, so this resolves the Jinja that dbt would resolve -- `ref`,
`source`, `var` -- and executes the same model files. The SQL under test is the
SQL that ships; only the templating is done here instead of by dbt.

What this is NOT: a reimplementation of dbt. It handles exactly the three tags
the models use, and it fails loudly on anything else rather than silently
leaving a `{{ ... }}` in the SQL for SQLite to choke on later.

    python scripts/build_marts.py
"""

from __future__ import annotations

import pathlib
import re
import sqlite3
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
DBT = ROOT / "dbt"
WAREHOUSE = ROOT / "data" / "warehouse.db"

# dbt_project.yml declares these; duplicated here rather than parsed because
# pulling in a YAML dependency to read two integers is a worse trade.
VARS = {"lag_window_hours": 6, "min_plausible_tat": 2}

# Order matters: a model is created before anything that refs it.
MODELS = [
    ("stg_trip", "view", "models/staging/stg_trip.sql"),
    ("mart_store_daily", "table", "models/marts/mart_store_daily.sql"),
    ("mart_promise_error", "table", "models/marts/mart_promise_error.sql"),
]
TESTS = sorted((DBT / "tests").glob("*.sql"))

_SOURCE = re.compile(r"\{\{\s*source\(\s*'[^']+'\s*,\s*'(\w+)'\s*\)\s*\}\}")
_REF = re.compile(r"\{\{\s*ref\(\s*'(\w+)'\s*\)\s*\}\}")
_VAR = re.compile(r"\{\{\s*var\(\s*'(\w+)'\s*\)\s*\}\}")
_ANY_TAG = re.compile(r"\{\{.*?\}\}|\{%.*?%\}", re.S)


def compile_sql(path: pathlib.Path) -> str:
    """Resolve the Jinja dbt would resolve, and refuse anything else."""
    sql = path.read_text(encoding="utf-8")
    sql = _SOURCE.sub(lambda m: m.group(1), sql)
    sql = _REF.sub(lambda m: m.group(1), sql)

    def var(match):
        name = match.group(1)
        if name not in VARS:
            raise KeyError(f"{path.name} uses var('{name}'), which is not declared")
        return str(VARS[name])

    sql = _VAR.sub(var, sql)
    leftover = _ANY_TAG.search(sql)
    if leftover:
        # Left in place, SQLite would fail with a syntax error pointing at a
        # column that does not exist, which is a long way from "this script does
        # not understand that tag".
        raise NotImplementedError(
            f"{path.name} uses a template tag this builder does not handle: "
            f"{leftover.group(0)[:60]}")
    return sql


def build(verbose: bool = True) -> dict:
    if not WAREHOUSE.exists():
        raise FileNotFoundError(
            f"no warehouse at {WAREHOUSE} - run `run demo` first")

    conn = sqlite3.connect(WAREHOUSE)
    built = []
    try:
        for name, materialisation, relative in MODELS:
            sql = compile_sql(DBT / relative)
            conn.executescript(f"DROP VIEW IF EXISTS {name}")
            conn.executescript(f"DROP TABLE IF EXISTS {name}")
            keyword = "VIEW" if materialisation == "view" else "TABLE"
            conn.executescript(f"CREATE {keyword} {name} AS {sql}")
            built.append(name)
            if verbose:
                print(f"  built {materialisation:5}  {name}")

        failures = []
        for path in TESTS:
            rows = conn.execute(compile_sql(path)).fetchall()
            ok = not rows
            if not ok:
                failures.append((path.stem, len(rows)))
            if verbose:
                print(f"  {'PASS' if ok else 'FAIL':5}         {path.stem}"
                      + ("" if ok else f"  ({len(rows)} offending rows)"))
        conn.commit()
    finally:
        conn.close()

    return {"models": built, "tests": len(TESTS), "failures": failures}


def main() -> int:
    print("dbt models (compiled here, SQLite target)")
    result = build()
    print()
    if result["failures"]:
        for name, count in result["failures"]:
            print(f"  FAILED {name}: {count} rows")
        return 1

    conn = sqlite3.connect(WAREHOUSE)
    rows = conn.execute(
        "SELECT store_id, trips_measured, mean_signed_error_minutes, "
        "mean_absolute_error_minutes, p50_error_minutes, p90_error_minutes, "
        "breach_rate FROM mart_promise_error ORDER BY store_id").fetchall()
    pooled = conn.execute(
        "SELECT COUNT(*), ROUND(AVG(promise_error_minutes), 2), "
        "ROUND(AVG(ABS(promise_error_minutes)), 2) FROM stg_trip "
        "WHERE is_measurable AND promise_error_minutes IS NOT NULL").fetchone()
    conn.close()

    print("The finding - promise error by store")
    print(f"  {'store':8}{'trips':>8}{'signed':>9}{'MAE':>8}{'p50':>8}{'p90':>8}{'breach':>9}")
    for r in rows:
        print(f"  {r[0]:8}{r[1]:>8}{r[2]:>9}{r[3]:>8}{r[4]:>8}{r[5]:>8}{r[6]:>9.1%}")
    print(f"  {'pooled':8}{pooled[0]:>8}{pooled[1]:>9}{pooled[2]:>8}")
    print()
    print("  The pooled row is the point: one number for both stores reports a")
    print("  formula that is roughly 8 minutes optimistic, and hides that it is")
    print("  more than twice as wrong at one store as at the other.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
