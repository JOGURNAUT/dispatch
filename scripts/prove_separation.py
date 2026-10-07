"""Proof that the transformation logic does not need an engine to run.

The claim is easy to make and easy to doubt, so this checks it three ways
instead of asserting it:

  1. pyspark, confluent_kafka and apache-airflow are not importable here
  2. no module under dispatch/ imports any of them -- checked by parsing the
     AST, not by grepping, so a lazily imported name inside a function is still
     caught
  3. the whole pipeline runs anyway, end to end, on generated data

Run it in front of someone who asks what "no engine imports" buys you:

    python scripts/prove_separation.py
"""

from __future__ import annotations

import ast
import importlib.util
import pathlib
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
ENGINES = ("pyspark", "confluent_kafka", "airflow", "pendulum")
CORE = ROOT / "dispatch"


def _rule(title: str) -> None:
    # ASCII, no ANSI. This is meant to be run live, and a Windows console is
    # cp1252: a box-drawing character kills the script on its own banner,
    # before it has proved anything.
    print(f"\n{title}")
    print("-" * 66)


def check_not_installed() -> list[str]:
    _rule("1. Is an engine installed on this machine?")
    missing = []
    for name in ENGINES:
        present = importlib.util.find_spec(name) is not None
        print(f"   {'INSTALLED' if present else 'not installed':>14}   {name}")
        if not present:
            missing.append(name)
    return missing


def imported_engines(path: pathlib.Path) -> set[str]:
    """Engine modules a file imports, found by parsing rather than grepping.

    An import inside a function body is still an import; a regex over the first
    twenty lines would miss it and the claim would be false in exactly the case
    somebody would check.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        else:
            continue
        for name in names:
            root = name.split(".")[0]
            if root in ENGINES:
                found.add(root)
    return found


def check_core_is_clean() -> bool:
    _rule("2. Does any module in dispatch/ import one?")
    clean = True
    for path in sorted(CORE.glob("*.py")):
        engines = imported_engines(path)
        mark = "clean" if not engines else f"IMPORTS {', '.join(sorted(engines))}"
        print(f"   {mark:>14}   dispatch/{path.name}")
        clean &= not engines
    return clean


def check_pipeline_runs() -> bool:
    _rule("3. Does the pipeline run anyway?")
    data = ROOT / "data"
    for stale in ("bronze", "silver", "warehouse.db"):
        target = data / stale
        if target.is_dir():
            for f in target.glob("*"):
                f.unlink()
        elif target.exists():
            target.unlink()

    steps = [
        ("generate 5,000 trips",
         [sys.executable, "-m", "generator.produce", "--trips", "5000",
          "--days", "14", "--out", "data/raw/events.jsonl"]),
        ("bronze -> silver -> gold",
         [sys.executable, "-m", "transforms.run_pipeline", "all",
          "--source", "data/raw/events.jsonl", "--as-of", "2026-09-16T00:00:00"]),
        ("CDC dimension load",
         [sys.executable, "-m", "transforms.load_dimension", "--demo"]),
        ("94 tests",
         [sys.executable, "-m", "pytest", "tests/", "-q"]),
    ]

    ok = True
    for label, command in steps:
        start = time.perf_counter()
        done = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        elapsed = time.perf_counter() - start
        passed = done.returncode == 0
        ok &= passed
        print(f"   {'ok' if passed else 'FAILED':>14}   {label:<26} {elapsed:5.1f}s")
        if not passed:
            print(done.stdout[-1500:] or done.stderr[-1500:])
    return ok


def main() -> int:
    print("\nCan the rules run without the engines?")

    missing = check_not_installed()
    clean = check_core_is_clean()
    runs = check_pipeline_runs()

    _rule("Verdict")
    if missing and clean and runs:
        print(f"   {len(missing)} of {len(ENGINES)} engines are absent from this machine,")
        print("   no module in dispatch/ imports any of them,")
        print("   and the full pipeline plus every test ran regardless.")
        print("\n   That is what the separation buys: the logic that can be wrong")
        print("   is testable in seconds, on a laptop, with nothing installed.")
        return 0

    if not missing:
        print("   Every engine happens to be installed here, so absence proves")
        print("   nothing today -- but checks 2 and 3 still hold.")
    if not clean:
        print("   A module under dispatch/ imports an engine. The claim is broken.")
    if not runs:
        print("   The pipeline did not complete.")
    return 0 if (clean and runs) else 1


if __name__ == "__main__":
    sys.exit(main())
