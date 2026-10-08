"""Write docs/results.html from the warehouse, through scripts/report_template.html.

Generated, never written by hand. A results page with numbers typed into it stops
being true the first time the pipeline runs again, and then it is a screenshot
pretending to be a report. Every figure here is queried at build time and the
page stamps which run produced it.

The markup lives in a separate template rather than inside this file. Two
reasons: an f-string full of CSS means doubling every brace, which is a trap
nobody survives twice; and the template can be opened, edited and reloaded
without reading any Python.

Substitution is `{lower_snake}` only, so CSS braces pass through untouched, and a
placeholder with no value raises instead of rendering as literal text -- a page
that silently ships `{pooled_mean}` to a reader is worse than one that fails to
build.

    python scripts/build_report.py      (or: run report)
"""

from __future__ import annotations

import math
import pathlib
import re
import sqlite3
import sys
from datetime import UTC, datetime

ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

WAREHOUSE = ROOT / "data" / "warehouse.db"
TEMPLATE = ROOT / "scripts" / "report_template.html"
OUT = ROOT / "docs" / "results.html"

CHECK = ('<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3.5 8.5l3 3 6-7" '
         'fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" '
         'stroke-linejoin="round"/></svg>')
CROSS = ('<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M4.5 4.5l7 7M11.5 4.5l-7 7" '
         'fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"/></svg>')


# ------------------------------------------------------------------ warehouse

def fetch() -> dict:
    if not WAREHOUSE.exists():
        raise FileNotFoundError(f"no warehouse at {WAREHOUSE} - run `run demo` first")
    conn = sqlite3.connect(WAREHOUSE)
    conn.row_factory = sqlite3.Row
    q = lambda sql: [dict(r) for r in conn.execute(sql).fetchall()]  # noqa: E731

    try:
        have = {r["name"] for r in q(
            "SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
        missing = [t for t in ("stg_trip", "mart_store_daily", "mart_promise_error")
                   if t not in have]
        if missing:
            raise RuntimeError(
                f"the warehouse has no {', '.join(missing)} - run `run marts` first, "
                f"which builds the dbt models this page reads")

        stores = q("""SELECT store_id, trips_measured, mean_signed_error_minutes,
                             mean_absolute_error_minutes, p50_error_minutes,
                             p90_error_minutes, breach_rate
                      FROM mart_promise_error ORDER BY store_id""")
        pooled = q("""SELECT ROUND(AVG(promise_error_minutes), 2) AS mean
                      FROM stg_trip
                      WHERE is_measurable AND promise_error_minutes IS NOT NULL""")[0]
        daily = q("""SELECT store_id, date_key, breach_rate
                     FROM mart_store_daily
                     WHERE breach_rate IS NOT NULL
                     ORDER BY date_key, store_id""")
        completeness = {r["completeness"]: r["n"] for r in q(
            "SELECT completeness, COUNT(*) AS n FROM fct_trip GROUP BY completeness")}
        gates = [dict(r) for r in conn.execute(
            """SELECT stage, check_name, passed, detail FROM run_audit
               WHERE batch_id = (SELECT batch_id FROM run_audit
                                 ORDER BY recorded_at DESC, rowid DESC LIMIT 1)
               ORDER BY stage, check_name""").fetchall()]
        totals = q("SELECT (SELECT COUNT(*) FROM fct_trip) AS trips, "
                   "(SELECT COUNT(*) FROM quarantine) AS quarantined")[0]
    finally:
        conn.close()

    if len(stores) < 2:
        raise RuntimeError(
            f"this page compares two stores and the warehouse has {len(stores)}")

    bronze = sorted((ROOT / "data" / "bronze").glob("dt=*.jsonl"))
    silver = sorted((ROOT / "data" / "silver").glob("dt=*.jsonl"))
    count = lambda ps: sum(sum(1 for _ in p.open(encoding="utf-8")) for p in ps)  # noqa: E731
    bronze_rows, silver_rows = count(bronze), count(silver)

    return {"stores": stores, "pooled": pooled["mean"], "daily": daily,
            "completeness": completeness, "gates": gates, "totals": totals,
            "bronze_rows": bronze_rows, "collapsed": bronze_rows - silver_rows}


# ------------------------------------------------------------------- geometry

def signed(v, nd=2) -> str:
    return f"{v:+.{nd}f}"


def nice_ceiling(v: float, steps=(1, 2, 2.5, 5, 10)) -> float:
    """Smallest round number at or above v, for an axis top that reads cleanly."""
    if v <= 0:
        return 1.0
    mag = 10 ** math.floor(math.log10(v))
    for s in steps:
        if s * mag >= v:
            return s * mag
    return 10 * mag


def number_line(a_mean: float, b_mean: float, pooled: float) -> dict:
    """Positions along the minutes axis, as percentages of its width.

    A number line rather than a bar chart: three values on one scale is a
    question about where they sit relative to each other, and three bars answer
    a different question -- how big each one is -- while using far more space to
    do it. The pooled marker landing between the two stores IS the finding, and
    on a shared axis you can see that without reading any label.
    """
    top = max(a_mean, b_mean, pooled)
    axis_max = max(2, math.ceil(top * 1.15 / 2) * 2)
    step = 2 if axis_max <= 16 else 5
    pos = lambda v: round(v / axis_max * 100, 2)  # noqa: E731
    ticks = "\n        ".join(
        f'<div class="tick" style="left: {pos(t)}%;"><span>{t}</span></div>'
        for t in range(0, int(axis_max) + 1, step))
    lo, hi = sorted((pos(a_mean), pos(b_mean)))
    return {"a_pos": pos(a_mean), "b_pos": pos(b_mean), "pooled_pos": pos(pooled),
            "gap_left": lo, "gap_width": round(hi - lo, 2), "axis_ticks": ticks}


def breach_chart(dates, a_rates, b_rates, a_name, b_name) -> dict:
    """Two lines over the trading period, with a hover column per day."""
    n = len(dates)
    ymax = nice_ceiling(max(max(a_rates), max(b_rates)) * 1.1)
    x = lambda i: round((i + 0.5) / n * 1000, 1)        # noqa: E731
    y = lambda r: round(100 - r / ymax * 100, 2)        # noqa: E731
    pts = lambda rs: " ".join(f"{x(i)},{y(r)}" for i, r in enumerate(rs))  # noqa: E731
    pct = lambda r: f"{r * 100:.1f}%"                   # noqa: E731

    cols = []
    for i, (d, ra, rb) in enumerate(zip(dates, a_rates, b_rates, strict=True)):
        # Tooltips in the last few columns open leftwards, or they are clipped
        # by the edge of the card exactly where the eye ends up.
        flip = " flip" if i >= n - 4 else ""
        cols.append(
            f'<div class="col{flip}" tabindex="0" aria-label="{d}: {a_name} '
            f'{pct(ra)}, {b_name} {pct(rb)}">'
            f'<span class="xh"></span>'
            f'<span class="pt b" style="bottom: {round(rb / ymax * 100, 2)}%;"></span>'
            f'<span class="pt a" style="bottom: {round(ra / ymax * 100, 2)}%;"></span>'
            f'<div class="tip"><b>{d}</b><i class="a"></i>{a_name} {pct(ra)}<br>'
            f'<i class="b"></i>{b_name} {pct(rb)}</div></div>')

    # Keep the two end labels apart, or they collide whenever the lines converge.
    ta, tb = y(a_rates[-1]), y(b_rates[-1])
    if abs(ta - tb) < 14:
        mid = (ta + tb) / 2
        ta, tb = (mid - 7, mid + 7) if ta <= tb else (mid + 7, mid - 7)

    return {"line_a_points": pts(a_rates), "line_b_points": pts(b_rates),
            "y_max_label": f"{ymax * 100:.0f}%", "y_mid_label": f"{ymax * 50:.0f}%",
            "a_end_top": round(ta, 2), "b_end_top": round(tb, 2),
            "a_end_val": pct(a_rates[-1]), "b_end_val": pct(b_rates[-1]),
            "day_columns": "\n            ".join(cols),
            "x_first_label": dates[0], "x_last_label": dates[-1],
            "days_worse": sum(ra > rb for ra, rb in zip(a_rates, b_rates, strict=True)),
            "n_days": n}


def store_row(s: dict, cls: str) -> str:
    return (f'<tr><td class="store"><span class="sw {cls}"></span>{s["store_id"]}</td>'
            f'<td class="n">{s["trips_measured"]:,}</td>'
            f'<td class="n">{signed(s["mean_signed_error_minutes"])}</td>'
            f'<td class="n">{s["mean_absolute_error_minutes"]:.2f}</td>'
            f'<td class="n">{signed(s["p50_error_minutes"])}</td>'
            f'<td class="n">{signed(s["p90_error_minutes"])}</td>'
            f'<td class="n">{s["breach_rate"] * 100:.1f}%</td></tr>')


def gate_row(g: dict) -> str:
    badge = (f'<span class="badge pass">{CHECK}PASS</span>' if g["passed"]
             else f'<span class="badge fail">{CROSS}FAIL</span>')
    return (f'<tr><td class="stage">{g["stage"]}</td>'
            f'<td class="check">{g["check_name"]}</td>'
            f'<td>{badge}</td><td class="detail">{g["detail"]}</td></tr>')


# -------------------------------------------------------------------- render

def render(template: str, values: dict) -> str:
    """Substitute {lower_snake} only, so CSS braces need no escaping.

    A missing placeholder raises. Rendering it as literal text would ship a page
    with `{pooled_mean}` where a number belongs, and nothing would have failed.
    """
    def sub(m):
        key = m.group(1)
        if key not in values:
            raise KeyError(f"template placeholder {{{key}}} has no value")
        return str(values[key])
    return re.sub(r"\{([a-z][a-z0-9_]*)\}", sub, template)


def build(d: dict) -> str:
    # A is whichever store the formula is more wrong about. Chosen from the data
    # rather than named in the source, so the page stays correct if that flips.
    a, b = sorted(d["stores"], key=lambda s: -s["mean_signed_error_minutes"])[:2]
    a_mean = a["mean_signed_error_minutes"]
    b_mean = b["mean_signed_error_minutes"]
    pooled = d["pooled"]

    by_store = {}
    for row in d["daily"]:
        by_store.setdefault(row["store_id"], {})[row["date_key"]] = row["breach_rate"]
    # Only days both stores reported, so the two lines share an x-axis.
    dates = sorted(set(by_store.get(a["store_id"], {}))
                   & set(by_store.get(b["store_id"], {})))
    if not dates:
        raise RuntimeError("no day has a breach rate for both stores")
    a_rates = [by_store[a["store_id"]][day] for day in dates]
    b_rates = [by_store[b["store_id"]][day] for day in dates]
    labels = [datetime.fromisoformat(day).strftime("%b %d") for day in dates]

    complete = d["completeness"].get("complete", 0)
    broken = sum(n for state, n in d["completeness"].items() if state != "complete")
    total = complete + broken or 1
    passed = sum(1 for g in d["gates"] if g["passed"])

    values = {
        "generated_at": datetime.now(UTC).replace(tzinfo=None)
                                .strftime("%Y-%m-%d %H:%M UTC"),
        "a_name": a["store_id"], "b_name": b["store_id"],
        "ratio": f"{a_mean / b_mean:.1f}" if b_mean else "—",
        "a_mean": signed(a_mean), "b_mean": signed(b_mean),
        "pooled_mean": signed(pooled), "gap": f"{a_mean - b_mean:.2f}",
        **number_line(a_mean, b_mean, pooled),
        **breach_chart(labels, a_rates, b_rates, a["store_id"], b["store_id"]),
        "store_rows": "\n              ".join(
            [store_row(a, "a"), store_row(b, "b")]),
        "events_landed": f"{d['bronze_rows']:,}",
        "collapsed_silver": f"{d['collapsed']:,}",
        "trips": f"{d['totals']['trips']:,}",
        "quarantined": f"{d['totals']['quarantined']:,}",
        "complete_n": f"{complete:,}", "complete_pct": f"{complete / total:.1%}",
        "broken_n": f"{broken:,}", "broken_pct": f"{broken / total:.1%}",
        "complete_n_raw": complete, "broken_n_raw": broken,
        "gates_passed": passed, "gates_total": len(d["gates"]),
        "gates_tally_icon": CHECK if passed == len(d["gates"]) else CROSS,
        "gates_tally_class": "" if passed == len(d["gates"]) else "fail",
        "gate_rows": "\n            ".join(gate_row(g) for g in d["gates"]),
    }
    return render(TEMPLATE.read_text(encoding="utf-8"), values)


def main() -> int:
    data = fetch()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(build(data), encoding="utf-8")
    a, b = sorted(data["stores"], key=lambda s: -s["mean_signed_error_minutes"])[:2]
    print(f"wrote {OUT.relative_to(ROOT)}  ({OUT.stat().st_size/1024:.0f} KB)")
    print(f"  {a['store_id']} {signed(a['mean_signed_error_minutes'])} vs "
          f"{b['store_id']} {signed(b['mean_signed_error_minutes'])}, "
          f"pooled {signed(data['pooled'])}")
    print(f"  {len(data['gates'])} gate verdicts, "
          f"{data['totals']['trips']:,} trips")
    return 0


if __name__ == "__main__":
    sys.exit(main())
