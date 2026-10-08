"""
Reference filler for template.html: it shows what every {placeholder} expects
and how to compute the chart geometry. Port the functions into your generator,
or call render() directly.

Values marked EXAMPLE are stand-ins because the brief didn't include them
(per-store MAE/p50/p90/breach, the daily breach series, gate names and details).
Replace them with real warehouse values.
"""
import math
import re
from pathlib import Path

CHECK = ('<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3.5 8.5l3 3 6-7" '
         'fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" '
         'stroke-linejoin="round"/></svg>')
CROSS = ('<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M4.5 4.5l7 7M11.5 4.5l-7 7" '
         'fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"/></svg>')


def signed(v, nd=2):
    return f"{v:+.{nd}f}"


def nice_ceiling(v, steps=(1, 2, 2.5, 5, 10)):
    """Smallest 'nice' number at or above v (e.g. 11.2 -> 12.5 is avoided; we prefer 12)."""
    mag = 10 ** math.floor(math.log10(v))
    for s in steps:
        if s * mag >= v:
            return s * mag
    return 10 * mag


def number_line(a_mean, b_mean, pooled):
    """Positions on the minutes axis, as % of its width. Assumes errors are >= 0 (late)."""
    top = max(a_mean, b_mean, pooled)
    axis_max = math.ceil(top * 1.15 / 2) * 2          # even number with headroom
    step = 2 if axis_max <= 16 else 5
    pos = lambda v: round(v / axis_max * 100, 2)
    ticks = "\n        ".join(
        f'<div class="tick" style="left: {pos(t)}%;"><span>{t}</span></div>'
        for t in range(0, axis_max + 1, step)
    )
    lo, hi = sorted((pos(a_mean), pos(b_mean)))
    return {
        "a_pos": pos(a_mean), "b_pos": pos(b_mean), "pooled_pos": pos(pooled),
        "gap_left": lo, "gap_width": round(hi - lo, 2), "axis_ticks": ticks,
    }


def breach_chart(dates, a_rates, b_rates, a_name, b_name):
    """dates: list of display strings; rates: fractions 0..1, one per day."""
    n = len(dates)
    ymax = nice_ceiling(max(max(a_rates), max(b_rates)) * 1.1)
    x = lambda i: round((i + 0.5) / n * 1000, 1)
    y = lambda r: round(100 - r / ymax * 100, 2)
    pts = lambda rs: " ".join(f"{x(i)},{y(r)}" for i, r in enumerate(rs))
    pct = lambda r: f"{r * 100:.1f}%"

    cols = []
    for i, (d, ra, rb) in enumerate(zip(dates, a_rates, b_rates)):
        flip = " flip" if i >= n - 4 else ""            # tooltips near the right edge open leftwards
        cols.append(
            f'<div class="col{flip}" tabindex="0" aria-label="{d}: {a_name} {pct(ra)}, {b_name} {pct(rb)}">'
            f'<span class="xh"></span>'
            f'<span class="pt b" style="bottom: {round(rb / ymax * 100, 2)}%;"></span>'
            f'<span class="pt a" style="bottom: {round(ra / ymax * 100, 2)}%;"></span>'
            f'<div class="tip"><b>{d}</b><i class="a"></i>{a_name} {pct(ra)}<br>'
            f'<i class="b"></i>{b_name} {pct(rb)}</div></div>'
        )

    # end labels: keep them at least 14% of plot height apart so they never overlap
    ta, tb = y(a_rates[-1]), y(b_rates[-1])
    if abs(ta - tb) < 14:
        mid = (ta + tb) / 2
        ta, tb = (mid - 7, mid + 7) if ta <= tb else (mid + 7, mid - 7)

    return {
        "line_a_points": pts(a_rates), "line_b_points": pts(b_rates),
        "y_max_label": f"{ymax * 100:.0f}%", "y_mid_label": f"{ymax * 50:.0f}%",
        "a_end_top": round(ta, 2), "b_end_top": round(tb, 2),
        "a_end_val": pct(a_rates[-1]), "b_end_val": pct(b_rates[-1]),
        "day_columns": "\n            ".join(cols),
        "x_first_label": dates[0], "x_last_label": dates[-1],
        "days_worse": sum(ra > rb for ra, rb in zip(a_rates, b_rates)), "n_days": n,
    }


def store_row(name, cls, trips, mean, mae, p50, p90, breach):
    return (f'<tr><td class="store"><span class="sw {cls}"></span>{name}</td>'
            f'<td class="n">{trips:,}</td><td class="n">{signed(mean)}</td>'
            f'<td class="n">{mae:.2f}</td><td class="n">{signed(p50)}</td>'
            f'<td class="n">{signed(p90)}</td><td class="n">{breach * 100:.1f}%</td></tr>')


def gate_row(stage, check, passed, detail):
    badge = (f'<span class="badge pass">{CHECK}PASS</span>' if passed
             else f'<span class="badge fail">{CROSS}FAIL</span>')
    return (f'<tr><td class="stage">{stage}</td><td class="check">{check}</td>'
            f'<td>{badge}</td><td class="detail">{detail}</td></tr>')


def render(template: str, v: dict) -> str:
    """Substitute {lower_snake} tokens only, so the CSS braces never need escaping."""
    def sub(m):
        k = m.group(1)
        if k not in v:
            raise KeyError(f"template placeholder {{{k}}} has no value")
        return str(v[k])
    return re.sub(r"\{([a-z][a-z0-9_]*)\}", sub, template)


if __name__ == "__main__":
    # ---- figures from the brief ----
    a_name, b_name = "NORTHGATE", "RIVERSIDE"          # A = the store with the larger error
    a_mean, b_mean, pooled = 10.01, 4.74, 7.79
    complete, broken = 4972, 28

    # ---- EXAMPLE values: replace with warehouse values ----
    a_trips, b_trips = 2894, 2106               # the split implied by the pooled mean
    dates = [f"Jun {d:02d}" for d in range(1, 15)]
    a_rates = [.31, .29, .34, .30, .33, .28, .35, .32, .30, .36, .31, .29, .33, .32]
    b_rates = [.14, .12, .15, .13, .16, .11, .14, .15, .12, .13, .17, .12, .14, .13]
    gates = [
        ("bronze", "schema_conforms", True, "Every landed event parses against the registered schema."),
        ("bronze", "no_null_keys", True, "No event is missing event_id, trip_id or ts."),
        ("silver", "dedup_effective", True, "No duplicate event_id remains after collapsing."),
        ("silver", "monotonic_status", True, "Each trip's status sequence moves forward only."),
        ("silver", "late_events_bounded", True, "Every late event falls inside the watermark."),
        ("gold", "one_row_per_trip", True, "fact_trip has exactly one row per trip_id."),
        ("gold", "fk_integrity", True, "Every fact row resolves to a store, courier and date."),
        ("warehouse", "row_count_reconciles", True, "Postgres counts match the gold layer."),
        ("dbt", "mart_tests", True, "All dbt not-null, unique and relationship tests pass."),
    ]

    total = complete + broken
    passed = sum(g[2] for g in gates)
    v = {
        "generated_at": "2026-10-09 01:40 IST",
        "a_name": a_name, "b_name": b_name,
        "ratio": f"{a_mean / b_mean:.1f}",
        "a_mean": signed(a_mean), "b_mean": signed(b_mean), "pooled_mean": signed(pooled),
        "gap": f"{a_mean - b_mean:.2f}",
        **number_line(a_mean, b_mean, pooled),
        **breach_chart(dates, a_rates, b_rates, a_name, b_name),
        "store_rows": "\n              ".join([
            store_row(a_name, "a", a_trips, a_mean, 10.62, 9.40, 17.85, sum(a_rates) / 14),
            store_row(b_name, "b", b_trips, b_mean, 5.88, 4.31, 10.92, sum(b_rates) / 14),
        ]),
        "events_landed": f"{31305:,}", "collapsed_silver": f"{1468:,}",
        "trips": f"{5000:,}", "quarantined": f"{0:,}",
        "complete_n": f"{complete:,}", "complete_pct": f"{complete / total:.1%}",
        "broken_n": f"{broken:,}", "broken_pct": f"{broken / total:.1%}",
        "complete_n_raw": complete, "broken_n_raw": broken,
        "gates_passed": passed, "gates_total": len(gates),
        "gates_tally_icon": CHECK if passed == len(gates) else CROSS,
        "gates_tally_class": "" if passed == len(gates) else "fail",
        "gate_rows": "\n            ".join(gate_row(*g) for g in gates),
    }
    here = Path(__file__).parent
    (here / "preview.html").write_text(render((here / "template.html").read_text(), v))
    print("wrote preview.html")
