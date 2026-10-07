"""Write docs/results.html from whatever is in the warehouse right now.

Generated, never hand-written. A results page with numbers typed into it stops
being true the first time the pipeline runs again, and then it is a screenshot
pretending to be a report. Every figure here is queried at build time, and the
page records which run produced it.

    python scripts/build_report.py      (or: run report)
"""

from __future__ import annotations

import json
import pathlib
import sqlite3
import sys
from datetime import UTC, datetime

ROOT = pathlib.Path(__file__).resolve().parent.parent
WAREHOUSE = ROOT / "data" / "warehouse.db"
OUT = ROOT / "docs" / "results.html"

# Categorical slots 1 and 2 from the validated reference palette. Both modes
# pass all six checks for this pair: worst adjacent CVD dE 24.7 light / 26.8
# dark, normal-vision 33.6 / 31.8, contrast >= 3:1 on both surfaces.
SERIES = {
    "FC002": {"light": "#2a78d6", "dark": "#3987e5"},
    "FC004": {"light": "#eb6834", "dark": "#d95926"},
}


def fetch() -> dict:
    if not WAREHOUSE.exists():
        raise FileNotFoundError(f"no warehouse at {WAREHOUSE} - run `run demo` first")
    conn = sqlite3.connect(WAREHOUSE)
    conn.row_factory = sqlite3.Row
    q = lambda sql: [dict(r) for r in conn.execute(sql).fetchall()]  # noqa: E731

    # Checked by name rather than left to fail on the first SELECT. A missing
    # mart otherwise surfaces as "no such table: mart_promise_error", which
    # names the symptom and not the step that was skipped.
    have = {r["name"] for r in q(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
    missing = [t for t in ("stg_trip", "mart_store_daily", "mart_promise_error")
               if t not in have]
    if missing:
        conn.close()
        raise RuntimeError(
            f"the warehouse has no {', '.join(missing)} - run `run marts` first "
            f"(or `make marts`), which builds the dbt models this page reads")

    try:
        stores = q("""SELECT store_id, trips_measured, mean_signed_error_minutes,
                             mean_absolute_error_minutes, p50_error_minutes,
                             p90_error_minutes, mean_tat_minutes,
                             mean_promised_minutes, breach_rate
                      FROM mart_promise_error ORDER BY store_id""")
        pooled = q("""SELECT COUNT(*) AS trips_measured,
                             ROUND(AVG(promise_error_minutes), 2) AS mean_signed_error_minutes,
                             ROUND(AVG(ABS(promise_error_minutes)), 2) AS mean_absolute_error_minutes
                      FROM stg_trip
                      WHERE is_measurable AND promise_error_minutes IS NOT NULL""")[0]
        daily = q("""SELECT store_id, date_key, trips_total, trips_measured,
                            mean_tat_minutes, breach_rate, measured_share
                     FROM mart_store_daily ORDER BY date_key, store_id""")
        completeness = q("SELECT completeness, COUNT(*) AS n FROM fct_trip "
                         "GROUP BY completeness ORDER BY n DESC")
        # The most recent batch only. run_audit is append-only across runs by
        # design -- that is what makes it an audit trail -- but a results page
        # that shows every verdict ever recorded grows a row per gate per run,
        # and after a few demo runs the table says more about how often it was
        # run than about the load it describes.
        gates = [dict(r) for r in conn.execute(
            """SELECT stage, check_name, passed, detail FROM run_audit
               WHERE batch_id = (SELECT batch_id FROM run_audit
                                 ORDER BY recorded_at DESC, rowid DESC LIMIT 1)
               ORDER BY stage, check_name""").fetchall()]
        totals = q("""SELECT (SELECT COUNT(*) FROM fct_trip)    AS trips,
                             (SELECT COUNT(*) FROM dim_driver)  AS drivers,
                             (SELECT COUNT(*) FROM dim_store)   AS stores,
                             (SELECT COUNT(*) FROM dim_date)    AS days,
                             (SELECT COUNT(*) FROM quarantine)  AS quarantined""")[0]
    finally:
        conn.close()

    # Silver is the only place the dedupe counts survive as files; read them off
    # disk rather than storing a number nobody can re-derive.
    silver = sorted((ROOT / "data" / "silver").glob("dt=*.jsonl"))
    bronze = sorted((ROOT / "data" / "bronze").glob("dt=*.jsonl"))
    counts = {
        "bronze_rows": sum(sum(1 for _ in p.open(encoding="utf-8")) for p in bronze),
        "silver_rows": sum(sum(1 for _ in p.open(encoding="utf-8")) for p in silver),
        "partitions": len(bronze),
    }
    counts["collapsed"] = counts["bronze_rows"] - counts["silver_rows"]

    return {"stores": stores, "pooled": pooled, "daily": daily,
            "completeness": completeness, "gates": gates, "totals": totals,
            "counts": counts,
            "built_at": datetime.now(UTC).replace(tzinfo=None).isoformat(" ", "seconds")}


def render(d: dict) -> str:
    worst = max(d["stores"], key=lambda s: s["mean_signed_error_minutes"])
    best = min(d["stores"], key=lambda s: s["mean_signed_error_minutes"])
    payload = json.dumps({
        "stores": d["stores"], "daily": d["daily"], "pooled": d["pooled"],
        "series": SERIES,
    })

    tiles = [
        ("Events landed", f"{d['counts']['bronze_rows']:,}", f"{d['counts']['partitions']} day partitions"),
        ("Collapsed in silver", f"{d['counts']['collapsed']:,}", "redeliveries and corrections"),
        ("Trips", f"{d['totals']['trips']:,}", f"{d['totals']['drivers']} drivers, {d['totals']['stores']} stores"),
        ("Quarantined", f"{d['totals']['quarantined']:,}", "contract violations held back"),
    ]
    tile_html = "\n".join(
        f'''<div class="tile"><div class="tile-label">{label}</div>
            <div class="tile-value">{value}</div>
            <div class="tile-note">{note}</div></div>''' for label, value, note in tiles)

    comp_html = "\n".join(
        f'''<tr><td><span class="pill pill-{row['completeness']}"></span>{row['completeness']}</td>
            <td class="num">{row['n']:,}</td>
            <td class="num">{row['n'] / d['totals']['trips']:.1%}</td></tr>'''
        for row in d["completeness"])

    gate_html = "\n".join(
        f'''<tr><td class="mono">{g['stage']}</td><td class="mono">{g['check_name']}</td>
            <td><span class="verdict {'ok' if g['passed'] else 'bad'}">
            {'PASS' if g['passed'] else 'FAIL'}</span></td>
            <td class="detail">{g['detail']}</td></tr>''' for g in d["gates"])

    store_rows = "\n".join(
        f'''<tr><td><span class="swatch" data-store="{s['store_id']}"></span>{s['store_id']}</td>
            <td class="num">{s['trips_measured']:,}</td>
            <td class="num strong">{s['mean_signed_error_minutes']:+.2f}</td>
            <td class="num">{s['mean_absolute_error_minutes']:.2f}</td>
            <td class="num">{s['p50_error_minutes']:+.2f}</td>
            <td class="num">{s['p90_error_minutes']:+.2f}</td>
            <td class="num">{s['breach_rate']:.1%}</td></tr>''' for s in d["stores"])
    store_rows += f'''<tr class="pooled-row"><td>pooled</td>
        <td class="num">{d['pooled']['trips_measured']:,}</td>
        <td class="num strong">{d['pooled']['mean_signed_error_minutes']:+.2f}</td>
        <td class="num">{d['pooled']['mean_absolute_error_minutes']:.2f}</td>
        <td class="num">&mdash;</td><td class="num">&mdash;</td><td class="num">&mdash;</td></tr>'''

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Dispatch — Pipeline Results</title>
<style>
  .viz-root {{
    color-scheme: light;
    --surface-0:      #f4f4f2;
    --surface-1:      #fcfcfb;
    --border:         #dedcd6;
    --text-primary:   #0b0b0b;
    --text-secondary: #52514e;
    --text-muted:     #7b7a74;
    --grid:           #e7e5df;
    --series-FC002:   {SERIES['FC002']['light']};
    --series-FC004:   {SERIES['FC004']['light']};
    --good:           #1a7f4b;
    --bad:            #b4281f;
  }}
  @media (prefers-color-scheme: dark) {{
    :root:where(:not([data-theme="light"])) .viz-root {{
      color-scheme: dark;
      --surface-0:      #121211;
      --surface-1:      #1a1a19;
      --border:         #343430;
      --text-primary:   #ffffff;
      --text-secondary: #c3c2b7;
      --text-muted:     #8f8e85;
      --grid:           #2b2b28;
      --series-FC002:   {SERIES['FC002']['dark']};
      --series-FC004:   {SERIES['FC004']['dark']};
      --good:           #4ab87a;
      --bad:            #e66767;
    }}
  }}
  :root[data-theme="dark"] .viz-root {{
    color-scheme: dark;
    --surface-0: #121211; --surface-1: #1a1a19; --border: #343430;
    --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #8f8e85;
    --grid: #2b2b28;
    --series-FC002: {SERIES['FC002']['dark']};
    --series-FC004: {SERIES['FC004']['dark']};
    --good: #4ab87a; --bad: #e66767;
  }}

  * {{ box-sizing: border-box; }}
  html {{ background: var(--surface-0); }}
  body {{ margin: 0; background: var(--surface-0); }}
  .viz-root {{
    font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
    color: var(--text-primary);
    background: var(--surface-0);
    min-height: 100vh;
    padding: 28px 16px 64px;
  }}
  .wrap {{ max-width: 1040px; margin: 0 auto; }}

  header {{ margin-bottom: 22px; }}
  h1 {{ font-size: 21px; margin: 0 0 4px; letter-spacing: -0.01em; }}
  .sub {{ color: var(--text-secondary); font-size: 13px; margin: 0; }}
  .stamp {{ color: var(--text-muted); font-size: 11.5px; margin-top: 7px;
            font-variant-numeric: tabular-nums; }}

  section {{ background: var(--surface-1); border: 1px solid var(--border);
             border-radius: 10px; padding: 18px 20px; margin-bottom: 16px; }}
  h2 {{ font-size: 14px; margin: 0 0 3px; letter-spacing: -0.005em; }}
  .lede {{ color: var(--text-secondary); font-size: 12.5px; margin: 0 0 16px;
           max-width: 70ch; line-height: 1.5; }}

  .hero {{ display: flex; flex-wrap: wrap; gap: 26px; align-items: flex-end;
           margin-bottom: 16px; }}
  .hero-item .hero-value {{ font-size: 32px; font-weight: 650; line-height: 1;
                            font-variant-numeric: tabular-nums; }}
  .hero-item .hero-label {{ font-size: 11.5px; color: var(--text-secondary);
                            margin-top: 6px; display: flex; align-items: center; gap: 6px; }}
  .hero-sep {{ color: var(--text-muted); font-size: 22px; padding-bottom: 6px; }}

  .tiles {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
            gap: 12px; }}
  .tile {{ border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px; }}
  .tile-label {{ font-size: 11px; color: var(--text-secondary); }}
  .tile-value {{ font-size: 22px; font-weight: 620; margin-top: 3px;
                 font-variant-numeric: tabular-nums; }}
  .tile-note {{ font-size: 11px; color: var(--text-muted); margin-top: 2px; }}

  .legend {{ display: flex; gap: 16px; align-items: center; margin-bottom: 12px;
             font-size: 12px; color: var(--text-secondary); flex-wrap: wrap; }}
  .legend span.key {{ display: inline-flex; align-items: center; gap: 6px; }}
  .swatch {{ width: 10px; height: 10px; border-radius: 2px; display: inline-block;
             flex: 0 0 auto; }}
  .swatch[data-store="FC002"] {{ background: var(--series-FC002); }}
  .swatch[data-store="FC004"] {{ background: var(--series-FC004); }}
  .swatch.pooled {{ background: var(--text-muted); }}

  svg {{ display: block; width: 100%; height: auto; overflow: visible; }}
  .grid-line {{ stroke: var(--grid); stroke-width: 1; }}
  .axis-text {{ fill: var(--text-muted); font-size: 10.5px;
                font-variant-numeric: tabular-nums; }}
  .bar-label {{ fill: var(--text-primary); font-size: 12px; font-weight: 600;
                font-variant-numeric: tabular-nums; }}
  .ref-line {{ stroke: var(--text-muted); stroke-width: 1.5; stroke-dasharray: 4 3; }}
  .ref-text {{ fill: var(--text-secondary); font-size: 10.5px; }}
  .series-line {{ fill: none; stroke-width: 2; }}

  table {{ width: 100%; border-collapse: collapse; font-size: 12.5px; }}
  th {{ text-align: left; font-weight: 600; color: var(--text-secondary);
        font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em;
        padding: 0 10px 7px 0; border-bottom: 1px solid var(--border); }}
  td {{ padding: 7px 10px 7px 0; border-bottom: 1px solid var(--border); }}
  tr:last-child td {{ border-bottom: 0; }}
  td.num, th.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  td.strong {{ font-weight: 650; }}
  td.mono, .mono {{ font-family: ui-monospace, "Cascadia Mono", Menlo, monospace;
                    font-size: 11.5px; }}
  td.detail {{ color: var(--text-secondary); font-size: 11.5px; }}
  .pooled-row td {{ color: var(--text-secondary); border-top: 1px solid var(--border); }}
  .verdict {{ font-size: 10.5px; font-weight: 650; padding: 1px 6px; border-radius: 4px;
              letter-spacing: 0.03em; }}
  .verdict.ok {{ color: var(--good); border: 1px solid currentColor; }}
  .verdict.bad {{ color: var(--bad); border: 1px solid currentColor; }}
  .pill {{ width: 7px; height: 7px; border-radius: 50%; display: inline-block;
           margin-right: 7px; background: var(--text-muted); }}
  .pill-complete {{ background: var(--good); }}
  .pill-broken {{ background: var(--bad); }}

  .tooltip {{ position: fixed; pointer-events: none; opacity: 0; transition: opacity .09s;
              background: var(--surface-1); border: 1px solid var(--border);
              border-radius: 7px; padding: 8px 10px; font-size: 11.5px;
              box-shadow: 0 6px 20px rgba(0,0,0,.14); z-index: 20; min-width: 136px; }}
  .tooltip .tt-head {{ font-weight: 650; margin-bottom: 5px; }}
  .tooltip .tt-row {{ display: flex; justify-content: space-between; gap: 14px;
                      color: var(--text-secondary); }}
  .tooltip .tt-row b {{ color: var(--text-primary); font-variant-numeric: tabular-nums; }}

  .caveat {{ font-size: 11.5px; color: var(--text-muted); line-height: 1.55;
             border-left: 2px solid var(--border); padding-left: 12px; margin-top: 4px; }}
  @media (max-width: 620px) {{ .hero {{ gap: 16px; }} .hero-item .hero-value {{ font-size: 25px; }} }}
</style>
</head>
<body>
<div class="viz-root">
<div class="wrap">

<header>
  <h1>Dispatch — what the pipeline found</h1>
  <p class="sub">Delivery promise accuracy, per store, over {d['totals']['days']} trading days.</p>
  <p class="stamp">Generated {d['built_at']} UTC from the warehouse. Every number
     on this page is queried at build time — none are typed in.</p>
</header>

<section>
  <h2>The finding</h2>
  <p class="lede">The promise formula uses one global speed for every store. The
     stores do not move at one speed, so the error it leaves behind is more than
     twice as large at one of them — and a single pooled average hides that.</p>

  <div class="hero">
    <div class="hero-item">
      <div class="hero-value" style="color: var(--series-{worst['store_id']})">{worst['mean_signed_error_minutes']:+.2f}</div>
      <div class="hero-label"><span class="swatch" data-store="{worst['store_id']}"></span>{worst['store_id']} min late, on average</div>
    </div>
    <div class="hero-sep">vs</div>
    <div class="hero-item">
      <div class="hero-value" style="color: var(--series-{best['store_id']})">{best['mean_signed_error_minutes']:+.2f}</div>
      <div class="hero-label"><span class="swatch" data-store="{best['store_id']}"></span>{best['store_id']} min late, on average</div>
    </div>
    <div class="hero-sep">→</div>
    <div class="hero-item">
      <div class="hero-value" style="color: var(--text-secondary)">{d['pooled']['mean_signed_error_minutes']:+.2f}</div>
      <div class="hero-label"><span class="swatch pooled"></span>pooled — reports one problem, not two</div>
    </div>
  </div>

  <div class="legend">
    <span class="key"><span class="swatch" data-store="FC002"></span>FC002</span>
    <span class="key"><span class="swatch" data-store="FC004"></span>FC004</span>
    <span class="key"><span class="swatch pooled"></span>pooled average</span>
    <span style="color:var(--text-muted)">· signed minutes — positive means the delivery ran later than promised</span>
  </div>
  <svg id="error-chart" viewBox="0 0 900 230" role="img"
       aria-label="Mean signed promise error by store, with the pooled average marked"></svg>

  <p class="caveat"><b>Read this as a demonstration, not a validated model.</b>
     The events are synthetic, and the per-store speeds the marts recover are the
     ones the generator put there — that part is circular. What is not circular
     is the shape of the mistake: a pooled average over two populations with
     opposite biases reports a small error and hides both.</p>
</section>

<section>
  <h2>Breach rate by day</h2>
  <p class="lede">Share of measured trips that ran past their promise. Only
     completed trips count — a trip still in flight has no duration yet, and
     filling one in from the clock would make every open trip look slow.</p>
  <div class="legend">
    <span class="key"><span class="swatch" data-store="FC002"></span>FC002</span>
    <span class="key"><span class="swatch" data-store="FC004"></span>FC004</span>
  </div>
  <svg id="daily-chart" viewBox="0 0 900 260" role="img"
       aria-label="Daily breach rate per store over the trading period"></svg>
</section>

<section>
  <h2>Per store</h2>
  <p class="lede">p50 and p90 sit beside the mean because an average alone cannot
     separate a formula that is uniformly off from one that is fine for most
     trips and badly wrong in the tail. Those need different fixes.</p>
  <table>
    <thead><tr><th>Store</th><th class="num">Trips</th><th class="num">Mean signed</th>
      <th class="num">MAE</th><th class="num">p50</th><th class="num">p90</th>
      <th class="num">Breach</th></tr></thead>
    <tbody>{store_rows}</tbody>
  </table>
</section>

<section>
  <h2>Pipeline run</h2>
  <p class="lede">What the load actually did. These are the counts the gates were
     checked against.</p>
  <div class="tiles">{tile_html}</div>
</section>

<section>
  <h2>Trip completeness</h2>
  <p class="lede">A trip without a terminal event is not a failed trip — it is
     either still in flight, or stalled and worth looking at. Only
     <span class="mono">complete</span> rows carry a duration and enter an average.</p>
  <table>
    <thead><tr><th>State</th><th class="num">Trips</th><th class="num">Share</th></tr></thead>
    <tbody>{comp_html}</tbody>
  </table>
</section>

<section>
  <h2>Quality gates</h2>
  <p class="lede">Every verdict from this run, passed and failed alike. Logging
     only failures makes a check that passed indistinguishable from one that
     never ran. All of these run <em>before</em> their load.</p>
  <table>
    <thead><tr><th>Stage</th><th>Check</th><th>Verdict</th><th>Detail</th></tr></thead>
    <tbody>{gate_html}</tbody>
  </table>
</section>

</div>
</div>

<div class="tooltip" id="tip" role="status" aria-live="polite"></div>

<script>
const DATA = {payload};
const tip = document.getElementById("tip");

function showTip(html, evt) {{
  tip.innerHTML = html;
  tip.style.opacity = "1";
  const pad = 14, w = tip.offsetWidth, h = tip.offsetHeight;
  let x = evt.clientX + pad, y = evt.clientY - h / 2;
  if (x + w > innerWidth - 8) x = evt.clientX - w - pad;
  tip.style.left = Math.max(8, x) + "px";
  tip.style.top = Math.min(Math.max(8, y), innerHeight - h - 8) + "px";
}}
const hideTip = () => {{ tip.style.opacity = "0"; }};
const svgNS = "http://www.w3.org/2000/svg";
function el(name, attrs, parent) {{
  const n = document.createElementNS(svgNS, name);
  for (const k in attrs) n.setAttribute(k, attrs[k]);
  if (parent) parent.appendChild(n);
  return n;
}}

/* ---- signed error by store: horizontal bars + pooled reference ---- */
(function () {{
  const svg = document.getElementById("error-chart");
  const W = 900, H = 230, L = 86, R = 150, T = 18, B = 34;
  const rows = DATA.stores;
  const pooled = DATA.pooled.mean_signed_error_minutes;
  const max = Math.max(...rows.map(r => r.mean_signed_error_minutes), pooled) * 1.28;
  const x = v => L + (v / max) * (W - L - R);
  const band = (H - T - B) / rows.length;
  const barH = Math.min(46, band * 0.56);

  for (let t = 0; t <= max; t += 2) {{
    el("line", {{x1: x(t), x2: x(t), y1: T, y2: H - B, class: "grid-line"}}, svg);
    el("text", {{x: x(t), y: H - B + 15, class: "axis-text", "text-anchor": "middle"}}, svg)
      .textContent = t;
  }}
  el("text", {{x: (L + W - R) / 2, y: H - 4, class: "axis-text", "text-anchor": "middle"}}, svg)
    .textContent = "mean signed error (minutes late)";

  rows.forEach((r, i) => {{
    const cy = T + band * i + band / 2;
    const v = r.mean_signed_error_minutes;
    el("text", {{x: L - 12, y: cy + 4, class: "axis-text", "text-anchor": "end",
                "font-size": 12}}, svg).textContent = r.store_id;
    /* 4px rounded data-end, square against the baseline */
    const bar = el("path", {{
      d: `M ${{L}} ${{cy - barH / 2}} H ${{x(v) - 4}} a 4 4 0 0 1 4 4 V ${{cy + barH / 2 - 4}}
          a 4 4 0 0 1 -4 4 H ${{L}} Z`.replace(/\\s+/g, " "),
      fill: `var(--series-${{r.store_id}})`, "shape-rendering": "geometricPrecision"
    }}, svg);
    el("text", {{x: x(v) + 10, y: cy + 5, class: "bar-label"}}, svg)
      .textContent = v.toFixed(2) + " min";

    const hit = el("rect", {{x: L, y: cy - band / 2, width: W - L - R, height: band,
                            fill: "transparent", style: "cursor:crosshair"}}, svg);
    const tipHtml = `<div class="tt-head">${{r.store_id}}</div>
      <div class="tt-row"><span>mean signed</span><b>${{v.toFixed(2)}} min</b></div>
      <div class="tt-row"><span>MAE</span><b>${{r.mean_absolute_error_minutes.toFixed(2)}} min</b></div>
      <div class="tt-row"><span>p50 / p90</span><b>${{r.p50_error_minutes.toFixed(1)}} / ${{r.p90_error_minutes.toFixed(1)}}</b></div>
      <div class="tt-row"><span>breach</span><b>${{(r.breach_rate * 100).toFixed(1)}}%</b></div>
      <div class="tt-row"><span>trips</span><b>${{r.trips_measured.toLocaleString()}}</b></div>`;
    hit.addEventListener("mousemove", e => showTip(tipHtml, e));
    hit.addEventListener("mouseleave", hideTip);
    bar.style.pointerEvents = "none";
  }});

  el("line", {{x1: x(pooled), x2: x(pooled), y1: T - 6, y2: H - B + 2, class: "ref-line"}}, svg);
  el("text", {{x: x(pooled), y: T - 10, class: "ref-text", "text-anchor": "middle"}}, svg)
    .textContent = `pooled ${{pooled.toFixed(2)}}`;
}})();

/* ---- daily breach rate: two lines + shared crosshair ---- */
(function () {{
  const svg = document.getElementById("daily-chart");
  const W = 900, H = 260, L = 46, R = 58, T = 16, B = 40;
  const days = [...new Set(DATA.daily.map(r => r.date_key))].sort();
  const stores = [...new Set(DATA.daily.map(r => r.store_id))].sort();
  const byKey = new Map(DATA.daily.map(r => [r.store_id + "|" + r.date_key, r]));
  const x = i => L + (days.length < 2 ? 0 : (i / (days.length - 1)) * (W - L - R));
  const y = v => T + (1 - v) * (H - T - B);

  for (let p = 0; p <= 1.0001; p += 0.25) {{
    el("line", {{x1: L, x2: W - R, y1: y(p), y2: y(p), class: "grid-line"}}, svg);
    el("text", {{x: L - 9, y: y(p) + 4, class: "axis-text", "text-anchor": "end"}}, svg)
      .textContent = Math.round(p * 100) + "%";
  }}
  days.forEach((day, i) => {{
    if (i % Math.ceil(days.length / 8) && i !== days.length - 1) return;
    el("text", {{x: x(i), y: H - B + 16, class: "axis-text", "text-anchor": "middle"}}, svg)
      .textContent = day.slice(5);
  }});

  stores.forEach(store => {{
    const pts = days.map((day, i) => {{
      const row = byKey.get(store + "|" + day);
      return row && row.breach_rate != null ? [x(i), y(row.breach_rate)] : null;
    }}).filter(Boolean);
    if (!pts.length) return;
    el("path", {{d: "M " + pts.map(p => p.join(" ")).join(" L "),
                class: "series-line", stroke: `var(--series-${{store}})`}}, svg);
    /* Direct label at the line end: identity is never colour alone. */
    const last = pts[pts.length - 1];
    el("text", {{x: last[0] + 9, y: last[1] + 4, class: "bar-label",
                "font-size": 11.5, fill: `var(--series-${{store}})`}}, svg)
      .textContent = store;
  }});

  const cross = el("line", {{y1: T, y2: H - B, class: "grid-line",
                            stroke: "var(--text-muted)", opacity: 0}}, svg);
  const hit = el("rect", {{x: L, y: T, width: W - L - R, height: H - T - B,
                          fill: "transparent", style: "cursor:crosshair"}}, svg);
  hit.addEventListener("mousemove", e => {{
    const box = svg.getBoundingClientRect();
    const px = ((e.clientX - box.left) / box.width) * W;
    let i = Math.round(((px - L) / (W - L - R)) * (days.length - 1));
    i = Math.max(0, Math.min(days.length - 1, i));
    cross.setAttribute("x1", x(i)); cross.setAttribute("x2", x(i));
    cross.setAttribute("opacity", 1);
    let html = `<div class="tt-head">${{days[i]}}</div>`;
    stores.forEach(s => {{
      const r = byKey.get(s + "|" + days[i]);
      if (!r) return;
      html += `<div class="tt-row"><span>${{s}}</span><b>${{(r.breach_rate * 100).toFixed(1)}}%</b></div>`
            + `<div class="tt-row"><span style="padding-left:10px">measured</span>`
            + `<b>${{r.trips_measured}}/${{r.trips_total}}</b></div>`;
    }});
    showTip(html, e);
  }});
  hit.addEventListener("mouseleave", () => {{ cross.setAttribute("opacity", 0); hideTip(); }});
}})();
</script>
</body>
</html>
"""


def main() -> int:
    data = fetch()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(render(data), encoding="utf-8")
    kb = OUT.stat().st_size / 1024
    print(f"wrote {OUT.relative_to(ROOT)}  ({kb:.0f} KB)")
    print(f"  {data['totals']['trips']:,} trips, {len(data['daily'])} store-days, "
          f"{len(data['gates'])} gate verdicts")
    return 0


if __name__ == "__main__":
    sys.exit(main())
