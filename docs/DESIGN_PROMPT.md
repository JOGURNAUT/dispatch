# Prompt for a redesign of the results page

Paste everything below the line into a fresh Claude conversation. Attach a
screenshot of `docs/results.html` — it reads far faster than a description, and
the complaint this prompt exists to fix is visual.

Regenerate the page first so the screenshot is current:

```
.\run published
```

---

I need you to redesign a results page for a data-engineering project. Give me
back a single HTML file. I will wire the numbers back in myself.

## Who reads it and for how long

A hiring manager, over a shared screen, for about twenty seconds, while I talk
over it. This is not a dashboard anyone returns to — it is the "so what" of a
project walkthrough. If a person cannot tell what the finding is without me
narrating it, the page has failed.

They are technical, reading for clarity rather than depth.

## The project

`Dispatch` is a last-mile delivery pipeline: Kafka to Spark Structured Streaming
to a bronze/silver/gold lake to a star schema, orchestrated by Airflow, with dbt
marts and eight blocking data-quality gates. The events are synthetic, from a
seeded generator, with defects injected on purpose.

## The finding the page exists to deliver

The delivery-promise formula uses **one global speed for every store**. The
stores do not move at one speed, so the error it leaves behind is more than twice
as large at one store as the other — **and a single pooled average hides that
completely.**

```
NORTHGATE    +10.16 minutes late on average   (11,083 trips)
RIVERSIDE     +4.61 minutes late on average   ( 8,229 trips)
pooled        +7.79                           one problem where there are two
```

That contrast is the entire page. Everything else is supporting evidence for it.

## What the page looks like right now

Six sections, stacked, roughly equal visual weight:

1. **Header** — title, one-line subtitle, a "generated at `<time>` from the
   warehouse" stamp.
2. **"Mean signed error per trip"** — a bespoke horizontal **number line**: an
   axis from 0 to 12 minutes with three labelled markers on it (the two stores
   and the pooled figure), plus a bracket between the two stores annotated
   "5.55 min apart". Then a caveat paragraph about the synthetic data.
3. **"Breach rate by day"** — a two-series inline-SVG line chart over 14 days,
   with a hover column per day showing a tooltip carrying both percentages.
4. **"Per store"** — a table: store, trips, mean signed error, MAE, p50, p90,
   breach rate.
5. **"Pipeline run"** — four stat tiles: events landed 125,264, collapsed in
   silver 5,948, trips 20,000, quarantined 0.
6. **"Trip completeness"** then **"Quality gates"** — two more tables, the second
   listing eight checks with PASS badges and a detail sentence each.

## What is wrong with it

This is the part I most want your judgement on. My own read:

- **The number line has to be decoded.** It is a clever custom chart, and that is
  the problem: before learning anything the reader must work out what the axis
  is, what "signed error" means, and what the bracket measures. Twenty seconds
  does not survive a chart with a learning curve.
- **The heading leads with jargon.** "Mean signed error per trip" is the correct
  name for the statistic and tells a reader nothing about why they should care.
- **The daily chart hides its data behind hover.** Nothing is readable in a
  screenshot, on a phone, or by anyone not moving a mouse — which is most of the
  situations this page is actually seen in.
- **Six sections of equal weight means no headline.** "Trip completeness" takes
  roughly the same visual space as the finding.
- **The headline claims "2.2x"**, a ratio the chart beneath it never shows, so
  the reader cannot check the claim against the picture.

Tell me if you disagree with any of that. I am not attached to the number line,
the section order, or the section count.

## What I want

The finding legible in **under five seconds**, with its strongest supporting
evidence on the same screen at 1440px. Everything else can sit below the fold.

Think about what form actually suits "two numbers that should have been one".
Three markers on an axis is one answer. It may not be the best one.

## Hard constraints

- **One self-contained HTML file.** No build step, no framework, no npm.
- **No external requests at all.** No CDN, no web fonts, no remote images. It is
  served from GitHub Pages and must render with the network off.
- **Inline SVG only** for charts. No charting library.
- **Light and dark mode**, both deliberate. Use `prefers-color-scheme` and also
  honour `data-theme="dark"` and `data-theme="light"` on `:root`.
- **Phone width with no horizontal scrolling.**
- **Colour must never be the only carrier of meaning.** Put a direct label beside
  anything identified by colour; assume a colourblind reader and a greyscale
  print.
- **Nothing important may be hover-only.** Hover may add detail; it may not be
  the only way to read a value.

## How the numbers get in — please follow this exactly

The file is a **template**. A Python script replaces every `{lower_snake}` token
with a value, and raises if one has no value. There is no f-string and no
escaping involved: **write normal CSS, braces and all.** Only `{lower_snake}` is
touched, so `{` in `@media` blocks and `{}` in CSS rules are safe and must be
left as they are.

Use these exact placeholder names where those values belong, and invent new ones
in the same style if your design needs values the current one does not:

```
{a_name} {a_mean}        first store and its mean signed error
{b_name} {b_mean}        second store and its mean signed error
{pooled_mean} {gap} {ratio}
{trips} {events_landed} {collapsed_silver} {quarantined}
{complete_n} {complete_pct} {broken_n} {broken_pct}
{n_days} {days_worse}
{generated_at}
{store_rows} {gate_rows} {gates_passed} {gates_total}
{line_a_points} {line_b_points} {day_columns} {axis_ticks}
```

A further seventeen placeholders exist only to position the current number line
— `{a_pos}`, `{gap_left}`, `{gap_width}` and similar. They belong to that chart
and die with it, so ignore them unless you keep something shaped the same way.

`{store_rows}`, `{gate_rows}`, `{day_columns}` and the `_points` values are
pre-rendered HTML or SVG fragments. If you change the shape of a table or a
chart, say plainly what markup you now expect those to contain and I will change
the Python that emits them.

## Things not to do

- No logo, no invented company name, no nav bar beyond what is there.
- Do not make it look like a SaaS analytics product. It is an engineering result.
- **Do not drop the caveat.** The data is synthetic and the page must say so,
  directly under the chart it qualifies, not in a footnote.
- Do not remove the quality-gate table. Eight checks that all passed is part of
  the point.
- No animation beyond a hover state.

## Palette (keep unless you have a reason)

```
light surface  #fcfcfb      dark surface  #1a1a19
light text     #0b0b0b      dark text     #ffffff
series 1       #2a78d6      dark          #3987e5
series 2       #eb6834      dark          #d95926
```

These two series colours were checked for colourblind separation and contrast in
both modes. If you change them, tell me what you checked.
