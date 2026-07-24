"""The HTML shell.

Kept as one string rather than a Jinja file so the dashboard has no template-directory
dependency and the whole renderer is two importable modules. Colours are declared once as CSS
custom properties and re-declared for dark mode, so every chart inherits the theme rather than
carrying baked-in hex.
"""

from __future__ import annotations

CSS = """
:root {
  color-scheme: light;
  --page: #f9f9f7;
  --surface: #fcfcfb;
  --text: #0b0b0b;
  --text-secondary: #52514e;
  --muted: #898781;
  --grid: #e1e0d9;
  --axis: #c3c2b7;
  --border: rgba(11, 11, 11, 0.10);
  --series-0: #2a78d6;
  --series-1: #eb6834;
  --series-2: #1baf7a;
  --series-3: #eda100;
}
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) {
    color-scheme: dark;
    --page: #0d0d0d;
    --surface: #1a1a19;
    --text: #ffffff;
    --text-secondary: #c3c2b7;
    --muted: #898781;
    --grid: #2c2c2a;
    --axis: #383835;
    --border: rgba(255, 255, 255, 0.10);
    --series-0: #3987e5;
    --series-1: #d95926;
    --series-2: #199e70;
    --series-3: #c98500;
  }
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --page: #0d0d0d;
  --surface: #1a1a19;
  --text: #ffffff;
  --text-secondary: #c3c2b7;
  --grid: #2c2c2a;
  --axis: #383835;
  --border: rgba(255, 255, 255, 0.10);
  --series-0: #3987e5;
  --series-1: #d95926;
  --series-2: #199e70;
  --series-3: #c98500;
}

* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--page);
  color: var(--text);
  font: 15px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
  -webkit-font-smoothing: antialiased;
}
.wrap { max-width: 1120px; margin: 0 auto; padding: 32px 20px 72px; }

header { display: flex; flex-wrap: wrap; gap: 16px; align-items: baseline; justify-content: space-between; margin-bottom: 8px; }
h1 { font-size: 26px; margin: 0; letter-spacing: -0.01em; }
h2 { font-size: 18px; margin: 40px 0 4px; letter-spacing: -0.01em; }
h3 { font-size: 14px; margin: 24px 0 8px; color: var(--text-secondary); font-weight: 600; }
.sub { color: var(--text-secondary); font-size: 13px; margin: 0; }
.section-note { color: var(--text-secondary); font-size: 13px; margin: 0 0 16px; max-width: 70ch; }

.badge { display: inline-flex; align-items: center; gap: 7px; padding: 5px 11px; border-radius: 999px; border: 1px solid var(--border); font-size: 12px; font-weight: 600; letter-spacing: 0.02em; background: var(--surface); }

.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; margin: 20px 0 8px; }
.tile { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 14px 16px; }
.tile-label { font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; }
.tile-value { font-size: 26px; font-weight: 650; margin: 4px 0 2px; letter-spacing: -0.02em; font-variant-numeric: tabular-nums; }
.tile-note { font-size: 12px; color: var(--text-secondary); }

.card { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 18px 20px; margin: 12px 0; overflow-x: auto; }
.grid2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 12px; }

svg.chart { width: 100%; height: auto; display: block; overflow: visible; }
.bar { fill: var(--series-0); }
.bar-row:hover .bar { fill: var(--series-1); }
.series-fill-0 { fill: var(--series-0); } .series-fill-1 { fill: var(--series-1); }
.series-fill-2 { fill: var(--series-2); } .series-fill-3 { fill: var(--series-3); }
.line { fill: none; stroke-width: 2; stroke-linejoin: round; stroke-linecap: round; }
.dot { opacity: 0; }
.dot:hover { opacity: 1; }
polyline.series-0, .series-label.series-0 { stroke: var(--series-0); }
polyline.series-1, .series-label.series-1 { stroke: var(--series-1); }
polyline.series-2, .series-label.series-2 { stroke: var(--series-2); }
polyline.series-3, .series-label.series-3 { stroke: var(--series-3); }
circle.series-0 { fill: var(--series-0); } circle.series-1 { fill: var(--series-1); }
circle.series-2 { fill: var(--series-2); } circle.series-3 { fill: var(--series-3); }
.series-label { stroke: none; font-size: 12px; font-weight: 600; }
.series-label.series-0 { fill: var(--series-0); } .series-label.series-1 { fill: var(--series-1); }
.series-label.series-2 { fill: var(--series-2); } .series-label.series-3 { fill: var(--series-3); }
.grid { stroke: var(--grid); stroke-width: 1; }
.axis-label { fill: var(--muted); font-size: 11px; }
.axis-title { fill: var(--text-secondary); font-size: 11px; font-weight: 600; }
.cat-label { fill: var(--text-secondary); font-size: 12px; }
.val-label { fill: var(--text); font-size: 12px; font-variant-numeric: tabular-nums; }

table { border-collapse: collapse; width: 100%; font-size: 13px; }
th, td { padding: 7px 10px; text-align: left; border-bottom: 1px solid var(--border); white-space: nowrap; }
th { color: var(--muted); font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
tbody tr:hover { background: color-mix(in srgb, var(--series-0) 7%, transparent); }

.dot-status { display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 7px; vertical-align: baseline; }
ul.failures { list-style: none; padding: 0; margin: 8px 0 0; font-size: 13px; }
ul.failures li { padding: 7px 0; border-bottom: 1px solid var(--border); }
.muted, .empty { color: var(--muted); font-size: 13px; }
code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; background: color-mix(in srgb, var(--muted) 14%, transparent); padding: 1px 5px; border-radius: 4px; }
footer { margin-top: 48px; padding-top: 16px; border-top: 1px solid var(--border); color: var(--muted); font-size: 12px; }
button.theme { background: var(--surface); color: var(--text-secondary); border: 1px solid var(--border); border-radius: 999px; padding: 5px 12px; font: inherit; font-size: 12px; cursor: pointer; }
"""

PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>StreamLake — {month} lakehouse dashboard</title>
<style>{css}</style>
</head>
<body>
<div class="wrap">

<header>
  <div>
    <h1>StreamLake</h1>
    <p class="sub">NYC yellow taxi · {month} · batch and streaming lakehouse with enforced data contracts</p>
  </div>
  <div style="display:flex;gap:10px;align-items:center">
    <span class="badge">{status_dot}contracts {contract_status_label}</span>
    <button class="theme" onclick="var r=document.documentElement;r.dataset.theme=r.dataset.theme==='dark'?'light':'dark'">theme</button>
  </div>
</header>

<div class="tiles">{tiles}</div>

<h2>Demand</h2>
<p class="section-note">Where the trips are and when they happen. Every number below is computed
from the silver table that passed its contract — rows that failed validity rules were
quarantined upstream and are counted separately on the pipeline-health page.</p>

<div class="grid2">
  <div class="card">
    <h3>Trips by borough</h3>
    {borough_bar}
  </div>
  <div class="card">
    <h3>Trips by hour of day — top three boroughs</h3>
    {hourly_chart}
  </div>
</div>

<div class="card">
  <h3>Trips per day across the month</h3>
  {daily_chart}
</div>

<div class="card">
  <h3>Borough detail</h3>
  {borough_table}
</div>

<h2>Revenue</h2>
<p class="section-note">Revenue concentration by pickup zone, and how riders pay for it.</p>

<div class="grid2">
  <div class="card">
    <h3>Top zones by revenue</h3>
    {zone_bar}
  </div>
  <div class="card">
    <h3>Payment mix by trips</h3>
    {payment_bar}
    <h3>Zone detail</h3>
    {zone_table}
  </div>
</div>

<h2>Pipeline health</h2>
<p class="section-note">The page a static dashboard does not have. A chart cannot tell you it is
showing three-day-old numbers, and an aggregate hides a duplicated grain perfectly well. These
are the checks that ran on the data above, and how far behind each table is.</p>

<div class="card">
  <h3>Data freshness — lag behind the end of the loaded period</h3>
  {freshness_table}
</div>

<div class="card">
  <h3>Contract results for this run</h3>
  {contract_table}
  <h3>Breaches</h3>
  <ul class="failures">{failure_html}</ul>
</div>

<div class="grid2">
  <div class="card">
    <h3>Quarantined rows by reason</h3>
    <p class="muted">Rejected at the bronze&rarr;silver hop, kept with their reason rather than dropped.</p>
    {quarantine_bar}
  </div>
  <div class="card">
    <h3>Streaming arm — most recent one-minute windows</h3>
    <p class="muted">Kafka &rarr; Structured Streaming &rarr; Iceberg, deduplicated within the watermark.</p>
    {stream_table}
  </div>
</div>

<footer>
  Generated {generated_at} · {contracts_checked} contract checks across {contracts_count} contracts
  · {errors} errors, {warnings} warnings · rendered from the DuckDB marts by
  <code>streamlake dashboard</code>
</footer>

</div>
</body>
</html>
"""


def render_page(
    *,
    month: str,
    generated_at: str,
    contract_status: str,
    contract_counts: dict,
    tiles: str,
    borough_bar: str,
    borough_table: str,
    hourly_chart: str,
    daily_chart: str,
    zone_bar: str,
    zone_table: str,
    payment_bar: str,
    freshness_table: str,
    contract_table: str,
    failure_html: str,
    quarantine_bar: str,
    stream_table: str,
) -> str:
    from streamlake.dashboard.charts import status_dot

    status_class = {
        "PASSED": "good",
        "PASSED_WITH_WARNINGS": "warning",
        "FAILED": "critical",
    }.get(contract_status, "warning")

    return PAGE.format(
        css=CSS,
        month=month,
        generated_at=generated_at,
        status_dot=status_dot(status_class),
        contract_status_label=contract_status.replace("_", " ").lower(),
        tiles=tiles,
        borough_bar=borough_bar,
        borough_table=borough_table,
        hourly_chart=hourly_chart,
        daily_chart=daily_chart,
        zone_bar=zone_bar,
        zone_table=zone_table,
        payment_bar=payment_bar,
        freshness_table=freshness_table,
        contract_table=contract_table,
        failure_html=failure_html,
        quarantine_bar=quarantine_bar,
        stream_table=stream_table,
        contracts_count=contract_counts.get("contracts", 0),
        contracts_checked=contract_counts.get("checks", 0),
        errors=contract_counts.get("errors", 0),
        warnings=contract_counts.get("warnings", 0),
    )
