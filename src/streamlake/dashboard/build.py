"""Render the BI dashboard from the warehouse marts.

Two pages in one file: **KPIs** (what the data says) and **Pipeline health** (whether you should
believe it). The second page is the one that justifies the whole project — a freshness monitor
plus the contract results for the run that produced these numbers. A static chart cannot tell
you it is showing you three-day-old data; this can.

Output is a single self-contained HTML file with no external requests, so it opens from disk,
survives being emailed, and can be committed as evidence of a run.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

from streamlake.config import Config, get_config
from streamlake.dashboard import charts
from streamlake.dashboard.template import render_page
from streamlake.logging_utils import banner, get_logger

log = get_logger(__name__)

MARTS_SCHEMA = "analytics_marts"
# SVG viewBox widths chosen to match where each chart actually renders. A viewBox much narrower
# than its container gets scaled up and its 12px labels arrive as 17px; one much wider shrinks
# them to 6px. Matching the two keeps type sizes consistent across the page.
CARD_WIDTH = 520
FULL_WIDTH = 1060
STAGING_SCHEMA = "analytics_staging"


def _connect(cfg: Config):
    import duckdb

    db_path = Path(str(cfg.require("paths.warehouse_db")))
    if not db_path.is_absolute():
        db_path = cfg.root / db_path
    if not db_path.exists():
        raise RuntimeError(f"no warehouse at {db_path} — run `make warehouse` and `make dbt`")
    return duckdb.connect(str(db_path), read_only=True)


def _rows(con, sql: str) -> list[dict[str, Any]]:
    cursor = con.execute(sql)
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def _contract_summary(cfg: Config) -> dict[str, Any]:
    path = cfg.path("reports") / "contract_summary.json"
    if path.exists():
        return json.loads(path.read_text())
    # Fall back to rebuilding it, so the dashboard is never blank just because the summary task
    # has not run in this session.
    from streamlake.contracts.summary import summarise

    return summarise(cfg)


def collect(cfg: Config) -> dict[str, Any]:
    con = _connect(cfg)
    try:
        boroughs = _rows(
            con,
            f"""
            SELECT pickup_borough, trips, revenue, avg_ticket, avg_distance_mi,
                   avg_duration_min, avg_tip_pct, trip_share_pct, revenue_share_pct,
                   first_pickup_ts, last_pickup_ts
            FROM {MARTS_SCHEMA}.mart_borough_summary
            ORDER BY trips DESC
            """,
        )

        top_boroughs = [b["pickup_borough"] for b in boroughs[:3]]
        quoted = ", ".join(f"'{b}'" for b in top_boroughs) or "''"
        hourly = _rows(
            con,
            f"""
            SELECT pickup_hour, pickup_borough, sum(trips) AS trips
            FROM {MARTS_SCHEMA}.fct_hourly_demand
            WHERE pickup_borough IN ({quoted})
            GROUP BY 1, 2
            ORDER BY 1
            """,
        )

        zones = _rows(
            con,
            f"""
            SELECT pickup_zone, pickup_borough, sum(trips) AS trips,
                   round(sum(revenue), 2) AS revenue,
                   round(sum(revenue) / sum(trips), 2) AS revenue_per_trip
            FROM {MARTS_SCHEMA}.fct_trip_daily_zone
            GROUP BY 1, 2
            ORDER BY revenue DESC
            LIMIT 12
            """,
        )

        payments = _rows(
            con,
            f"""
            SELECT payment_type_desc, sum(trips) AS trips, round(sum(revenue), 2) AS revenue
            FROM {STAGING_SCHEMA}.stg_payment_mix
            GROUP BY 1
            ORDER BY trips DESC
            """,
        )

        coverage = _rows(
            con,
            f"""
            SELECT count(DISTINCT pickup_zone) AS zones,
                   count(DISTINCT pickup_borough) AS boroughs,
                   min(pickup_date) AS first_date,
                   max(pickup_date) AS last_date
            FROM {MARTS_SCHEMA}.fct_trip_daily_zone
            """,
        )[0]

        daily = _rows(
            con,
            f"""
            SELECT pickup_date, sum(trips) AS trips, round(sum(revenue), 2) AS revenue
            FROM {MARTS_SCHEMA}.fct_trip_daily_zone
            GROUP BY 1
            ORDER BY 1
            """,
        )

        freshness = _rows(
            con,
            f"""
            SELECT table_name, layer, watermark_ts, row_count, lag_hours, freshness_status
            FROM {MARTS_SCHEMA}.mart_data_freshness
            ORDER BY lag_hours DESC
            """,
        )

        quarantine: list[dict[str, Any]] = []
        raw_schema = str(cfg.require("warehouse.schema_raw"))
        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            f"WHERE table_schema = '{raw_schema}' AND table_name = 'quarantine_reasons'"
        ).fetchone()[0]
        if exists:
            quarantine = _rows(
                con,
                f"SELECT reject_reason, rows FROM {raw_schema}.quarantine_reasons ORDER BY rows DESC",
            )

        stream: list[dict[str, Any]] = []
        stream_exists = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            f"WHERE table_schema = '{raw_schema}' AND table_name = 'trip_metrics_1m'"
        ).fetchone()[0]
        if stream_exists:
            stream = _rows(
                con,
                f"""
                SELECT window_start, pickup_borough, trips, revenue
                FROM {raw_schema}.trip_metrics_1m
                ORDER BY window_start DESC
                LIMIT 20
                """,
            )
    finally:
        con.close()

    return {
        "boroughs": boroughs,
        "coverage": coverage,
        "hourly": hourly,
        "zones": zones,
        "payments": payments,
        "daily": daily,
        "freshness": freshness,
        "quarantine": quarantine,
        "stream": stream,
        "contracts": _contract_summary(cfg),
    }


def _table(headers: list[str], rows: list[list[str]], *, align_right: set[int] | None = None) -> str:
    align_right = align_right or set()
    head = "".join(
        f'<th class="{"num" if i in align_right else ""}">{escape(h)}</th>'
        for i, h in enumerate(headers)
    )
    body = "".join(
        "<tr>"
        + "".join(
            f'<td class="{"num" if i in align_right else ""}">{cell}</td>'
            for i, cell in enumerate(row)
        )
        + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def build_html(data: dict[str, Any], cfg: Config) -> str:
    boroughs = data["boroughs"]
    total_trips = sum(b["trips"] for b in boroughs)
    total_revenue = sum(b["revenue"] for b in boroughs)
    avg_ticket = total_revenue / total_trips if total_trips else 0
    quarantined = sum(q["rows"] for q in data["quarantine"])
    contracts = data["contracts"]

    # --- page 1: KPIs -------------------------------------------------------------------
    tiles = [
        ("Trips", f"{total_trips:,}", f"{cfg.month} · yellow taxi"),
        ("Revenue", f"${charts.compact(total_revenue)}", "total fares collected"),
        ("Average ticket", f"${avg_ticket:,.2f}", "revenue per trip"),
        ("Zones covered", f"{data['coverage']['zones']}", f"pickup zones across {data['coverage']['boroughs']} boroughs"),
        ("Quarantined", f"{quarantined:,}", f"{100 * quarantined / (total_trips + quarantined):.2f}% of ingested rows"),
    ]
    tile_html = "".join(
        f'<div class="tile"><div class="tile-label">{escape(label)}</div>'
        f'<div class="tile-value">{escape(value)}</div>'
        f'<div class="tile-note">{escape(note)}</div></div>'
        for label, value, note in tiles
    )

    borough_bar = charts.hbar(
        [b["pickup_borough"] for b in boroughs],
        [float(b["trips"]) for b in boroughs],
        width=CARD_WIDTH,
        label_width=120,
    )

    hours = sorted({h["pickup_hour"] for h in data["hourly"]})
    series = []
    for slot, borough in enumerate(dict.fromkeys(h["pickup_borough"] for h in data["hourly"])):
        by_hour = {h["pickup_hour"]: float(h["trips"]) for h in data["hourly"] if h["pickup_borough"] == borough}
        series.append(charts.Series(borough, [by_hour.get(hour, 0.0) for hour in hours], slot))
    hourly_chart = charts.multiline(
        [f"{h:02d}" for h in hours],
        series,
        width=CARD_WIDTH,
        height=300,
        y_label="trips per hour of day",
    )

    zone_bar = charts.hbar(
        [z["pickup_zone"] for z in data["zones"]],
        [float(z["revenue"]) for z in data["zones"]],
        value_format="${:,.0f}",
        width=CARD_WIDTH,
        label_width=190,
    )

    payment_bar = charts.stacked_bar(
        [(p["payment_type_desc"], float(p["trips"])) for p in data["payments"]],
        width=CARD_WIDTH,
    )

    borough_table = _table(
        ["Borough", "Trips", "Share", "Revenue", "Avg ticket", "Avg miles", "Avg minutes", "Avg tip %"],
        [
            [
                escape(b["pickup_borough"]),
                f"{b['trips']:,}",
                f"{b['trip_share_pct']:.1f}%",
                f"${b['revenue']:,.0f}",
                f"${b['avg_ticket']:.2f}",
                f"{b['avg_distance_mi']:.2f}",
                f"{b['avg_duration_min']:.1f}",
                f"{b['avg_tip_pct']:.1f}%" if b["avg_tip_pct"] is not None else "—",
            ]
            for b in boroughs
        ],
        align_right={1, 2, 3, 4, 5, 6, 7},
    )

    zone_table = _table(
        ["Pickup zone", "Borough", "Trips", "Revenue", "Revenue / trip"],
        [
            [
                escape(z["pickup_zone"]),
                escape(z["pickup_borough"]),
                f"{z['trips']:,}",
                f"${z['revenue']:,.0f}",
                f"${z['revenue_per_trip']:.2f}",
            ]
            for z in data["zones"]
        ],
        align_right={2, 3, 4},
    )

    # --- page 2: pipeline health --------------------------------------------------------
    status_map = {"fresh": "good", "stale": "warning", "breached": "critical"}
    freshness_table = _table(
        ["Table", "Layer", "Watermark", "Rows", "Lag (h)", "Status"],
        [
            [
                escape(f["table_name"]),
                escape(f["layer"]),
                escape(str(f["watermark_ts"])),
                f"{f['row_count']:,}",
                f"{f['lag_hours']:.1f}",
                charts.status_dot(status_map.get(f["freshness_status"], "warning"))
                + escape(f["freshness_status"]),
            ]
            for f in data["freshness"]
        ],
        align_right={3, 4},
    )

    contract_rows = [
        [
            escape(c["contract"]),
            escape(c["dataset"]),
            f"{c['row_count']:,}",
            f"{c['checks_total'] - c['checks_failed']}/{c['checks_total']}",
            f"{c['duration_seconds']:.2f}s",
            charts.status_dot(
                "good" if c["status"] == "PASSED"
                else ("warning" if c["status"] == "PASSED_WITH_WARNINGS" else "critical")
            )
            + escape(c["status"].replace("_", " ").lower()),
        ]
        for c in contracts.get("by_contract", [])
    ]
    contract_table = _table(
        ["Contract", "Dataset", "Rows", "Checks passed", "Duration", "Status"],
        contract_rows,
        align_right={2, 3, 4},
    )

    failures = contracts.get("failures", [])
    failure_html = (
        "".join(
            f'<li>{charts.status_dot("critical" if f["severity"] == "error" else "warning")}'
            f'<code>{escape(f["contract"])}</code> · <strong>{escape(f["check"])}</strong> — '
            f'observed {escape(str(f["observed"]))}, expected {escape(str(f["expected"]))}</li>'
            for f in failures
        )
        if failures
        else '<li class="muted">No breaches in the latest run.</li>'
    )

    quarantine_bar = charts.hbar(
        [q["reject_reason"].replace("_", " ") for q in data["quarantine"]],
        [float(q["rows"]) for q in data["quarantine"]],
        width=CARD_WIDTH,
        label_width=190,
    ) if data["quarantine"] else '<p class="empty">no quarantined rows recorded</p>'

    stream_table = (
        _table(
            ["Window start", "Borough", "Trips", "Revenue"],
            [
                [
                    escape(str(s["window_start"])),
                    escape(s["pickup_borough"]),
                    f"{s['trips']:,}",
                    f"${s['revenue']:,.2f}",
                ]
                for s in data["stream"]
            ],
            align_right={2, 3},
        )
        if data["stream"]
        else '<p class="empty">the streaming arm has not been exported yet — run `make stream` then `make export warehouse`</p>'
    )

    daily_series = [charts.Series("trips", [float(d["trips"]) for d in data["daily"]], 0)]
    daily_chart = charts.multiline(
        [str(d["pickup_date"])[5:] for d in data["daily"]],
        daily_series,
        width=FULL_WIDTH,
        height=300,
        y_label="trips per day",
    )

    return render_page(
        month=cfg.month,
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        contract_status=contracts.get("status", "UNKNOWN"),
        contract_counts=contracts,
        tiles=tile_html,
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
    )


def run(cfg: Config | None = None) -> dict[str, str]:
    cfg = cfg or get_config()
    banner(log, "DASHBOARD | rendering from the warehouse marts")

    data = collect(cfg)
    html = build_html(data, cfg)

    target = cfg.root / "dashboard" / "build" / "index.html"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(html)

    log.info("dashboard written: %s (%.0f KB)", target, target.stat().st_size / 1024)
    return {"path": str(target), "contract_status": data["contracts"].get("status", "UNKNOWN")}


if __name__ == "__main__":  # pragma: no cover
    run()
