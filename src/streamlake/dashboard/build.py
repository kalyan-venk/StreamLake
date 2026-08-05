"""Render the BI dashboard from the warehouse marts.

Two pages in one file: KPIs (what the data says) and pipeline health (whether you should believe
it). The second page carries a freshness monitor plus the contract results for the run that
produced these numbers, so a reader can see they are looking at three-day-old data.

Output is one self-contained HTML file with no external requests, which is why it can be
committed as evidence of a run.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
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
STREAM_EMPTY = (
    '<p class="empty">the streaming arm has not been exported yet, '
    "run <code>make stream</code> then <code>make export warehouse</code></p>"
)
FULL_WIDTH = 1060
STAGING_SCHEMA = "analytics_staging"


def _connect(cfg: Config):
    import duckdb

    db_path = Path(str(cfg.require("paths.warehouse_db")))
    if not db_path.is_absolute():
        db_path = cfg.root / db_path
    if not db_path.exists():
        raise RuntimeError(f"no warehouse at {db_path}, run `make warehouse` and `make dbt`")
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
        categories = _rows(
            con,
            f"""
            SELECT category, txns, fraud_txns, fraud_rate, total_amt, avg_amt,
                   txn_share_pct, amt_share_pct, first_trans_time, last_trans_time
            FROM {MARTS_SCHEMA}.mart_category_summary
            ORDER BY txns DESC
            """,
        )

        top_categories = [c["category"] for c in categories[:3]]
        quoted = ", ".join(f"'{c}'" for c in top_categories) or "''"
        hourly = _rows(
            con,
            f"""
            SELECT trans_hour_ts, category, sum(txns) AS txns
            FROM {MARTS_SCHEMA}.fct_category_hourly_fraud
            WHERE category IN ({quoted})
            GROUP BY 1, 2
            ORDER BY 1
            LIMIT 3000
            """,
        )

        merchants = _rows(
            con,
            f"""
            SELECT merchant, category, txns, fraud_txns,
                   round(fraud_rate * 100, 2) AS fraud_rate_pct
            FROM {STAGING_SCHEMA}.stg_merchant_risk
            ORDER BY fraud_rate DESC, txns DESC
            LIMIT 12
            """,
        )

        states = _rows(
            con,
            f"""
            SELECT state, sum(txns) AS txns, round(sum(total_amt), 2) AS total_amt
            FROM {MARTS_SCHEMA}.fct_state_hourly_volume
            GROUP BY 1
            ORDER BY total_amt DESC
            LIMIT 12
            """,
        )

        coverage = _rows(
            con,
            f"""
            SELECT
                (SELECT count(DISTINCT category)
                 FROM {MARTS_SCHEMA}.fct_category_hourly_fraud)  AS categories,
                (SELECT count(DISTINCT merchant)
                 FROM {STAGING_SCHEMA}.stg_merchant_risk)        AS merchants
            """,
        )[0]

        daily = _rows(
            con,
            f"""
            SELECT date_trunc('day', trans_hour_ts) AS trans_date,
                   sum(txns) AS txns, sum(fraud_txns) AS fraud_txns,
                   round(sum(total_amt), 2) AS total_amt
            FROM {MARTS_SCHEMA}.fct_category_hourly_fraud
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
                f"SELECT reject_reason, rows FROM {raw_schema}.quarantine_reasons "
                "ORDER BY rows DESC",
            )

        stream: list[dict[str, Any]] = []
        stream_exists = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            f"WHERE table_schema = '{raw_schema}' AND table_name = 'txn_metrics_1m'"
        ).fetchone()[0]
        if stream_exists:
            stream = _rows(
                con,
                f"""
                SELECT window_start, category, txns, fraud_txns, total_amt
                FROM {raw_schema}.txn_metrics_1m
                ORDER BY window_start DESC
                LIMIT 20
                """,
            )
    finally:
        con.close()

    return {
        "categories": categories,
        "coverage": coverage,
        "hourly": hourly,
        "merchants": merchants,
        "states": states,
        "daily": daily,
        "freshness": freshness,
        "quarantine": quarantine,
        "stream": stream,
        "contracts": _contract_summary(cfg),
    }


def _table(
    headers: list[str], rows: list[list[str]], *, align_right: set[int] | None = None
) -> str:
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
    categories = data["categories"]
    total_txns = sum(c["txns"] for c in categories)
    total_fraud = sum(c["fraud_txns"] for c in categories)
    total_amt = sum(c["total_amt"] for c in categories)
    fraud_rate = total_fraud / total_txns if total_txns else 0
    quarantined = sum(q["rows"] for q in data["quarantine"])
    contracts = data["contracts"]

    # page 1: KPIs
    tiles = [
        ("Transactions", f"{total_txns:,}", "card_transactions_sparkov · 2019-01 to 2020-12"),
        ("Fraud rate", f"{100 * fraud_rate:.3f}%", f"{total_fraud:,} flagged of {total_txns:,}"),
        ("Total amount", f"${charts.compact(total_amt)}", "sum of amt across all transactions"),
        (
            "Categories covered",
            f"{data['coverage']['categories']}",
            f"across {data['coverage']['merchants']:,} merchants in the leaderboard",
        ),
        (
            "Quarantined",
            f"{quarantined:,}",
            f"{100 * quarantined / (total_txns + quarantined):.4f}% of ingested rows"
            if (total_txns + quarantined)
            else "0.00%",
        ),
    ]
    tile_html = "".join(
        f'<div class="tile"><div class="tile-label">{escape(label)}</div>'
        f'<div class="tile-value">{escape(value)}</div>'
        f'<div class="tile-note">{escape(note)}</div></div>'
        for label, value, note in tiles
    )

    category_bar = charts.hbar(
        [c["category"] for c in categories],
        [float(c["txns"]) for c in categories],
        width=CARD_WIDTH,
        label_width=130,
    )

    hours = sorted({h["trans_hour_ts"] for h in data["hourly"]})
    series = []
    for slot, category in enumerate(dict.fromkeys(h["category"] for h in data["hourly"])):
        by_hour = {
            h["trans_hour_ts"]: float(h["txns"])
            for h in data["hourly"]
            if h["category"] == category
        }
        series.append(charts.Series(category, [by_hour.get(hour, 0.0) for hour in hours], slot))
    hourly_chart = charts.multiline(
        [str(h)[5:13] for h in hours],
        series,
        width=CARD_WIDTH,
        height=300,
        y_label="transactions per hour, top 3 categories",
    )

    merchant_bar = charts.hbar(
        [m["merchant"].replace("fraud_", "") for m in data["merchants"]],
        [float(m["fraud_rate_pct"]) for m in data["merchants"]],
        value_format="{:.1f}",
        unit="%",
        width=CARD_WIDTH,
        label_width=190,
    )

    state_bar = charts.hbar(
        [s["state"] for s in data["states"]],
        [float(s["total_amt"]) for s in data["states"]],
        value_format="${:,.0f}",
        width=CARD_WIDTH,
        label_width=90,
    )

    category_table = _table(
        [
            "Category",
            "Txns",
            "Share",
            "Fraud txns",
            "Fraud rate",
            "Total amt",
            "Avg amt",
        ],
        [
            [
                escape(c["category"]),
                f"{c['txns']:,}",
                f"{c['txn_share_pct']:.1f}%",
                f"{c['fraud_txns']:,}",
                f"{100 * c['fraud_rate']:.3f}%",
                f"${c['total_amt']:,.0f}",
                f"${c['avg_amt']:.2f}",
            ]
            for c in categories
        ],
        align_right={1, 2, 3, 4, 5, 6},
    )

    merchant_table = _table(
        ["Merchant", "Category", "Txns", "Fraud txns", "Fraud rate"],
        [
            [
                escape(m["merchant"]),
                escape(m["category"]),
                f"{m['txns']:,}",
                f"{m['fraud_txns']:,}",
                f"{m['fraud_rate_pct']:.1f}%",
            ]
            for m in data["merchants"]
        ],
        align_right={2, 3, 4},
    )

    # page 2: pipeline health
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
                "good"
                if c["status"] == "PASSED"
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
            f"<li>{charts.status_dot('critical' if f['severity'] == 'error' else 'warning')}"
            f"<code>{escape(f['contract'])}</code> · <strong>{escape(f['check'])}</strong>, "
            f"observed {escape(str(f['observed']))}, expected {escape(str(f['expected']))}</li>"
            for f in failures
        )
        if failures
        else '<li class="muted">No breaches in the latest run.</li>'
    )

    quarantine_bar = (
        charts.hbar(
            [q["reject_reason"].replace("_", " ") for q in data["quarantine"]],
            [float(q["rows"]) for q in data["quarantine"]],
            width=CARD_WIDTH,
            label_width=190,
        )
        if data["quarantine"]
        else '<p class="empty">no quarantined rows recorded (Sparkov is a clean synthetic feed)</p>'
    )

    stream_table = (
        _table(
            ["Window start", "Category", "Txns", "Fraud txns", "Total amt"],
            [
                [
                    escape(str(s["window_start"])),
                    escape(s["category"]),
                    f"{s['txns']:,}",
                    f"{s['fraud_txns']:,}",
                    f"${s['total_amt']:,.2f}",
                ]
                for s in data["stream"]
            ],
            align_right={2, 3, 4},
        )
        if data["stream"]
        else STREAM_EMPTY
    )

    daily_series = [
        charts.Series("txns", [float(d["txns"]) for d in data["daily"]], 0),
    ]
    daily_chart = charts.multiline(
        [str(d["trans_date"])[:10] for d in data["daily"]],
        daily_series,
        width=FULL_WIDTH,
        height=300,
        y_label="transactions per day",
    )

    return render_page(
        generated_at=datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        contract_status=contracts.get("status", "UNKNOWN"),
        contract_counts=contracts,
        tiles=tile_html,
        category_bar=category_bar,
        category_table=category_table,
        hourly_chart=hourly_chart,
        daily_chart=daily_chart,
        merchant_bar=merchant_bar,
        merchant_table=merchant_table,
        state_bar=state_bar,
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
