"""Roll every contract report from a run into one summary.

One file, every contract, in pipeline order, with the failures first. The Airflow DAG runs it
with ``trigger_rule="all_done"`` so a failed run still produces one.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from streamlake.config import Config, get_config
from streamlake.logging_utils import get_logger

log = get_logger(__name__)

# Pipeline order, so the summary reads top to bottom like the DAG runs.
ORDER = [
    "bronze_trips",
    "bronze_zones",
    "silver_trips",
    "silver_dim_zone",
    "gold_daily_zone_kpis",
    "gold_hourly_demand",
    "stream_trip_metrics_1m",
]


def load_latest_reports(cfg: Config | None = None) -> list[dict[str, Any]]:
    cfg = cfg or get_config()
    directory = cfg.path("reports") / "contracts"
    if not directory.exists():
        return []

    reports = []
    for path in directory.glob("*__latest.json"):
        try:
            reports.append(json.loads(path.read_text()))
        except json.JSONDecodeError:
            log.warning("skipping unreadable report: %s", path)

    reports.sort(key=lambda r: ORDER.index(r["contract"]) if r["contract"] in ORDER else len(ORDER))
    return reports


def summarise(cfg: Config | None = None) -> dict[str, Any]:
    cfg = cfg or get_config()
    reports = load_latest_reports(cfg)

    failed_checks = [
        {"contract": r["contract"], **check}
        for r in reports
        for check in r["results"]
        if not check["passed"]
    ]

    summary = {
        "generated_at": datetime.now(UTC).isoformat(),
        "contracts": len(reports),
        "checks": sum(r["checks_total"] for r in reports),
        "checks_failed": len(failed_checks),
        "errors": len([c for c in failed_checks if c["severity"] == "error"]),
        "warnings": len([c for c in failed_checks if c["severity"] == "warn"]),
        "status": "FAILED"
        if any(c["severity"] == "error" for c in failed_checks)
        else ("PASSED_WITH_WARNINGS" if failed_checks else "PASSED"),
        "by_contract": [
            {
                "contract": r["contract"],
                "dataset": r["dataset"],
                "status": r["status"],
                "row_count": r["row_count"],
                "checks_total": r["checks_total"],
                "checks_failed": r["checks_failed"],
                "duration_seconds": r["duration_seconds"],
            }
            for r in reports
        ],
        "failures": failed_checks,
    }

    path = Path(cfg.path("reports") / "contract_summary.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, default=str))

    log.info(
        "contract summary: %s — %d contracts, %d checks, %d errors, %d warnings -> %s",
        summary["status"],
        summary["contracts"],
        summary["checks"],
        summary["errors"],
        summary["warnings"],
        path,
    )
    return summary


if __name__ == "__main__":  # pragma: no cover
    summarise()
