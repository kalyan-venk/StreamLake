"""The contract engine: run a contract against a DataFrame and fail loudly on breach.

Every hop of the pipeline (bronze, silver, gold, each streaming micro-batch, and the curated
export) hands its output to ``validate`` before the next hop is allowed to read it. A breach
raises :class:`DataContractViolation`, which fails the task, which fails the Airflow DAG, bad
data stops moving instead of quietly arriving in a dashboard.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from streamlake.contracts.checks import (
    TOTAL_ROWS,
    CheckResult,
    ValidationContext,
    compile_check,
)
from streamlake.contracts.spec import Contract, load_contract, load_contracts
from streamlake.logging_utils import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

log = get_logger(__name__)

EXAMPLE_LIMIT = 5


class DataContractViolation(RuntimeError):
    """Raised when an error-severity check fails and the engine is in 'fail' mode."""

    def __init__(self, report: ContractReport):
        self.report = report
        failed = ", ".join(r.check for r in report.failed(severity="error"))
        super().__init__(
            f"data contract '{report.contract}' violated on {report.dataset}: {failed}"
        )


@dataclass
class ContractReport:
    contract: str
    dataset: str
    stage: str
    row_count: int
    results: list[CheckResult] = field(default_factory=list)
    started_at: str = ""
    duration_seconds: float = 0.0
    mode: str = "fail"

    def failed(self, severity: str | None = None) -> list[CheckResult]:
        return [
            r for r in self.results if not r.passed and (severity is None or r.severity == severity)
        ]

    @property
    def ok(self) -> bool:
        return not self.failed(severity="error")

    @property
    def status(self) -> str:
        if self.failed(severity="error"):
            return "FAILED"
        if self.failed(severity="warn"):
            return "PASSED_WITH_WARNINGS"
        return "PASSED"

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "dataset": self.dataset,
            "stage": self.stage,
            "status": self.status,
            "row_count": self.row_count,
            "started_at": self.started_at,
            "duration_seconds": round(self.duration_seconds, 3),
            "mode": self.mode,
            "checks_total": len(self.results),
            "checks_failed": len(self.failed()),
            "results": [r.to_dict() for r in self.results],
        }


def _log_report(report: ContractReport) -> None:
    log.info(
        "contract %s on %s -> %s (%d rows, %d/%d checks passed, %.2fs)",
        report.contract,
        report.dataset,
        report.status,
        report.row_count,
        len(report.results) - len(report.failed()),
        len(report.results),
        report.duration_seconds,
    )
    for result in report.results:
        mark = "PASS" if result.passed else ("WARN" if result.severity == "warn" else "FAIL")
        line = f"  [{mark}] {result.check}: observed={result.observed} expected={result.expected}"
        (log.info if result.passed else log.error)(line)
        for example in result.examples:
            log.error("         example: %s", example)


def write_report(report: ContractReport, reports_dir: Path) -> Path:
    target = reports_dir / "contracts"
    target.mkdir(parents=True, exist_ok=True)
    stamp = report.started_at.replace(":", "").replace("-", "").replace(".", "")[:15]
    path = target / f"{report.contract}__{stamp}.json"
    path.write_text(json.dumps(report.to_dict(), indent=2, default=str))
    # A stable filename makes the dashboard and the runbook trivial to point at.
    (target / f"{report.contract}__latest.json").write_text(
        json.dumps(report.to_dict(), indent=2, default=str)
    )
    return path


def validate(
    df: DataFrame,
    contract: Contract,
    *,
    stage: str = "",
    ctx: ValidationContext | None = None,
    mode: str = "fail",
    reports_dir: Path | None = None,
    collect_examples: bool = True,
) -> ContractReport:
    """Run every check in ``contract`` against ``df`` in a single aggregate pass."""
    from pyspark.sql import functions as F

    ctx = ctx or ValidationContext()
    started = datetime.now(UTC)

    compiled = [compile_check(spec, df, ctx, uid) for uid, spec in enumerate(contract.all_checks)]

    # One pass: count(*) plus every check's aggregate expressions.
    aggregates = {TOTAL_ROWS: F.count(F.lit(1))}
    for check in compiled:
        aggregates.update(check.aggregates)
    row = df.agg(*[expr.alias(name) for name, expr in aggregates.items()]).collect()[0].asDict()

    results = [check.evaluate(row) for check in compiled]

    # Second pass only for what actually broke, and only for the offending rows.
    if collect_examples:
        for result in results:
            if result.passed or not result.failing_filter:
                continue
            try:
                sample = df.where(result.failing_filter).limit(EXAMPLE_LIMIT).collect()
                result.examples = [r.asDict(recursive=True) for r in sample]
            except Exception as exc:  # pragma: no cover - example collection is best-effort
                log.warning("could not collect examples for %s: %s", result.check, exc)

    report = ContractReport(
        contract=contract.name,
        dataset=contract.dataset,
        stage=stage or contract.layer,
        row_count=int(row.get(TOTAL_ROWS) or 0),
        results=results,
        started_at=started.isoformat(),
        duration_seconds=(datetime.now(UTC) - started).total_seconds(),
        mode=mode,
    )

    _log_report(report)
    if reports_dir is not None:
        write_report(report, reports_dir)

    if mode == "fail" and not report.ok:
        raise DataContractViolation(report)
    return report


def enforce(
    df: DataFrame,
    contract_name: str,
    *,
    cfg: Any = None,
    stage: str = "",
    as_of: datetime | None = None,
    mode: str | None = None,
) -> ContractReport:
    from streamlake.config import get_config

    cfg = cfg or get_config()
    contracts_dir = Path(cfg.require("contracts.dir"))
    if not contracts_dir.is_absolute():
        contracts_dir = cfg.root / contracts_dir
    contract = load_contract(contracts_dir / f"{contract_name}.yml")
    return validate(
        df,
        contract,
        stage=stage,
        ctx=ValidationContext(as_of=as_of or datetime.now(UTC)),
        mode=mode or str(cfg.get("contracts.on_violation", "fail")),
        reports_dir=cfg.path("reports"),
    )


__all__ = [
    "ContractReport",
    "DataContractViolation",
    "ValidationContext",
    "enforce",
    "load_contract",
    "load_contracts",
    "validate",
    "write_report",
]
