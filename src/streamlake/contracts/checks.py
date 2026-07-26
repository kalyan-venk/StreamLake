"""Check implementations for the contract engine.

Every check compiles itself into Spark **aggregate expressions** rather than running its own
query. The engine collects all of them into a single ``df.agg(...)`` call, so validating a table
with twenty assertions costs one pass over the data, not twenty. Only checks that actually
failed pay for a second, filtered pass to collect example rows.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from streamlake.contracts.spec import CheckSpec, parse_duration

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

TOTAL_ROWS = "total_rows"


def _slug(text: str) -> str:
    # Aggregate aliases must be plain identifiers, not the human-readable check label.
    return "".join(ch if ch.isalnum() else "_" for ch in str(text))


@dataclass
class ValidationContext:
    # Freshness is measured against the *logical* run time, not wall clock: a backfill of
    # January data is not stale just because it is July.
    as_of: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class CheckResult:
    check: str
    type: str
    severity: str
    passed: bool
    observed: Any
    expected: str
    description: str = ""
    detail: str = ""
    failing_rows: int | None = None
    failing_filter: str | None = None
    examples: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "check": self.check,
            "type": self.type,
            "severity": self.severity,
            "passed": self.passed,
            "observed": self.observed,
            "expected": self.expected,
            "description": self.description,
            "detail": self.detail,
            "failing_rows": self.failing_rows,
            "examples": self.examples,
        }


@dataclass
class CompiledCheck:
    spec: CheckSpec
    aggregates: dict[str, Any]
    evaluate: Callable[[dict[str, Any]], CheckResult]


_REGISTRY: dict[str, Callable[..., CompiledCheck]] = {}


def register(name: str) -> Callable[[Callable[..., CompiledCheck]], Callable[..., CompiledCheck]]:
    def wrapper(fn: Callable[..., CompiledCheck]) -> Callable[..., CompiledCheck]:
        _REGISTRY[name] = fn
        return fn

    return wrapper


def compile_check(
    spec: CheckSpec, df: DataFrame, ctx: ValidationContext, uid: int = 0
) -> CompiledCheck:
    if spec.type not in _REGISTRY:
        raise ValueError(f"unknown check type {spec.type!r}; known types: {sorted(_REGISTRY)}")
    return _REGISTRY[spec.type](spec, df, ctx, uid)


def _result(spec: CheckSpec, **kwargs: Any) -> CheckResult:
    return CheckResult(
        check=spec.label,
        type=spec.type,
        severity=spec.severity,
        description=spec.description,
        **kwargs,
    )


def _missing_columns(spec: CheckSpec, df: DataFrame, columns: list[str]) -> CheckResult | None:
    absent = [c for c in columns if c not in df.columns]
    if not absent:
        return None
    return _result(
        spec,
        passed=False,
        observed=f"missing columns {absent}",
        expected=f"columns {columns} present",
        detail="the check could not run because the column does not exist",
    )


# ---------------------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------------------


@register("schema")
def _schema(spec: CheckSpec, df: DataFrame, ctx: ValidationContext, uid: int = 0) -> CompiledCheck:
    declared = spec.require("columns")
    strict = bool(spec.param("strict", False))
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}

    def evaluate(_: dict[str, Any]) -> CheckResult:
        problems: list[str] = []
        for col in declared:
            name, expected_type = col["name"], col.get("type")
            if name not in actual:
                problems.append(f"missing column '{name}'")
            elif expected_type and actual[name] != expected_type:
                problems.append(f"'{name}' is {actual[name]}, expected {expected_type}")
        if strict:
            undeclared = sorted(set(actual) - {c["name"] for c in declared})
            problems += [f"undeclared column '{c}'" for c in undeclared]
        return _result(
            spec,
            passed=not problems,
            observed=("; ".join(problems) if problems else "schema matches"),
            expected=f"{len(declared)} declared columns" + (" (strict)" if strict else ""),
            detail="; ".join(problems),
        )

    return CompiledCheck(spec=spec, aggregates={}, evaluate=evaluate)


# ---------------------------------------------------------------------------------------
# row-level assertions
# ---------------------------------------------------------------------------------------


@register("not_null")
def _not_null(
    spec: CheckSpec, df: DataFrame, ctx: ValidationContext, uid: int = 0
) -> CompiledCheck:
    from pyspark.sql import functions as F

    columns = list(spec.require("columns"))
    guard = _missing_columns(spec, df, columns)
    if guard:
        return CompiledCheck(spec, {}, lambda _: guard)

    aggs = {f"c{uid}_nulls_{_slug(c)}": F.sum(F.col(c).isNull().cast("long")) for c in columns}
    predicate = " OR ".join(f"{c} IS NULL" for c in columns)

    def evaluate(row: dict[str, Any]) -> CheckResult:
        per_column = {c: int(row.get(f"c{uid}_nulls_{_slug(c)}") or 0) for c in columns}
        failing = sum(per_column.values())
        offenders = {k: v for k, v in per_column.items() if v}
        return _result(
            spec,
            passed=failing == 0,
            observed=f"{failing} null values" + (f" in {offenders}" if offenders else ""),
            expected="0 nulls",
            failing_rows=failing,
            failing_filter=predicate if failing else None,
        )

    return CompiledCheck(spec=spec, aggregates=aggs, evaluate=evaluate)


@register("unique")
def _unique(spec: CheckSpec, df: DataFrame, ctx: ValidationContext, uid: int = 0) -> CompiledCheck:
    from pyspark.sql import functions as F

    columns = list(spec.require("columns"))
    guard = _missing_columns(spec, df, columns)
    if guard:
        return CompiledCheck(spec, {}, lambda _: guard)

    alias = f"c{uid}_distinct"
    aggs = {alias: F.count_distinct(F.struct(*[F.col(c) for c in columns]))}

    def evaluate(row: dict[str, Any]) -> CheckResult:
        distinct = int(row.get(alias) or 0)
        total = int(row.get(TOTAL_ROWS) or 0)
        duplicates = total - distinct
        return _result(
            spec,
            passed=duplicates == 0,
            observed=f"{distinct} distinct of {total} rows ({duplicates} duplicates)",
            expected=f"{columns} unique",
            failing_rows=max(duplicates, 0),
        )

    return CompiledCheck(spec=spec, aggregates=aggs, evaluate=evaluate)


@register("row_count")
def _row_count(
    spec: CheckSpec, df: DataFrame, ctx: ValidationContext, uid: int = 0
) -> CompiledCheck:
    minimum = spec.param("min")
    maximum = spec.param("max")

    def evaluate(row: dict[str, Any]) -> CheckResult:
        total = int(row.get(TOTAL_ROWS) or 0)
        ok = (minimum is None or total >= minimum) and (maximum is None or total <= maximum)
        return _result(
            spec,
            passed=ok,
            observed=total,
            expected=f"between {minimum if minimum is not None else '-inf'} "
            f"and {maximum if maximum is not None else '+inf'}",
        )

    return CompiledCheck(spec=spec, aggregates={}, evaluate=evaluate)


@register("accepted_range")
def _accepted_range(
    spec: CheckSpec, df: DataFrame, ctx: ValidationContext, uid: int = 0
) -> CompiledCheck:
    from pyspark.sql import functions as F

    column = spec.require("column")
    guard = _missing_columns(spec, df, [column])
    if guard:
        return CompiledCheck(spec, {}, lambda _: guard)

    minimum, maximum = spec.param("min"), spec.param("max")
    # Nulls are the not_null check's business, so they are excluded here on purpose:
    # one failed assertion should point at one root cause.
    clauses = []
    if minimum is not None:
        clauses.append(f"{column} < {minimum}")
    if maximum is not None:
        clauses.append(f"{column} > {maximum}")
    predicate = f"{column} IS NOT NULL AND ({' OR '.join(clauses)})"

    alias = f"c{uid}_range"
    min_alias, max_alias = f"{alias}__min", f"{alias}__max"
    aggs = {
        alias: F.sum(F.expr(predicate).cast("long")),
        min_alias: F.min(F.col(column)),
        max_alias: F.max(F.col(column)),
    }

    def evaluate(row: dict[str, Any]) -> CheckResult:
        failing = int(row.get(alias) or 0)
        return _result(
            spec,
            passed=failing == 0,
            observed=f"{failing} rows outside range "
            f"(actual min={row.get(min_alias)}, max={row.get(max_alias)})",
            expected=f"{column} between {minimum} and {maximum}",
            failing_rows=failing,
            failing_filter=predicate if failing else None,
        )

    return CompiledCheck(spec=spec, aggregates=aggs, evaluate=evaluate)


@register("accepted_values")
def _accepted_values(
    spec: CheckSpec, df: DataFrame, ctx: ValidationContext, uid: int = 0
) -> CompiledCheck:
    from pyspark.sql import functions as F

    column = spec.require("column")
    guard = _missing_columns(spec, df, [column])
    if guard:
        return CompiledCheck(spec, {}, lambda _: guard)

    values = list(spec.require("values"))
    rendered = ", ".join(repr(v) if isinstance(v, str) else str(v) for v in values)
    predicate = f"{column} IS NOT NULL AND {column} NOT IN ({rendered})"
    alias = f"c{uid}_values"
    aggs = {alias: F.sum(F.expr(predicate).cast("long"))}

    def evaluate(row: dict[str, Any]) -> CheckResult:
        failing = int(row.get(alias) or 0)
        return _result(
            spec,
            passed=failing == 0,
            observed=f"{failing} rows outside the accepted set",
            expected=f"{column} in ({rendered})",
            failing_rows=failing,
            failing_filter=predicate if failing else None,
        )

    return CompiledCheck(spec=spec, aggregates=aggs, evaluate=evaluate)


@register("null_rate")
def _null_rate(
    spec: CheckSpec, df: DataFrame, ctx: ValidationContext, uid: int = 0
) -> CompiledCheck:
    from pyspark.sql import functions as F

    column = spec.require("column")
    guard = _missing_columns(spec, df, [column])
    if guard:
        return CompiledCheck(spec, {}, lambda _: guard)

    threshold = float(spec.require("max"))
    alias = f"c{uid}_nullrate"
    aggs = {alias: F.sum(F.col(column).isNull().cast("long"))}

    def evaluate(row: dict[str, Any]) -> CheckResult:
        nulls = int(row.get(alias) or 0)
        total = int(row.get(TOTAL_ROWS) or 0) or 1
        rate = nulls / total
        return _result(
            spec,
            passed=rate <= threshold,
            observed=f"{rate:.4f} ({nulls}/{total})",
            expected=f"null rate <= {threshold}",
            failing_rows=nulls,
            failing_filter=f"{column} IS NULL" if rate > threshold else None,
        )

    return CompiledCheck(spec=spec, aggregates=aggs, evaluate=evaluate)


@register("expression")
def _expression(
    spec: CheckSpec, df: DataFrame, ctx: ValidationContext, uid: int = 0
) -> CompiledCheck:
    from pyspark.sql import functions as F

    expr = str(spec.require("expr"))
    # A NULL result is treated as a violation: "unknown" is not "satisfied".
    predicate = f"NOT coalesce({expr}, false)"
    alias = f"c{uid}_expr"
    aggs = {alias: F.sum(F.expr(predicate).cast("long"))}

    def evaluate(row: dict[str, Any]) -> CheckResult:
        failing = int(row.get(alias) or 0)
        return _result(
            spec,
            passed=failing == 0,
            observed=f"{failing} rows violate the expression",
            expected=expr,
            failing_rows=failing,
            failing_filter=predicate if failing else None,
        )

    return CompiledCheck(spec=spec, aggregates=aggs, evaluate=evaluate)


@register("freshness")
def _freshness(
    spec: CheckSpec, df: DataFrame, ctx: ValidationContext, uid: int = 0
) -> CompiledCheck:
    from pyspark.sql import functions as F

    column = spec.require("column")
    guard = _missing_columns(spec, df, [column])
    if guard:
        return CompiledCheck(spec, {}, lambda _: guard)

    max_age = parse_duration(spec.require("max_age"))
    alias = f"c{uid}_fresh"
    aggs = {alias: F.max(F.col(column))}

    def evaluate(row: dict[str, Any]) -> CheckResult:
        latest = row.get(alias)
        if latest is None:
            return _result(
                spec,
                passed=False,
                observed="no rows / no timestamp",
                expected=f"max({column}) newer than {max_age:.0f}s before the run time",
            )
        if isinstance(latest, datetime):
            latest_dt = latest if latest.tzinfo else latest.replace(tzinfo=UTC)
        else:  # date
            latest_dt = datetime(latest.year, latest.month, latest.day, tzinfo=UTC)
        age = (ctx.as_of - latest_dt).total_seconds()
        return _result(
            spec,
            passed=age <= max_age,
            observed=f"latest={latest_dt.isoformat()} age={age / 3600:.1f}h",
            expected=f"age <= {max_age / 3600:.1f}h as of {ctx.as_of.isoformat()}",
            detail="freshness is measured against the logical run time, not wall clock",
        )

    return CompiledCheck(spec=spec, aggregates=aggs, evaluate=evaluate)
