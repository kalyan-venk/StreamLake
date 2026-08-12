# Databricks notebook source
from __future__ import annotations

# MAGIC %md
# MAGIC # StreamLake batch spine on Databricks Free Edition
# MAGIC
# MAGIC A port of `src/streamlake/batch/{bronze,silver,gold}.py` plus the YAML data-contract engine
# MAGIC (`src/streamlake/contracts/{spec,checks,engine}.py`) onto a Databricks cluster. The
# MAGIC **transformation logic below is copied, not rewritten**: column renames, PII masking, the
# MAGIC validity rules, the five gold aggregations, and every contract check are the same code that
# MAGIC runs locally. Three things had to change, because they are properties of the local
# MAGIC environment, not of the transformations themselves:
# MAGIC
# MAGIC 1. **Spark session.** Locally `build_spark()` constructs a `SparkSession` wired to a local
# MAGIC    Iceberg-on-filesystem (or MinIO) catalog. On Databricks a `SparkSession` already exists
# MAGIC    as the notebook-global `spark`; this notebook uses that instead of building its own.
# MAGIC 2. **Table format.** Locally, tables are Apache Iceberg on a `hadoop`-type catalog backed by
# MAGIC    the local filesystem or MinIO. Databricks Free Edition has no Iceberg REST catalog
# MAGIC    and no S3-compatible object store configured, but it does have Delta Lake natively, zero
# MAGIC    setup. Every table below is written as **Delta** (the brief's bonus item) rather than
# MAGIC    Iceberg. The storage format changes; the schema and the row-level logic that produces
# MAGIC    each row do not.
# MAGIC 3. **Source location.** Locally the pipeline downloads Sparkov's CSVs to `data/raw/`. There
# MAGIC    is no headless download step here: the CSVs are uploaded to DBFS by hand first, and the
# MAGIC    widgets below point at wherever they land.
# MAGIC
# MAGIC One more deliberate difference, disclosed rather than hidden: locally, bronze and silver use
# MAGIC Iceberg's **dynamic partition overwrite** so a re-run replaces only the touched partitions.
# MAGIC Delta managed tables here use a plain `mode("overwrite")` on the whole table instead, which
# MAGIC is the ordinary idempotent-rerun pattern for a single-shot Free Edition notebook (no
# MAGIC incremental/streaming backfill story is being claimed here). That is a storage-idempotency
# MAGIC detail, not a change to what any row's value is.
# MAGIC
# MAGIC **Status of this file: written and reviewed, not yet executed on a Databricks cluster.**
# MAGIC The logic was smoke-tested locally against a small Sparkov sample with real Spark; the real
# MAGIC full-scale counts get filled into the root `README.md` after a cluster run.

# COMMAND ----------

# MAGIC %md ## 0. Widgets : set these to wherever the Sparkov files landed on DBFS

# COMMAND ----------

dbutils.widgets.text(
    "train_csv_path", "/FileStore/tables/streamlake/credit_card_transaction_train.csv"
)
dbutils.widgets.text(
    "test_csv_path", "/FileStore/tables/streamlake/credit_card_transaction_test.csv"
)
dbutils.widgets.text("category_ref_csv_path", "/FileStore/tables/streamlake/category_channel.csv")
dbutils.widgets.text("db_prefix", "streamlake")  # -> streamlake_bronze / _silver / _gold databases

TRAIN_CSV = dbutils.widgets.get("train_csv_path")
TEST_CSV = dbutils.widgets.get("test_csv_path")
CATEGORY_REF_CSV = dbutils.widgets.get("category_ref_csv_path")
DB_PREFIX = dbutils.widgets.get("db_prefix")

BRONZE_DB = f"{DB_PREFIX}_bronze"
SILVER_DB = f"{DB_PREFIX}_silver"
GOLD_DB = f"{DB_PREFIX}_gold"

for db in (BRONZE_DB, SILVER_DB, GOLD_DB):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

print(f"train:        {TRAIN_CSV}")
print(f"test:         {TEST_CSV}")
print(f"category ref: {CATEGORY_REF_CSV}")
print(f"databases:    {BRONZE_DB}, {SILVER_DB}, {GOLD_DB}")

# COMMAND ----------

# MAGIC %md ## 1. The contract engine
# MAGIC
# MAGIC Ported near-verbatim from `src/streamlake/contracts/spec.py`, `checks.py`, `engine.py`. The
# MAGIC only real change: `enforce()` locally loads a contract's YAML off the local filesystem
# MAGIC (`conf/contracts/<name>.yml`); here the same YAML text is embedded as Python strings in the
# MAGIC next cell and parsed with the identical `yaml.safe_load`, so the checks and thresholds a
# MAGIC contract enforces are functionally identical to the committed `.yml` files (same checks,
# MAGIC thresholds, and types), just loaded from a string instead of a path. The embedded strings
# MAGIC drop every `description` and `owner` field the `.yml` files carry, human-readable metadata
# MAGIC that does not change what a contract enforces, so the text below is not byte-for-byte
# MAGIC identical to the committed files. Every check type (`schema`, `not_null`, `unique`,
# MAGIC `row_count`, `accepted_range`, `accepted_values`, `null_rate`, `expression`, `freshness`)
# MAGIC compiles to a Spark aggregate expression and all of them run in one `df.agg(...)` pass,
# MAGIC exactly as locally.

# COMMAND ----------

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import yaml
from pyspark.sql import functions as F

Severity = str  # "error" | "warn"

_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(second|minute|hour|day|week)s?\s*$", re.I)
_UNIT_SECONDS = {"second": 1, "minute": 60, "hour": 3600, "day": 86400, "week": 604800}


def parse_duration(text: str | int | float) -> float:
    if isinstance(text, (int, float)):
        return float(text)
    match = _DURATION.match(str(text))
    if not match:
        raise ValueError(f"cannot parse duration: {text!r}")
    return float(match.group(1)) * _UNIT_SECONDS[match.group(2).lower()]


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type: str | None = None
    nullable: bool = True
    description: str = ""


@dataclass(frozen=True)
class SchemaSpec:
    columns: tuple[ColumnSpec, ...] = ()
    strict: bool = False


@dataclass(frozen=True)
class CheckSpec:
    type: str
    severity: Severity = "error"
    description: str = ""
    params: dict[str, Any] = field(default_factory=dict)

    def param(self, key: str, default: Any = None) -> Any:
        return self.params.get(key, default)

    def require(self, key: str) -> Any:
        if key not in self.params:
            raise KeyError(f"check '{self.type}' requires parameter '{key}'")
        return self.params[key]

    @property
    def label(self) -> str:
        bits = [self.type]
        for key in ("column", "columns", "expr"):
            if key not in self.params:
                continue
            value = self.params[key]
            if isinstance(value, list):
                names = [v["name"] if isinstance(v, dict) else str(v) for v in value]
                bits.append(",".join(names) if len(names) <= 4 else f"{len(names)} columns")
            else:
                bits.append(str(value))
            break
        return ":".join(bits)


@dataclass(frozen=True)
class Contract:
    name: str
    dataset: str
    description: str = ""
    owner: str = ""
    layer: str = ""
    schema: SchemaSpec = field(default_factory=SchemaSpec)
    checks: tuple[CheckSpec, ...] = ()

    @property
    def all_checks(self) -> tuple[CheckSpec, ...]:
        derived: list[CheckSpec] = []
        if self.schema.columns:
            derived.append(
                CheckSpec(
                    type="schema",
                    severity="error",
                    description="declared columns exist with the declared types",
                    params={
                        "columns": [
                            {"name": c.name, "type": c.type, "nullable": c.nullable}
                            for c in self.schema.columns
                        ],
                        "strict": self.schema.strict,
                    },
                )
            )
            not_null = [c.name for c in self.schema.columns if not c.nullable]
            if not_null:
                derived.append(
                    CheckSpec(
                        type="not_null",
                        severity="error",
                        description="columns declared NOT NULL in the schema block",
                        params={"columns": not_null},
                    )
                )
        return tuple(derived) + self.checks


def _parse_check(raw: dict[str, Any]) -> CheckSpec:
    payload = dict(raw)
    check_type = payload.pop("type")
    severity = str(payload.pop("severity", "error")).lower()
    description = str(payload.pop("description", ""))
    return CheckSpec(type=check_type, severity=severity, description=description, params=payload)


def load_contract_from_text(name: str, text: str) -> Contract:
    raw = yaml.safe_load(text) or {}
    schema_raw = raw.get("schema") or {}
    columns = tuple(
        ColumnSpec(
            name=c["name"],
            type=c.get("type"),
            nullable=bool(c.get("nullable", True)),
            description=c.get("description", ""),
        )
        for c in (schema_raw.get("columns") or [])
    )
    return Contract(
        name=raw.get("name") or name,
        dataset=raw["dataset"],
        description=raw.get("description", ""),
        owner=raw.get("owner", ""),
        layer=raw.get("layer", ""),
        schema=SchemaSpec(columns=columns, strict=bool(schema_raw.get("strict", False))),
        checks=tuple(_parse_check(c) for c in (raw.get("checks") or [])),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check implementations
# MAGIC Copied from `src/streamlake/contracts/checks.py` verbatim (pure PySpark, no filesystem
# MAGIC dependency, nothing here needed to change for Databricks).

# COMMAND ----------

TOTAL_ROWS = "total_rows"


def _slug(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in str(text))


@dataclass
class ValidationContext:
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


@dataclass
class CompiledCheck:
    spec: CheckSpec
    aggregates: dict[str, Any]
    evaluate: Callable[[dict[str, Any]], CheckResult]


_REGISTRY: dict[str, Callable[..., CompiledCheck]] = {}


def register(name: str):
    def wrapper(fn):
        _REGISTRY[name] = fn
        return fn
    return wrapper


def compile_check(spec: CheckSpec, df, ctx: ValidationContext, uid: int = 0) -> CompiledCheck:
    if spec.type not in _REGISTRY:
        raise ValueError(f"unknown check type {spec.type!r}; known types: {sorted(_REGISTRY)}")
    return _REGISTRY[spec.type](spec, df, ctx, uid)


def _result(spec: CheckSpec, **kwargs: Any) -> CheckResult:
    return CheckResult(check=spec.label, type=spec.type, severity=spec.severity,
                        description=spec.description, **kwargs)


def _missing_columns(spec, df, columns):
    absent = [c for c in columns if c not in df.columns]
    if not absent:
        return None
    return _result(spec, passed=False, observed=f"missing columns {absent}",
                    expected=f"columns {columns} present",
                    detail="the check could not run because the column does not exist")


@register("schema")
def _schema(spec, df, ctx, uid=0):
    declared = spec.require("columns")
    strict = bool(spec.param("strict", False))
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}

    def evaluate(_):
        problems = []
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


@register("not_null")
def _not_null(spec, df, ctx, uid=0):
    columns = list(spec.require("columns"))
    guard = _missing_columns(spec, df, columns)
    if guard:
        return CompiledCheck(spec, {}, lambda _: guard)
    aggs = {f"c{uid}_nulls_{_slug(c)}": F.sum(F.col(c).isNull().cast("long")) for c in columns}
    predicate = " OR ".join(f"{c} IS NULL" for c in columns)

    def evaluate(row):
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
def _unique(spec, df, ctx, uid=0):
    columns = list(spec.require("columns"))
    guard = _missing_columns(spec, df, columns)
    if guard:
        return CompiledCheck(spec, {}, lambda _: guard)
    alias = f"c{uid}_distinct"
    aggs = {alias: F.count_distinct(F.struct(*[F.col(c) for c in columns]))}

    def evaluate(row):
        distinct = int(row.get(alias) or 0)
        total = int(row.get(TOTAL_ROWS) or 0)
        duplicates = total - distinct
        return _result(spec, passed=duplicates == 0,
                        observed=f"{distinct} distinct of {total} rows ({duplicates} duplicates)",
                        expected=f"{columns} unique", failing_rows=max(duplicates, 0))
    return CompiledCheck(spec=spec, aggregates=aggs, evaluate=evaluate)


@register("row_count")
def _row_count(spec, df, ctx, uid=0):
    minimum = spec.param("min")
    maximum = spec.param("max")

    def evaluate(row):
        total = int(row.get(TOTAL_ROWS) or 0)
        ok = (minimum is None or total >= minimum) and (maximum is None or total <= maximum)
        return _result(spec, passed=ok, observed=total,
                        expected=f"between {minimum if minimum is not None else '-inf'} "
                                 f"and {maximum if maximum is not None else '+inf'}")
    return CompiledCheck(spec=spec, aggregates={}, evaluate=evaluate)


@register("accepted_range")
def _accepted_range(spec, df, ctx, uid=0):
    column = spec.require("column")
    guard = _missing_columns(spec, df, [column])
    if guard:
        return CompiledCheck(spec, {}, lambda _: guard)
    minimum, maximum = spec.param("min"), spec.param("max")
    clauses = []
    if minimum is not None:
        clauses.append(f"{column} < {minimum}")
    if maximum is not None:
        clauses.append(f"{column} > {maximum}")
    predicate = f"{column} IS NOT NULL AND ({' OR '.join(clauses)})"
    alias = f"c{uid}_range"
    min_alias, max_alias = f"{alias}__min", f"{alias}__max"
    aggs = {alias: F.sum(F.expr(predicate).cast("long")), min_alias: F.min(F.col(column)),
            max_alias: F.max(F.col(column))}

    def evaluate(row):
        failing = int(row.get(alias) or 0)
        return _result(spec, passed=failing == 0,
                        observed=f"{failing} rows outside range "
                                 f"(actual min={row.get(min_alias)}, max={row.get(max_alias)})",
                        expected=f"{column} between {minimum} and {maximum}",
                        failing_rows=failing, failing_filter=predicate if failing else None)
    return CompiledCheck(spec=spec, aggregates=aggs, evaluate=evaluate)


@register("accepted_values")
def _accepted_values(spec, df, ctx, uid=0):
    column = spec.require("column")
    guard = _missing_columns(spec, df, [column])
    if guard:
        return CompiledCheck(spec, {}, lambda _: guard)
    values = list(spec.require("values"))
    rendered = ", ".join(repr(v) if isinstance(v, str) else str(v) for v in values)
    predicate = f"{column} IS NOT NULL AND {column} NOT IN ({rendered})"
    alias = f"c{uid}_values"
    aggs = {alias: F.sum(F.expr(predicate).cast("long"))}

    def evaluate(row):
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
def _null_rate(spec, df, ctx, uid=0):
    column = spec.require("column")
    guard = _missing_columns(spec, df, [column])
    if guard:
        return CompiledCheck(spec, {}, lambda _: guard)
    threshold = float(spec.require("max"))
    alias = f"c{uid}_nullrate"
    aggs = {alias: F.sum(F.col(column).isNull().cast("long"))}

    def evaluate(row):
        nulls = int(row.get(alias) or 0)
        total = int(row.get(TOTAL_ROWS) or 0) or 1
        rate = nulls / total
        return _result(spec, passed=rate <= threshold, observed=f"{rate:.4f} ({nulls}/{total})",
                        expected=f"null rate <= {threshold}", failing_rows=nulls,
                        failing_filter=f"{column} IS NULL" if rate > threshold else None)
    return CompiledCheck(spec=spec, aggregates=aggs, evaluate=evaluate)


@register("expression")
def _expression(spec, df, ctx, uid=0):
    expr = str(spec.require("expr"))
    predicate = f"NOT coalesce({expr}, false)"
    alias = f"c{uid}_expr"
    aggs = {alias: F.sum(F.expr(predicate).cast("long"))}

    def evaluate(row):
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
def _freshness(spec, df, ctx, uid=0):
    column = spec.require("column")
    guard = _missing_columns(spec, df, [column])
    if guard:
        return CompiledCheck(spec, {}, lambda _: guard)
    max_age = parse_duration(spec.require("max_age"))
    alias = f"c{uid}_fresh"
    aggs = {alias: F.max(F.col(column))}

    def evaluate(row):
        latest = row.get(alias)
        if latest is None:
            return _result(spec, passed=False, observed="no rows / no timestamp",
                            expected=f"max({column}) newer than {max_age:.0f}s before the run time")
        if isinstance(latest, datetime):
            latest_dt = latest if latest.tzinfo else latest.replace(tzinfo=UTC)
        elif isinstance(latest, str):
            latest_dt = datetime.strptime(latest[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        else:
            latest_dt = datetime(latest.year, latest.month, latest.day, tzinfo=UTC)
        age = (ctx.as_of - latest_dt).total_seconds()
        return _result(spec, passed=age <= max_age,
                        observed=f"latest={latest_dt.isoformat()} age={age / 3600:.1f}h",
                        expected=f"age <= {max_age / 3600:.1f}h as of {ctx.as_of.isoformat()}",
                        detail="freshness is measured against the logical run time, not wall clock")
    return CompiledCheck(spec=spec, aggregates=aggs, evaluate=evaluate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `validate()` : one aggregate pass per hop
# MAGIC Ported from `src/streamlake/contracts/engine.py`. Same one-`df.agg(...)`-call design as
# MAGIC locally: every check compiles to an aggregate expression, they are all evaluated together,
# MAGIC and only a check that actually failed pays for a second, filtered pass to print example
# MAGIC rows.

# COMMAND ----------

class DataContractViolation(RuntimeError):
    def __init__(self, contract_name: str, dataset: str, failed_checks: list[str]):
        super().__init__(
            f"data contract '{contract_name}' violated on {dataset}: {', '.join(failed_checks)}"
        )


def validate(df, contract: Contract, *, stage: str = "", as_of: datetime | None = None,
             mode: str = "fail") -> list[CheckResult]:
    ctx = ValidationContext(as_of=as_of or datetime.now(UTC))
    compiled = [compile_check(spec, df, ctx, uid) for uid, spec in enumerate(contract.all_checks)]

    aggregates = {TOTAL_ROWS: F.count(F.lit(1))}
    for check in compiled:
        aggregates.update(check.aggregates)
    row = df.agg(*[expr.alias(name) for name, expr in aggregates.items()]).collect()[0].asDict()

    results = [check.evaluate(row) for check in compiled]

    total = int(row.get(TOTAL_ROWS) or 0)
    failed_error = [r for r in results if not r.passed and r.severity == "error"]
    failed_warn = [r for r in results if not r.passed and r.severity == "warn"]
    status = "FAILED" if failed_error else ("PASSED_WITH_WARNINGS" if failed_warn else "PASSED")

    print(
        f"\ncontract {contract.name} on {contract.dataset} -> {status} "
        f"({total} rows, {len(results) - len(failed_error) - len(failed_warn)}/"
        f"{len(results)} checks clean)"
    )
    for r in results:
        mark = "PASS" if r.passed else ("WARN" if r.severity == "warn" else "FAIL")
        print(f"  [{mark}] {r.check}: observed={r.observed} expected={r.expected}")

    if mode == "fail" and failed_error:
        raise DataContractViolation(
            contract.name, contract.dataset, [r.check for r in failed_error]
        )
    return results

# COMMAND ----------

# MAGIC %md ## 2. Contract specs : the same YAML as `conf/contracts/*.yml`, embedded

# COMMAND ----------

CONTRACTS = {
    "bronze_transactions": """
name: bronze_transactions
dataset: streamlake_bronze.transactions_raw
layer: bronze
schema:
  strict: false
  columns:
    - name: trans_num
      type: string
      nullable: false
    - name: trans_date_trans_time
      type: timestamp
    - name: cc_num
      type: bigint
    - name: merchant
      type: string
    - name: category
      type: string
    - name: amt
      type: double
    - name: is_fraud
      type: int
    - name: unix_time
      type: int
    - name: source_file
      type: string
      nullable: false
    - name: source_split
      type: string
      nullable: false
    - name: batch_id
      type: string
      nullable: false
    - name: ingested_at
      type: timestamp
      nullable: false
checks:
  - type: row_count
    min: 1500000
    max: 2500000
  - type: unique
    columns: [trans_num]
    severity: warn
  - type: freshness
    column: trans_date_trans_time
    max_age: 3 days
""",
    "bronze_category_ref": """
name: bronze_category_ref
dataset: streamlake_bronze.category_ref_raw
layer: bronze
schema:
  strict: true
  columns:
    - name: category
      type: string
      nullable: false
    - name: channel
      type: string
      nullable: false
checks:
  - type: row_count
    min: 14
    max: 14
  - type: unique
    columns: [category]
  - type: accepted_values
    column: channel
    values: ["online", "in_person", "general"]
""",
    "silver_transactions": """
name: silver_transactions
dataset: streamlake_silver.transactions
layer: silver
schema:
  strict: true
  columns:
    - name: trans_num
      type: string
      nullable: false
    - name: trans_time
      type: timestamp
      nullable: false
    - name: trans_date
      type: date
      nullable: false
    - name: trans_hour
      type: int
      nullable: false
    - name: unix_time
      type: int
    - name: cc_num_last4
      type: string
      nullable: false
    - name: cc_num_hash
      type: string
      nullable: false
    - name: merchant
      type: string
      nullable: false
    - name: category
      type: string
      nullable: false
    - name: channel
      type: string
      nullable: false
    - name: amt
      type: double
      nullable: false
    - name: gender
      type: string
    - name: city
      type: string
    - name: state
      type: string
      nullable: false
    - name: zip
      type: int
    - name: city_pop
      type: int
    - name: job
      type: string
    - name: cardholder_age
      type: int
    - name: merch_lat
      type: double
      nullable: false
    - name: merch_long
      type: double
      nullable: false
    - name: distance_km
      type: double
    - name: is_fraud
      type: int
      nullable: false
    - name: merch_zipcode
      type: int
    - name: source_file
      type: string
      nullable: false
    - name: source_split
      type: string
      nullable: false
    - name: batch_id
      type: string
      nullable: false
    - name: ingested_at
      type: timestamp
      nullable: false
checks:
  - type: unique
    columns: [trans_num]
  - type: row_count
    min: 1500000
    max: 2000000
  - type: not_null
    columns: [cc_num_last4, cc_num_hash]
  - type: accepted_range
    column: amt
    min: 0
    max: 30000
  - type: accepted_values
    column: category
    values: [entertainment, food_dining, gas_transport, grocery_net, grocery_pos,
             health_fitness, home, kids_pets, misc_net, misc_pos, personal_care,
             shopping_net, shopping_pos, travel]
  - type: accepted_values
    column: is_fraud
    values: [0, 1]
  - type: accepted_range
    column: merch_lat
    min: -90
    max: 90
  - type: accepted_range
    column: merch_long
    min: -180
    max: 180
  - type: accepted_range
    column: cardholder_age
    min: 0
    max: 100
    severity: warn
  - type: accepted_range
    column: distance_km
    min: 0
    max: 20000
    severity: warn
  - type: null_rate
    column: merch_zipcode
    max: 0.30
  - type: freshness
    column: trans_time
    max_age: 3 days
""",
    "silver_dim_category": """
name: silver_dim_category
dataset: streamlake_silver.dim_category
layer: silver
schema:
  strict: true
  columns:
    - name: category
      type: string
      nullable: false
    - name: channel
      type: string
      nullable: false
checks:
  - type: unique
    columns: [category]
  - type: row_count
    min: 14
    max: 14
""",
    "gold_category_hourly_fraud": """
name: gold_category_hourly_fraud
dataset: streamlake_gold.category_hourly_fraud
layer: gold
schema:
  strict: true
  columns:
    - name: trans_hour_ts
      type: timestamp
      nullable: false
    - name: category
      type: string
      nullable: false
    - name: txns
      type: bigint
      nullable: false
    - name: fraud_txns
      type: bigint
      nullable: false
    - name: total_amt
      type: double
      nullable: false
    - name: avg_amt
      type: double
    - name: fraud_rate
      type: double
      nullable: false
checks:
  - type: unique
    columns: [trans_hour_ts, category]
  - type: row_count
    min: 1000
    max: 500000
  - type: expression
    expr: txns > 0
  - type: expression
    expr: fraud_txns <= txns
  - type: accepted_range
    column: fraud_rate
    min: 0
    max: 1
  - type: expression
    expr: total_amt >= 0
  - type: freshness
    column: trans_hour_ts
    max_age: 3 days
""",
    "gold_state_hourly_volume": """
name: gold_state_hourly_volume
dataset: streamlake_gold.state_hourly_volume
layer: gold
schema:
  strict: true
  columns:
    - name: trans_hour_ts
      type: timestamp
      nullable: false
    - name: state
      type: string
      nullable: false
    - name: txns
      type: bigint
      nullable: false
    - name: total_amt
      type: double
      nullable: false
    - name: avg_amt
      type: double
checks:
  - type: unique
    columns: [trans_hour_ts, state]
  - type: row_count
    min: 1000
    max: 1000000
  - type: expression
    expr: txns > 0
""",
}

# Period the combined train+test files actually cover, matching conf/streamlake.yml exactly
# (verified against the downloaded data and the root README, not re-derived here).
PERIOD_END = datetime(2021, 1, 1, tzinfo=UTC)


def enforce(df, contract_name: str, *, stage: str = "", as_of: datetime | None = None):
    contract = load_contract_from_text(contract_name, CONTRACTS[contract_name])
    return validate(df, contract, stage=stage, as_of=as_of)

# COMMAND ----------

# MAGIC %md ## 3. Transform logic : ported from `src/streamlake/transforms.py`, unchanged

# COMMAND ----------

RAW_TO_SILVER = {
    "trans_date_trans_time": "trans_time", "cc_num": "cc_num", "merchant": "merchant",
    "category": "category", "amt": "amt", "gender": "gender", "city": "city", "state": "state",
    "zip": "zip", "city_pop": "city_pop", "job": "job", "dob": "dob", "trans_num": "trans_num",
    "unix_time": "unix_time", "merch_lat": "merch_lat", "merch_long": "merch_long",
    "is_fraud": "is_fraud", "merch_zipcode": "merch_zipcode",
}

CATEGORIES = ("entertainment", "food_dining", "gas_transport", "grocery_net", "grocery_pos",
              "health_fitness", "home", "kids_pets", "misc_net", "misc_pos", "personal_care",
              "shopping_net", "shopping_pos", "travel")

TRANS_NUM_PATTERN = r"^[0-9a-f]{32}$"
CC_HASH_SALT_ENV = "STREAMLAKE_PII_SALT"
_DEFAULT_SALT = "streamlake-local-dev-salt-not-for-production"
EARTH_RADIUS_KM = 6371.0088


def _hash_salt() -> str:
    # Same env-var-with-fallback pattern as src/streamlake/transforms.py._hash_salt(). Set
    # STREAMLAKE_PII_SALT as a cluster environment variable (Compute -> cluster -> Advanced
    # Options -> Spark -> Environment Variables) to use a non-default salt on Databricks; if
    # unset, this falls back to the same local-dev default the pipeline uses everywhere else.
    import os

    return os.environ.get(CC_HASH_SALT_ENV, _DEFAULT_SALT)

SILVER_COLUMNS = [
    "trans_num", "trans_time", "trans_date", "trans_hour", "unix_time", "cc_num_last4",
    "cc_num_hash", "merchant", "category", "channel", "amt", "gender", "city", "state", "zip",
    "city_pop", "job", "cardholder_age", "merch_lat", "merch_long", "distance_km", "is_fraud",
    "merch_zipcode", "source_file", "source_split", "batch_id", "ingested_at",
]


def add_ingestion_metadata(df, *, source: str, split: str, batch_id: str):
    return (df.withColumn("source_file", F.lit(source))
              .withColumn("source_split", F.lit(split))
              .withColumn("batch_id", F.lit(batch_id))
              .withColumn("ingested_at", F.current_timestamp()))


def rename_to_silver(df):
    for raw, silver in RAW_TO_SILVER.items():
        if raw in df.columns and raw != silver:
            df = df.withColumnRenamed(raw, silver)
    return df


def normalize_timestamps(df, column="trans_time"):
    if column in df.columns:
        df = df.withColumn(column, F.col(column).cast("timestamp"))
    return df


def mask_card_number(df, column="cc_num"):
    as_string = F.col(column).cast("string")
    salt = _hash_salt()
    return (df.withColumn(f"{column}_last4", F.substring(as_string, -4, 4))
              .withColumn(f"{column}_hash", F.sha2(F.concat(F.lit(salt), as_string), 256))
              .drop(column))


def derive_age(df, dob_column="dob", as_of=None):
    ref = F.col("trans_time") if as_of is None else F.lit(as_of).cast("timestamp")
    age = F.floor(F.months_between(ref, F.to_date(F.col(dob_column))) / F.lit(12.0))
    return df.withColumn("cardholder_age", age.cast("int")).drop(dob_column)


def haversine_km(lat1, lon1, lat2, lon2):
    lat1_r, lon1_r = F.radians(F.col(lat1)), F.radians(F.col(lon1))
    lat2_r, lon2_r = F.radians(F.col(lat2)), F.radians(F.col(lon2))
    dlat, dlon = lat2_r - lat1_r, lon2_r - lon1_r
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1_r) * F.cos(lat2_r) * F.pow(F.sin(dlon / 2), 2)
    c = F.lit(2.0) * F.asin(F.sqrt(a))
    return F.round(F.lit(EARTH_RADIUS_KM) * c, 3)


def strip_home_coordinates(df):
    return df.drop("lat", "long")


def reject_reason():
    bad_id = F.col("trans_num").isNull() | (~F.col("trans_num").rlike(TRANS_NUM_PATTERN))
    return (
        F.when(bad_id, "invalid_trans_num")
        .when(F.col("trans_time").isNull(), "missing_timestamp")
        .when(F.col("amt").isNull() | (F.col("amt") <= 0), "non_positive_amount")
        .when(F.col("amt") > 30000, "implausible_amount")
        .when(~F.col("category").isin(*CATEGORIES), "invalid_category")
        .when(F.col("is_fraud").isNull() | (~F.col("is_fraud").isin(0, 1)), "invalid_is_fraud_flag")
        .when(
            F.col("merch_lat").isNull()
            | (F.col("merch_lat") < -90)
            | (F.col("merch_lat") > 90)
            | F.col("merch_long").isNull()
            | (F.col("merch_long") < -180)
            | (F.col("merch_long") > 180),
            "invalid_merchant_coordinates",
        )
        .otherwise(None)
    )


def derive_transaction_fields(df):
    return (
        df.withColumn("trans_date", F.to_date("trans_time"))
        .withColumn("trans_hour", F.hour("trans_time"))
    )


def enrich_with_category(df, category_ref):
    ref = category_ref.select(F.col("category").alias("_cat"), F.col("channel"))
    return df.join(F.broadcast(ref), df.category == ref._cat, "left").drop("_cat")

# COMMAND ----------

# MAGIC %md ## 4. Bronze : land the raw files as Delta, unchanged except for lineage columns

# COMMAND ----------

from datetime import datetime as _dt

BATCH_ID = _dt.now(UTC).strftime("%Y%m%dT%H%M%SZ")
print(f"batch_id={BATCH_ID}")


def _read_split(path, split):
    raw = (spark.read.option("header", True).option("inferSchema", True).csv(path)
           .drop("_c0", "Unnamed: 0"))
    print(f"read {len(raw.columns)} columns from {path} ({split} split)")
    return add_ingestion_metadata(raw, source=path.split("/")[-1], split=split, batch_id=BATCH_ID)


train = _read_split(TRAIN_CSV, "train")
test = _read_split(TEST_CSV, "test")
bronze_txns = train.unionByName(test)

# Left unpartitioned on purpose: Iceberg's `days(trans_date_trans_time)` transform locally
# truncates the timestamp to a day before partitioning; Delta's plain `.partitionBy()` has no
# such transform, and partitioning directly on a full-precision timestamp would create one
# partition per distinct second, which is wrong, not equivalent. Adding a derived date column
# just to partition on it would be a real schema change bronze never had locally, not a storage
# detail, so it is left out rather than silently introduced. 1.85M rows does not need
# partitioning to perform on a Free Edition single-node cluster.
(bronze_txns.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{BRONZE_DB}.transactions_raw"))

category_ref = (spark.read.option("header", True).option("inferSchema", True).csv(CATEGORY_REF_CSV))
category_ref.write.format("delta").mode("overwrite").saveAsTable(f"{BRONZE_DB}.category_ref_raw")

bronze_txns_tbl = spark.table(f"{BRONZE_DB}.transactions_raw")
enforce(bronze_txns_tbl, "bronze_transactions", stage="bronze", as_of=PERIOD_END)
enforce(spark.table(f"{BRONZE_DB}.category_ref_raw"), "bronze_category_ref", stage="bronze")

bronze_counts = {
    "transactions": bronze_txns_tbl.count(),
    "category_ref": spark.table(f"{BRONZE_DB}.category_ref_raw").count(),
    "train_rows": train.count(),
    "test_rows": test.count(),
}
print("bronze written:", bronze_counts)

# COMMAND ----------

# MAGIC %md ## 5. Silver : conform, mask PII, quarantine, dedup

# COMMAND ----------

from pyspark.sql import Window

MAX_REJECT_RATE = 0.10

bronze = spark.table(f"{BRONZE_DB}.transactions_raw")
category_ref = spark.table(f"{BRONZE_DB}.category_ref_raw")

renamed = normalize_timestamps(rename_to_silver(bronze))
with_distance = renamed.withColumn(
    "distance_km", haversine_km("lat", "long", "merch_lat", "merch_long")
)
conformed = (derive_transaction_fields(
                strip_home_coordinates(
                    derive_age(mask_card_number(with_distance))))
             .withColumn("reject_reason", reject_reason()))
conformed.cache()

rejected = conformed.where(F.col("reject_reason").isNotNull())
accepted = conformed.where(F.col("reject_reason").isNull())

newest = Window.partitionBy("trans_num").orderBy(
    F.col("ingested_at").desc(), F.col("batch_id").desc()
)
deduped = (enrich_with_category(accepted, category_ref)
           .withColumn("_rn", F.row_number().over(newest))
           .where(F.col("_rn") == 1)
           .select(*SILVER_COLUMNS))

(deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{SILVER_DB}.transactions"))

(rejected.select("trans_num", "reject_reason", "trans_time", "amt", "category", "source_file",
                  "batch_id", "ingested_at")
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{SILVER_DB}.transactions_quarantine"))

category_dim = category_ref.select(F.col("category"), F.col("channel"))
category_dim.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.dim_category")

total = conformed.count()
silver_tbl = spark.table(f"{SILVER_DB}.transactions")
quarantine_tbl = spark.table(f"{SILVER_DB}.transactions_quarantine")
kept = silver_tbl.count()
dropped = quarantine_tbl.count()
reject_rate = dropped / total if total else 0.0

print(f"silver: {total} in -> {kept} kept, {dropped} quarantined ({reject_rate:.4%}), "
      f"{total - dropped - kept} deduplicated")
for row in quarantine_tbl.groupBy("reject_reason").count().orderBy(F.desc("count")).collect():
    print(f"  quarantined {row['reject_reason']:<28} {row['count']:>8}")

conformed.unpersist()

enforce(silver_tbl, "silver_transactions", stage="silver", as_of=PERIOD_END)
enforce(category_dim, "silver_dim_category", stage="silver")

if reject_rate > MAX_REJECT_RATE:
    raise RuntimeError(
        f"quarantine rate {reject_rate:.2%} exceeds the {MAX_REJECT_RATE:.0%} budget"
    )

silver_counts = {"input": total, "kept": kept, "quarantined": dropped}
print("silver written:", silver_counts)

# COMMAND ----------

# MAGIC %md ## 6. Gold : the five fraud-KPI aggregates

# COMMAND ----------

MERCHANT_MIN_VOLUME = 20

txns = spark.table(f"{SILVER_DB}.transactions")
txns.cache()

category_hourly = (
    txns.withColumn("trans_hour_ts", F.date_trunc("hour", F.col("trans_time")))
    .groupBy("trans_hour_ts", "category")
    .agg(F.count(F.lit(1)).alias("txns"),
         F.sum(F.col("is_fraud")).cast("long").alias("fraud_txns"),
         F.round(F.sum("amt"), 2).alias("total_amt"),
         F.round(F.avg("amt"), 3).alias("avg_amt"))
    .withColumn("fraud_rate", F.round(F.col("fraud_txns") / F.col("txns"), 6))
)
category_hourly.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_DB}.category_hourly_fraud")

state_hourly = (
    txns.withColumn("trans_hour_ts", F.date_trunc("hour", F.col("trans_time")))
    .groupBy("trans_hour_ts", "state")
    .agg(F.count(F.lit(1)).alias("txns"),
         F.round(F.sum("amt"), 2).alias("total_amt"),
         F.round(F.avg("amt"), 3).alias("avg_amt"))
)
state_hourly.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_DB}.state_hourly_volume")

daily_per_card = txns.groupBy("cc_num_hash", "trans_date").agg(
    F.count(F.lit(1)).alias("txns_that_day"),
    F.round(F.sum("amt"), 2).alias("amt_that_day"),
    F.round(F.max("amt"), 2).alias("max_amt_that_day"),
)
rolling = Window.partitionBy("cc_num_hash").orderBy(
    F.datediff(F.col("trans_date"), F.lit("1970-01-01")).cast("long")
).rangeBetween(-6, 0)
card_velocity = (daily_per_card
                  .withColumn("txns_trailing_7d", F.sum("txns_that_day").over(rolling))
                  .withColumn("amt_trailing_7d", F.round(F.sum("amt_that_day").over(rolling), 2)))
card_velocity.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_DB}.card_velocity")

merchant_risk = (
    txns.groupBy("merchant", "category")
    .agg(F.count(F.lit(1)).alias("txns"),
         F.sum(F.col("is_fraud")).cast("long").alias("fraud_txns"),
         F.round(F.sum("amt"), 2).alias("total_amt"))
    .withColumn("fraud_rate", F.round(F.col("fraud_txns") / F.col("txns"), 6))
    .where(F.col("txns") >= MERCHANT_MIN_VOLUME)
)
merchant_risk.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_DB}.merchant_risk_leaderboard")

geo_anomaly = txns.groupBy("is_fraud").agg(
    F.count(F.lit(1)).alias("txns"),
    F.round(F.avg("distance_km"), 3).alias("avg_distance_km"),
    F.round(F.expr("percentile_approx(distance_km, 0.5)"), 3).alias("p50_distance_km"),
    F.round(F.expr("percentile_approx(distance_km, 0.9)"), 3).alias("p90_distance_km"),
    F.round(F.expr("percentile_approx(distance_km, 0.99)"), 3).alias("p99_distance_km"),
)
geo_anomaly.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_DB}.geo_distance_anomaly")

txns.unpersist()

enforce(spark.table(f"{GOLD_DB}.category_hourly_fraud"), "gold_category_hourly_fraud",
        stage="gold", as_of=PERIOD_END)
enforce(spark.table(f"{GOLD_DB}.state_hourly_volume"), "gold_state_hourly_volume", stage="gold")

gold_counts = {
    "category_hourly_fraud": spark.table(f"{GOLD_DB}.category_hourly_fraud").count(),
    "state_hourly_volume": spark.table(f"{GOLD_DB}.state_hourly_volume").count(),
    "card_velocity": spark.table(f"{GOLD_DB}.card_velocity").count(),
    "merchant_risk_leaderboard": spark.table(f"{GOLD_DB}.merchant_risk_leaderboard").count(),
    "geo_distance_anomaly": spark.table(f"{GOLD_DB}.geo_distance_anomaly").count(),
}
print("gold written:", gold_counts)

# COMMAND ----------

# MAGIC %md ## 7. Summary : copy these real numbers into the root README after a cluster run

# COMMAND ----------

print("=" * 70)
print("BRONZE:", bronze_counts)
print("SILVER:", silver_counts)
print("GOLD:  ", gold_counts)
print("cluster:", spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "unknown"),
      "| Spark", spark.version)
print("=" * 70)
