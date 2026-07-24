"""Data-contract specification: the declarative half of the contract engine.

A contract is a YAML file that states what a dataset must look like at a given hop of the
pipeline. It is deliberately *not* Python: the contract is meant to be readable by whoever owns
the data, reviewable in a pull request, and diffable when expectations change.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

Severity = str  # "error" | "warn"

_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(second|minute|hour|day|week)s?\s*$", re.I)
_UNIT_SECONDS = {
    "second": 1,
    "minute": 60,
    "hour": 3600,
    "day": 86400,
    "week": 604800,
}


def parse_duration(text: str | int | float) -> float:
    """'90 minutes' -> 5400.0 seconds. Bare numbers are already seconds."""
    if isinstance(text, (int, float)):
        return float(text)
    match = _DURATION.match(str(text))
    if not match:
        raise ValueError(f"cannot parse duration: {text!r} (expected e.g. '2 hours', '45 days')")
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
    # strict=True also fails on columns that exist but were never declared, which is how you
    # notice an upstream team quietly adding a field you are not validating.
    strict: bool = False

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)


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
                # The schema check carries column *definitions*, not names.
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
    source: Path | None = None

    @property
    def all_checks(self) -> tuple[CheckSpec, ...]:
        """Schema declarations are compiled into checks so there is one execution path."""
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
    if severity not in ("error", "warn"):
        raise ValueError(f"unknown severity {severity!r} (expected 'error' or 'warn')")
    return CheckSpec(type=check_type, severity=severity, description=description, params=payload)


def load_contract(path: str | Path) -> Contract:
    path = Path(path)
    with open(path) as fh:
        raw = yaml.safe_load(fh) or {}

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
        name=raw.get("name") or path.stem,
        dataset=raw["dataset"],
        description=raw.get("description", ""),
        owner=raw.get("owner", ""),
        layer=raw.get("layer", ""),
        schema=SchemaSpec(columns=columns, strict=bool(schema_raw.get("strict", False))),
        checks=tuple(_parse_check(c) for c in (raw.get("checks") or [])),
        source=path,
    )


def load_contracts(directory: str | Path) -> dict[str, Contract]:
    directory = Path(directory)
    return {c.name: c for c in (load_contract(p) for p in sorted(directory.glob("*.yml")))}
