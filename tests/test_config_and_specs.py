"""Tests for config loading and contract-spec parsing — no Spark needed."""

from __future__ import annotations

import textwrap

import pytest

from streamlake.config import load_config
from streamlake.contracts.spec import load_contract, load_contracts


def test_env_placeholders_are_expanded(tmp_path, monkeypatch):
    config = tmp_path / "c.yml"
    config.write_text(
        textwrap.dedent(
            """
            paths: {raw: data/raw, curated: data/curated, reports: _reports,
                    checkpoints: checkpoints, warehouse_db: data/w.duckdb}
            dataset: {month: "${STREAMLAKE_MONTH:2024-01}"}
            lakehouse:
              catalog: lakehouse
              warehouse: warehouse
              namespaces: {silver: silver}
            """
        )
    )
    assert load_config(config).month == "2024-01"

    monkeypatch.setenv("STREAMLAKE_MONTH", "2024-06")
    assert load_config(config).month == "2024-06"


def test_table_identifier_is_fully_qualified(tmp_path):
    config = tmp_path / "c.yml"
    config.write_text(
        "paths: {raw: r, curated: c, reports: p, checkpoints: k, warehouse_db: w}\n"
        "lakehouse: {catalog: lakehouse, warehouse: warehouse, namespaces: {silver: silver}}\n"
    )
    assert load_config(config).table("silver", "trips") == "lakehouse.silver.trips"


def test_missing_required_key_raises(tmp_path):
    config = tmp_path / "c.yml"
    config.write_text("paths: {raw: r, curated: c, reports: p, checkpoints: k, warehouse_db: w}\n")
    with pytest.raises(KeyError):
        load_config(config).require("lakehouse.catalog")


def test_schema_block_compiles_into_checks(tmp_path):
    contract_file = tmp_path / "x.yml"
    contract_file.write_text(
        textwrap.dedent(
            """
            name: x
            dataset: a.b.c
            schema:
              strict: true
              columns:
                - {name: id, type: string, nullable: false}
                - {name: label, type: string}
            checks:
              - type: row_count
                min: 1
            """
        )
    )
    contract = load_contract(contract_file)
    kinds = [c.type for c in contract.all_checks]
    # The declared schema becomes a schema check plus a not_null check for the NOT NULL columns,
    # so there is one execution path rather than two.
    assert kinds == ["schema", "not_null", "row_count"]
    assert contract.all_checks[1].params["columns"] == ["id"]


def test_severity_must_be_known(tmp_path):
    contract_file = tmp_path / "x.yml"
    contract_file.write_text(
        "name: x\ndataset: a.b.c\nchecks:\n  - type: row_count\n    severity: maybe\n"
    )
    with pytest.raises(ValueError, match="unknown severity"):
        load_contract(contract_file)


def test_shipped_contracts_all_parse():
    """Every contract in conf/contracts must load and reference a known check type."""
    from pathlib import Path

    from streamlake.contracts.checks import _REGISTRY

    root = Path(__file__).resolve().parents[1] / "conf" / "contracts"
    contracts = load_contracts(root)
    assert len(contracts) >= 7

    for contract in contracts.values():
        assert contract.dataset.count(".") == 2, f"{contract.name} dataset must be catalog.ns.table"
        for check in contract.all_checks:
            assert check.type in _REGISTRY, f"{contract.name} uses unknown check '{check.type}'"


def test_check_label_handles_schema_column_dicts(tmp_path):
    """Regression: the schema check carries column definitions, not names, and the label
    builder used to crash trying to join dicts."""
    contract_file = tmp_path / "x.yml"
    contract_file.write_text(
        "name: x\ndataset: a.b.c\nschema:\n  columns:\n    - {name: id, type: string}\n"
    )
    contract = load_contract(contract_file)
    assert contract.all_checks[0].label == "schema:id"
