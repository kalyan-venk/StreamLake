"""Configuration loading for StreamLake.

One YAML file (conf/streamlake.yml) drives every job. Values may embed ``${VAR:default}``
placeholders that are resolved from the process environment at load time, which is what lets
the same code run against a local filesystem lakehouse, MinIO, or real S3 without edits.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}")

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "conf" / "streamlake.yml"


def _expand(value: Any) -> Any:
    if isinstance(value, str):

        def sub(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            env = os.environ.get(name)
            return env if env is not None else (default or "")

        return _PLACEHOLDER.sub(sub, value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


@dataclass(frozen=True)
class Config:
    data: dict[str, Any]
    root: Path

    def get(self, path: str, default: Any = None) -> Any:
        node: Any = self.data
        for part in path.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    def require(self, path: str) -> Any:
        value = self.get(path)
        if value in (None, ""):
            raise KeyError(f"missing required config key: {path}")
        return value

    def path(self, key: str) -> Path:
        raw = str(self.require(f"paths.{key}"))
        resolved = Path(raw) if Path(raw).is_absolute() else self.root / raw
        return resolved

    # derived values
    @property
    def period_start(self) -> str:
        return str(self.require("dataset.period_start"))

    @property
    def period_end(self) -> str:
        return str(self.require("dataset.period_end"))

    @property
    def catalog(self) -> str:
        return str(self.require("lakehouse.catalog"))

    def table(self, layer: str, name: str) -> str:
        """e.g. lakehouse.silver.transactions"""
        namespace = self.require(f"lakehouse.namespaces.{layer}")
        return f"{self.catalog}.{namespace}.{name}"

    @property
    def warehouse_uri(self) -> str:
        # Iceberg wants an absolute URI, not a relative path: file:// locally, s3:// in cloud.
        raw = str(self.require("lakehouse.warehouse"))
        if "://" in raw:
            return raw
        p = Path(raw) if Path(raw).is_absolute() else self.root / raw
        p.mkdir(parents=True, exist_ok=True)
        return p.as_uri()

    def raw_train_file(self) -> Path:
        return self.path("raw") / "credit_card_transaction_train.csv"

    def raw_test_file(self) -> Path:
        return self.path("raw") / "credit_card_transaction_test.csv"

    def category_ref_file(self) -> Path:
        # A small, hand-authored reference (category -> channel), checked into the repo rather
        # than downloaded: Sparkov ships no such lookup, and 14 rows is not worth a network call.
        return self.root / "conf" / "reference" / "category_channel.csv"

    def curated_dir(self, name: str) -> Path:
        return self.path("curated") / name


def load_config(path: str | Path | None = None) -> Config:
    config_path = (
        Path(path) if path else Path(os.environ.get("STREAMLAKE_CONFIG", DEFAULT_CONFIG_PATH))
    )
    with open(config_path) as fh:
        raw = yaml.safe_load(fh)
    return Config(data=_expand(raw), root=REPO_ROOT)


@lru_cache(maxsize=1)
def get_config() -> Config:
    return load_config()
