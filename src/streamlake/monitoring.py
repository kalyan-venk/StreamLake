"""Datadog metric emission: consumer lag, throughput, dedup count, quarantine count, late-arrival
count.

Same credential pattern as `vaultex/.env.datadog`: `DD_API_KEY` and `DD_SITE`, read from the
process environment, sourced from a local, git-excluded `.env.datadog` file that is never
committed. Optional-if-no-key on purpose, a laptop running the free local demo without a Datadog
trial account should never have to configure monitoring to make the pipeline run, and the test
suite runs fully offline: every call in this module is a no-op when `DD_API_KEY` is unset.
"""

from __future__ import annotations

import os
import time
from typing import Any

from streamlake.logging_utils import get_logger

log = get_logger(__name__)

_DD_API_KEY_ENV = "DD_API_KEY"
_DD_SITE_ENV = "DD_SITE"
_DEFAULT_SITE = "datadoghq.com"
_METRIC_PREFIX = "streamlake"


def enabled() -> bool:
    return bool(os.environ.get(_DD_API_KEY_ENV))


def emit(
    metric: str, value: float, *, tags: dict[str, str] | None = None, type_: str = "gauge"
) -> bool:
    """Send one metric point. Returns True if it was actually sent, False if monitoring is off.

    Never raises: a Datadog outage or a bad trial key must not fail a pipeline run. The caller
    gets a bool back so a test can assert "monitoring is off" without asserting on log lines.
    """
    if not enabled():
        log.debug(
            "monitoring disabled (no %s set), skipping metric %s=%s",
            _DD_API_KEY_ENV,
            metric,
            value,
        )
        return False

    import requests

    site = os.environ.get(_DD_SITE_ENV, _DEFAULT_SITE)
    api_key = os.environ[_DD_API_KEY_ENV]
    tag_list = [f"{k}:{v}" for k, v in (tags or {}).items()]

    payload = {
        "series": [
            {
                "metric": f"{_METRIC_PREFIX}.{metric}",
                "type": type_,
                "points": [{"timestamp": int(time.time()), "value": value}],
                "tags": tag_list,
            }
        ]
    }
    try:
        response = requests.post(
            f"https://api.{site}/api/v2/series",
            json=payload,
            headers={"DD-API-KEY": api_key, "Content-Type": "application/json"},
            timeout=5,
        )
        if response.status_code >= 300:
            log.warning("datadog emit failed: %s %s", response.status_code, response.text[:200])
            return False
        return True
    except Exception as exc:  # pragma: no cover - network failure path, never fatal
        log.warning("datadog emit raised %s, continuing without monitoring", exc)
        return False


def emit_producer_stats(stats: dict[str, Any]) -> None:
    emit("producer.events_sent", stats.get("sent", 0), type_="count")
    emit("producer.duplicates_injected", stats.get("duplicates", 0), type_="count")


def emit_consumer_batch(*, batch_id: int, rows: int) -> None:
    """Throughput for one micro-batch: window rows merged, not raw events (Spark does not expose
    per-batch dedup/late-drop counts to `foreachBatch`; those are read from `query.lastProgress`'s
    `stateOperators` at the end of the run instead, see `emit_consumer_state_ops`)."""
    emit("consumer.batch_rows", rows, tags={"batch_id": str(batch_id)}, type_="count")


def emit_consumer_state_ops_totals(reconciliation: dict[str, int] | None) -> None:
    """Dedup-removed and late-dropped counts for the whole run, summed by
    ``consumer.summarize_state_ops`` across every micro-batch's own state-operator metrics
    rather than a hand count, so the number is exactly what Spark itself tracked."""
    if not reconciliation:
        return
    emit("consumer.dedup_removed", reconciliation.get("dedup_removed", 0), type_="count")
    emit("consumer.late_dropped", reconciliation.get("late_dropped", 0), type_="count")


def emit_consumer_lag(lag_seconds: float) -> None:
    emit("consumer.lag_seconds", lag_seconds)


def emit_quarantine_count(count: int, *, stage: str) -> None:
    emit("quarantine.rows", count, tags={"stage": stage}, type_="count")
