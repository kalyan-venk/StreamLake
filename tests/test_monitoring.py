"""Tests for the Datadog monitoring shim. No network, no Spark: this must pass offline."""

from __future__ import annotations

from streamlake import monitoring


def test_disabled_without_api_key(monkeypatch):
    monkeypatch.delenv("DD_API_KEY", raising=False)
    assert monitoring.enabled() is False


def test_emit_is_a_noop_without_api_key(monkeypatch):
    monkeypatch.delenv("DD_API_KEY", raising=False)
    # Must not raise, must not attempt a network call, and must report that it did nothing.
    assert monitoring.emit("test.metric", 1.0) is False


def test_enabled_with_api_key(monkeypatch):
    monkeypatch.setenv("DD_API_KEY", "fake-key-for-testing")
    assert monitoring.enabled() is True


def test_emit_helpers_never_raise_offline(monkeypatch):
    monkeypatch.delenv("DD_API_KEY", raising=False)
    # Every helper the pipeline actually calls must be safe to call with no credentials at all,
    # since the free local demo has none.
    monitoring.emit_producer_stats({"sent": 10, "duplicates": 1})
    monitoring.emit_quarantine_count(0, stage="silver")
    monitoring.emit_consumer_batch(batch_id=0, rows=5)
    monitoring.emit_consumer_state_ops_totals(None)
    monitoring.emit_consumer_state_ops_totals({"dedup_removed": 3, "late_dropped": 1})
    monitoring.emit_consumer_lag(12.5)
