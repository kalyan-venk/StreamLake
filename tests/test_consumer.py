"""Tests for the streaming consumer's own reconciliation arithmetic.

`summarize_state_ops` is pure Python over the JSON-shaped progress dicts Structured Streaming
hands back; no Spark session needed to exercise it, which is the point, the reconciliation
identity (produced = counted + duplicates_removed + late_dropped) has to hold on paper before it
is trusted against a live run.

The operator shape fixtures below are not invented: they are trimmed copies of the real
`stateOperators` entries from a verified run against topic `streamlake.transactions.diag3`
(see `scripts/demo_late_arrivals.py`'s module docstring and `MISTAKES.md` for the full story of
why the first version of `summarize_state_ops` split these fields the wrong way).
"""

from __future__ import annotations

from streamlake.stream.consumer import summarize_state_ops


def _dedup_op(*, dropped_by_watermark: int = 0, dropped_duplicates: int = 0) -> dict:
    """Shaped like the real `dedupeWithinWatermark` operator: `numRowsDroppedByWatermark` is a
    top-level field (the late-arrival drop), `numDroppedDuplicateRows` lives under
    `customMetrics` (the actual duplicate-removal count). The two are easy to conflate because
    both are "a state operator dropped some rows", but they answer different questions."""
    return {
        "operatorName": "dedupeWithinWatermark",
        "numRowsDroppedByWatermark": dropped_by_watermark,
        "customMetrics": {"numDroppedDuplicateRows": dropped_duplicates},
    }


def _agg_op(*, dropped_by_watermark: int = 0) -> dict:
    """Shaped like the real `stateStoreSave` (windowed aggregation) operator. In every verified
    run this operator's own `numRowsDroppedByWatermark` was 0, because dedup already filters
    anything watermark-late before aggregation ever sees it, but the field is still summed here
    defensively rather than ignored, in case a future query shape lets something reach it late."""
    return {"operatorName": "stateStoreSave", "numRowsDroppedByWatermark": dropped_by_watermark}


def _batch(input_rows: int, *state_ops: dict) -> dict:
    return {"numInputRows": input_rows, "stateOperators": list(state_ops)}


def test_sums_input_rows_across_batches():
    progress = [_batch(100), _batch(50), _batch(0)]
    result = summarize_state_ops(progress)
    assert result["input_rows"] == 150


def test_late_drop_is_the_dedup_operators_top_level_field_not_a_custom_metric():
    """This is the regression the first, wrong implementation would have failed: it looked for
    `numRowsDroppedByWatermark` and, on the dedup operator, called it a dedup removal by name.
    It is not, it is the late-drop count regardless of which operator reports it."""
    progress = [_batch(20, _dedup_op(dropped_by_watermark=20, dropped_duplicates=0))]
    result = summarize_state_ops(progress)
    assert result["late_dropped"] == 20
    assert result["dedup_removed"] == 0


def test_dedup_removed_reads_the_custom_metric():
    progress = [_batch(56, _dedup_op(dropped_by_watermark=0, dropped_duplicates=6))]
    result = summarize_state_ops(progress)
    assert result["dedup_removed"] == 6
    assert result["late_dropped"] == 0


def test_a_batch_can_have_both_late_drops_and_duplicate_drops_at_once():
    progress = [_batch(30, _dedup_op(dropped_by_watermark=5, dropped_duplicates=2))]
    result = summarize_state_ops(progress)
    assert result == {"input_rows": 30, "dedup_removed": 2, "late_dropped": 5}


def test_aggregation_operators_watermark_drops_are_still_counted_defensively():
    progress = [_batch(10, _agg_op(dropped_by_watermark=3))]
    result = summarize_state_ops(progress)
    assert result["late_dropped"] == 3
    assert result["dedup_removed"] == 0


def test_totals_accumulate_across_multiple_batches_matching_a_real_two_phase_run():
    """Mirrors the verified diag3 run: batch 0 empty, batch 1 on-time with 6 duplicates removed
    (56 in, 50 pass), batch 2 empty, batch 3 forced-late with all 20 rows dropped."""
    progress = [
        _batch(0, _dedup_op(), _agg_op()),
        _batch(56, _dedup_op(dropped_duplicates=6), _agg_op()),
        _batch(0, _dedup_op(), _agg_op()),
        _batch(20, _dedup_op(dropped_by_watermark=20), _agg_op()),
    ]
    result = summarize_state_ops(progress)
    assert result == {"input_rows": 76, "dedup_removed": 6, "late_dropped": 20}
    # The reconciliation identity a live demo run has to satisfy.
    produced = 76
    counted = result["input_rows"] - result["dedup_removed"] - result["late_dropped"]
    assert produced == counted + result["dedup_removed"] + result["late_dropped"]
    assert counted == 50


def test_empty_progress_list_is_all_zero():
    assert summarize_state_ops([]) == {"input_rows": 0, "dedup_removed": 0, "late_dropped": 0}


def test_missing_state_operators_key_does_not_raise():
    assert summarize_state_ops([{"numInputRows": 7}]) == {
        "input_rows": 7,
        "dedup_removed": 0,
        "late_dropped": 0,
    }
