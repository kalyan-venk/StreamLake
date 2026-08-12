"""Tests for the real-time fraud scorer (streamlake.scoring).

Pure Python: no Spark, no Kafka. A small separable dataset is fit so the test is hermetic (it does
not need `make batch` or a trained artifact on disk), then a sample event is pushed through the
scorer and the decision is asserted, which is the proof that the event-in / decision-out path works
end to end. The Spark streaming job (streamlake.stream.scorer) calls the exact same
`FraudScorer.score_one` / `score_frame` per event, so exercising them here exercises the scoring the
stream does.
"""

from __future__ import annotations

import pandas as pd
import pytest

from streamlake.scoring import (
    APPROVE,
    CATEGORIES,
    DECLINE,
    REVIEW,
    FraudScorer,
    event_to_features,
)


def _training_frame() -> pd.DataFrame:
    """A small, deliberately separable feed: fraud is the big-amount, long-distance travel charges
    at 3am, legit is the small local grocery run in the afternoon. Enough signal for a linear model
    to learn a real boundary so the decision bands mean something in the assertions below."""
    rows = []
    for i in range(400):
        rows.append(
            {
                "amt": 15.0 + (i % 20),
                "distance_km": 5.0 + (i % 10),
                "category": "grocery_pos",
                "trans_hour": 14,
                "is_fraud": 0,
            }
        )
    for i in range(40):
        rows.append(
            {
                "amt": 900.0 + (i % 50) * 10,
                "distance_km": 4000.0 + (i % 30) * 10,
                "category": "shopping_net",
                "trans_hour": 3,
                "is_fraud": 1,
            }
        )
    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def scorer() -> FraudScorer:
    return FraudScorer.train(_training_frame())


def test_event_to_features_derives_hour_from_iso_event_ts():
    feats = event_to_features(
        {"amt": 100.0, "distance_km": 50.0, "category": "travel", "event_ts": "2020-06-01T03:15:00"}
    )
    assert feats["hour"] == 3
    assert feats["category"] == "travel"
    # amt is log1p-compressed, not raw.
    assert feats["amt_log"] == pytest.approx(4.61512, rel=1e-4)


def test_event_to_features_tolerates_missing_fields():
    feats = event_to_features({})
    assert feats == {"amt_log": 0.0, "distance_km": 0.0, "hour": 0, "category": "unknown"}


def test_unknown_category_encodes_without_crashing(scorer):
    # A category the model never saw must score, not raise (one-hot handle_unknown="ignore").
    out = scorer.score_one(
        {"trans_num": "x" * 32, "amt": 50.0, "distance_km": 10.0, "category": "brand_new_category",
         "event_ts": "2020-06-01T12:00:00"}
    )
    assert out["decision"] in (APPROVE, REVIEW, DECLINE)


def test_score_one_returns_a_decision_record(scorer):
    fraud_event = {
        "trans_num": "a" * 32,
        "amt": 1200.0,
        "distance_km": 4200.0,
        "category": "shopping_net",
        "event_ts": "2020-06-01T03:00:00",
        "is_fraud": 1,
    }
    out = scorer.score_one(fraud_event)
    assert set(out) == {"trans_num", "amt", "category", "fraud_probability", "decision", "reason"}
    assert 0.0 <= out["fraud_probability"] <= 1.0
    assert out["decision"] in (APPROVE, REVIEW, DECLINE)


def test_obvious_fraud_scores_higher_than_obvious_legit(scorer):
    fraud = scorer.score_one(
        {"trans_num": "a" * 32, "amt": 1500.0, "distance_km": 4500.0,
         "category": "shopping_net", "event_ts": "2020-06-01T03:00:00"}
    )
    legit = scorer.score_one(
        {"trans_num": "b" * 32, "amt": 20.0, "distance_km": 6.0,
         "category": "grocery_pos", "event_ts": "2020-06-01T14:00:00"}
    )
    assert fraud["fraud_probability"] > legit["fraud_probability"]
    # On this separable feed the risky charge should at least reach the review band, and the
    # everyday grocery charge should be auto-approved.
    assert fraud["decision"] in (REVIEW, DECLINE)
    assert legit["decision"] == APPROVE


def test_thresholds_are_ordered_and_bands_are_consistent(scorer):
    assert 0.0 <= scorer.review_threshold <= scorer.decline_threshold <= 1.0
    assert scorer.decision_for(scorer.decline_threshold)[0] == DECLINE
    midpoint = (scorer.review_threshold + scorer.decline_threshold) / 2
    assert scorer.decision_for(midpoint)[0] == REVIEW
    assert scorer.decision_for(scorer.review_threshold / 2)[0] == APPROVE


def test_score_frame_matches_score_one(scorer):
    events = [
        {"amt": 1500.0, "distance_km": 4500.0, "category": "shopping_net", "trans_hour": 3},
        {"amt": 20.0, "distance_km": 6.0, "category": "grocery_pos", "trans_hour": 14},
    ]
    feats = pd.DataFrame([event_to_features(e) for e in events])
    batch = scorer.score_frame(feats)
    singles = [scorer.score_one(e)["fraud_probability"] for e in events]
    for b, s in zip(batch, singles, strict=True):
        assert float(b) == pytest.approx(s, abs=1e-6)


def test_save_and_load_roundtrip(scorer, tmp_path):
    path = scorer.save(tmp_path / "scorer.joblib")
    reloaded = FraudScorer.load(path)
    assert reloaded.review_threshold == scorer.review_threshold
    assert reloaded.decline_threshold == scorer.decline_threshold
    event = {"amt": 1500.0, "distance_km": 4500.0, "category": "shopping_net", "trans_hour": 3}
    assert reloaded.score_one(event)["fraud_probability"] == pytest.approx(
        scorer.score_one(event)["fraud_probability"], abs=1e-9
    )


def test_all_categories_are_known_to_the_encoder():
    # Guards against transforms.CATEGORIES and scoring.CATEGORIES drifting apart.
    from streamlake.transforms import CATEGORIES as TRANSFORM_CATEGORIES

    assert set(CATEGORIES) == set(TRANSFORM_CATEGORIES)
