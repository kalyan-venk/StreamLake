"""Real-time fraud scoring: turn one transaction event into an approve / review / decline call.

This is the one place that defines how a transaction becomes a decision. The offline trainer, the
unit test, the in-process demo, and the Spark Structured Streaming scorer all import from here, so a
transaction scored live off Kafka goes through exactly the same feature derivation and threshold
logic as one scored in a notebook. If the two ever drifted, the live decisions and the offline
evaluation would stop describing the same model.

The model is deliberately small: a scikit-learn logistic regression over four features that are
already carried on every event (`amt`, `distance_km`, the transaction hour, and `category`). Those
are the same silver-layer fields the batch pipeline computes in `transforms.py`, so scoring reuses
the pipeline's own features rather than inventing new ones. A logistic regression, not a gradient
boosting model, because the point of this module is the end-to-end path (event in, decision out,
one definition shared across batch and stream), and a linear model is enough to prove that path on
Sparkov while staying trivially serialisable and fast enough to score a Kafka micro-batch.

The features never include `is_fraud`. That column is the label: it is used to fit the model and to
score the demo, and it is never read as an input, which is what keeps the scorer honest when a live
event arrives with no label at all.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

# The 14 Sparkov categories, kept in sync with transforms.CATEGORIES. Declared explicitly so the
# one-hot encoder has a fixed, known column space: a live event carrying a category the model never
# saw in training is encoded as all-zeros rather than crashing the transform.
CATEGORIES = (
    "entertainment",
    "food_dining",
    "gas_transport",
    "grocery_net",
    "grocery_pos",
    "health_fitness",
    "home",
    "kids_pets",
    "misc_net",
    "misc_pos",
    "personal_care",
    "shopping_net",
    "shopping_pos",
    "travel",
)

NUMERIC_FEATURES = ("amt_log", "distance_km", "hour")
CATEGORICAL_FEATURES = ("category",)
LABEL = "is_fraud"

# Where the trained artifact lands by default. Kept out of git (it is a build output, rebuilt from
# the curated layer by `scripts/train_scorer.py`), same policy as data/ and warehouse/.
DEFAULT_MODEL_PATH = Path(__file__).resolve().parents[2] / "models" / "fraud_scorer.joblib"

APPROVE = "APPROVE"
REVIEW = "REVIEW"
DECLINE = "DECLINE"


def _hour_from_event(event: dict[str, Any]) -> int:
    """Transaction hour 0-23. Prefer an explicit `trans_hour`, else derive it from `event_ts`.

    A live Kafka event carries `event_ts` (an ISO-8601 string) but not a pre-split hour; a row read
    straight from the curated silver table carries `trans_hour` already. Handling both means the
    same feature code serves the stream and the offline evaluation without a separate path.
    """
    if event.get("trans_hour") is not None:
        return int(event["trans_hour"])
    ts = event.get("event_ts") or event.get("trans_time")
    if ts is None:
        return 0
    if isinstance(ts, datetime):
        return ts.hour
    text = str(ts).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).hour
    except ValueError:
        # `YYYY-MM-DD HH:MM:SS` without the 'T' separator: pull the hour field directly.
        return int(text[11:13]) if len(text) >= 13 else 0


def event_to_features(event: dict[str, Any]) -> dict[str, Any]:
    """One event dict -> the feature dict the model expects. Tolerant of missing/None fields.

    `amt` is log1p-compressed because transaction amounts are heavily right-skewed (a handful of
    thousand-dollar charges next to a mass of small ones); the log keeps a linear model from being
    dominated by the tail. Everything else is passed through as the model's ColumnTransformer wants
    it.
    """
    import math

    amt = event.get("amt")
    amt = float(amt) if amt is not None else 0.0
    distance = event.get("distance_km")
    distance = float(distance) if distance is not None else 0.0
    category = event.get("category")
    category = str(category) if category is not None else "unknown"
    return {
        "amt_log": math.log1p(max(amt, 0.0)),
        "distance_km": distance,
        "hour": _hour_from_event(event),
        "category": category,
    }


@dataclass
class FraudScorer:
    """A fitted logistic-regression scorer plus the two thresholds that turn a probability into a
    decision. Serialisable as a single joblib file so the trainer, the demo, the test, and the
    streaming job all load the identical model and the identical thresholds."""

    pipeline: Any  # sklearn Pipeline
    review_threshold: float
    decline_threshold: float
    metadata: dict[str, Any] = field(default_factory=dict)

    def decision_for(self, probability: float) -> tuple[str, str]:
        """Map a fraud probability to a decision and a one-line reason. Two thresholds, three bands:
        below `review` is auto-approved, between the two goes to a human review queue, at or above
        `decline` is auto-declined."""
        if probability >= self.decline_threshold:
            return DECLINE, f"p(fraud) {probability:.3f} >= decline {self.decline_threshold:.3f}"
        if probability >= self.review_threshold:
            return REVIEW, f"p(fraud) {probability:.3f} >= review {self.review_threshold:.3f}"
        return APPROVE, f"p(fraud) {probability:.3f} < review {self.review_threshold:.3f}"

    def score_frame(self, frame: pd.DataFrame) -> Any:
        """Fraud probabilities for a whole DataFrame of already-built features. Vectorised, so a
        Spark micro-batch collected to pandas is one call, not a Python loop over rows."""
        columns = list(NUMERIC_FEATURES + CATEGORICAL_FEATURES)
        return self.pipeline.predict_proba(frame[columns])[:, 1]

    def score_one(self, event: dict[str, Any]) -> dict[str, Any]:
        """One raw event dict -> a decision record. The unit of the whole real-time path."""
        import pandas as pd

        features = event_to_features(event)
        probability = float(self.score_frame(pd.DataFrame([features]))[0])
        decision, reason = self.decision_for(probability)
        return {
            "trans_num": event.get("trans_num"),
            "amt": event.get("amt"),
            "category": event.get("category"),
            "fraud_probability": round(probability, 6),
            "decision": decision,
            "reason": reason,
        }

    def save(self, path: str | Path = DEFAULT_MODEL_PATH) -> Path:
        import joblib

        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(
            {
                "pipeline": self.pipeline,
                "review_threshold": self.review_threshold,
                "decline_threshold": self.decline_threshold,
                "metadata": self.metadata,
            },
            path,
        )
        return path

    @classmethod
    def load(cls, path: str | Path = DEFAULT_MODEL_PATH) -> FraudScorer:
        import joblib

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(
                f"no trained scorer at {path}; build one first with "
                "`python scripts/train_scorer.py` (it trains from the curated silver export)"
            )
        blob = joblib.load(path)
        return cls(
            pipeline=blob["pipeline"],
            review_threshold=blob["review_threshold"],
            decline_threshold=blob["decline_threshold"],
            metadata=blob.get("metadata", {}),
        )

    @classmethod
    def train(
        cls,
        frame: pd.DataFrame,
        *,
        review_quantile: float = 0.90,
        decline_quantile: float = 0.99,
        metadata: dict[str, Any] | None = None,
    ) -> FraudScorer:
        """Fit the logistic regression and set the two decision thresholds.

        `frame` is expected to carry the raw silver columns `amt`, `distance_km`, `category`, one
        of `trans_hour`/`event_ts`, and the `is_fraud` label. Features are derived here through the
        same `event_to_features` the live path uses, so training and serving cannot disagree about
        what a feature is.

        The thresholds are set from the *training* score distribution, never the holdout: `decline`
        at the 99th percentile of predicted fraud probability, `review` at the 90th. That is a
        policy choice (decline the riskiest ~1%, review the next ~9%, approve the rest), not a
        fitted parameter, and it is deliberately picked on train so the holdout numbers the trainer
        prints describe an operating point that was not tuned against the holdout.
        """
        import numpy as np
        import pandas as pd
        from sklearn.compose import ColumnTransformer
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder, StandardScaler

        feats = pd.DataFrame([event_to_features(row) for row in frame.to_dict("records")])
        y = frame[LABEL].astype(int).to_numpy()

        pre = ColumnTransformer(
            [
                ("num", StandardScaler(), list(NUMERIC_FEATURES)),
                (
                    "cat",
                    OneHotEncoder(categories=[list(CATEGORIES)], handle_unknown="ignore"),
                    list(CATEGORICAL_FEATURES),
                ),
            ]
        )
        # class_weight="balanced" because fraud is ~0.5% of the feed; without it the model would
        # minimise loss by predicting "not fraud" for everything and never flag a thing.
        pipeline = Pipeline(
            [
                ("features", pre),
                (
                    "clf",
                    LogisticRegression(max_iter=1000, class_weight="balanced", C=1.0),
                ),
            ]
        )
        pipeline.fit(feats[list(NUMERIC_FEATURES + CATEGORICAL_FEATURES)], y)

        train_scores = pipeline.predict_proba(
            feats[list(NUMERIC_FEATURES + CATEGORICAL_FEATURES)]
        )[:, 1]
        review_threshold = float(np.quantile(train_scores, review_quantile))
        decline_threshold = float(np.quantile(train_scores, decline_quantile))

        meta = dict(metadata or {})
        meta.update(
            {
                "n_train": int(len(y)),
                "train_fraud": int(y.sum()),
                "review_quantile": review_quantile,
                "decline_quantile": decline_quantile,
            }
        )
        return cls(
            pipeline=pipeline,
            review_threshold=review_threshold,
            decline_threshold=decline_threshold,
            metadata=meta,
        )
